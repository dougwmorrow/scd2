"""Shared schema metadata queries for BCP column validation and staging table creation.

Used by:
  - bcp_csv.py: column reorder before BCP write (P0-1)
  - cdc_polars.py / scd2_polars.py: staging table PK types (P0-3)
  - schema_evolution.py: column comparison for drift detection (P0-2)
  - cdc_polars.py / scd2_polars.py: PK dtype alignment before joins (P0-12)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

import connections

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnMetadata:
    """Column metadata from INFORMATION_SCHEMA.COLUMNS."""

    column_name: str
    ordinal_position: int
    data_type: str
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None

    @property
    def full_type(self) -> str:
        """Return the full SQL Server type string (e.g. NVARCHAR(255), BIGINT)."""
        upper = self.data_type.upper()
        if upper in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "VARBINARY"):
            if self.character_maximum_length == -1:
                return f"{upper}(MAX)"
            elif self.character_maximum_length is not None:
                return f"{upper}({self.character_maximum_length})"
            return f"{upper}(MAX)"
        if upper in ("DECIMAL", "NUMERIC"):
            p = self.numeric_precision or 18
            s = self.numeric_scale or 0
            return f"{upper}({p},{s})"
        return upper


def get_column_metadata(full_table_name: str) -> list[ColumnMetadata]:
    """Read column metadata from INFORMATION_SCHEMA.COLUMNS, ordered by ORDINAL_POSITION.

    Args:
        full_table_name: e.g. 'UDM_Stage.DNA.ACCT_cdc'

    Returns:
        List of ColumnMetadata, ordered by ORDINAL_POSITION.
    """
    parts = full_table_name.split(".")
    db, schema, table = parts[0], parts[1], parts[2]

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, "
            f"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
            f"FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            f"ORDER BY ORDINAL_POSITION",
            schema, table,
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    return [
        ColumnMetadata(
            column_name=row[0],
            ordinal_position=row[1],
            data_type=row[2],
            character_maximum_length=row[3],
            numeric_precision=row[4],
            numeric_scale=row[5],
        )
        for row in rows
    ]


def get_target_column_order(
    full_table_name: str,
    exclude_columns: set[str] | None = None,
) -> list[str]:
    """Get ordered list of column names for a target table.

    Args:
        full_table_name: Fully qualified table name.
        exclude_columns: Column names to exclude (e.g. {'_scd2_key'}).

    Returns:
        Column names in ORDINAL_POSITION order, excluding specified columns.
    """
    if exclude_columns is None:
        exclude_columns = set()

    columns = get_column_metadata(full_table_name)
    return [
        c.column_name for c in columns
        if c.column_name not in exclude_columns
    ]


def get_column_types(
    full_table_name: str,
    column_names: list[str],
) -> dict[str, str]:
    """Get SQL Server types for specific columns.

    Args:
        full_table_name: Fully qualified table name.
        column_names: Columns to look up.

    Returns:
        Dict mapping column_name -> full SQL type string (e.g. 'BIGINT', 'NVARCHAR(255)').
    """
    all_cols = get_column_metadata(full_table_name)
    col_map = {c.column_name: c.full_type for c in all_cols}

    result = {}
    for name in column_names:
        if name in col_map:
            result[name] = col_map[name]
        else:
            logger.warning(
                "Column %s not found in %s INFORMATION_SCHEMA — defaulting to NVARCHAR(255)",
                name, full_table_name,
            )
            result[name] = "NVARCHAR(255)"

    return result


# ---------------------------------------------------------------------------
# P0-12: PK dtype alignment before Polars joins
# ---------------------------------------------------------------------------

# Rank numeric Polars dtypes from narrowest to widest for safe widening casts.
_NUMERIC_RANK: dict[pl.DataType, int] = {
    pl.Int8: 1, pl.Int16: 2, pl.Int32: 3, pl.Int64: 4,
    pl.UInt8: 1, pl.UInt16: 2, pl.UInt32: 3, pl.UInt64: 4,
    pl.Float32: 5, pl.Float64: 6,
}


def _wider_dtype(a: pl.DataType, b: pl.DataType) -> pl.DataType:
    """Return the wider of two Polars dtypes for safe casting.

    Rules:
      - Both numeric: return the wider type (by rank).
      - One or both string: return Utf8 (safest common type).
      - Unknown combo: return Utf8.
    """
    rank_a = _NUMERIC_RANK.get(a)
    rank_b = _NUMERIC_RANK.get(b)

    if rank_a is not None and rank_b is not None:
        return a if rank_a >= rank_b else b

    # One or both non-numeric — Utf8 is the safe fallback
    return pl.Utf8


def align_pk_dtypes(
    df_a: pl.DataFrame,
    df_b: pl.DataFrame,
    pk_columns: list[str],
    context: str = "",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """P0-12: Ensure PK columns have matching Polars dtypes before joins.

    ConnectorX may return the same column with different Polars dtypes
    across extraction paths (e.g., Oracle NUMBER(10) as Int32 in one path
    vs Int64 in another). Mismatched PK dtypes can cause:
      - SchemaError in older Polars (crash — annoying but safe)
      - Silent coercion in newer Polars (usually correct)
      - Zero-match joins for Utf8 vs numeric mismatch (silent data loss)

    Args:
        df_a: First DataFrame.
        df_b: Second DataFrame.
        pk_columns: Primary key columns to align.
        context: Description for log messages (e.g., "CDC run_cdc").

    Returns:
        Tuple of (df_a, df_b) with PK columns cast to matching dtypes.
    """
    for col in pk_columns:
        if col not in df_a.columns or col not in df_b.columns:
            continue

        dtype_a = df_a[col].dtype
        dtype_b = df_b[col].dtype

        if dtype_a == dtype_b:
            continue

        target = _wider_dtype(dtype_a, dtype_b)

        # Utf8 vs numeric is the dangerous case — escalate to ERROR
        a_is_string = dtype_a in (pl.Utf8, pl.String)
        b_is_string = dtype_b in (pl.Utf8, pl.String)
        if a_is_string != b_is_string:
            logger.error(
                "P0-12: PK column [%s] has string/numeric dtype mismatch: %s vs %s — "
                "casting both to %s. This indicates unstable ConnectorX typing that "
                "should be investigated.%s",
                col, dtype_a, dtype_b, target,
                f" ({context})" if context else "",
            )
        else:
            logger.warning(
                "P0-12: PK column [%s] dtype mismatch: %s vs %s — casting both to %s%s",
                col, dtype_a, dtype_b, target,
                f" ({context})" if context else "",
            )

        df_a = df_a.with_columns(pl.col(col).cast(target))
        df_b = df_b.with_columns(pl.col(col).cast(target))

    return df_a, df_b

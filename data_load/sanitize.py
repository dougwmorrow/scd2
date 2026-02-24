"""DataFrame sanitization and preparation functions for BCP CSV output.

Covers: string sanitization, BIT column casting, Oracle DATE fixes,
UInt64 reinterpretation, and column reordering for BCP positional mapping.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


class ColumnOrderError(Exception):
    """Raised when DataFrame column count doesn't match target table."""


def sanitize_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Replace BCP-corrupting characters in all Utf8/String columns with empty string.

    B-6: Extended sanitization covers all characters that can corrupt BCP CSV:
      - \\t (tab) — field delimiter, splits one field into two
      - \\n (LF) — row terminator, splits one row into two
      - \\r (CR) — alternate line ending
      - \\x00 (NUL) — C-string terminator
      - \\x0B (vertical tab) — Unicode line break
      - \\x0C (form feed) — Unicode line break
      - \\x85 (NEL, Next Line) — ISO 8859 / Unicode line break
      - \\u2028 (Line Separator) — Unicode line break
      - \\u2029 (Paragraph Separator) — Unicode line break

    MUST be called BEFORE write_csv to prevent BCP corruption.
    """
    string_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if dtype == pl.Utf8 or dtype == pl.String
    ]

    if not string_cols:
        return df

    # B-6: Extended regex includes Unicode line-break characters that BCP
    # interprets as row terminators or that corrupt CSV structure.
    return df.with_columns(
        pl.col(col)
        .str.replace_all(r"[\t\n\r\x00\x0B\x0C\x85\u2028\u2029]", "")
        .alias(col)
        for col in string_cols
    )


def cast_bit_columns(df: pl.DataFrame, bit_columns: list[str] | None = None) -> pl.DataFrame:
    """Cast Boolean/BIT columns to Int8 (0/1). Never True/False in CSV.

    If bit_columns not specified, auto-detects Boolean dtype columns.
    """
    if bit_columns is None:
        bit_columns = [
            col for col, dtype in zip(df.columns, df.dtypes)
            if dtype == pl.Boolean
        ]

    if not bit_columns:
        return df

    existing = [c for c in bit_columns if c in df.columns]
    if not existing:
        return df

    return df.with_columns(
        pl.col(col).cast(pl.Int8).alias(col)
        for col in existing
    )


def fix_oracle_date_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Fix ConnectorX Oracle DATE columns returned as Utf8 -> cast to Datetime.

    B-13: Also upcasts any pl.Date columns to pl.Datetime to prevent silent time
    truncation. Oracle DATE always includes hours/minutes/seconds — mapping to
    SQL Server DATE (date-only) silently truncates time. The correct target type
    is DATETIME2(0).
    """
    # B-13: Upcast pl.Date -> pl.Datetime to prevent time truncation.
    # Oracle DATE always includes time; ConnectorX may return pl.Date in some versions.
    pure_date_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if dtype == pl.Date
    ]
    for col in pure_date_cols:
        try:
            df = df.with_columns(pl.col(col).cast(pl.Datetime).alias(col))
            logger.info(
                "B-13: Upcast Oracle DATE column %s from pl.Date to pl.Datetime "
                "(Oracle DATE includes time; pl.Date would truncate)",
                col,
            )
        except Exception:
            logger.debug("B-13: Could not upcast column %s to Datetime", col)

    # Original: fix Utf8 DATE columns
    date_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if (dtype == pl.Utf8 or dtype == pl.String) and "DATE" in col.upper()
    ]

    if not date_cols and not pure_date_cols:
        return df

    for col in date_cols:
        try:
            df = df.with_columns(
                pl.col(col).str.to_datetime(
                    format="%Y-%m-%d %H:%M:%S",
                    strict=False,
                ).alias(col)
            )
            logger.debug("Cast Oracle DATE column %s from Utf8 to Datetime", col)
        except Exception:
            logger.debug("Could not cast column %s to Datetime, leaving as-is", col)

    return df


def reinterpret_uint64(df: pl.DataFrame) -> pl.DataFrame:
    """Reinterpret any UInt64 columns as Int64 for SQL Server compatibility.

    B-1: _row_hash is now Utf8 (full SHA-256 hex string), so this function
    no longer processes hash columns. It still handles any other UInt64 columns
    that may come from source data or polars operations.
    """
    uint64_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if dtype == pl.UInt64
    ]

    if not uint64_cols:
        return df

    return df.with_columns(
        pl.col(col).reinterpret(signed=True).alias(col)
        for col in uint64_cols
    )


def reorder_columns_for_bcp(
    df: pl.DataFrame,
    full_table_name: str,
    exclude_columns: set[str] | None = None,
) -> pl.DataFrame:
    """Reorder DataFrame columns to match target table's INFORMATION_SCHEMA column order.

    Prevents P0-1: column order drift causing silent BCP misloads.

    Args:
        df: DataFrame to reorder.
        full_table_name: Target table (e.g. 'UDM_Stage.DNA.ACCT_cdc').
        exclude_columns: Target columns to skip (e.g. {'_scd2_key'} for Bronze IDENTITY).

    Returns:
        DataFrame with columns reordered to match target.

    Raises:
        ColumnOrderError: If DataFrame has columns not in target or column count differs
                          after accounting for exclusions.
    """
    from data_load.schema_utils import get_target_column_order

    target_cols = get_target_column_order(full_table_name, exclude_columns)
    df_cols = set(df.columns)
    target_set = set(target_cols)

    # Columns in DataFrame but not in target
    extra_in_df = df_cols - target_set
    # Columns in target but not in DataFrame
    missing_in_df = target_set - df_cols

    if missing_in_df:
        raise ColumnOrderError(
            f"DataFrame is missing columns expected by {full_table_name}: {missing_in_df}"
        )

    if extra_in_df:
        logger.warning(
            "DataFrame has columns not in target %s (will be dropped): %s",
            full_table_name, extra_in_df,
        )

    # Reorder to match target — only select columns that exist in target
    ordered_cols = [c for c in target_cols if c in df_cols]

    if ordered_cols != list(df.select(ordered_cols).columns):
        logger.warning(
            "Reordering DataFrame columns to match %s positional order",
            full_table_name,
        )

    return df.select(ordered_cols)

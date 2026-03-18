"""DataFrame sanitization and preparation functions for BCP CSV output.

Covers: string sanitization, BIT column casting, Oracle DATE fixes,
UInt64 reinterpretation, and column reordering for BCP positional mapping.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


# Item-25: Module-level constant — avoids re-creating the list on every
# _looks_like_datetime_column() invocation (called once per string column per table).
_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",       # Standard: 2025-01-15 10:30:00
    "%Y-%m-%dT%H:%M:%S",       # ISO-8601: 2025-01-15T10:30:00
    "%Y-%m-%d %H:%M:%S%.f",    # With fractional seconds: 2025-01-15 10:30:00.123
    "%Y-%m-%dT%H:%M:%S%.f",    # ISO-8601 with fractions: 2025-01-15T10:30:00.123
    "%d-%b-%Y %H:%M:%S",       # Oracle NLS: 15-JAN-2025 10:30:00
    "%d-%b-%y %H:%M:%S",       # Oracle NLS short year: 15-JAN-25 10:30:00
]


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


def fix_oracle_date_columns(
    df: pl.DataFrame,
    stage_table: str | None = None,
) -> pl.DataFrame:
    """Fix ConnectorX Oracle DATE columns returned as Utf8 -> cast to Datetime.

    B-13: Also upcasts any pl.Date columns to pl.Datetime to prevent silent time
    truncation. Oracle DATE always includes hours/minutes/seconds — mapping to
    SQL Server DATE (date-only) silently truncates time. The correct target type
    is DATETIME2(0).

    Args:
        df: DataFrame with Oracle source columns.
        stage_table: Item-20 — Optional Stage table name (e.g. 'UDM_Stage.DNA.ACCT_cdc').
            If provided and the table exists, INFORMATION_SCHEMA is checked to identify
            columns already typed as VARCHAR/NVARCHAR/CHAR. These are excluded from
            content-based datetime detection to prevent false positives on string ID
            columns that happen to contain datetime-formatted values.
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

    # Original: fix Utf8 DATE columns by name heuristic
    date_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if (dtype == pl.Utf8 or dtype == pl.String) and "DATE" in col.upper()
    ]

    # Item-20: If Stage table exists, identify columns explicitly typed as string
    # in INFORMATION_SCHEMA. These are excluded from content-based datetime detection
    # to prevent false positives on string ID columns (e.g. BATCH_ID with values
    # like "2025-01-15T10:30:00"). On first run (table doesn't exist), all columns
    # go through the heuristic as before.
    _STRING_SQL_TYPES = {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}
    known_string_cols: set[str] = set()
    if stage_table:
        try:
            from data_load.schema_utils import get_column_metadata
            for meta in get_column_metadata(stage_table):
                if meta.data_type.lower() in _STRING_SQL_TYPES:
                    known_string_cols.add(meta.column_name)
        except Exception:
            pass  # Stage table may not exist on first run — fall back to heuristic

    # O-1: Content-based detection for string columns that parse as datetimes
    # but don't have "DATE" in their name (e.g., CREATED_AT, MODIFIED_TS).
    string_cols_without_date = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if (dtype == pl.Utf8 or dtype == pl.String)
        and "DATE" not in col.upper()
        and col not in date_cols
    ]
    for col in string_cols_without_date:
        # Item-20: Skip content-based detection for columns explicitly typed as
        # string in the Stage table — casting these would corrupt the data.
        if col in known_string_cols:
            continue
        if _looks_like_datetime_column(df, col):
            date_cols.append(col)
            logger.info(
                "O-1: Content-based detection identified %s as a datetime column "
                "(name heuristic missed it — no 'DATE' in column name)",
                col,
            )

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


def _looks_like_datetime_column(df: pl.DataFrame, col: str, sample_size: int = 20) -> bool:
    """O-1: Content-based datetime detection for string columns.

    Samples up to sample_size non-null values and checks if they match
    common datetime patterns. Returns True if >=90% of sampled values
    parse as datetimes.

    Item-10: Improved robustness over original implementation:
      - Sample size raised from 10 to 20 to reduce false positive risk.
      - Match threshold raised from 80% to 90%.
      - Multiple datetime formats checked (space-separated, ISO-8601 T-separator,
        date-only, Oracle NLS_DATE_FORMAT variants).
    """
    if len(df) == 0:
        return False

    # Get non-null sample
    sample = df[col].drop_nulls().head(sample_size)
    if len(sample) == 0:
        return False

    # Item-10: Try multiple datetime formats to cover Oracle NLS_DATE_FORMAT
    # variants and ISO-8601. Order matters — most common first for efficiency.
    # Item-25: _DATETIME_FORMATS is defined at module level to avoid per-call allocation.
    for fmt in _DATETIME_FORMATS:
        try:
            parsed = sample.str.to_datetime(format=fmt, strict=False)
            non_null_count = parsed.drop_nulls().len()
            # Item-10: Raised from 80% to 90% to reduce false positives.
            if non_null_count >= len(sample) * 0.9:
                return True
        except Exception:
            continue

    return False


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

    # Item-21: Compare against DataFrame's original column order (filtered to
    # target columns) to detect whether reordering is actually needed.
    # Previous code compared ordered_cols against df.select(ordered_cols).columns
    # which always matched (select returns columns in the order given).
    original_order = [c for c in df.columns if c in target_set]
    if ordered_cols != original_order:
        logger.warning(
            "Reordering DataFrame columns to match %s positional order",
            full_table_name,
        )

    return df.select(ordered_cols)

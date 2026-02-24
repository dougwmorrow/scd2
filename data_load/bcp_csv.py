"""BCP CSV helper functions — single source of truth for CSV contract enforcement.

All CSV writers MUST use these functions to produce files matching the BCP CSV Contract:
  - Delimiter: tab (\\t)
  - Row terminator: LF only
  - Header: none
  - Quoting: quote_style='never'
  - NULL: empty string
  - Datetime format: '%Y-%m-%d %H:%M:%S.%3f'
  - BIT columns: Int8 (0/1)
  - Hash columns: Full SHA-256 hex string, VARCHAR(64) (B-1)
  - UInt64 (non-hash): .reinterpret(signed=True) -> Int64
  - String sanitization: remove \\t \\n \\r \\x00 \\x0B \\x0C \\x85 \\u2028 \\u2029 (B-6)
  - Batch size: 4096

B-1 MITIGATION — quote_style='never' data corruption prevention:
  Polars docs warn: quote_style='never' "never puts quotes around fields, even if
  that results in invalid CSV data." Embedded tabs split fields into two columns;
  embedded newlines split rows. sanitize_strings() MUST be called on every DataFrame
  BEFORE write_bcp_csv(). It strips \\t \\n \\r \\x00 from all Utf8/String columns.
  prepare_dataframe_for_bcp() calls sanitize_strings() automatically. Direct callers
  of write_bcp_csv() MUST sanitize first — write_bcp_csv() validates this.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import config

# Re-export moved symbols for backward compatibility within this package.
# External consumers should import from the new locations directly.
from data_load.row_hash import add_row_hash, add_row_hash_fallback  # noqa: F401
from data_load.sanitize import (  # noqa: F401
    ColumnOrderError,
    cast_bit_columns,
    fix_oracle_date_columns,
    reinterpret_uint64,
    reorder_columns_for_bcp,
    sanitize_strings,
)
from data_load.bcp_format import generate_bcp_format_file  # noqa: F401

logger = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    """W-7: Raised when DataFrame schemas don't match before concatenation."""


def validate_schema_before_concat(
    dfs: list[pl.DataFrame],
    context: str,
) -> None:
    """W-7: Validate all DataFrames have identical schemas before concatenation.

    Polars' vertical_relaxed and diagonal_relaxed concat modes silently coerce
    columns to common supertypes (e.g., Int64 -> Float64), losing precision for
    large integers (>2^53). This guard catches schema mismatches explicitly.

    Args:
        dfs: List of DataFrames to validate.
        context: Description of where the concat happens (for error messages).

    Raises:
        SchemaValidationError: If schemas differ between DataFrames.
    """
    if len(dfs) <= 1:
        return

    reference = dfs[0].schema
    for i, df in enumerate(dfs[1:], 1):
        if df.schema != reference:
            mismatches = {
                col: (reference.get(col), df.schema.get(col))
                for col in set(reference) | set(df.schema)
                if reference.get(col) != df.schema.get(col)
            }
            raise SchemaValidationError(
                f"W-7: Schema mismatch before concat in {context} at DataFrame "
                f"index {i}: {mismatches}. This would cause silent type coercion "
                f"(e.g., Int64 -> Float64) that can corrupt precision-sensitive "
                f"values like PKs or monetary amounts."
            )


def add_extracted_at(df: pl.DataFrame) -> pl.DataFrame:
    """Add _extracted_at column with current UTC timestamp."""
    now = datetime.now(timezone.utc)
    return df.with_columns(
        pl.lit(now).alias("_extracted_at")
    )


def _validate_sanitized(df: pl.DataFrame) -> None:
    """B-1: Verify string columns don't contain BCP-corrupting characters.

    Samples up to 10,000 rows for performance. Raises if unsanitized
    characters are found — caller must use sanitize_strings() first.
    """
    string_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if dtype == pl.Utf8 or dtype == pl.String
    ]
    if not string_cols or len(df) == 0:
        return

    sample = df.head(min(10_000, len(df)))
    for col in string_cols:
        has_bad = sample[col].str.contains(r"[\t\n\r\x0B\x0C\x85\u2028\u2029]").any()
        if has_bad:
            logger.error(
                "B-1: Column [%s] contains tab/newline characters after sanitization. "
                "This WILL corrupt BCP CSV output. Ensure sanitize_strings() was called.",
                col,
            )
            raise ValueError(
                f"B-1: Column [{col}] contains unsanitized tab/newline characters. "
                f"Call sanitize_strings() before write_bcp_csv()."
            )


def write_bcp_csv(df: pl.DataFrame, path: str | Path) -> Path:
    """Write DataFrame to BCP CSV file following the BCP CSV Contract.

    Args:
        df: DataFrame to write (must already be sanitized/cast).
        path: Output file path.

    Returns:
        Path to the written CSV file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # B-1: Validate that string columns have been sanitized before writing.
    # quote_style='never' means any remaining \t \n \r will corrupt the CSV.
    _validate_sanitized(df)

    df.write_csv(
        path,
        separator=config.CSV_SEPARATOR,
        quote_style=config.CSV_QUOTE_STYLE,
        null_value=config.CSV_NULL_VALUE,
        include_header=config.CSV_HAS_HEADER,
        datetime_format=config.CSV_DATETIME_FORMAT,
        batch_size=config.CSV_BATCH_SIZE,
    )

    # V-6: BOM detection guard — Polars does not write BOMs by default, but
    # platform quirks, intermediate processing, or future regressions could
    # introduce one. A BOM (0xEF 0xBB 0xBF) prepended to the file corrupts
    # the first field of the first row (e.g. PK "12345" becomes "\xEF\xBB\xBF12345").
    _strip_bom_if_present(path)

    logger.info("Wrote BCP CSV: %s (%d rows)", path, len(df))
    return path


def _strip_bom_if_present(path: Path) -> None:
    """V-6: Detect and strip UTF-8 BOM from the beginning of a file."""
    _BOM = b"\xef\xbb\xbf"
    try:
        with open(path, "rb") as f:
            first_bytes = f.read(3)
        if first_bytes == _BOM:
            logger.warning(
                "V-6: UTF-8 BOM detected in %s — stripping to prevent "
                "first-field corruption in BCP load",
                path,
            )
            with open(path, "rb") as f:
                content = f.read()
            with open(path, "wb") as f:
                f.write(content[3:])
    except OSError:
        logger.debug("V-6: Could not check BOM for %s — continuing", path)


def prepare_dataframe_for_bcp(
    df: pl.DataFrame,
    bit_columns: list[str] | None = None,
    fix_oracle_dates: bool = False,
    source_is_oracle: bool = False,
) -> pl.DataFrame:
    """Full pipeline: fix dates -> hash -> timestamp -> sanitize -> cast -> reinterpret.

    This is the main entry point for preparing a DataFrame for BCP CSV output.

    Args:
        df: Raw DataFrame from extraction.
        bit_columns: Explicit list of BIT columns, or None to auto-detect Booleans.
        fix_oracle_dates: Whether to fix Oracle DATE columns returned as Utf8.
        source_is_oracle: E-1 — If True, normalize empty strings to NULL before hashing.

    Returns:
        DataFrame ready for write_bcp_csv().
    """
    if fix_oracle_dates:
        df = fix_oracle_date_columns(df)

    df = add_row_hash(df, source_is_oracle=source_is_oracle)
    df = add_extracted_at(df)
    # W-12: Release over-allocated memory from hash concat + NFC normalization.
    if len(df) > 100_000:
        df.shrink_to_fit(in_place=True)
    df = sanitize_strings(df)
    df = cast_bit_columns(df, bit_columns)
    df = reinterpret_uint64(df)

    return df

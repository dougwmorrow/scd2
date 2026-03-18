"""File extraction — reads Excel, CSV, text, and JSON files into Polars DataFrames.

Supported file types:
    xlsx, xls  — Excel workbooks (calamine engine, Rust-native via fastexcel)
    csv        — Comma/delimiter-separated values
    txt        — Text files (delimiter-separated, same as CSV with configurable separator)
    json       — JSON array of objects or column-oriented JSON
    ndjson     — Newline-delimited JSON (one JSON object per line)

Multi-tab Excel: Each tab that should be consumed gets its own FileExtract row
with the same BasePath/FilePattern but a different SheetName. Each tab becomes
its own Stage/Bronze table pair.

HeaderRow/SkipRows semantics:
    HeaderRow — 0-indexed row number where column headers live (default 0 = first row).
        For Excel: rows before HeaderRow are skipped automatically.
        For CSV/text: rows before HeaderRow are skipped via skip_rows.
    SkipRows — Additional rows to skip AFTER the header row (e.g., a units row,
        a blank separator row). Applied by slicing the DataFrame after reading.

File modification time is captured in PipelineEventLog.Metadata for optional
skip-unchanged optimization on subsequent runs.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from data_load.bcp_csv import prepare_dataframe_for_bcp, write_bcp_csv

if TYPE_CHECKING:
    from orchestration.file_config import FileConfig

logger = logging.getLogger(__name__)

# Supported file types and their reader functions
_SUPPORTED_FILE_TYPES = {"xlsx", "xls", "csv", "txt", "json", "ndjson"}


def extract_file(
    file_config: FileConfig,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Extract data from a file into a Polars DataFrame and BCP CSV.

    Steps:
        1. Resolve file path via glob pattern (pick most recently modified)
        2. Validate file accessibility
        3. Read file into Polars DataFrame (Excel, CSV, text, JSON)
        4. Apply column mapping if configured
        5. Select specific columns if configured
        6. Validate extracted data
        7. Prepare DataFrame for BCP and write CSV

    Args:
        file_config: File configuration from FileExtract.
        output_dir: Directory for temp BCP CSV files.

    Returns:
        Tuple of (DataFrame, CSV path).

    Raises:
        FileNotFoundError: If no file matches the pattern.
        ValueError: If file validation fails.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Resolve file path
    file_path = _resolve_file_path(file_config.base_path, file_config.file_pattern)
    logger.info(
        "Resolved file for %s.%s: %s",
        file_config.source_name, file_config.table_name, file_path,
    )

    # Step 2: Validate accessibility
    _validate_file_accessibility(file_path)

    # Step 3: Read file
    file_type = file_config.file_type.lower()
    if file_type in ("xlsx", "xls"):
        df = _read_excel(file_path, file_config)
    elif file_type in ("csv", "txt"):
        df = _read_csv(file_path, file_config)
    elif file_type == "json":
        df = _read_json(file_path, file_config)
    elif file_type == "ndjson":
        df = _read_ndjson(file_path, file_config)
    else:
        raise ValueError(
            f"Unsupported file type '{file_config.file_type}' for "
            f"{file_config.source_name}.{file_config.table_name}. "
            f"Supported: {', '.join(sorted(_SUPPORTED_FILE_TYPES))}"
        )

    logger.info(
        "Read %d rows x %d columns from %s",
        len(df), len(df.columns), file_path.name,
    )

    # Step 4: Apply column mapping
    if file_config.column_mapping:
        df = _apply_column_mapping(df, file_config.column_mapping)

    # Step 5: Select specific columns if configured
    if file_config.columns_to_extract:
        missing = set(file_config.columns_to_extract) - set(df.columns)
        if missing:
            raise ValueError(
                f"ColumnsToExtract references missing columns: {sorted(missing)}. "
                f"Available: {df.columns}"
            )
        df = df.select(file_config.columns_to_extract)

    # Step 6: Validate
    _validate_file_data(df, file_config)

    # Step 7: Prepare for BCP and write CSV
    # source_is_oracle=False — file sources don't need Oracle empty-string normalization
    df = prepare_dataframe_for_bcp(df, source_is_oracle=False)

    csv_path = output_dir / f"{file_config.source_name}_{file_config.table_name}_file.csv"
    write_bcp_csv(df, csv_path)

    # Capture file mtime for skip-unchanged optimization
    mtime = os.path.getmtime(file_path)
    logger.info(
        "File mtime for %s.%s: %.0f (%s)",
        file_config.source_name, file_config.table_name,
        mtime, file_path.name,
    )

    return df, csv_path


# ---------------------------------------------------------------------------
# File resolution
# ---------------------------------------------------------------------------

def _resolve_file_path(base_path: str, file_pattern: str) -> Path:
    """Glob match within base_path, return the most recently modified file.

    Args:
        base_path: Directory to search in.
        file_pattern: Glob pattern (e.g., '*.xlsx', 'RATES_*.csv').

    Returns:
        Path to the most recently modified matching file.

    Raises:
        FileNotFoundError: If no files match the pattern.
    """
    base = Path(base_path)
    if not base.is_dir():
        raise FileNotFoundError(
            f"Base path does not exist or is not a directory: {base_path}"
        )

    matches = sorted(base.glob(file_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(
            f"No files matching pattern '{file_pattern}' in {base_path}"
        )

    if len(matches) > 1:
        logger.info(
            "Found %d files matching '%s' in %s — using most recent: %s",
            len(matches), file_pattern, base_path, matches[0].name,
        )

    return matches[0]


# ---------------------------------------------------------------------------
# File validation
# ---------------------------------------------------------------------------

def _validate_file_accessibility(file_path: Path) -> None:
    """Validate that the file exists, is readable, and is non-zero size.

    Raises:
        FileNotFoundError: If file doesn't exist.
        PermissionError: If file isn't readable.
        ValueError: If file is empty (zero bytes).
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"File not readable: {file_path}")

    if file_path.stat().st_size == 0:
        raise ValueError(f"File is empty (0 bytes): {file_path}")


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def _read_excel(file_path: Path, file_config: FileConfig) -> pl.DataFrame:
    """Read an Excel file into a Polars DataFrame.

    Uses the calamine engine (Rust-native via fastexcel) for .xlsx and .xls.

    Multi-tab handling: Each tab that should be consumed gets its own FileExtract
    row with a different SheetName value. If SheetName is NULL, reads the first sheet.

    HeaderRow/SkipRows:
        HeaderRow controls which row is the header via calamine's read_options.
        SkipRows (rows to skip after header) is applied by slicing the DataFrame
        since calamine does not support post-header row skipping natively.

    Args:
        file_path: Path to the Excel file.
        file_config: File configuration with sheet_name, header_row, skip_rows.

    Returns:
        Polars DataFrame.
    """
    # Top-level kwargs for pl.read_excel()
    excel_kwargs: dict = {}

    # Sheet selection — default to first sheet if not specified
    if file_config.sheet_name:
        excel_kwargs["sheet_name"] = file_config.sheet_name
    else:
        excel_kwargs["sheet_id"] = 1

    # Calamine engine options go through read_options dict
    # In calamine/fastexcel:
    #   skip_rows: skip N rows from the top of the sheet (before header detection)
    #   header_row: 0-indexed row (after skip_rows) to use as header
    # Our semantics: HeaderRow = 0-indexed row where headers live in the sheet.
    # We set skip_rows = HeaderRow so calamine skips junk rows, then header_row=0
    # so it treats the first remaining row as the header.
    read_options: dict = {}
    if file_config.header_row > 0:
        read_options["skip_rows"] = file_config.header_row
        read_options["header_row"] = 0

    if read_options:
        excel_kwargs["read_options"] = read_options

    try:
        df = pl.read_excel(file_path, engine="calamine", **excel_kwargs)
    except Exception as exc:
        logger.warning(
            "Calamine engine failed for %s, trying openpyxl: %s",
            file_path.name, exc,
        )
        # openpyxl fallback — does not support read_options
        openpyxl_kwargs = {
            k: v for k, v in excel_kwargs.items() if k != "read_options"
        }
        df = pl.read_excel(file_path, engine="openpyxl", **openpyxl_kwargs)

    # SkipRows: additional rows to skip AFTER the header (units row, blank row, etc.)
    # Applied by slicing the DataFrame since calamine handles this as skip_rows
    # from the top (which we already used for HeaderRow).
    if file_config.skip_rows > 0:
        if file_config.skip_rows >= len(df):
            logger.warning(
                "SkipRows=%d >= row count=%d for %s — returning empty DataFrame",
                file_config.skip_rows, len(df), file_path.name,
            )
            return df.clear()
        df = df.slice(file_config.skip_rows)
        logger.debug("Skipped %d rows after header", file_config.skip_rows)

    return df


def _read_csv(file_path: Path, file_config: FileConfig) -> pl.DataFrame:
    """Read a CSV or text file into a Polars DataFrame.

    Handles both .csv and .txt files. Text files are delimited files with a
    configurable separator (pipe, tab, semicolon, etc.).

    HeaderRow/SkipRows:
        HeaderRow → skip_rows (skip lines before the header)
        SkipRows → skip_rows_after_header (skip lines between header and data)

    Args:
        file_path: Path to the CSV/text file.
        file_config: File configuration with delimiter, encoding, header_row, skip_rows.

    Returns:
        Polars DataFrame.
    """
    read_kwargs: dict = {
        "infer_schema_length": 10000,
        "try_parse_dates": True,
    }

    if file_config.delimiter:
        read_kwargs["separator"] = file_config.delimiter

    if file_config.encoding and file_config.encoding.lower() != "utf-8":
        read_kwargs["encoding"] = file_config.encoding

    # HeaderRow: skip lines before the header row
    if file_config.header_row > 0:
        read_kwargs["skip_rows"] = file_config.header_row

    # SkipRows: skip lines after the header (units row, blank separator, etc.)
    if file_config.skip_rows > 0:
        read_kwargs["skip_rows_after_header"] = file_config.skip_rows

    return pl.read_csv(file_path, **read_kwargs)


def _read_json(file_path: Path, file_config: FileConfig) -> pl.DataFrame:
    """Read a JSON file into a Polars DataFrame.

    Expects either:
        - An array of objects: [{"col1": "val1", "col2": "val2"}, ...]
        - A column-oriented object: {"col1": ["val1", ...], "col2": ["val2", ...]}

    HeaderRow/SkipRows are ignored for JSON files (structure is self-describing).

    Args:
        file_path: Path to the JSON file.
        file_config: File configuration.

    Returns:
        Polars DataFrame.
    """
    return pl.read_json(file_path)


def _read_ndjson(file_path: Path, file_config: FileConfig) -> pl.DataFrame:
    """Read a newline-delimited JSON file into a Polars DataFrame.

    Each line is a separate JSON object. Common format for log files, API dumps,
    and streaming data exports.

    Example:
        {"id": 1, "name": "Alice", "amount": 100.50}
        {"id": 2, "name": "Bob", "amount": 200.75}

    HeaderRow/SkipRows are ignored for NDJSON files.

    Args:
        file_path: Path to the NDJSON file.
        file_config: File configuration.

    Returns:
        Polars DataFrame.
    """
    return pl.read_ndjson(file_path)


# ---------------------------------------------------------------------------
# Column mapping
# ---------------------------------------------------------------------------

def _apply_column_mapping(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    """Rename columns per FileExtract.ColumnMapping.

    Args:
        df: Input DataFrame.
        mapping: Dict of {file_column_name: target_column_name}.

    Returns:
        DataFrame with renamed columns.
    """
    # Only rename columns that exist in the DataFrame
    valid_renames = {k: v for k, v in mapping.items() if k in df.columns}

    if valid_renames:
        df = df.rename(valid_renames)
        logger.info("Applied column mapping: %d columns renamed", len(valid_renames))

    skipped = set(mapping.keys()) - set(valid_renames.keys())
    if skipped:
        logger.warning(
            "Column mapping references missing columns (skipped): %s",
            sorted(skipped),
        )

    return df


# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------

def _validate_file_data(df: pl.DataFrame, file_config: FileConfig) -> None:
    """Validate extracted DataFrame against FileExtract expectations.

    Checks:
        - Minimum row count (ExpectedMinRows)
        - Expected columns present (ExpectedColumns)
        - PK columns present (PrimaryKeyColumns)

    Raises:
        ValueError: If any validation fails.
    """
    # Min rows check
    if file_config.expected_min_rows is not None and len(df) < file_config.expected_min_rows:
        raise ValueError(
            f"File extraction for {file_config.source_name}.{file_config.table_name} "
            f"returned {len(df)} rows, below expected minimum of "
            f"{file_config.expected_min_rows}"
        )

    # Expected columns check
    if file_config.expected_columns:
        actual = set(df.columns)
        expected = set(file_config.expected_columns)
        missing = expected - actual
        if missing:
            raise ValueError(
                f"File for {file_config.source_name}.{file_config.table_name} "
                f"is missing expected columns: {sorted(missing)}. "
                f"Available: {sorted(actual)}"
            )

    # PK columns must be present
    if file_config.pk_column_names:
        actual = set(df.columns)
        missing_pks = set(file_config.pk_column_names) - actual
        if missing_pks:
            raise ValueError(
                f"File for {file_config.source_name}.{file_config.table_name} "
                f"is missing PK columns: {sorted(missing_pks)}. "
                f"Available: {sorted(actual)}"
            )

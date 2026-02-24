"""ConnectorX Oracle extraction -> Polars DataFrame -> BCP CSV.

Used when: Oracle source + no SourceIndexHint (bulk FULL scan, 10-20x faster than oracledb).
Supports partition_on for parallel reads when PartitionOn column is set.

W-6 TODO — ConnectorX Oracle contingency:
  ConnectorX Oracle support remains unreliable (Issue #644 panics on 9999-12-31
  dates; known memory leak in loop-based extraction via Rust-Python FFI).
  Two alternatives to evaluate:
    1. pydbzengine (released Feb 2025): Python interface to Debezium embedded
       engine via JPype. Supports Oracle CDC through log-based capture —
       structurally superior to hash-based comparison for change detection.
    2. oracledb with PyArrow backend: native Oracle driver with columnar output,
       avoiding the ConnectorX FFI boundary entirely.
  For immediate mitigation: evaluate process-level isolation (subprocess per
  table) to prevent ConnectorX memory accumulation across multi-table runs.

W-14 TODO — pydbzengine log-based Oracle CDC evaluation:
  pydbzengine (released Feb 2025) is the most significant development for
  Python-based CDC. Evaluation checklist:
    1. Prototype with a single low-volume Oracle table (e.g., ACCT)
    2. Evaluate Oracle LogMiner connector reliability, JPype overhead,
       memory footprint, and error handling
    3. Compare CDC accuracy against hash-based approach — does log-based
       capture detect changes the hash-based path misses (intermediate
       state changes, hard deletes)?
    4. If viable, create an alternative extraction path coexisting with
       the current approach (per-table config in UdmTablesList)
    5. Document evaluation findings in CLAUDE.md

W-15 TODO — Monitor ADBC/Columnar ecosystem for Oracle driver:
  Columnar (launched Oct 2025, backed by Databricks/dbt/Microsoft/Snowflake)
  released ADBC drivers for SQL Server and 9 other databases, delivering
  near-COPY performance via end-to-end Arrow RecordBatch passing. No Oracle
  ADBC driver exists yet. When available, it would be the ideal extraction
  path — columnar, zero-copy, standardized. DuckDB's ODBC scanner is a
  potential interim fallback if ConnectorX Oracle becomes untenable.

Provides two modes:
  - extract_oracle_connectorx(): Full table scan (small tables).
  - extract_oracle_connectorx_windowed(): Date-windowed extraction (large tables).

E-5 NOTE — ConnectorX connection lifecycle:
  ConnectorX opens N+1 connections per read_sql() call (1 metadata + N partitions).
  With E-1 forcing partition_num=1, each call opens only 2 connections — greatly
  reducing session exhaustion risk. ConnectorX manages connections internally via
  Rust; Python has no handle to explicitly close them. If Oracle session count
  approaches limits (ORA-00018), verify with V$SESSION and consider reducing
  concurrent pipeline workers. The M-3 FD monitoring in large_table_orchestrator
  provides indirect detection of connection leaks via file descriptor counts.

O-2 NOTE — Bind variable peeking (ConnectorX path):
  ConnectorX uses literal values in SQL (string interpolation, not bind variables),
  so Oracle's bind variable peeking does not apply here. Each execution gets a
  fresh hard parse with accurate cardinality estimates. The oracledb path
  (oracle_extractor.py) uses bind variables, where Oracle's Adaptive Cursor
  Sharing (11g+) self-corrects suboptimal plans after ~10 executions.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import connectorx as cx
import polars as pl

from data_load.bcp_csv import prepare_dataframe_for_bcp, validate_schema_before_concat, write_bcp_csv
from extract import cx_read_sql_safe
from sources import get_source

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)

# E-4: Sentinel date threshold. Oracle DATE values >= this cause ConnectorX
# Rust panics (issue #644). Wrapping in CASE WHEN to NULL-ify prevents the crash.
_SENTINEL_DATE_THRESHOLD = "9999-01-01"


def _get_oracle_date_columns(uri: str, schema: str, table: str) -> list[str]:
    """E-4: Query Oracle metadata to find DATE/TIMESTAMP columns.

    ConnectorX crashes with a Rust panic on DATE '9999-12-31' (issue #644).
    We need to identify DATE columns to wrap them in CASE WHEN.

    Works for both tables and views via ALL_TAB_COLUMNS.
    """
    query = (
        f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS "
        f"WHERE OWNER = '{schema.upper()}' AND TABLE_NAME = '{table.upper()}' "
        f"AND DATA_TYPE IN ('DATE', 'TIMESTAMP(6)', 'TIMESTAMP(3)') "
        f"ORDER BY COLUMN_ID"
    )
    try:
        df = cx.read_sql(uri, query, return_type="polars")
        cols = df["COLUMN_NAME"].to_list() if len(df) > 0 else []
        if cols:
            logger.debug("E-4: Found %d DATE/TIMESTAMP columns in %s.%s: %s", len(cols), schema, table, cols)
        return cols
    except Exception:
        logger.warning(
            "E-4: Could not query DATE columns for %s.%s — using SELECT * (risk of sentinel crash)",
            schema, table, exc_info=True,
        )
        return []


def _build_safe_select_with_uri(
    uri: str,
    schema: str,
    table: str,
    date_columns: list[str],
    hint: str = "",
    where_clause: str = "",
) -> str:
    """E-4: Build SELECT with sentinel-safe DATE columns using URI for metadata.

    Returns a SELECT statement where DATE columns are wrapped in:
      CASE WHEN col >= DATE '9999-01-01' THEN NULL ELSE col END AS col
    """
    if not date_columns:
        base = f"SELECT {hint} * FROM {schema}.{table}"
        return f"{base} {where_clause}".strip()

    date_set = {c.upper() for c in date_columns}

    meta_query = (
        f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS "
        f"WHERE OWNER = '{schema.upper()}' AND TABLE_NAME = '{table.upper()}' "
        f"ORDER BY COLUMN_ID"
    )
    try:
        df_meta = cx.read_sql(uri, meta_query, return_type="polars")
        all_columns = df_meta["COLUMN_NAME"].to_list()
    except Exception:
        logger.warning("E-4: Could not get column list for %s.%s — falling back to SELECT *", schema, table)
        base = f"SELECT {hint} * FROM {schema}.{table}"
        return f"{base} {where_clause}".strip()

    # Build explicit column list with CASE WHEN for DATE columns
    col_exprs = []
    for col in all_columns:
        if col.upper() in date_set:
            col_exprs.append(
                f"CASE WHEN {col} >= DATE '{_SENTINEL_DATE_THRESHOLD}' "
                f"THEN NULL ELSE {col} END AS {col}"
            )
        else:
            col_exprs.append(col)

    select_list = ", ".join(col_exprs)
    base = f"SELECT {hint} {select_list} FROM {schema}.{table}"
    return f"{base} {where_clause}".strip()


def extract_oracle_connectorx(
    table_config: TableConfig,
    output_dir: str | Path,
    partition_on: str | None = None,
    partition_num: int = 4,
) -> tuple[pl.DataFrame, Path]:
    """Extract from Oracle via ConnectorX into a Polars DataFrame and write BCP CSV.

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.
        partition_on: Column to partition on for parallel extraction.
        partition_num: Number of partitions for parallel extraction.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    source = get_source(table_config.source_name)
    uri = source.connectorx_uri()

    # E-4: Build safe query that NULL-ifies sentinel dates to prevent Rust panic
    hint = f"/*+ FULL({table_config.source_object_name}) */"
    date_cols = _get_oracle_date_columns(uri, table_config.source_schema_name, table_config.source_object_name)
    if date_cols:
        query = _build_safe_select_with_uri(
            uri, table_config.source_schema_name, table_config.source_object_name,
            date_cols, hint=hint,
        )
        logger.info("E-4: ConnectorX Oracle extract with %d sentinel-safe DATE columns: %s", len(date_cols), table_config.source_object_name)
    else:
        query = f"SELECT {hint} * FROM {table_config.source_schema_name}.{table_config.source_object_name}"
    logger.info("ConnectorX Oracle extract: %s", query[:200])

    cx_kwargs = {
        "conn": uri,
        "query": query,
        "return_type": "polars",
    }

    if partition_on:
        # E-1: Force partition_num=1 for CDC-sensitive extraction.
        # partition_num > 1 opens N independent Oracle connections, each with its own
        # statement-level SCN. On actively-written tables, this produces torn reads:
        # rows can be duplicated or missed between snapshots, causing phantom CDC changes.
        if partition_num > 1:
            logger.warning(
                "E-1: Overriding partition_num from %d to 1 for %s — "
                "CDC-sensitive extraction requires a single consistent snapshot. "
                "partition_num > 1 causes torn reads on actively-written tables.",
                partition_num, table_config.source_object_name,
            )
            partition_num = 1
        cx_kwargs["partition_on"] = partition_on
        cx_kwargs["partition_num"] = partition_num
        logger.info("Using partition_on=%s with %d partitions", partition_on, partition_num)

    # B-7: Use safe wrapper with Rust panic recovery and retry.
    df = cx_read_sql_safe(
        conn=cx_kwargs["conn"],
        query=cx_kwargs["query"],
        return_type=cx_kwargs["return_type"],
        partition_on=cx_kwargs.get("partition_on"),
        partition_num=cx_kwargs.get("partition_num"),
        context=f"Oracle extract {table_config.source_object_name}",
    )
    logger.info("Extracted %d rows, %d columns from %s", len(df), len(df.columns), table_config.source_full_table_name)

    # E-2: Supplement partitioned extraction with NULL partition column rows.
    # ConnectorX's range-based WHERE clauses never match NULL.
    if partition_on:
        df = _supplement_null_partition_rows(
            df, uri, query, partition_on, table_config,
        )

    # P2-3: Log partition skew warning if partition_on was used
    if partition_on and len(df) > 0:
        _log_partition_skew(df, partition_on, partition_num, table_config)

    # P3-5: Warn about large full-scan extractions
    if not partition_on and len(df) > 5_000_000:
        logger.warning(
            "P3-5: Full-scan extraction returned %d rows from %s. "
            "Consider using PartitionOn or date-windowed extraction for better performance.",
            len(df), table_config.source_full_table_name,
        )

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=True, source_is_oracle=True)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table_config.source_object_name}.csv",
    )

    return df, csv_path


def _supplement_null_partition_rows(
    df: pl.DataFrame,
    uri: str,
    base_query: str,
    partition_on: str,
    table_config,
) -> pl.DataFrame:
    """E-2: Fetch rows where partition column IS NULL and concat with main result.

    ConnectorX's partition logic generates WHERE clauses using > and < operators,
    which never match NULL. Any row with a NULL partition column is silently excluded.
    """
    try:
        # Append WHERE partition_col IS NULL to the base query
        null_query = f"{base_query} WHERE {partition_on} IS NULL"
        # If base query already has WHERE, use AND instead
        if " WHERE " in base_query.upper():
            null_query = f"{base_query} AND {partition_on} IS NULL"

        df_nulls = cx.read_sql(uri, null_query, return_type="polars")

        if len(df_nulls) > 0:
            logger.warning(
                "E-2: Found %d rows with NULL [%s] in %s — these would be silently "
                "excluded by ConnectorX partitioned extraction. Supplementing result.",
                len(df_nulls), partition_on, table_config.source_object_name,
            )
            # W-7: Validate schemas before diagonal_relaxed concat (highest-risk site —
            # ConnectorX may return different dtypes for partitioned vs non-partitioned).
            try:
                validate_schema_before_concat(
                    [df, df_nulls],
                    f"Oracle NULL partition supplement for {table_config.source_object_name}",
                )
            except Exception as e:
                logger.warning("W-7: %s — proceeding with diagonal_relaxed concat", e)
            df = pl.concat([df, df_nulls], how="diagonal_relaxed")
        else:
            logger.debug("E-2: No NULL [%s] rows in %s", partition_on, table_config.source_object_name)

    except Exception:
        logger.warning(
            "E-2: Failed to check NULL partition column rows for %s — "
            "continuing with partitioned result only",
            table_config.source_object_name, exc_info=True,
        )

    return df


def _log_partition_skew(
    df: pl.DataFrame,
    partition_on: str,
    partition_num: int,
    table_config,
) -> None:
    """P2-3: Log partition column statistics and warn about potential skew."""
    if partition_on not in df.columns:
        return

    try:
        col = df[partition_on]
        col_min = col.min()
        col_max = col.max()
        col_null = col.null_count()
        avg_per_partition = len(df) / partition_num if partition_num > 0 else len(df)

        logger.info(
            "P2-3: Partition stats for %s.%s on [%s]: min=%s, max=%s, "
            "nulls=%d, total=%d, partitions=%d, avg_per_partition=%.0f",
            table_config.source_name, table_config.source_object_name,
            partition_on, col_min, col_max,
            col_null, len(df), partition_num, avg_per_partition,
        )

        if col_null > len(df) * 0.1:
            logger.warning(
                "P2-3: Partition column [%s] has %d NULL values (%.1f%%) in %s. "
                "ConnectorX excludes NULL rows from partitioned reads. "
                "Consider a different partition_on column.",
                partition_on, col_null, col_null / len(df) * 100,
                table_config.source_object_name,
            )
    except Exception:
        logger.debug("Could not compute partition skew stats for %s", partition_on)


def extract_oracle_connectorx_windowed(
    table_config: TableConfig,
    output_dir: str | Path,
    start_date: date,
    end_date: date,
    partition_on: str | None = None,
    partition_num: int = 4,
) -> tuple[pl.DataFrame, Path]:
    """Extract a date window from Oracle via ConnectorX.

    For large tables where SourceIndexHint is not set. Uses the
    SourceAggregateColumnName for the WHERE clause.

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.
        start_date: Start of date range (inclusive).
        end_date: End of date range (exclusive).
        partition_on: Column to partition on for parallel extraction.
        partition_num: Number of partitions for parallel extraction.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    source = get_source(table_config.source_name)
    uri = source.connectorx_uri()

    date_col = table_config.source_aggregate_column_name
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    # O-1: Range predicates for index eligibility (replaces TRUNC on column).
    where_clause = (
        f"WHERE {date_col} >= TO_DATE('{start_date}', 'YYYY-MM-DD') "
        f"AND {date_col} < TO_DATE('{end_date}', 'YYYY-MM-DD')"
    )
    hint = f"/*+ FULL({table}) */"

    # E-4: Build safe query that NULL-ifies sentinel dates
    date_cols = _get_oracle_date_columns(uri, schema, table)
    if date_cols:
        query = _build_safe_select_with_uri(
            uri, schema, table, date_cols, hint=hint, where_clause=where_clause,
        )
        logger.info("E-4: ConnectorX Oracle windowed extract with %d sentinel-safe DATE columns", len(date_cols))
    else:
        query = f"SELECT {hint} * FROM {schema}.{table} {where_clause}"
    logger.info("ConnectorX Oracle windowed extract: %s [%s, %s)", table, start_date, end_date)

    cx_kwargs = {
        "conn": uri,
        "query": query,
        "return_type": "polars",
    }

    if partition_on:
        # E-1: Force single-partition for windowed CDC-sensitive extraction.
        if partition_num > 1:
            logger.warning(
                "E-1: Overriding partition_num from %d to 1 for windowed %s — "
                "CDC-sensitive extraction requires a single consistent snapshot.",
                partition_num, table,
            )
            partition_num = 1
        cx_kwargs["partition_on"] = partition_on
        cx_kwargs["partition_num"] = partition_num

    # B-7: Use safe wrapper with Rust panic recovery and retry.
    df = cx_read_sql_safe(
        conn=cx_kwargs["conn"],
        query=cx_kwargs["query"],
        return_type=cx_kwargs["return_type"],
        partition_on=cx_kwargs.get("partition_on"),
        partition_num=cx_kwargs.get("partition_num"),
        context=f"Oracle windowed extract {table} [{start_date}, {end_date})",
    )
    logger.info(
        "Extracted %d rows from %s.%s for [%s, %s)",
        len(df), schema, table, start_date, end_date,
    )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}_{start_date}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=True, source_is_oracle=True)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}_{start_date}.csv",
    )

    return df, csv_path

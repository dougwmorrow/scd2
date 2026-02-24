"""oracledb fallback extractor: date-chunked extraction with INDEX hints.

Used when: Oracle source + SourceIndexHint populated.
Uses oracledb thick mode via Oracle Instant Client 19c.

Provides two modes:
  - extract_oracle_oracledb(): Full range extraction (small tables, backward compat).
    P2-2: Pre-queries for distinct dates to skip empty days (weekends/holidays).
  - extract_oracle_oracledb_day(): Single-day extraction for large table per-day processing.

P3-2: Original implementation used TRUNC(date_col) in WHERE clauses for timezone safety.
O-1: Replaced TRUNC(date_col) with range predicates for 50-60x index performance.
     Range predicates (date_col >= :start AND date_col < :end) enable standard
     INDEX RANGE SCAN, whereas TRUNC(date_col) forces FULL TABLE SCAN or
     function-based index lookup.

O-2 NOTE — Bind variable peeking (oracledb path):
  oracledb uses bind variables for date parameters. Oracle's bind variable peeking
  optimizes the query plan for the first execution's cardinality — during backfill,
  the initial 365-day range may produce a suboptimal plan for subsequent 1-day queries.
  Adaptive Cursor Sharing (11g+) self-corrects after ~10 executions. This is acceptable
  for steady-state operation. If initial backfill performance is problematic, the
  ConnectorX path (which uses literals, bypassing bind peeking) is an alternative.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import oracledb
import polars as pl

import config
from data_load.bcp_csv import prepare_dataframe_for_bcp, validate_schema_before_concat, write_bcp_csv
from sources import get_source

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)

_thick_mode_initialized = False


def _ensure_thick_mode() -> None:
    global _thick_mode_initialized
    if not _thick_mode_initialized:
        oracledb.init_oracle_client(lib_dir=config.ORACLE_CLIENT_DIR)
        _thick_mode_initialized = True


def _get_distinct_dates(
    conn,
    schema: str,
    table: str,
    date_col: str,
    start_date: date,
    end_date: date,
) -> set[date]:
    """P2-2: Query Oracle for distinct dates in range to skip empty days.

    One extra query eliminates ~30% of empty per-day queries (weekends, holidays).
    """
    # O-1: Use range predicates instead of TRUNC(date_col) for index eligibility.
    # TRUNC on the column prevents standard INDEX RANGE SCAN (50-60x penalty).
    # Keep TRUNC in SELECT to get distinct dates, but not in WHERE.
    query = (
        f"SELECT DISTINCT TRUNC({date_col}) AS dt "
        f"FROM {schema}.{table} "
        f"WHERE {date_col} >= :start_dt AND {date_col} < :end_dt + 1"
    )
    cursor = conn.cursor()
    cursor.execute(query, start_dt=start_date, end_dt=end_date)
    rows = cursor.fetchall()
    cursor.close()

    dates = set()
    for row in rows:
        val = row[0]
        if val is not None:
            if hasattr(val, "date"):
                dates.add(val.date())
            else:
                dates.add(val)

    logger.info(
        "P2-2: Found %d distinct dates in %s.%s [%s to %s] (range has %d calendar days)",
        len(dates), schema, table, start_date, end_date,
        (end_date - start_date).days + 1,
    )
    return dates


def extract_oracle_oracledb(
    table_config: TableConfig,
    start_date: date,
    end_date: date,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Extract from Oracle via oracledb with per-day date chunks and INDEX hints.

    For small tables or backward compatibility. Accumulates all days into one DataFrame.

    P2-2: Pre-queries for distinct dates to skip empty days.
    P3-2: Uses TRUNC() for date boundary comparisons.

    Args:
        table_config: Table configuration with source details.
        start_date: Start of date range (inclusive).
        end_date: End of date range (inclusive).
        output_dir: Directory for output CSV file.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    _ensure_thick_mode()

    source = get_source(table_config.source_name)
    connect_params = source.oracledb_connect_params()

    date_col = table_config.source_aggregate_column_name
    index_hint = table_config.source_index_hint
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    all_chunks: list[pl.DataFrame] = []

    conn = oracledb.connect(**connect_params)
    try:
        # P2-2: Get distinct dates to skip empty days
        active_dates = _get_distinct_dates(
            conn, schema, table, date_col, start_date, end_date,
        )

        if not active_dates:
            logger.info(
                "No data found in %s.%s for date range [%s to %s]",
                schema, table, start_date, end_date,
            )
            df = pl.DataFrame()
            csv_path = Path(output_dir) / f"{table_config.source_name}_{table}.csv"
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            csv_path.write_text("")
            return df, csv_path

        skipped_days = 0
        current_date = start_date

        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)

            # P2-2: Skip dates with no data
            if current_date not in active_dates:
                skipped_days += 1
                current_date = next_date
                continue

            # O-1: Range predicates for index eligibility (replaces TRUNC on column).
            query = (
                f"SELECT /*+ INDEX({table} {index_hint}) */ * "
                f"FROM {schema}.{table} "
                f"WHERE {date_col} >= :start_dt "
                f"AND {date_col} < :end_dt"
            )

            logger.debug("oracledb chunk: %s [%s, %s)", table, current_date, next_date)

            cursor = conn.cursor()
            cursor.execute(query, start_dt=current_date, end_dt=next_date)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            if rows:
                chunk_df = pl.DataFrame(
                    {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                )
                all_chunks.append(chunk_df)
                logger.debug("  chunk %s: %d rows", current_date, len(chunk_df))

            current_date = next_date

        if skipped_days > 0:
            logger.info(
                "P2-2: Skipped %d empty days for %s.%s",
                skipped_days, schema, table,
            )
    finally:
        conn.close()

    if all_chunks:
        # W-7: Validate schemas before multi-chunk concat — oracledb may return
        # different dtypes across date chunks (e.g., empty chunk → Null dtype).
        validate_schema_before_concat(
            all_chunks, f"oracledb multi-chunk extraction for {table}")
        df = pl.concat(all_chunks)
    else:
        df = pl.DataFrame()

    logger.info(
        "oracledb extracted %d rows from %s.%s [%s to %s]",
        len(df), schema, table, start_date, end_date,
    )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=True, source_is_oracle=True)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}.csv",
    )

    return df, csv_path


def extract_oracle_oracledb_full(
    table_config: TableConfig,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """P1-12: Full-scan extraction via oracledb with INDEX hint, no date range.

    For small tables with SourceIndexHint but no SourceAggregateColumnName.
    Avoids the misleading hardcoded date range (2000-01-01 to today).

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    _ensure_thick_mode()

    source = get_source(table_config.source_name)
    connect_params = source.oracledb_connect_params()

    index_hint = table_config.source_index_hint
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    query = (
        f"SELECT /*+ INDEX({table} {index_hint}) */ * "
        f"FROM {schema}.{table}"
    )

    logger.info("oracledb full-scan: %s (INDEX hint: %s)", table, index_hint)

    conn = oracledb.connect(**connect_params)
    try:
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        if rows:
            df = pl.DataFrame(
                {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            )
        else:
            df = pl.DataFrame()
    finally:
        conn.close()

    logger.info(
        "oracledb extracted %d rows from %s.%s (full scan)",
        len(df), schema, table,
    )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=True, source_is_oracle=True)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}.csv",
    )

    return df, csv_path


def extract_oracle_oracledb_day(
    table_config: TableConfig,
    target_date: date,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Extract a single day's data from Oracle via oracledb with INDEX hints.

    For large table per-day processing (P1-6). Processes one day at a time,
    keeping memory bounded. The orchestrator drives the day loop.

    P3-2: Uses TRUNC() for date boundary comparison.

    Args:
        table_config: Table configuration with source details.
        target_date: The specific date to extract (inclusive).
        output_dir: Directory for output CSV file.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    _ensure_thick_mode()

    source = get_source(table_config.source_name)
    connect_params = source.oracledb_connect_params()

    date_col = table_config.source_aggregate_column_name
    index_hint = table_config.source_index_hint
    schema = table_config.source_schema_name
    table = table_config.source_object_name
    next_date = target_date + timedelta(days=1)

    # O-1: Range predicates for index eligibility (replaces TRUNC on column).
    query = (
        f"SELECT /*+ INDEX({table} {index_hint}) */ * "
        f"FROM {schema}.{table} "
        f"WHERE {date_col} >= :start_dt "
        f"AND {date_col} < :end_dt"
    )

    logger.debug("oracledb single-day: %s [%s, %s)", table, target_date, next_date)

    conn = oracledb.connect(**connect_params)
    try:
        cursor = conn.cursor()
        cursor.execute(query, start_dt=target_date, end_dt=next_date)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        if rows:
            df = pl.DataFrame(
                {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            )
        else:
            df = pl.DataFrame()
    finally:
        conn.close()

    logger.info(
        "oracledb extracted %d rows from %s.%s for date %s",
        len(df), schema, table, target_date,
    )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}_{target_date}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=True, source_is_oracle=True)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}_{target_date}.csv",
    )

    return df, csv_path

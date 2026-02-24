"""Orchestrator for large tables (date-chunked — incremental extraction).

Data flow per table, per day:
  Source -> Windowed extract (single day) -> Polars DataFrame
  -> add _row_hash + _extracted_at
  -> Write BCP CSV
  -> Ensure stage/bronze tables exist
  -> Schema evolution: detect new/removed/changed columns (P0-2)
  -> Column sync: auto-populate UdmTablesColumnsList + discover PKs
  -> Windowed CDC (compare only within date window, P1-3/P1-4)
  -> Targeted SCD2 (PK-scoped Bronze read, P1-3)
  -> Checkpoint date as SUCCESS (P1-5)
  -> CSV cleanup
  -> Next day...

Architecture decisions:
  - Process one day at a time to bound memory (P2-4)
  - Per-day checkpoint enables resume after failure (P1-6)
  - Delete detection scoped to extraction window (P1-4)
  - Bronze read targeted to PKs in current day's data (P1-3)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import config
from extract.router import extract_windowed
from schema.column_sync import sync_columns
from observability.event_tracker import PipelineEventTracker
from orchestration.guards import run_daily_extraction_guard
from orchestration.pipeline_state import get_dates_to_process, save_checkpoint
from orchestration.pipeline_steps import cleanup_csvs, run_cdc_promotion, run_scd2_promotion
from schema.evolution import SchemaEvolutionError, SchemaEvolutionResult, evolve_schema, validate_source_schema
from schema.table_creator import ensure_bronze_table, ensure_bronze_point_in_time_index, ensure_bronze_unique_active_index, ensure_stage_table
from orchestration.table_lock import acquire_table_lock, keep_lock_alive, release_table_lock

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)

# Feature flags
USE_POLARS_CDC = True
USE_POLARS_SCD2 = True

# P2-6: Only disable/rebuild indexes if inserts exceed this fraction of existing rows.
# For a 3B-row table with 50K daily inserts, index rebuild is counterproductive.
INDEX_REBUILD_THRESHOLD = 0.10

# P1-13: Absolute ceiling for first runs (no historical baseline).
MAX_ROWS_PER_DAY = 10_000_000


def process_large_table(
    table_config: TableConfig,
    event_tracker: PipelineEventTracker,
    output_dir: str | Path | None = None,
    force: bool = False,
) -> bool:
    """Process a single large table through the per-day pipeline.

    Iterates over each date that needs processing (from pipeline_state),
    running: extract → CDC → SCD2 → checkpoint for each day.

    Args:
        table_config: Table configuration.
        event_tracker: Event tracker for PipelineEventLog.
        output_dir: Directory for temp CSV files.
        force: If True, reprocess all dates in the lookback window.

    Returns:
        True if all dates succeeded, False if any date failed.
    """
    if output_dir is None:
        output_dir = config.CSV_OUTPUT_DIR

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    table_name = table_config.source_object_name
    source_name = table_config.source_name
    date_column = table_config.source_aggregate_column_name

    if not date_column:
        logger.error(
            "No SourceAggregateColumnName for %s.%s — cannot process as large table",
            source_name, table_name,
        )
        return False

    # P1-2: Acquire table lock to prevent concurrent runs
    lock_conn = acquire_table_lock(source_name, table_name)
    if lock_conn is None:
        logger.warning(
            "Skipping %s.%s — another pipeline run holds the lock",
            source_name, table_name,
        )
        # OBS-3: Write SKIPPED event so lock contention is visible in
        # PipelineEventLog. Without this, skipped tables are indistinguishable
        # from tables that were never scheduled.
        with event_tracker.track("TABLE_TOTAL", table_config) as skip_event:
            skip_event.status = "SKIPPED"
            skip_event.event_detail = "Lock held by another run"
        return False

    try:
        return _process_large_table_locked(
            table_config, event_tracker, output_dir, force,
            table_name, source_name, date_column, lock_conn,
        )
    finally:
        release_table_lock(lock_conn, source_name, table_name)


def _process_large_table_locked(
    table_config: TableConfig,
    event_tracker: PipelineEventTracker,
    output_dir: Path,
    force: bool,
    table_name: str,
    source_name: str,
    date_column: str,
    lock_conn: object | None = None,
) -> bool:
    """Inner processing logic after lock is acquired."""
    # L-4: Check for NULL date column values in source (once per table, not per day)
    _check_null_date_column(table_config, source_name, table_name, date_column)

    # Determine dates to process
    dates = get_dates_to_process(table_config, event_tracker.batch_id)

    if not dates:
        logger.info("No dates to process for %s.%s — up to date", source_name, table_name)
        return True

    logger.info(
        "Processing %s.%s: %d dates from %s to %s",
        source_name, table_name, len(dates), dates[0], dates[-1],
    )

    all_succeeded = True
    tables_created = False

    try:
        with event_tracker.track("TABLE_TOTAL", table_config) as total_event:
            total_rows = 0
            days_succeeded = 0
            days_failed = 0

            for day_idx, target_date in enumerate(dates, 1):
                # P1-14: Lock heartbeat every 10 days to prevent connection timeout
                if lock_conn is not None and day_idx % 10 == 0:
                    if not keep_lock_alive(lock_conn, source_name, table_name):
                        logger.error(
                            "P1-14: Lock connection lost for %s.%s after %d days — "
                            "stopping to prevent concurrent run conflicts",
                            source_name, table_name, day_idx,
                        )
                        all_succeeded = False
                        break

                # P3-8: Memory pressure check before each day
                _check_memory_pressure(source_name, table_name, target_date)

                try:
                    day_rows = _process_single_day(
                        table_config,
                        event_tracker,
                        target_date,
                        date_column,
                        output_dir,
                        force,
                    )
                    total_rows += day_rows
                    days_succeeded += 1

                    # P2-8: Progress logging for backfills
                    if len(dates) > 5:
                        logger.info(
                            "Progress %s.%s: day %d/%d (%s), %d rows this day, %d total rows",
                            source_name, table_name, day_idx, len(dates),
                            target_date, day_rows, total_rows,
                        )

                    # Checkpoint on success
                    save_checkpoint(
                        table_name, source_name, target_date,
                        "SUCCESS", event_tracker.batch_id,
                    )

                except SchemaEvolutionError:
                    logger.exception(
                        "Schema evolution error for %s.%s on %s — stopping table",
                        source_name, table_name, target_date,
                    )
                    save_checkpoint(
                        table_name, source_name, target_date,
                        "FAILED", event_tracker.batch_id,
                        failed_step="SCHEMA_EVOLUTION",
                    )
                    all_succeeded = False
                    break  # Schema errors affect all dates — stop

                except Exception:
                    logger.exception(
                        "Failed to process %s.%s for date %s — continuing to next date",
                        source_name, table_name, target_date,
                    )
                    save_checkpoint(
                        table_name, source_name, target_date,
                        "FAILED", event_tracker.batch_id,
                        failed_step="CDC_OR_SCD2",
                    )
                    days_failed += 1
                    all_succeeded = False
                    # Continue to next date — don't stop on transient failures

            total_event.rows_processed = total_rows
            if not all_succeeded:
                total_event.status = "FAILED"
                total_event.error_message = (
                    f"{days_failed} of {len(dates)} dates failed"
                )

            logger.info(
                "Large table %s.%s complete: days=%d, succeeded=%d, failed=%d, total_rows=%d",
                source_name, table_name, len(dates), days_succeeded, days_failed, total_rows,
            )

    except Exception:
        logger.exception("Failed to process large table %s.%s", source_name, table_name)
        return False

    return all_succeeded


def _process_single_day(
    table_config: TableConfig,
    event_tracker: PipelineEventTracker,
    target_date: date,
    date_column: str,
    output_dir: Path,
    force: bool,
) -> int:
    """Process a single day through extract → CDC → SCD2.

    Args:
        table_config: Table configuration.
        event_tracker: Event tracker.
        target_date: The date to process.
        date_column: Business date column name.
        output_dir: Directory for temp CSV files.
        force: Skip guards if True.

    Returns:
        Number of rows extracted for this day.

    Raises:
        SchemaEvolutionError: If schema drift has type changes.
        Exception: Any other processing error.
    """
    table_name = table_config.source_object_name
    source_name = table_config.source_name
    next_date = target_date + timedelta(days=1)

    # --- EXTRACT single day ---
    with event_tracker.track("EXTRACT", table_config) as extract_event:
        # OBS-2: Tag event with target date for per-day diagnostic filtering.
        extract_event.event_detail = str(target_date)
        df, csv_path = extract_windowed(table_config, target_date, output_dir)
        extract_event.rows_processed = len(df)
        # W-12: Release over-allocated memory from extraction operations.
        if len(df) > 100_000:
            df.shrink_to_fit(in_place=True)

    # P3-11: Intermediate checkpoint — extraction succeeded, CDC pending.
    # On retry, this date will be reprocessed (detect_gaps only treats SUCCESS
    # as complete), but the checkpoint distinguishes "extraction OK, CDC failed"
    # from "extraction itself failed" for debugging.
    if len(df) > 0:
        save_checkpoint(
            table_name, source_name, target_date,
            "EXTRACTED", event_tracker.batch_id,
        )

    if len(df) == 0:
        logger.debug(
            "Empty extraction for %s.%s on %s — skipping CDC/SCD2",
            source_name, table_name, target_date,
        )
        # L-6: Save SUCCESS checkpoint for empty days inside _process_single_day
        # to close the timing gap between returning and the caller's checkpoint.
        save_checkpoint(
            table_name, source_name, target_date,
            "SUCCESS", event_tracker.batch_id,
        )
        return 0

    # --- P1-13: Per-day extraction guard ---
    if not force:
        guard_ok = run_daily_extraction_guard(
            table_config, len(df), event_tracker.batch_id, target_date,
            drop_threshold=0,
            first_run_ceiling=MAX_ROWS_PER_DAY,
        )
        if not guard_ok:
            raise RuntimeError(
                f"P1-13: Day {target_date} extraction ({len(df)} rows) exceeds guard "
                f"threshold. Use --force to override."
            )

    # --- E-11: PRE-PROCESSING SCHEMA VALIDATION ---
    schema_warnings = validate_source_schema(table_config, df)
    if schema_warnings:
        has_missing = any("MISSING" in w for w in schema_warnings)
        if has_missing:
            raise RuntimeError(
                f"E-11: Schema validation failed for {table_config.source_name}."
                f"{table_config.source_object_name}: {'; '.join(schema_warnings)}"
            )

    # --- ENSURE TABLES (only needed on first day) ---
    stage_created = ensure_stage_table(table_config, df)
    bronze_created = ensure_bronze_table(table_config, df)

    # --- SCHEMA EVOLUTION (P0-2) ---
    # B-3: Capture result to condition E-12 warning during schema migration runs.
    schema_result: SchemaEvolutionResult | None = None
    if not stage_created and not bronze_created:
        schema_result = evolve_schema(table_config, df)
        if schema_result.hash_affecting_change:
            logger.warning(
                "B-3: Schema migration detected for %s.%s — %d column(s) added: %s. "
                "All row hashes will change (new column included in hash). "
                "Expect mass CDC updates — this is correct behavior, not a bug.",
                source_name, table_name,
                len(schema_result.columns_added), schema_result.columns_added,
            )

    # --- COLUMN SYNC ---
    if stage_created or bronze_created:
        sync_columns(table_config)

    # SCD-1: Ensure unique filtered index on Bronze to prevent duplicate
    # active rows from INSERT-first retry. Created once; idempotent.
    if table_config.pk_columns:
        ensure_bronze_unique_active_index(table_config, table_config.pk_columns)
        # V-9: Ensure point-in-time lookup index for historical queries.
        ensure_bronze_point_in_time_index(table_config, table_config.pk_columns)

    # --- WINDOWED CDC (P1-3/P1-4) ---
    cdc_result = run_cdc_promotion(
        table_config, df, event_tracker, schema_result, output_dir,
        windowed=True,
        date_column=date_column,
        target_date=target_date,
        next_date=next_date,
    )

    # P2-10: Release extraction DataFrame before SCD2 to reduce peak memory.
    # At this point, CDC has captured everything needed in cdc_result.df_current
    # and cdc_result.deleted_pks. The original extraction df is no longer needed.
    extracted_row_count = len(df)
    del df
    _check_memory_pressure(source_name, table_name, target_date)

    # --- TARGETED SCD2 (P1-3) ---
    run_scd2_promotion(
        table_config, cdc_result, event_tracker, output_dir,
        targeted=True,
        target_date=target_date,
        index_rebuild_threshold=INDEX_REBUILD_THRESHOLD,
    )

    # --- CSV CLEANUP for this day ---
    with event_tracker.track("CSV_CLEANUP", table_config) as cleanup_event:
        cleanup_event.event_detail = str(target_date)  # OBS-2
        cleaned = cleanup_csvs(output_dir, table_config)
        cleanup_event.rows_processed = cleaned

    return extracted_row_count


def _check_null_date_column(
    table_config: TableConfig,
    source_name: str,
    table_name: str,
    date_column: str,
) -> None:
    """L-4: Check for NULL values in the date column that would be silently excluded.

    Windowed extraction uses WHERE date_column >= X AND date_column < Y, which
    excludes rows where date_column IS NULL. These rows never enter the pipeline.
    """
    try:
        from sources import get_source, SourceType

        source = get_source(source_name)
        if source.source_type == SourceType.ORACLE:
            conn_uri = source.connectorx_uri()
        else:
            conn_uri = source.connectorx_uri()

        import connectorx as cx
        query = (
            f"SELECT COUNT(*) AS null_count FROM {table_config.source_full_table_name} "
            f"WHERE [{date_column}] IS NULL"
        ) if source.source_type != SourceType.ORACLE else (
            f"SELECT COUNT(*) AS null_count FROM {table_config.source_full_table_name} "
            f"WHERE {date_column} IS NULL"
        )

        df = cx.read_sql(conn_uri, query, return_type="polars")
        null_count = df[0, 0] if len(df) > 0 else 0

        if null_count and null_count > 0:
            logger.warning(
                "L-4: %s.%s has %d rows with NULL %s — these are excluded from "
                "windowed extraction. Consider: (a) fixing the source view, "
                "(b) using COALESCE in SourceAggregateColumnName, or "
                "(c) processing as a small table if row count permits.",
                source_name, table_name, null_count, date_column,
            )
    except Exception:
        logger.debug(
            "L-4: Could not check NULL date column count for %s.%s — continuing",
            source_name, table_name,
            exc_info=True,
        )


def _check_memory_pressure(source_name: str, table_name: str, target_date: date) -> None:
    """P3-8 + M-3: Check memory and FD usage before processing a day.

    Logs warnings and triggers GC if memory usage is high.
    M-3: Monitors open file descriptors to detect ConnectorX FD leaks.
    """
    try:
        import gc
        import psutil
        proc = psutil.Process()
        mem = psutil.virtual_memory()

        if mem.percent > 95:
            logger.error(
                "P3-8: CRITICAL memory pressure (%.1f%%) before %s.%s date %s. "
                "Triggering GC and pausing is recommended.",
                mem.percent, source_name, table_name, target_date,
            )
            gc.collect()
        elif mem.percent > 85:
            logger.warning(
                "P3-8: High memory usage (%.1f%%) before %s.%s date %s. "
                "Triggering garbage collection.",
                mem.percent, source_name, table_name, target_date,
            )
            gc.collect()

        # M-3: Monitor file descriptor count to detect ConnectorX FD leaks.
        # Default RHEL ulimit -n is 1024. ConnectorX opens N+1 FDs per read_sql.
        try:
            fd_count = proc.num_fds()
            if fd_count > 800:
                logger.error(
                    "M-3: HIGH FD count (%d) for %s.%s before date %s — "
                    "approaching default ulimit (1024). Set ulimit -n 65536. "
                    "ConnectorX may be leaking connections.",
                    fd_count, source_name, table_name, target_date,
                )
            elif fd_count > 500:
                logger.warning(
                    "M-3: Elevated FD count (%d) for %s.%s before date %s. "
                    "Monitor for ConnectorX connection leaks.",
                    fd_count, source_name, table_name, target_date,
                )
        except AttributeError:
            pass  # num_fds() not available on all platforms

    except ImportError:
        pass  # psutil not installed — skip memory/FD check




"""Orchestrator for small tables (no date column — full extract each run).

Data flow per table:
  Source -> ConnectorX/oracledb extract -> Polars DataFrame
  -> add _row_hash + _extracted_at
  -> Write BCP CSV
  -> Ensure stage/bronze tables exist
  -> Schema evolution: detect new/removed/changed columns (P0-2)
  -> Column sync: auto-populate UdmTablesColumnsList + discover PKs from source
  -> Empty extraction guard: skip CDC if row count drops >90% (P1-1)
  -> CDC promotion (hash comparison, I/U/D with NULL PK filter)
  -> SCD2 promotion (2-step UPDATE+INSERT)
  -> CSV cleanup
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import config
from extract.router import extract_full
from schema.column_sync import sync_columns
from observability.event_tracker import PipelineEventTracker
from orchestration.guards import run_extraction_guard
from orchestration.pipeline_steps import cleanup_csvs, run_cdc_promotion, run_scd2_promotion
from schema.evolution import SchemaEvolutionError, SchemaEvolutionResult, evolve_schema, validate_source_schema
from schema.table_creator import ensure_bronze_table, ensure_bronze_point_in_time_index, ensure_bronze_unique_active_index, ensure_stage_table
from orchestration.table_lock import acquire_table_lock, release_table_lock

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)

# Feature flags
USE_POLARS_CDC = True
USE_POLARS_SCD2 = True

# P3-1: Small table size guard threshold.
# If extraction exceeds this row count, log WARNING suggesting reclassification.
SMALL_TABLE_SIZE_THRESHOLD = 10_000_000

# S-2: Absolute ceiling on first-run extraction for small tables.
# Prevents OOM when a large table is misconfigured as small (no SourceAggregateColumnName).
FIRST_RUN_MAX_ROWS = 50_000_000


def process_small_table(
    table_config: TableConfig,
    event_tracker: PipelineEventTracker,
    output_dir: str | Path | None = None,
    force: bool = False,
    refresh_pks: bool = False,
) -> bool:
    """Process a single small table through the full pipeline.

    Args:
        table_config: Table configuration.
        event_tracker: Event tracker for PipelineEventLog.
        output_dir: Directory for temp CSV files. Defaults to config.CSV_OUTPUT_DIR.
        force: If True, skip empty extraction guard (P1-1). For intentional reloads.
        refresh_pks: P1-10 — If True, re-discover PKs even if columns already populated.

    Returns:
        True if the pipeline succeeded, False if it failed.
    """
    if output_dir is None:
        output_dir = config.CSV_OUTPUT_DIR

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    table_name = table_config.source_object_name
    source_name = table_config.source_name

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
        with event_tracker.track("TABLE_TOTAL", table_config) as total_event:
            # --- EXTRACT ---
            with event_tracker.track("EXTRACT", table_config) as extract_event:
                df, csv_path = extract_full(table_config, output_dir)
                extract_event.rows_processed = len(df)

            if len(df) == 0:
                logger.warning("Empty extraction for %s.%s — skipping", source_name, table_name)
                total_event.rows_processed = 0
                return True

            # P3-1: Size guard — warn if "small" table is growing large
            if len(df) > SMALL_TABLE_SIZE_THRESHOLD:
                logger.warning(
                    "P3-1 SIZE GUARD: %s.%s extracted %d rows (threshold=%d). "
                    "Consider reclassifying as a large table in UdmTablesList by setting "
                    "SourceAggregateColumnName to enable date-windowed extraction.",
                    source_name, table_name, len(df), SMALL_TABLE_SIZE_THRESHOLD,
                )

            # ST-1 + P2-12: Memory-based size guard — estimate memory for CDC comparison.
            # CDC loads both fresh + existing in memory simultaneously.
            if not _check_small_table_memory(df, source_name, table_name):
                total_event.rows_processed = len(df)
                total_event.status = "FAILED"
                total_event.error_message = (
                    f"ST-1: Estimated CDC peak memory exceeds hard ceiling "
                    f"({_MEM_HARD_CEILING_GB} GB). Reclassify as large table."
                )
                return False

            # --- E-11: PRE-PROCESSING SCHEMA VALIDATION ---
            # Catches column renames/drops before they propagate to CDC/SCD2.
            schema_warnings = validate_source_schema(table_config, df)
            if schema_warnings:
                # Missing columns are ERROR-level — skip table to prevent corruption
                has_missing = any("MISSING" in w for w in schema_warnings)
                if has_missing:
                    total_event.rows_processed = len(df)
                    total_event.status = "FAILED"
                    total_event.error_message = "; ".join(schema_warnings)
                    return False

            # --- ENSURE TABLES ---
            stage_created = ensure_stage_table(table_config, df)
            bronze_created = ensure_bronze_table(table_config, df)
            total_event.table_created = stage_created or bronze_created

            # --- SCHEMA EVOLUTION (P0-2) ---
            # Detect new/removed/changed columns. Runs on every run (not just first).
            # Raises SchemaEvolutionError on type changes (cannot auto-resolve).
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
            # Auto-populate UdmTablesColumnsList + discover PKs from source.
            # Runs once per table (skips if already populated). Reloads columns
            # into table_config so pk_columns is available for CDC/SCD2.
            if stage_created or bronze_created:
                sync_columns(table_config, refresh_pks=refresh_pks)
            elif refresh_pks:
                # P1-10: Re-discover PKs even if columns already populated
                sync_columns(table_config, refresh_pks=True)

            # SCD-1: Ensure unique filtered index on Bronze to prevent duplicate
            # active rows from INSERT-first retry. Created once; idempotent.
            if table_config.pk_columns:
                ensure_bronze_unique_active_index(table_config, table_config.pk_columns)
                # V-9: Ensure point-in-time lookup index for historical queries.
                ensure_bronze_point_in_time_index(table_config, table_config.pk_columns)

            # --- P1-1: EMPTY EXTRACTION GUARD ---
            if not force and not stage_created:
                guard_ok = run_extraction_guard(
                    table_config, len(df), event_tracker.batch_id,
                    first_run_ceiling=FIRST_RUN_MAX_ROWS,
                )
                if not guard_ok:
                    total_event.rows_processed = len(df)
                    total_event.status = "FAILED"
                    total_event.error_message = (
                        f"Extraction row count dropped >90% vs previous run. "
                        f"Fresh={len(df)}. Use --force to override."
                    )
                    return False

            # OBS-1: BCP_LOAD event removed — it wrapped an empty block and tracked
            # no actual BCP work. BCP timing is captured within CDC_PROMOTION
            # (staging table loads) and SCD2_PROMOTION (Bronze INSERT/UPDATE loads).

            # --- CDC PROMOTION ---
            cdc_result = run_cdc_promotion(
                table_config, df, event_tracker, schema_result, output_dir,
            )

            # --- SCD2 PROMOTION ---
            run_scd2_promotion(
                table_config, cdc_result, event_tracker, output_dir,
            )

            # --- CSV CLEANUP ---
            with event_tracker.track("CSV_CLEANUP", table_config) as cleanup_event:
                cleaned = cleanup_csvs(output_dir, table_config)
                cleanup_event.rows_processed = cleaned

            total_event.rows_processed = len(df)

        logger.info("Successfully processed %s.%s", source_name, table_name)
        return True

    except SchemaEvolutionError:
        logger.exception(
            "Schema evolution error for %s.%s — skipping table", source_name, table_name
        )
        return False
    except Exception:
        logger.exception("Failed to process %s.%s", source_name, table_name)
        return False
    finally:
        release_table_lock(lock_conn, source_name, table_name)


# ---------------------------------------------------------------------------
# P2-12: Memory-based size guard
# ---------------------------------------------------------------------------

# P2-12: Threshold in GB: warn if estimated CDC memory exceeds this.
_MEM_WARN_THRESHOLD_GB = 8.0
# ST-1: Hard ceiling in GB: ERROR and skip if estimated peak memory exceeds this.
# Anti-join peak = ~3× DataFrame size (both frames + hash table).
_MEM_HARD_CEILING_GB = 20.0


def _check_small_table_memory(
    df: pl.DataFrame,
    source_name: str,
    table_name: str,
) -> bool:
    """P2-12 + ST-1: Check estimated CDC comparison memory.

    CDC requires both the fresh extraction and the full Stage read in
    memory simultaneously. Anti-join peaks at ~3× DataFrame size.

    Returns:
        True if safe to proceed, False if memory would exceed hard ceiling.
    """
    estimated_bytes = df.estimated_size("b")
    # Anti-join peak = ~3× DataFrame size (fresh + existing + hash table)
    estimated_peak_gb = (estimated_bytes * 3) / (1024 ** 3)

    if estimated_peak_gb > _MEM_HARD_CEILING_GB:
        logger.error(
            "ST-1 MEMORY GUARD: %s.%s estimated CDC peak memory %.1f GB "
            "(hard ceiling=%.1f GB). %d rows × %d cols. "
            "This table MUST be reclassified as a large table by setting "
            "SourceAggregateColumnName in UdmTablesList. Skipping.",
            source_name, table_name, estimated_peak_gb,
            _MEM_HARD_CEILING_GB, len(df), len(df.columns),
        )
        return False

    if estimated_peak_gb > _MEM_WARN_THRESHOLD_GB:
        logger.warning(
            "P2-12 MEMORY GUARD: %s.%s estimated CDC peak memory %.1f GB "
            "(warn threshold=%.1f GB). %d rows × %d cols. "
            "Consider reclassifying as large table or reducing --workers.",
            source_name, table_name, estimated_peak_gb,
            _MEM_WARN_THRESHOLD_GB, len(df), len(df.columns),
        )

    return True


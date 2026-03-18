"""Orchestrator for network drive metadata scanning (os.walk → CDC → SCD2).

Mirrors process_file_table() (orchestration/file_tables.py) exactly,
replacing file reading with drive scanning. All downstream pipeline
functions (CDC, SCD2, schema evolution, column sync) work unchanged
because DriveConfig duck-types the TableConfig interface.

Data flow per drive:
  Network drive mount → os.walk() + os.stat() → Polars DataFrame
  → add _row_hash + _extracted_at
  → Write BCP CSV
  → Ensure stage/bronze tables exist
  → Schema evolution (P0-2)
  → Column sync with explicit PKs (drive_name, full_file_path)
  → Empty extraction guard (P1-1) — critical for detecting mount failures
  → CDC promotion (hash comparison: new/deleted/changed files)
  → SCD2 promotion (historical tracking of file metadata changes)
  → CSV cleanup
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import config
from data_load.schema_utils import clear_column_metadata_cache
from extract.drive_scanner import scan_drive
from schema.column_sync import sync_columns
from observability.event_tracker import PipelineEventTracker
from orchestration.guards import run_extraction_guard
from orchestration.pipeline_steps import cleanup_csvs, run_cdc_promotion, run_scd2_promotion
from schema.evolution import SchemaEvolutionError, SchemaEvolutionResult, evolve_schema
from schema.table_creator import (
    ensure_bronze_table,
    ensure_bronze_point_in_time_index,
    ensure_bronze_unique_active_index,
    ensure_stage_table,
)
from orchestration.table_lock import acquire_table_lock, release_table_lock

if TYPE_CHECKING:
    import polars as pl
    from orchestration.drive_config import DriveConfig

logger = logging.getLogger(__name__)

# Drives with millions of files can be very large — raise ceiling
FIRST_RUN_MAX_ROWS = 500_000_000

# P2-12: Memory guard thresholds
_MEM_WARN_THRESHOLD_GB = 8.0
_MEM_HARD_CEILING_GB = 20.0


def process_drive_table(
    drive_config: DriveConfig,
    event_tracker: PipelineEventTracker,
    output_dir: str | Path | None = None,
    force: bool = False,
) -> bool:
    """Process a single network drive through the full metadata scan pipeline.

    Args:
        drive_config: Drive configuration (duck-types TableConfig).
        event_tracker: Event tracker for PipelineEventLog.
        output_dir: Directory for temp CSV files. Defaults to config.CSV_OUTPUT_DIR.
        force: If True, skip empty extraction guard (P1-1).

    Returns:
        True if the pipeline succeeded, False if it failed.
    """
    if output_dir is None:
        output_dir = config.CSV_OUTPUT_DIR

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    table_name = drive_config.table_name
    source_name = drive_config.source_name
    drive_name = drive_config.drive_name

    # M-5: Clear INFORMATION_SCHEMA cache
    clear_column_metadata_cache()

    # P1-2: Acquire table lock
    lock_conn = acquire_table_lock(source_name, table_name)
    if lock_conn is None:
        logger.warning(
            "Skipping %s.%s (%s) — another pipeline run holds the lock",
            source_name, table_name, drive_name,
        )
        with event_tracker.track("TABLE_TOTAL", drive_config) as skip_event:
            skip_event.status = "SKIPPED"
            skip_event.event_detail = "Lock held by another run"
        return False

    try:
        with event_tracker.track("TABLE_TOTAL", drive_config) as total_event:
            # --- EXTRACT (SCAN) ---
            with event_tracker.track("EXTRACT", drive_config) as extract_event:
                df, csv_path = scan_drive(drive_config, output_dir)
                extract_event.rows_processed = len(df)
                extract_event.event_detail = f"{drive_name}: {drive_config.mount_path}"

            if len(df) == 0:
                logger.warning(
                    "Empty scan for %s (%s) — mount may be unavailable",
                    drive_name, drive_config.mount_path,
                )
                total_event.rows_processed = 0
                return True

            logger.info(
                "Scanned %s: %d files found", drive_name, len(df),
            )

            # Memory guard
            if not _check_drive_memory(df, source_name, table_name, drive_name):
                total_event.rows_processed = len(df)
                total_event.status = "FAILED"
                total_event.error_message = (
                    f"Estimated CDC peak memory exceeds ceiling "
                    f"({_MEM_HARD_CEILING_GB} GB). Drive has too many files."
                )
                return False

            # --- ENSURE TABLES ---
            stage_created = ensure_stage_table(drive_config, df)
            bronze_created = ensure_bronze_table(drive_config, df)
            total_event.table_created = stage_created or bronze_created

            # --- SCHEMA EVOLUTION ---
            schema_result: SchemaEvolutionResult | None = None
            if not stage_created and not bronze_created:
                schema_result = evolve_schema(drive_config, df)
                if schema_result.hash_affecting_change:
                    logger.warning(
                        "B-3: Schema migration for %s.%s — %d column(s) added: %s.",
                        source_name, table_name,
                        len(schema_result.columns_added), schema_result.columns_added,
                    )

            # --- COLUMN SYNC ---
            if stage_created or bronze_created:
                sync_columns(
                    drive_config,
                    file_pk_columns=drive_config.pk_column_names or None,
                )

            # Ensure Bronze indexes
            if drive_config.pk_columns:
                ensure_bronze_unique_active_index(drive_config, drive_config.pk_columns)
                ensure_bronze_point_in_time_index(drive_config, drive_config.pk_columns)

            # --- EXTRACTION GUARD ---
            # Critical for drive scanner: if mount drops, scan returns 0 rows.
            # Without guard, CDC would mark every file as deleted.
            if not force and not stage_created:
                guard_ok = run_extraction_guard(
                    drive_config, len(df), event_tracker.batch_id,
                    first_run_ceiling=FIRST_RUN_MAX_ROWS,
                )
                if not guard_ok:
                    total_event.rows_processed = len(df)
                    total_event.status = "FAILED"
                    total_event.error_message = (
                        f"File count dropped >90% vs previous run. "
                        f"Current={len(df)}. Mount may be offline. "
                        f"Use --force to override."
                    )
                    return False

            # --- CDC PROMOTION ---
            cdc_result = run_cdc_promotion(
                drive_config, df, event_tracker, schema_result, output_dir,
            )

            # --- SCD2 PROMOTION ---
            run_scd2_promotion(
                drive_config, cdc_result, event_tracker, output_dir,
            )

            # --- CSV CLEANUP ---
            with event_tracker.track("CSV_CLEANUP", drive_config) as cleanup_event:
                cleaned = cleanup_csvs(output_dir, drive_config)
                cleanup_event.rows_processed = cleaned

            total_event.rows_processed = len(df)

        logger.info(
            "Successfully scanned %s.%s (%s): %d files",
            source_name, table_name, drive_name, len(df),
        )
        return True

    except SchemaEvolutionError:
        logger.exception(
            "Schema evolution error for %s.%s — skipping", source_name, table_name,
        )
        return False
    except (FileNotFoundError, PermissionError) as exc:
        logger.error(
            "Drive scan error for %s (%s): %s", drive_name, drive_config.mount_path, exc,
        )
        return False
    except Exception:
        logger.exception("Failed to scan %s.%s (%s)", source_name, table_name, drive_name)
        return False
    finally:
        release_table_lock(lock_conn, source_name, table_name)


def _check_drive_memory(
    df: pl.DataFrame,
    source_name: str,
    table_name: str,
    drive_name: str,
) -> bool:
    """Check estimated CDC comparison memory for a drive scan result."""
    estimated_bytes = df.estimated_size("b")
    estimated_peak_gb = (estimated_bytes * 3) / (1024 ** 3)

    if estimated_peak_gb > _MEM_HARD_CEILING_GB:
        logger.error(
            "MEMORY GUARD: %s.%s (%s) estimated CDC peak %.1f GB "
            "(ceiling=%.1f GB). %d files. Skipping.",
            source_name, table_name, drive_name, estimated_peak_gb,
            _MEM_HARD_CEILING_GB, len(df),
        )
        return False

    if estimated_peak_gb > _MEM_WARN_THRESHOLD_GB:
        logger.warning(
            "MEMORY GUARD: %s.%s (%s) estimated CDC peak %.1f GB "
            "(warn=%.1f GB). %d files.",
            source_name, table_name, drive_name, estimated_peak_gb,
            _MEM_WARN_THRESHOLD_GB, len(df),
        )

    return True

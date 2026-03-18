"""Orchestrator for file-based tables (Excel/CSV → CDC → SCD2).

Mirrors process_small_table() (orchestration/small_tables.py) exactly,
replacing database extraction with file extraction. All downstream pipeline
functions (CDC, SCD2, schema evolution, column sync) work unchanged because
FileConfig duck-types the TableConfig interface.

Data flow per file:
  File (Excel/CSV) → Polars DataFrame
  → add _row_hash + _extracted_at
  → Write BCP CSV
  → Ensure stage/bronze tables exist
  → Schema evolution (P0-2)
  → Column sync with explicit PKs from FileExtract
  → Empty extraction guard (P1-1)
  → CDC promotion (hash comparison, I/U/D)
  → SCD2 promotion (2-step UPDATE+INSERT)
  → CSV cleanup
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import config
from data_load.schema_utils import clear_column_metadata_cache
from extract.file_extractor import extract_file
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
    from orchestration.file_config import FileConfig

logger = logging.getLogger(__name__)

# Reuse same thresholds as small_tables.py
SMALL_TABLE_SIZE_THRESHOLD = 100_000_000
FIRST_RUN_MAX_ROWS = 100_000_000

# P2-12: Memory-based size guard thresholds (same as small_tables.py)
_MEM_WARN_THRESHOLD_GB = 8.0
_MEM_HARD_CEILING_GB = 20.0


def process_file_table(
    file_config: FileConfig,
    event_tracker: PipelineEventTracker,
    output_dir: str | Path | None = None,
    force: bool = False,
) -> bool:
    """Process a single file-based table through the full pipeline.

    Args:
        file_config: File configuration (duck-types TableConfig).
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

    table_name = file_config.table_name
    source_name = file_config.source_name

    # M-5: Clear INFORMATION_SCHEMA cache at the start of each table
    clear_column_metadata_cache()

    # P1-2: Acquire table lock to prevent concurrent runs
    lock_conn = acquire_table_lock(source_name, table_name)
    if lock_conn is None:
        logger.warning(
            "Skipping %s.%s — another pipeline run holds the lock",
            source_name, table_name,
        )
        # OBS-3: Write SKIPPED event
        with event_tracker.track("TABLE_TOTAL", file_config) as skip_event:
            skip_event.status = "SKIPPED"
            skip_event.event_detail = "Lock held by another run"
        return False

    try:
        with event_tracker.track("TABLE_TOTAL", file_config) as total_event:
            # --- EXTRACT ---
            with event_tracker.track("EXTRACT", file_config) as extract_event:
                df, csv_path = extract_file(file_config, output_dir)
                extract_event.rows_processed = len(df)

            if len(df) == 0:
                logger.warning("Empty extraction for %s.%s — skipping", source_name, table_name)
                total_event.rows_processed = 0
                return True

            # P3-1: Size guard
            if len(df) > SMALL_TABLE_SIZE_THRESHOLD:
                logger.warning(
                    "P3-1 SIZE GUARD: %s.%s extracted %d rows (threshold=%d). "
                    "File source may be too large for full-extract pattern.",
                    source_name, table_name, len(df), SMALL_TABLE_SIZE_THRESHOLD,
                )

            # ST-1 + P2-12: Memory-based size guard
            if not _check_file_table_memory(df, source_name, table_name):
                total_event.rows_processed = len(df)
                total_event.status = "FAILED"
                total_event.error_message = (
                    f"ST-1: Estimated CDC peak memory exceeds hard ceiling "
                    f"({_MEM_HARD_CEILING_GB} GB). File too large for in-memory CDC."
                )
                return False

            # No E-11 source schema validation for file sources — files don't have
            # a prior source schema in UdmTablesColumnsList to compare against.

            # --- ENSURE TABLES ---
            stage_created = ensure_stage_table(file_config, df)
            bronze_created = ensure_bronze_table(file_config, df)
            total_event.table_created = stage_created or bronze_created

            # --- SCHEMA EVOLUTION (P0-2) ---
            schema_result: SchemaEvolutionResult | None = None
            if not stage_created and not bronze_created:
                schema_result = evolve_schema(file_config, df)
                if schema_result.hash_affecting_change:
                    logger.warning(
                        "B-3: Schema migration detected for %s.%s — %d column(s) added: %s. "
                        "All row hashes will change. Expect mass CDC updates.",
                        source_name, table_name,
                        len(schema_result.columns_added), schema_result.columns_added,
                    )

            # --- COLUMN SYNC ---
            # Pass file_pk_columns so sync_columns skips source DB PK discovery
            if stage_created or bronze_created:
                sync_columns(
                    file_config,
                    file_pk_columns=file_config.pk_column_names or None,
                )

            # SCD-1: Ensure unique filtered index on Bronze
            if file_config.pk_columns:
                ensure_bronze_unique_active_index(file_config, file_config.pk_columns)
                ensure_bronze_point_in_time_index(file_config, file_config.pk_columns)

            # --- P1-1: EMPTY EXTRACTION GUARD ---
            if not force and not stage_created:
                guard_ok = run_extraction_guard(
                    file_config, len(df), event_tracker.batch_id,
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

            # --- CDC PROMOTION ---
            cdc_result = run_cdc_promotion(
                file_config, df, event_tracker, schema_result, output_dir,
            )

            # --- SCD2 PROMOTION ---
            run_scd2_promotion(
                file_config, cdc_result, event_tracker, output_dir,
            )

            # --- CSV CLEANUP ---
            with event_tracker.track("CSV_CLEANUP", file_config) as cleanup_event:
                cleaned = cleanup_csvs(output_dir, file_config)
                cleanup_event.rows_processed = cleaned

            total_event.rows_processed = len(df)

        logger.info("Successfully processed file %s.%s", source_name, table_name)
        return True

    except SchemaEvolutionError:
        logger.exception(
            "Schema evolution error for %s.%s — skipping", source_name, table_name,
        )
        return False
    except (FileNotFoundError, PermissionError, ValueError) as exc:
        logger.error(
            "File extraction error for %s.%s: %s", source_name, table_name, exc,
        )
        return False
    except Exception:
        logger.exception("Failed to process file %s.%s", source_name, table_name)
        return False
    finally:
        release_table_lock(lock_conn, source_name, table_name)


# ---------------------------------------------------------------------------
# Memory guard (mirrors small_tables._check_small_table_memory)
# ---------------------------------------------------------------------------

def _check_file_table_memory(
    df: pl.DataFrame,
    source_name: str,
    table_name: str,
) -> bool:
    """P2-12 + ST-1: Check estimated CDC comparison memory for file tables.

    Returns:
        True if safe to proceed, False if memory would exceed hard ceiling.
    """
    estimated_bytes = df.estimated_size("b")
    estimated_peak_gb = (estimated_bytes * 3) / (1024 ** 3)

    if estimated_peak_gb > _MEM_HARD_CEILING_GB:
        logger.error(
            "ST-1 MEMORY GUARD: %s.%s estimated CDC peak memory %.1f GB "
            "(hard ceiling=%.1f GB). %d rows × %d cols. Skipping.",
            source_name, table_name, estimated_peak_gb,
            _MEM_HARD_CEILING_GB, len(df), len(df.columns),
        )
        return False

    if estimated_peak_gb > _MEM_WARN_THRESHOLD_GB:
        logger.warning(
            "P2-12 MEMORY GUARD: %s.%s estimated CDC peak memory %.1f GB "
            "(warn threshold=%.1f GB). %d rows × %d cols.",
            source_name, table_name, estimated_peak_gb,
            _MEM_WARN_THRESHOLD_GB, len(df), len(df.columns),
        )

    return True

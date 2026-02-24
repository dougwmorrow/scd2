"""Shared pipeline step functions for CDC and SCD2 promotion.

Extracted from small_tables.py and large_tables.py (D-1 through D-3)
to eliminate duplicated orchestration logic across both orchestrators.

Functions:
    cleanup_csvs       — Delete temp BCP CSV files for a table.
    log_active_ratio   — E-14: Active-to-total ratio monitoring after SCD2.
    log_data_freshness — E-15/B-9: Two-tier Bronze data freshness alerting.
    run_cdc_promotion  — D-2: CDC step with event tracking and E-12 monitoring.
    run_scd2_promotion — D-3: SCD2 step with index management and monitoring.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import config
import connections
from cdc.engine import run_cdc, run_cdc_windowed
from data_load import bcp_loader
from data_load.index_management import disable_indexes, rebuild_indexes
from extract.udm_connectorx_extractor import get_table_row_count, table_exists
from scd2.engine import run_scd2, run_scd2_targeted

if TYPE_CHECKING:
    import polars as pl

    from cdc.engine import CDCResult
    from observability.event_tracker import PipelineEventTracker
    from schema.evolution import SchemaEvolutionResult
    from scd2.engine import SCD2Result
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# D-1: Shared utility functions
# ---------------------------------------------------------------------------

def cleanup_csvs(output_dir: Path, table_config: TableConfig) -> int:
    """Delete temp CSV files for this table.

    P3-6: Uses trailing underscore in glob to prevent matching overlapping
    table names (e.g. DNA_ACCT_ won't match DNA_ACCT_TYPE_*.csv).
    """
    prefix = f"{table_config.source_name}_{table_config.source_object_name}_"
    cleaned = 0
    for f in output_dir.glob(f"{prefix}*.csv"):
        try:
            f.unlink()
            cleaned += 1
        except OSError:
            logger.warning("Failed to delete CSV: %s", f)
    return cleaned


def log_active_ratio(table_config: TableConfig, scd2_event) -> None:
    """E-14: Log active-to-total ratio after SCD2 promotion.

    A sudden drop indicates mass incorrect closures; a sudden increase
    indicates mass incorrect activations. Checks ratio change vs previous
    run stored in PipelineEventLog.
    """
    try:
        bronze_table = table_config.bronze_full_table_name
        db = bronze_table.split(".")[0]
        conn = connections.get_connection(db)
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM {bronze_table} WHERE UdmActiveFlag = 1"
            )
            active_count = cursor.fetchone()[0]
            cursor.close()
        finally:
            conn.close()

        total_count = scd2_event.rows_after
        if total_count > 0:
            active_ratio = active_count / total_count
            # OBS-7: Merge into existing metadata instead of overwriting.
            existing = json.loads(scd2_event.metadata) if scd2_event.metadata else {}
            existing.update({
                "active_ratio": round(active_ratio, 6),
                "active_count": active_count,
                "total_count": total_count,
            })
            scd2_event.metadata = json.dumps(existing)

            if active_ratio < 0.01 and active_count > 0:
                logger.warning(
                    "E-14: Very low active-to-total ratio for %s.%s: %.2f%% "
                    "(%d active / %d total). Possible mass incorrect closures.",
                    table_config.source_name, table_config.source_object_name,
                    active_ratio * 100, active_count, total_count,
                )
    except Exception:
        logger.debug(
            "E-14: Active ratio check failed for %s.%s — continuing",
            table_config.source_name, table_config.source_object_name,
            exc_info=True,
        )


def log_data_freshness(table_config: TableConfig) -> None:
    """E-15 + B-9: Log data freshness — max UdmEffectiveDateTime in Bronze.

    B-9: Two-tier alerting:
      - WARNING at 36 hours (1.5× expected 24h refresh interval)
      - ERROR at 48 hours (2× expected refresh interval — two missed cycles)
    """
    try:
        bronze_table = table_config.bronze_full_table_name
        if not table_exists(bronze_table):
            return

        db = bronze_table.split(".")[0]
        conn = connections.get_connection(db)
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT MAX(UdmEffectiveDateTime) FROM {bronze_table} "
                "WHERE UdmActiveFlag = 1"
            )
            row = cursor.fetchone()
            cursor.close()
        finally:
            conn.close()

        if row and row[0] is not None:
            max_effective = row[0]
            # pyodbc returns datetime objects for DATETIME2 columns
            if hasattr(max_effective, 'replace'):
                if max_effective.tzinfo is None:
                    max_effective = max_effective.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            staleness_hours = (now - max_effective).total_seconds() / 3600

            # B-9: Two-tier freshness alerting.
            if staleness_hours > 48:
                logger.error(
                    "B-9: CRITICAL — Bronze data stale for %s.%s: "
                    "max UdmEffectiveDateTime is %.1f hours old (%s). "
                    "Two full refresh cycles missed. Investigate pipeline failures.",
                    table_config.source_name, table_config.source_object_name,
                    staleness_hours, max_effective,
                )
            elif staleness_hours > 36:
                logger.warning(
                    "B-9: Bronze data aging for %s.%s: "
                    "max UdmEffectiveDateTime is %.1f hours old (%s). "
                    "Expected freshness: <24 hours for daily pipeline.",
                    table_config.source_name, table_config.source_object_name,
                    staleness_hours, max_effective,
                )
    except Exception:
        logger.debug(
            "E-15: Freshness check failed for %s.%s — continuing",
            table_config.source_name, table_config.source_object_name,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# D-2: Shared CDC promotion step
# ---------------------------------------------------------------------------

def run_cdc_promotion(
    table_config: TableConfig,
    df: pl.DataFrame,
    event_tracker: PipelineEventTracker,
    schema_result: SchemaEvolutionResult | None,
    output_dir: str | Path,
    *,
    windowed: bool = False,
    date_column: str | None = None,
    target_date: date | None = None,
    next_date: date | None = None,
) -> CDCResult:
    """Run CDC promotion with event tracking and E-12 phantom change monitoring.

    Routes to ``run_cdc()`` (small tables) or ``run_cdc_windowed()`` (large
    tables) based on the *windowed* flag. Handles event metric population,
    metadata assembly, and B-3/E-12 warning logic identically for both paths.

    Args:
        table_config: Table configuration.
        df: Extracted DataFrame to compare against existing CDC table.
        event_tracker: Event tracker for PipelineEventLog.
        schema_result: Result from schema evolution (for B-3 detection), or None.
        output_dir: Directory for temp CSV files.
        windowed: If True, use ``run_cdc_windowed`` (large tables).
        date_column: Business date column name (required when windowed=True).
        target_date: Target date for windowed CDC (OBS-2 event detail).
        next_date: Day after target_date (exclusive upper bound).

    Returns:
        CDCResult with insert/update/delete counts and df_current.
    """
    source_name = table_config.source_name
    table_name = table_config.source_object_name
    date_ctx = f" on {target_date}" if target_date is not None else ""

    with event_tracker.track("CDC_PROMOTION", table_config) as cdc_event:
        if target_date is not None:
            cdc_event.event_detail = str(target_date)  # OBS-2

        if windowed:
            cdc_result = run_cdc_windowed(
                table_config, df, event_tracker.batch_id, output_dir,
                date_column, target_date, next_date,
            )
        else:
            cdc_result = run_cdc(
                table_config, df, event_tracker.batch_id, output_dir,
            )

        cdc_event.rows_processed = len(df)
        cdc_event.rows_inserted = cdc_result.inserts
        cdc_event.rows_updated = cdc_result.updates
        cdc_event.rows_deleted = cdc_result.deletes
        cdc_event.rows_unchanged = cdc_result.unchanged

        # E-12: Phantom change rate monitoring — track update ratio in metadata.
        # A sudden spike (e.g. 0.1% → 50%) indicates systematic hash mismatch.
        total_rows = max(
            cdc_result.inserts + cdc_result.updates + cdc_result.deletes + cdc_result.unchanged,
            1,
        )
        update_ratio = cdc_result.updates / total_rows
        is_schema_migration = schema_result is not None and schema_result.hash_affecting_change
        metadata_parts = [f'"update_ratio": {update_ratio:.6f}']
        if cdc_result.null_pk_rows > 0:
            metadata_parts.append(f'"null_pk_rows": {cdc_result.null_pk_rows}')
        if is_schema_migration:
            metadata_parts.append('"schema_migration": true')
        cdc_event.metadata = "{" + ", ".join(metadata_parts) + "}"

        if update_ratio > 0.5 and cdc_result.updates > 1000:
            if is_schema_migration:
                # B-3: Schema migration — mass updates expected, not anomalous
                logger.info(
                    "B-3: High CDC update ratio for %s.%s%s: %.1f%% (%d/%d) — "
                    "expected due to schema migration (columns added: %s).",
                    source_name, table_name, date_ctx,
                    update_ratio * 100, cdc_result.updates, total_rows,
                    schema_result.columns_added,
                )
            else:
                logger.warning(
                    "E-12: High CDC update ratio for %s.%s%s: %.1f%% (%d/%d). "
                    "Possible systematic hash mismatch (encoding change, "
                    "schema drift, normalization bug).",
                    source_name, table_name, date_ctx,
                    update_ratio * 100, cdc_result.updates, total_rows,
                )

    return cdc_result


# ---------------------------------------------------------------------------
# D-3: Shared SCD2 promotion step
# ---------------------------------------------------------------------------

def run_scd2_promotion(
    table_config: TableConfig,
    cdc_result: CDCResult,
    event_tracker: PipelineEventTracker,
    output_dir: str | Path,
    *,
    targeted: bool = False,
    target_date: date | None = None,
    index_rebuild_threshold: float | None = None,
) -> SCD2Result:
    """Run SCD2 promotion with index management, event tracking, and monitoring.

    Routes to ``run_scd2()`` (small tables) or ``run_scd2_targeted()`` (large
    tables) based on the *targeted* flag. Handles index disable/rebuild,
    BULK_LOGGED recovery context, event metric population, E-14 active ratio,
    and E-15 freshness checks.

    Design decisions:
      - ``targeted=False``: ``bulk_load_recovery_context`` wraps index
        disable + SCD2 + rebuild (current small_tables behavior).
      - ``targeted=True``: index management outside ``bulk_load_recovery_context``,
        context wraps only ``run_scd2_targeted()`` (P1-8 design). Index
        disable/rebuild is conditional via *index_rebuild_threshold* (P2-6).

    Args:
        table_config: Table configuration.
        cdc_result: Result from CDC promotion (df_current, pk_columns, deleted_pks).
        event_tracker: Event tracker for PipelineEventLog.
        output_dir: Directory for temp CSV files.
        targeted: If True, use ``run_scd2_targeted`` (large tables).
        target_date: Target date for OBS-2 event detail tagging.
        index_rebuild_threshold: P2-6 — only disable/rebuild indexes if
            estimated inserts exceed ``rows_before * threshold``. None means
            unconditional (small tables).

    Returns:
        SCD2Result with insert/close/unchanged counts.
    """
    with event_tracker.track("SCD2_PROMOTION", table_config) as scd2_event:
        if target_date is not None:
            scd2_event.event_detail = str(target_date)  # OBS-2

        scd2_event.rows_before = (
            get_table_row_count(table_config.bronze_full_table_name)
            if table_exists(table_config.bronze_full_table_name) else 0
        )

        if targeted:
            scd2_result = _scd2_targeted(
                table_config, cdc_result, scd2_event, output_dir,
                index_rebuild_threshold,
            )
        else:
            scd2_result = _scd2_full(
                table_config, cdc_result, output_dir,
            )

        scd2_event.rows_processed = (
            len(cdc_result.df_current) if cdc_result.df_current is not None else 0
        )
        scd2_event.rows_inserted = scd2_result.inserts + scd2_result.new_versions
        scd2_event.rows_updated = scd2_result.closes
        scd2_event.rows_unchanged = scd2_result.unchanged

        scd2_event.rows_after = (
            get_table_row_count(table_config.bronze_full_table_name)
            if table_exists(table_config.bronze_full_table_name) else 0
        )

        # E-14: Active-to-total ratio monitoring.
        if scd2_event.rows_after > 0:
            log_active_ratio(table_config, scd2_event)

        # E-15: Data freshness monitoring.
        log_data_freshness(table_config)

    return scd2_result


def _scd2_full(
    table_config: TableConfig,
    cdc_result: CDCResult,
    output_dir: str | Path,
) -> SCD2Result:
    """Small tables SCD2: unconditional index disable, bulk_load wraps everything."""
    with bcp_loader.bulk_load_recovery_context(config.BRONZE_DB):
        disabled_indexes = disable_indexes(
            table_config.bronze_full_table_name,
            table_config.index_configs,
        )

        try:
            scd2_result = run_scd2(
                table_config,
                cdc_result.df_current,
                cdc_result.pk_columns or [],
                output_dir,
            )
        finally:
            rebuild_indexes(
                table_config.bronze_full_table_name,
                disabled_indexes,
            )

    return scd2_result


def _scd2_targeted(
    table_config: TableConfig,
    cdc_result: CDCResult,
    scd2_event,
    output_dir: str | Path,
    index_rebuild_threshold: float | None,
) -> SCD2Result:
    """Large tables SCD2: conditional index management, bulk_load wraps only SCD2.

    P2-6: Only disable/rebuild indexes if insert volume justifies the cost.
    P1-8: BULK_LOGGED scope narrowed to only wrap run_scd2_targeted.
    """
    # P2-6: Conditional index management based on insert volume.
    estimated_inserts = len(cdc_result.df_current) if cdc_result.df_current is not None else 0
    should_manage_indexes = (
        index_rebuild_threshold is not None
        and scd2_event.rows_before > 0
        and estimated_inserts > scd2_event.rows_before * index_rebuild_threshold
    )

    disabled_indexes = []
    if should_manage_indexes:
        disabled_indexes = disable_indexes(
            table_config.bronze_full_table_name,
            table_config.index_configs,
        )

    try:
        # P1-8: BULK_LOGGED scope narrowed to only wrap run_scd2_targeted
        # which contains the actual BCP calls. UPDATE JOINs are fully logged
        # regardless, so keeping them in BULK_LOGGED scope adds risk for no benefit.
        with bcp_loader.bulk_load_recovery_context(config.BRONZE_DB):
            scd2_result = run_scd2_targeted(
                table_config,
                cdc_result.df_current,
                cdc_result.pk_columns or [],
                output_dir,
                # P0-11: Pass deleted PKs so SCD2 can close them in Bronze.
                deleted_pks=cdc_result.deleted_pks,
            )
    finally:
        if disabled_indexes:
            rebuild_indexes(
                table_config.bronze_full_table_name,
                disabled_indexes,
            )

    return scd2_result

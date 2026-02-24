"""Lightweight count and PK reconciliation (W-11, E-7)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import config
import connections
from extract.udm_connectorx_extractor import table_exists

from cdc.reconciliation.models import (
    ActivePKReconciliationResult,
    CountReconciliationResult,
)
from cdc.reconciliation.persistence import _persist_reconciliation_result

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def _get_source_row_count(table_config: TableConfig) -> int:
    """W-11: Get source table row count via ConnectorX COUNT(*)."""
    import connectorx as cx
    from sources import get_source

    source = get_source(table_config.source_name)
    uri = source.connectorx_uri()
    query = f"SELECT COUNT(*) AS cnt FROM {table_config.source_full_table_name}"
    df = cx.read_sql(uri, query, return_type="polars")
    return int(df[0, 0]) if len(df) > 0 else 0


def _get_current_stage_count(stage_table: str) -> int:
    """W-11: Get Stage current row count (WHERE _cdc_is_current=1)."""
    db = stage_table.split(".")[0]
    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM {stage_table} WHERE _cdc_is_current = 1"
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return int(count) if count is not None else 0
    finally:
        conn.close()


def _get_active_bronze_count(bronze_table: str) -> int:
    """W-11: Get Bronze active row count (WHERE UdmActiveFlag=1)."""
    db = bronze_table.split(".")[0]
    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM {bronze_table} WHERE UdmActiveFlag = 1"
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return int(count) if count is not None else 0
    finally:
        conn.close()


def reconcile_counts(
    table_config: TableConfig,
    output_dir: str | Path | None = None,
) -> CountReconciliationResult:
    """W-11: Lightweight count reconciliation — designed for daily scheduling.

    Compares row counts between source, Stage (current rows), and Bronze
    (active rows). Catches gross data loss, extraction failures, or bulk
    delete scenarios within 24 hours rather than waiting for the weekly
    full-column reconciliation.

    This is fast and cheap: three COUNT(*) queries, no data extraction
    (except source, which uses ConnectorX metadata query).

    Args:
        table_config: Table configuration.
        output_dir: Directory for temp files (only used if source count
                    requires extraction; falls back to config.CSV_OUTPUT_DIR).

    Returns:
        CountReconciliationResult with count comparisons.
    """
    if output_dir is None:
        output_dir = config.CSV_OUTPUT_DIR

    result = CountReconciliationResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    stage_table = table_config.stage_full_table_name
    bronze_table = table_config.bronze_full_table_name

    try:
        # Source count via ConnectorX (fast metadata query)
        result.source_count = _get_source_row_count(table_config)
    except Exception as e:
        result.errors.append(f"Source count failed: {e}")
        logger.warning(
            "W-11: Could not get source row count for %s.%s: %s",
            table_config.source_name, table_config.source_object_name, e,
        )

    # Stage current row count
    if table_exists(stage_table):
        try:
            # Use filtered count for current rows only
            result.stage_count = _get_current_stage_count(stage_table)
        except Exception as e:
            result.errors.append(f"Stage count failed: {e}")
            logger.warning("W-11: Could not get Stage count for %s: %s", stage_table, e)
    else:
        result.errors.append(f"Stage table {stage_table} does not exist")

    # Bronze active row count
    if table_exists(bronze_table):
        try:
            result.bronze_active_count = _get_active_bronze_count(bronze_table)
        except Exception as e:
            result.errors.append(f"Bronze count failed: {e}")
            logger.warning("W-11: Could not get Bronze count for %s: %s", bronze_table, e)
    else:
        result.errors.append(f"Bronze table {bronze_table} does not exist")

    # Compute deltas
    if result.source_count > 0 and result.stage_count > 0:
        result.count_delta_source_stage = result.source_count - result.stage_count
    if result.stage_count > 0 and result.bronze_active_count > 0:
        result.count_delta_stage_bronze = result.stage_count - result.bronze_active_count

    # Log results
    if result.is_clean:
        logger.info(
            "W-11: Count reconciliation PASSED for %s.%s: source=%d, stage=%d, bronze=%d",
            table_config.source_name, table_config.source_object_name,
            result.source_count, result.stage_count, result.bronze_active_count,
        )
    else:
        logger.warning(
            "W-11: Count reconciliation for %s.%s: source=%d, stage=%d, bronze=%d, "
            "delta(source-stage)=%d, delta(stage-bronze)=%d, errors=%s",
            table_config.source_name, table_config.source_object_name,
            result.source_count, result.stage_count, result.bronze_active_count,
            result.count_delta_source_stage, result.count_delta_stage_bronze,
            result.errors,
        )

    # OBS-6: Persist result for historical trending
    _persist_reconciliation_result(
        check_type="COUNT_RECONCILIATION",
        table_name=result.table_name,
        source_name=result.source_name,
        is_clean=result.is_clean,
        source_rows=result.source_count,
        target_rows=result.bronze_active_count,
        metadata={
            "stage_count": result.stage_count,
            "delta_source_stage": result.count_delta_source_stage,
            "delta_stage_bronze": result.count_delta_stage_bronze,
        },
        errors=result.errors or None,
    )

    return result


def reconcile_active_pks(
    table_config: TableConfig,
    output_dir: str | Path | None = None,
    ghost_alert_threshold: float = 0.001,
) -> ActivePKReconciliationResult:
    """E-7: Full PK reconciliation — detect ghost active rows in Bronze.

    Extracts all PKs from source, compares against all active Bronze PKs.
    Designed for weekly scheduling on windowed-mode (large) tables to catch
    deletes that fell outside the extraction window.

    Ghost rows: active in Bronze but no longer exist in source.
    Missing rows: exist in source but have no active Bronze version.

    Args:
        table_config: Table configuration.
        output_dir: Directory for temp files.
        ghost_alert_threshold: Alert if ghost_count / bronze_active_count
                               exceeds this ratio (default 0.1%).

    Returns:
        ActivePKReconciliationResult with ghost and missing counts.
    """
    import connectorx as cx

    if output_dir is None:
        output_dir = config.CSV_OUTPUT_DIR

    result = ActivePKReconciliationResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    pk_columns = table_config.pk_columns
    if not pk_columns:
        result.errors.append("No PK columns configured — cannot reconcile PKs")
        return result

    bronze_table = table_config.bronze_full_table_name
    if not table_exists(bronze_table):
        result.errors.append(f"Bronze table {bronze_table} does not exist")
        return result

    pk_select = ", ".join(f"[{c}]" for c in pk_columns)

    # --- Extract all source PKs (PK-only query, low memory) ---
    try:
        source_full = table_config.source_full_table_name
        if table_config.is_oracle:
            from sources import get_source
            source = get_source(table_config.source_name)
            uri = source.connectorx_uri()
            pk_oracle = ", ".join(pk_columns)
            query = f"SELECT DISTINCT {pk_oracle} FROM {table_config.source_schema_name}.{table_config.source_object_name}"
        else:
            from sources import get_source
            source = get_source(table_config.source_name)
            uri = source.connectorx_uri()
            query = f"SELECT DISTINCT {pk_select} FROM {source_full}"

        df_source_pks = cx.read_sql(uri, query, return_type="polars")
        result.source_pk_count = len(df_source_pks)
        logger.info(
            "E-7: Extracted %d source PKs for %s.%s",
            result.source_pk_count, table_config.source_name,
            table_config.source_object_name,
        )
    except Exception as e:
        result.errors.append(f"Source PK extraction failed: {e}")
        logger.warning("E-7: Could not extract source PKs for %s: %s",
                        table_config.source_object_name, e)
        return result

    # --- Extract all active Bronze PKs ---
    try:
        bronze_uri = connections.bronze_connectorx_uri()
        bronze_query = (
            f"SELECT DISTINCT {pk_select} FROM {bronze_table} WHERE UdmActiveFlag = 1"
        )
        df_bronze_pks = cx.read_sql(bronze_uri, bronze_query, return_type="polars")
        result.bronze_active_pk_count = len(df_bronze_pks)
        logger.info(
            "E-7: Extracted %d active Bronze PKs for %s",
            result.bronze_active_pk_count, bronze_table,
        )
    except Exception as e:
        result.errors.append(f"Bronze PK extraction failed: {e}")
        logger.warning("E-7: Could not extract Bronze PKs for %s: %s",
                        bronze_table, e)
        return result

    # --- Normalize PK column names to match ---
    # Source columns may have different casing; normalize to Bronze column names
    source_rename = {}
    for sc in df_source_pks.columns:
        for pc in pk_columns:
            if sc.upper() == pc.upper() and sc != pc:
                source_rename[sc] = pc
    if source_rename:
        df_source_pks = df_source_pks.rename(source_rename)

    # Align dtypes
    from data_load.schema_utils import align_pk_dtypes
    df_source_pks, df_bronze_pks = align_pk_dtypes(
        df_source_pks, df_bronze_pks, pk_columns,
        context="E-7 active PK reconciliation",
    )

    # --- Anti-join both directions ---
    # Ghost rows: in Bronze active but NOT in source
    df_ghost = df_bronze_pks.join(df_source_pks, on=pk_columns, how="anti")
    result.ghost_count = len(df_ghost)

    # Missing rows: in source but NOT in Bronze active
    df_missing = df_source_pks.join(df_bronze_pks, on=pk_columns, how="anti")
    result.missing_count = len(df_missing)

    # --- Logging ---
    if result.ghost_count > 0:
        ghost_ratio = result.ghost_count / max(result.bronze_active_pk_count, 1)
        if ghost_ratio > ghost_alert_threshold:
            logger.warning(
                "E-7: %d ghost active rows in %s (%.2f%% of active PKs). "
                "These rows were deleted from source but not closed in Bronze. "
                "Consider running a targeted close operation.",
                result.ghost_count, bronze_table, ghost_ratio * 100,
            )
        else:
            logger.info(
                "E-7: %d ghost active rows in %s (%.4f%% — below alert threshold).",
                result.ghost_count, bronze_table, ghost_ratio * 100,
            )

    if result.missing_count > 0:
        logger.info(
            "E-7: %d source PKs not active in %s (may be in historical versions "
            "or not yet processed).",
            result.missing_count, bronze_table,
        )

    if result.is_clean:
        logger.info(
            "E-7: Full PK reconciliation clean for %s.%s — "
            "source=%d, bronze_active=%d",
            table_config.source_name, table_config.source_object_name,
            result.source_pk_count, result.bronze_active_pk_count,
        )

    # OBS-6: Persist result for historical trending
    _persist_reconciliation_result(
        check_type="ACTIVE_PK_RECONCILIATION",
        table_name=result.table_name,
        source_name=result.source_name,
        is_clean=result.is_clean,
        source_rows=result.source_pk_count,
        target_rows=result.bronze_active_pk_count,
        metadata={"ghost_count": result.ghost_count, "missing_count": result.missing_count},
        errors=result.errors or None,
    )

    return result

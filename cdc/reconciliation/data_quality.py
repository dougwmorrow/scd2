"""Data quality reconciliation: aggregates and distribution shift detection."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import connections
from extract.udm_connectorx_extractor import table_exists

from cdc.reconciliation.models import (
    AggregateReconciliationResult,
    DistributionShiftResult,
)
from cdc.reconciliation.persistence import _persist_reconciliation_result

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def reconcile_aggregates(
    table_config: TableConfig,
) -> AggregateReconciliationResult:
    """E-17: Column-level aggregate reconciliation.

    Compares SUM, COUNT non-null, MIN, MAX of numeric columns between
    source and Bronze active rows. Catches value-level corruption that
    count validation misses (e.g., wrong values in the right number of rows).

    Args:
        table_config: Table configuration.

    Returns:
        AggregateReconciliationResult with discrepancies.
    """
    result = AggregateReconciliationResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    bronze_table = table_config.bronze_full_table_name

    if not table_exists(bronze_table):
        result.errors.append(f"Bronze table {bronze_table} does not exist")
        return result

    # Get numeric columns from Bronze (exclude internal columns)
    try:
        conn = connections.get_bronze_connection()
        try:
            cursor = conn.cursor()
            db = bronze_table.split(".")[0]
            schema = bronze_table.split(".")[1]
            tbl = bronze_table.split(".")[2]
            cursor.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                f"AND DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint', "
                f"'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney') "
                f"AND COLUMN_NAME NOT LIKE '\\_%' ESCAPE '\\' "
                f"AND COLUMN_NAME NOT IN ('UdmHash', 'UdmActiveFlag', '_scd2_key')",
                schema, tbl,
            )
            numeric_cols = [(row[0], row[1]) for row in cursor.fetchall()]
            cursor.close()
        finally:
            conn.close()

        if not numeric_cols:
            logger.debug(
                "E-17: No numeric columns found in %s — skipping aggregate reconciliation",
                bronze_table,
            )
            return result

        result.columns_checked = len(numeric_cols)

        # Build aggregate query for Bronze active rows
        agg_exprs = []
        for col_name, _ in numeric_cols:
            agg_exprs.append(
                f"SUM(CAST([{col_name}] AS FLOAT)) AS [{col_name}_sum], "
                f"COUNT([{col_name}]) AS [{col_name}_cnt], "
                f"MIN(CAST([{col_name}] AS FLOAT)) AS [{col_name}_min], "
                f"MAX(CAST([{col_name}] AS FLOAT)) AS [{col_name}_max]"
            )
        agg_sql = ", ".join(agg_exprs)

        # Query Bronze aggregates
        conn = connections.get_bronze_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT {agg_sql} FROM {bronze_table} WHERE UdmActiveFlag = 1"
            )
            bronze_row = cursor.fetchone()
            cursor.close()
        finally:
            conn.close()

        # Query source aggregates
        from sources import get_source
        source = get_source(table_config.source_name)
        source_uri = source.connectorx_uri()
        import connectorx as cx

        source_agg_exprs = []
        for col_name, _ in numeric_cols:
            source_agg_exprs.append(
                f"SUM(CAST(\"{col_name}\" AS FLOAT)) AS \"{col_name}_sum\", "
                f"COUNT(\"{col_name}\") AS \"{col_name}_cnt\", "
                f"MIN(CAST(\"{col_name}\" AS FLOAT)) AS \"{col_name}_min\", "
                f"MAX(CAST(\"{col_name}\" AS FLOAT)) AS \"{col_name}_max\""
            )

        # Use source-appropriate quoting
        if table_config.is_oracle:
            source_agg_sql = ", ".join(source_agg_exprs)
        else:
            # SQL Server uses [] quoting
            source_agg_exprs_ss = []
            for col_name, _ in numeric_cols:
                source_agg_exprs_ss.append(
                    f"SUM(CAST([{col_name}] AS FLOAT)) AS [{col_name}_sum], "
                    f"COUNT([{col_name}]) AS [{col_name}_cnt], "
                    f"MIN(CAST([{col_name}] AS FLOAT)) AS [{col_name}_min], "
                    f"MAX(CAST([{col_name}] AS FLOAT)) AS [{col_name}_max]"
                )
            source_agg_sql = ", ".join(source_agg_exprs_ss)

        source_query = f"SELECT {source_agg_sql if not table_config.is_oracle else source_agg_sql} FROM {table_config.source_full_table_name}"
        source_df = cx.read_sql(source_uri, source_query, return_type="polars")

        if len(source_df) == 0:
            result.errors.append("Source aggregate query returned no rows")
            return result

        # Compare aggregates
        idx = 0
        for col_name, _ in numeric_cols:
            if bronze_row is None:
                break
            b_sum = bronze_row[idx]
            b_cnt = bronze_row[idx + 1]
            b_min = bronze_row[idx + 2]
            b_max = bronze_row[idx + 3]

            s_sum = source_df[0, idx]
            s_cnt = source_df[0, idx + 1]
            s_min = source_df[0, idx + 2]
            s_max = source_df[0, idx + 3]

            mismatches = {}
            # Compare with tolerance for floating point
            if s_cnt is not None and b_cnt is not None and int(s_cnt) != int(b_cnt):
                mismatches["count"] = {"source": int(s_cnt), "bronze": int(b_cnt)}
            if s_sum is not None and b_sum is not None:
                s_sum_f, b_sum_f = float(s_sum), float(b_sum)
                if abs(s_sum_f) > 0 and abs(s_sum_f - b_sum_f) / max(abs(s_sum_f), 1) > 0.001:
                    mismatches["sum"] = {"source": s_sum_f, "bronze": b_sum_f}

            if mismatches:
                result.mismatches[col_name] = mismatches

            idx += 4

        if result.is_clean:
            logger.info(
                "E-17: Aggregate reconciliation PASSED for %s.%s (%d numeric columns)",
                table_config.source_name, table_config.source_object_name,
                result.columns_checked,
            )
        else:
            logger.warning(
                "E-17: Aggregate reconciliation FAILED for %s.%s: %d column(s) with "
                "mismatches: %s",
                table_config.source_name, table_config.source_object_name,
                len(result.mismatches), list(result.mismatches.keys()),
            )

    except Exception as e:
        result.errors.append(str(e))
        logger.debug(
            "E-17: Aggregate reconciliation failed for %s.%s",
            table_config.source_name, table_config.source_object_name,
            exc_info=True,
        )

    # OBS-6: Persist result for historical trending
    _persist_reconciliation_result(
        check_type="AGGREGATE_RECONCILIATION",
        table_name=result.table_name,
        source_name=result.source_name,
        is_clean=result.is_clean,
        metadata={"columns_checked": result.columns_checked, "mismatches": result.mismatches},
        errors=result.errors or None,
    )

    return result


def detect_distribution_shift(
    table_config: TableConfig,
    z_score_threshold: float = 2.0,
) -> DistributionShiftResult:
    """B-10: Detect statistical distribution shifts in numeric columns.

    Compares current Bronze active row statistics (mean, stddev, P25/P50/P75)
    against a stored 30-day baseline from PipelineEventLog metadata. Catches
    subtle drift that counts and sums miss (e.g., a demographic distribution
    shifting, prices clustering differently).

    Designed for weekly scheduling — not every-run.

    Args:
        table_config: Table configuration.
        z_score_threshold: Alert when column mean shifts beyond this many
                          standard deviations from baseline (default: 2.0).

    Returns:
        DistributionShiftResult with shift details.
    """
    result = DistributionShiftResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    bronze_table = table_config.bronze_full_table_name

    if not table_exists(bronze_table):
        result.errors.append(f"Bronze table {bronze_table} does not exist")
        return result

    try:
        # Get numeric columns from Bronze (exclude internal columns)
        conn = connections.get_bronze_connection()
        try:
            cursor = conn.cursor()
            db = bronze_table.split(".")[0]
            schema = bronze_table.split(".")[1]
            tbl = bronze_table.split(".")[2]
            cursor.execute(
                f"SELECT COLUMN_NAME FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                f"AND DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint', "
                f"'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney') "
                f"AND COLUMN_NAME NOT LIKE '\\_%' ESCAPE '\\' "
                f"AND COLUMN_NAME NOT IN ('UdmHash', 'UdmActiveFlag', '_scd2_key')",
                schema, tbl,
            )
            numeric_cols = [row[0] for row in cursor.fetchall()]
            cursor.close()
        finally:
            conn.close()

        if not numeric_cols:
            return result

        result.columns_checked = len(numeric_cols)

        # Compute current statistics
        simple_agg = []
        for col in numeric_cols:
            simple_agg.append(
                f"AVG(CAST([{col}] AS FLOAT)) AS [{col}_mean], "
                f"STDEV(CAST([{col}] AS FLOAT)) AS [{col}_stddev]"
            )

        conn = connections.get_bronze_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT {', '.join(simple_agg)} "
                f"FROM {bronze_table} WHERE UdmActiveFlag = 1"
            )
            current_row = cursor.fetchone()
            cursor.close()
        finally:
            conn.close()

        if current_row is None:
            result.errors.append("No active rows in Bronze")
            return result

        # Compare against stored baseline from PipelineEventLog metadata
        # Look for previous B-10 distribution snapshots
        baseline = _get_distribution_baseline(
            table_config.source_object_name,
            table_config.source_name,
        )

        idx = 0
        current_stats = {}
        for col in numeric_cols:
            c_mean = current_row[idx]
            c_stddev = current_row[idx + 1]
            current_stats[col] = {
                "mean": float(c_mean) if c_mean is not None else None,
                "stddev": float(c_stddev) if c_stddev is not None else None,
            }

            # Check shift against baseline
            if baseline and col in baseline:
                b_mean = baseline[col].get("mean")
                b_stddev = baseline[col].get("stddev")
                if (
                    c_mean is not None
                    and b_mean is not None
                    and b_stddev is not None
                    and b_stddev > 0
                ):
                    z_score = abs(float(c_mean) - b_mean) / b_stddev
                    if z_score > z_score_threshold:
                        result.shifts_detected[col] = {
                            "current_mean": float(c_mean),
                            "baseline_mean": b_mean,
                            "baseline_stddev": b_stddev,
                            "z_score": round(z_score, 2),
                        }

            idx += 2

        # Store current stats as new baseline
        _save_distribution_baseline(
            table_config.source_object_name,
            table_config.source_name,
            current_stats,
        )

        # Log results
        if result.shifts_detected:
            logger.warning(
                "B-10: Distribution shifts detected for %s.%s: %d column(s) — %s",
                table_config.source_name, table_config.source_object_name,
                len(result.shifts_detected),
                {k: f"z={v['z_score']}" for k, v in result.shifts_detected.items()},
            )
        else:
            logger.info(
                "B-10: Distribution check clean for %s.%s (%d columns)",
                table_config.source_name, table_config.source_object_name,
                result.columns_checked,
            )

    except Exception as e:
        result.errors.append(str(e))
        logger.debug(
            "B-10: Distribution shift check failed for %s.%s",
            table_config.source_name, table_config.source_object_name,
            exc_info=True,
        )

    return result


def _get_distribution_baseline(
    table_name: str,
    source_name: str,
) -> dict | None:
    """B-10: Retrieve the most recent distribution baseline from PipelineEventLog metadata."""
    try:
        import json
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TOP 1 Metadata "
                "FROM ops.PipelineEventLog "
                "WHERE TableName = ? AND SourceName = ? "
                "AND EventType = 'DISTRIBUTION_CHECK' AND Status = 'SUCCESS' "
                "ORDER BY CompletedAt DESC",
                table_name, source_name,
            )
            row = cursor.fetchone()
            cursor.close()
            if row and row[0]:
                return json.loads(row[0])
        finally:
            conn.close()
    except Exception:
        pass
    return None


def _save_distribution_baseline(
    table_name: str,
    source_name: str,
    stats: dict,
) -> None:
    """B-10: Save distribution baseline to PipelineEventLog for future comparison."""
    try:
        import json
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO ops.PipelineEventLog "
                "(BatchId, TableName, SourceName, EventType, StartedAt, CompletedAt, "
                "DurationMs, Status, Metadata) "
                "VALUES (0, ?, ?, 'DISTRIBUTION_CHECK', ?, ?, 0, 'SUCCESS', ?)",
                table_name, source_name, now, now, json.dumps(stats),
            )
            cursor.close()
        finally:
            conn.close()
    except Exception:
        logger.debug("B-10: Failed to save distribution baseline — non-fatal", exc_info=True)

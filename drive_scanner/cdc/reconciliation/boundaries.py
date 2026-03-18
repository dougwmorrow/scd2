"""Transformation boundary and referential integrity reconciliation."""

from __future__ import annotations

import logging

import connections
from extract.udm_connectorx_extractor import table_exists

from cdc.reconciliation.models import (
    ReferentialIntegrityResult,
    TransformationBoundaryResult,
)

logger = logging.getLogger(__name__)


def reconcile_transformation_boundary(
    source_table: str,
    target_table: str,
    pk_columns: list[str],
    source_layer: str = "Bronze",
    target_layer: str = "Silver",
    source_active_filter: str | None = "UdmActiveFlag = 1",
    null_rate_threshold: float = 0.10,
) -> TransformationBoundaryResult:
    """B-11: Reconcile data across transformation boundaries.

    Validates row counts, key existence, and NULL rate comparisons between
    adjacent medallion layers. Implements the "circuit breaker" pattern:
    returns a result that callers can use to block downstream processing.

    Args:
        source_table: Fully qualified source table (e.g., 'UDM_Bronze.DNA.ACCT_scd2_python').
        target_table: Fully qualified target table (e.g., 'UDM_Silver.DNA.ACCT').
        pk_columns: Primary key columns for key existence checks.
        source_layer: Name of the source layer (for logging).
        target_layer: Name of the target layer (for logging).
        source_active_filter: Optional WHERE clause for source (e.g., 'UdmActiveFlag = 1').
        null_rate_threshold: Alert if NULL rate difference exceeds this for any column.

    Returns:
        TransformationBoundaryResult with discrepancy details.
    """
    result = TransformationBoundaryResult(
        table_name=source_table.split(".")[-1],
        source_name=source_table.split(".")[1] if "." in source_table else "",
        source_layer=source_layer,
        target_layer=target_layer,
    )

    if not table_exists(source_table):
        result.errors.append(f"Source table {source_table} does not exist")
        return result

    if not table_exists(target_table):
        result.errors.append(f"Target table {target_table} does not exist")
        return result

    try:
        src_db = source_table.split(".")[0]
        tgt_db = target_table.split(".")[0]

        # Row counts
        src_where = f"WHERE {source_active_filter}" if source_active_filter else ""
        src_conn = connections.get_connection(src_db)
        try:
            cursor = src_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {source_table} {src_where}")
            result.source_count = cursor.fetchone()[0]
            cursor.close()
        finally:
            src_conn.close()

        tgt_conn = connections.get_connection(tgt_db)
        try:
            cursor = tgt_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            result.target_count = cursor.fetchone()[0]
            cursor.close()
        finally:
            tgt_conn.close()

        result.count_delta = result.source_count - result.target_count

        # Key existence check: orphaned keys in target not in source
        if pk_columns:
            pk_select = ", ".join(f"[{c}]" for c in pk_columns)
            join_cond = " AND ".join(f"t.[{c}] = s.[{c}]" for c in pk_columns)

            orphan_query = (
                f"SELECT COUNT(*) FROM {target_table} t "
                f"LEFT JOIN {source_table} s ON {join_cond} "
                f"{'AND ' + source_active_filter if source_active_filter else ''} "
                f"WHERE s.[{pk_columns[0]}] IS NULL"
            )

            tgt_conn = connections.get_connection(tgt_db)
            try:
                cursor = tgt_conn.cursor()
                cursor.execute(orphan_query)
                result.orphaned_keys = cursor.fetchone()[0]
                cursor.close()
            finally:
                tgt_conn.close()

        # NULL rate comparison
        src_conn = connections.get_connection(src_db)
        try:
            cursor = src_conn.cursor()
            src_schema = source_table.split(".")[1]
            src_tbl = source_table.split(".")[2]
            cursor.execute(
                f"SELECT COLUMN_NAME FROM [{src_db}].INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                f"AND COLUMN_NAME NOT LIKE '\\_%' ESCAPE '\\'",
                src_schema, src_tbl,
            )
            common_cols = [row[0] for row in cursor.fetchall()]
            cursor.close()
        finally:
            src_conn.close()

        # Check NULL rates for common columns (limited to first 20 for performance)
        for col in common_cols[:20]:
            try:
                src_conn = connections.get_connection(src_db)
                try:
                    cursor = src_conn.cursor()
                    cursor.execute(
                        f"SELECT CAST(SUM(CASE WHEN [{col}] IS NULL THEN 1 ELSE 0 END) AS FLOAT) "
                        f"/ NULLIF(COUNT(*), 0) FROM {source_table} {src_where}"
                    )
                    src_null_rate = cursor.fetchone()[0] or 0.0
                    cursor.close()
                finally:
                    src_conn.close()

                tgt_conn = connections.get_connection(tgt_db)
                try:
                    cursor = tgt_conn.cursor()
                    cursor.execute(
                        f"SELECT CAST(SUM(CASE WHEN [{col}] IS NULL THEN 1 ELSE 0 END) AS FLOAT) "
                        f"/ NULLIF(COUNT(*), 0) FROM {target_table}"
                    )
                    tgt_null_rate = cursor.fetchone()[0] or 0.0
                    cursor.close()
                finally:
                    tgt_conn.close()

                if abs(src_null_rate - tgt_null_rate) > null_rate_threshold:
                    result.null_rate_anomalies.append(
                        f"{col}: {source_layer} NULL rate={src_null_rate:.2%}, "
                        f"{target_layer} NULL rate={tgt_null_rate:.2%}"
                    )
            except Exception:
                pass  # Skip individual column failures

        # Log results
        if result.is_clean:
            logger.info(
                "B-11: %s→%s boundary check PASSED for %s: "
                "source=%d, target=%d",
                source_layer, target_layer, result.table_name,
                result.source_count, result.target_count,
            )
        else:
            logger.warning(
                "B-11: %s→%s boundary check FAILED for %s: "
                "count_delta=%d, orphaned_keys=%d, null_anomalies=%d",
                source_layer, target_layer, result.table_name,
                result.count_delta, result.orphaned_keys,
                len(result.null_rate_anomalies),
            )

    except Exception as e:
        result.errors.append(str(e))
        logger.debug(
            "B-11: Transformation boundary check failed for %s",
            source_table, exc_info=True,
        )

    return result


def check_referential_integrity(
    fact_table: str,
    dimension_table: str,
    fk_column: str,
    dimension_pk_column: str,
    temporal: bool = False,
    effective_dt_column: str = "UdmEffectiveDateTime",
    end_dt_column: str = "UdmEndDateTime",
) -> ReferentialIntegrityResult:
    """B-12: Check FK relationships survive SCD2 processing.

    For non-temporal lookups: verifies every fact FK resolves to exactly one
    active dimension row (WHERE UdmActiveFlag = 1).

    For temporal lookups: verifies every fact FK+timestamp resolves to exactly
    one dimension row using BETWEEN logic on effective/end dates.

    Args:
        fact_table: Fully qualified fact table name.
        dimension_table: Fully qualified dimension table name.
        fk_column: Foreign key column in the fact table.
        dimension_pk_column: Primary key column in the dimension table.
        temporal: If True, use temporal join logic instead of active-flag.
        effective_dt_column: Effective datetime column in dimension (for temporal).
        end_dt_column: End datetime column in dimension (for temporal).

    Returns:
        ReferentialIntegrityResult with orphaned and ambiguous FK counts.
    """
    result = ReferentialIntegrityResult(
        fact_table=fact_table,
        dimension_table=dimension_table,
        fk_column=fk_column,
    )

    if not table_exists(fact_table):
        result.errors.append(f"Fact table {fact_table} does not exist")
        return result

    if not table_exists(dimension_table):
        result.errors.append(f"Dimension table {dimension_table} does not exist")
        return result

    try:
        fact_db = fact_table.split(".")[0]

        conn = connections.get_connection(fact_db)
        try:
            cursor = conn.cursor()

            # Total fact rows with non-NULL FK
            cursor.execute(
                f"SELECT COUNT(*) FROM {fact_table} WHERE [{fk_column}] IS NOT NULL"
            )
            result.total_fact_rows = cursor.fetchone()[0]

            if temporal:
                # Temporal: fact FK + UdmEffectiveDateTime must match exactly one
                # dimension row where fact.EffectiveDT BETWEEN dim.EffectiveDT AND dim.EndDT
                orphan_query = (
                    f"SELECT COUNT(*) FROM {fact_table} f "
                    f"WHERE f.[{fk_column}] IS NOT NULL "
                    f"AND NOT EXISTS ("
                    f"  SELECT 1 FROM {dimension_table} d "
                    f"  WHERE d.[{dimension_pk_column}] = f.[{fk_column}] "
                    f"  AND f.[{effective_dt_column}] >= d.[{effective_dt_column}] "
                    f"  AND f.[{effective_dt_column}] < ISNULL(d.[{end_dt_column}], '9999-12-31')"
                    f")"
                )

                ambiguous_query = (
                    f"SELECT COUNT(*) FROM ("
                    f"  SELECT f.[{fk_column}], f.[{effective_dt_column}] "
                    f"  FROM {fact_table} f "
                    f"  INNER JOIN {dimension_table} d "
                    f"    ON d.[{dimension_pk_column}] = f.[{fk_column}] "
                    f"    AND f.[{effective_dt_column}] >= d.[{effective_dt_column}] "
                    f"    AND f.[{effective_dt_column}] < ISNULL(d.[{end_dt_column}], '9999-12-31') "
                    f"  GROUP BY f.[{fk_column}], f.[{effective_dt_column}] "
                    f"  HAVING COUNT(*) > 1"
                    f") AS ambig"
                )
            else:
                # Non-temporal: fact FK must exist in dimension active rows
                orphan_query = (
                    f"SELECT COUNT(*) FROM {fact_table} f "
                    f"WHERE f.[{fk_column}] IS NOT NULL "
                    f"AND NOT EXISTS ("
                    f"  SELECT 1 FROM {dimension_table} d "
                    f"  WHERE d.[{dimension_pk_column}] = f.[{fk_column}] "
                    f"  AND d.UdmActiveFlag = 1"
                    f")"
                )

                ambiguous_query = (
                    f"SELECT COUNT(*) FROM ("
                    f"  SELECT f.[{fk_column}] "
                    f"  FROM {fact_table} f "
                    f"  INNER JOIN {dimension_table} d "
                    f"    ON d.[{dimension_pk_column}] = f.[{fk_column}] "
                    f"    AND d.UdmActiveFlag = 1 "
                    f"  GROUP BY f.[{fk_column}] "
                    f"  HAVING COUNT(DISTINCT d._scd2_key) > 1"
                    f") AS ambig"
                )

            cursor.execute(orphan_query)
            result.orphaned_fks = cursor.fetchone()[0]

            cursor.execute(ambiguous_query)
            result.ambiguous_fks = cursor.fetchone()[0]

            cursor.close()
        finally:
            conn.close()

        # Log results
        if result.is_clean:
            logger.info(
                "B-12: Referential integrity PASSED for %s.%s → %s.%s: "
                "%d fact rows, 0 orphaned, 0 ambiguous",
                fact_table, fk_column, dimension_table, dimension_pk_column,
                result.total_fact_rows,
            )
        else:
            logger.warning(
                "B-12: Referential integrity FAILED for %s.%s → %s.%s: "
                "%d orphaned FKs, %d ambiguous FKs out of %d fact rows",
                fact_table, fk_column, dimension_table, dimension_pk_column,
                result.orphaned_fks, result.ambiguous_fks, result.total_fact_rows,
            )

    except Exception as e:
        result.errors.append(str(e))
        logger.debug(
            "B-12: Referential integrity check failed for %s → %s",
            fact_table, dimension_table, exc_info=True,
        )

    return result

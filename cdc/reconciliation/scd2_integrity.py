"""SCD2 structural integrity validation and Bronze reconciliation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

import config
import connections
from connections import cursor_for, quote_identifier, quote_table
from extract.udm_connectorx_extractor import read_bronze_table, read_stage_table, table_exists

from cdc.reconciliation.core import _RECON_MAX_ROWS
from cdc.reconciliation.models import BronzeReconciliationResult, SCD2IntegrityResult
from cdc.reconciliation.persistence import _persist_reconciliation_result

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def reconcile_bronze(table_config: TableConfig) -> BronzeReconciliationResult:
    """P2-13: Compare CDC Stage current rows vs Bronze active rows.

    Detects:
      - Hash mismatches (Stage _row_hash != Bronze UdmHash for matched PKs)
      - Orphaned Bronze rows (UdmActiveFlag=1 but not _cdc_is_current=1 in Stage)
      - Missing Bronze rows (in Stage current but not in Bronze active)
      - Duplicate active PKs in Bronze (P0-8 crash recovery artifacts)

    Args:
        table_config: Table configuration.

    Returns:
        BronzeReconciliationResult with discrepancy details.
    """
    result = BronzeReconciliationResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    stage_table = table_config.stage_full_table_name
    bronze_table = table_config.bronze_full_table_name
    pk_columns = table_config.pk_columns

    if not pk_columns:
        result.errors.append("No PK columns configured — cannot reconcile Bronze")
        return result

    if not table_exists(stage_table):
        result.errors.append(f"Stage table {stage_table} does not exist")
        return result

    if not table_exists(bronze_table):
        result.errors.append(f"Bronze table {bronze_table} does not exist")
        return result

    # C-7: Size guard for Bronze reconciliation
    from extract.udm_connectorx_extractor import get_table_row_count
    approx_bronze = get_table_row_count(bronze_table)
    if approx_bronze > _RECON_MAX_ROWS:
        result.errors.append(
            f"Bronze table has ~{approx_bronze:,} rows (limit={_RECON_MAX_ROWS:,}). "
            f"Use sampled reconciliation for large tables."
        )
        logger.error(
            "C-7: Bronze reconciliation skipped for %s.%s — ~%d rows exceeds limit",
            table_config.source_name, table_config.source_object_name, approx_bronze,
        )
        return result

    try:
        # Read current Stage rows
        df_stage = read_stage_table(stage_table)
        result.stage_current_rows = len(df_stage)

        # Read active Bronze rows
        df_bronze = read_bronze_table(bronze_table)
        result.bronze_active_rows = len(df_bronze)

        if len(df_stage) == 0 and len(df_bronze) == 0:
            return result

        # --- Duplicate active PK check ---
        if len(df_bronze) > 0:
            pk_counts = df_bronze.group_by(pk_columns).len()
            duplicates = pk_counts.filter(pl.col("len") > 1)
            result.duplicate_active_pks = len(duplicates)
            if result.duplicate_active_pks > 0:
                logger.warning(
                    "P2-13: Found %d PKs with duplicate active rows in Bronze %s "
                    "(likely crash recovery artifacts from P0-8 INSERT-first design)",
                    result.duplicate_active_pks, bronze_table,
                )

        # --- PK comparison ---
        df_stage_keys = df_stage.select(pk_columns + ["_row_hash"])
        df_bronze_keys = (
            df_bronze.select(pk_columns + ["UdmHash"])
            .unique(subset=pk_columns, keep="first")
        )

        # Orphaned: in Bronze active but not in Stage current
        df_orphaned = df_bronze_keys.join(df_stage_keys, on=pk_columns, how="anti")
        result.orphaned_bronze_rows = len(df_orphaned)

        # Missing: in Stage current but not in Bronze active
        df_missing = df_stage_keys.join(df_bronze_keys, on=pk_columns, how="anti")
        result.missing_bronze_rows = len(df_missing)

        # Hash comparison on matched PKs
        df_matched = df_stage_keys.join(
            df_bronze_keys, on=pk_columns, how="inner", suffix="_bronze",
        )
        result.matched_rows = len(df_matched)

        if len(df_matched) > 0:
            hash_mismatch_mask = (
                (pl.col("_row_hash") != pl.col("UdmHash"))
                | (pl.col("_row_hash").is_null() != pl.col("UdmHash").is_null())
            )
            result.hash_mismatches = len(df_matched.filter(hash_mismatch_mask))

        # Log results
        if result.is_clean:
            logger.info(
                "Bronze reconciliation PASSED for %s.%s: %d matched, 0 issues",
                table_config.source_name, table_config.source_object_name,
                result.matched_rows,
            )
        else:
            logger.warning(
                "Bronze reconciliation FAILED for %s.%s: matched=%d, "
                "hash_mismatches=%d, orphaned=%d, missing=%d, duplicate_pks=%d",
                table_config.source_name, table_config.source_object_name,
                result.matched_rows, result.hash_mismatches,
                result.orphaned_bronze_rows, result.missing_bronze_rows,
                result.duplicate_active_pks,
            )

        # V-3: Optionally run SCD2 structural integrity validation.
        scd2_integrity = validate_scd2_integrity(table_config)
        if not scd2_integrity.is_clean:
            if scd2_integrity.overlapping_intervals > 0:
                result.errors.append(
                    f"V-3: {scd2_integrity.overlapping_intervals} PKs with overlapping SCD2 intervals"
                )
            if scd2_integrity.zero_active_pks > 0:
                result.errors.append(
                    f"V-3: {scd2_integrity.zero_active_pks} PKs with zero active rows"
                )
            if scd2_integrity.version_gaps > 0:
                result.errors.append(
                    f"V-3: {scd2_integrity.version_gaps} PKs with gaps between SCD2 versions"
                )

    except Exception as e:
        result.errors.append(str(e))
        logger.exception(
            "Bronze reconciliation error for %s.%s",
            table_config.source_name, table_config.source_object_name,
        )

    # OBS-6: Persist result for historical trending
    _persist_reconciliation_result(
        check_type="BRONZE_RECONCILIATION",
        table_name=result.table_name,
        source_name=result.source_name,
        is_clean=result.is_clean,
        source_rows=result.stage_current_rows,
        target_rows=result.bronze_active_rows,
        mismatched_rows=result.hash_mismatches,
        source_only_rows=result.missing_bronze_rows,
        target_only_rows=result.orphaned_bronze_rows,
        metadata={"duplicate_active_pks": result.duplicate_active_pks},
        errors=result.errors or None,
    )

    return result


def validate_scd2_integrity(table_config: TableConfig) -> SCD2IntegrityResult:
    """V-3: Validate SCD2 structural integrity on a Bronze table.

    Runs three SQL checks:
      1. Overlapping [EffectiveDateTime, EndDateTime) intervals per PK
      2. PKs with zero active rows (all versions expired)
      3. Gaps between consecutive versions per PK

    Protected by _RECON_MAX_ROWS guard (same as reconcile_bronze).

    Args:
        table_config: Table configuration.

    Returns:
        SCD2IntegrityResult with violation counts and sample PKs.
    """
    result = SCD2IntegrityResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    bronze_table = table_config.bronze_full_table_name
    pk_columns = table_config.pk_columns

    if not pk_columns:
        result.errors.append("No PK columns configured — cannot validate SCD2 integrity")
        return result

    if not table_exists(bronze_table):
        result.errors.append(f"Bronze table {bronze_table} does not exist")
        return result

    # Size guard — reuse _RECON_MAX_ROWS
    from extract.udm_connectorx_extractor import get_table_row_count
    approx_rows = get_table_row_count(bronze_table)
    if approx_rows > _RECON_MAX_ROWS:
        result.errors.append(
            f"Bronze table has ~{approx_rows:,} rows (limit={_RECON_MAX_ROWS:,}). "
            f"SCD2 integrity validation skipped for large tables."
        )
        logger.warning(
            "V-3: SCD2 integrity validation skipped for %s.%s — ~%d rows exceeds limit",
            table_config.source_name, table_config.source_object_name, approx_rows,
        )
        return result

    # Item-13: Use quote_identifier/quote_table for all dynamic SQL (H-1).
    q_bronze = quote_table(bronze_table)
    pk_join = " AND ".join(
        f"a.{quote_identifier(c)} = b.{quote_identifier(c)}" for c in pk_columns
    )
    pk_group = ", ".join(quote_identifier(c) for c in pk_columns)
    pk_partition = ", ".join(quote_identifier(c) for c in pk_columns)

    # Item-16: Migrated from manual conn/try/finally to cursor_for().
    try:
        with cursor_for(config.BRONZE_DB) as cursor:

            # --- Check 1: Overlapping intervals ---
            # Two versions overlap if a.Effective < b.End AND b.Effective < a.End
            # Use ISNULL(EndDateTime, '9999-12-31') for active rows (NULL EndDateTime).
            overlap_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT {pk_group}
                    FROM {q_bronze} a
                    INNER JOIN {q_bronze} b
                        ON {pk_join}
                        AND a._scd2_key <> b._scd2_key
                        AND a.UdmEffectiveDateTime < ISNULL(b.UdmEndDateTime, '9999-12-31')
                        AND b.UdmEffectiveDateTime < ISNULL(a.UdmEndDateTime, '9999-12-31')
                ) AS overlap
            """
            cursor.execute(overlap_sql)
            result.overlapping_intervals = cursor.fetchone()[0]

            if result.overlapping_intervals > 0:
                sample_sql = f"""
                    SELECT TOP 5 {pk_group}
                    FROM {q_bronze} a
                    INNER JOIN {q_bronze} b
                        ON {pk_join}
                        AND a._scd2_key <> b._scd2_key
                        AND a.UdmEffectiveDateTime < ISNULL(b.UdmEndDateTime, '9999-12-31')
                        AND b.UdmEffectiveDateTime < ISNULL(a.UdmEndDateTime, '9999-12-31')
                    GROUP BY {pk_group}
                """
                cursor.execute(sample_sql)
                result.sample_overlapping_pks = [str(row) for row in cursor.fetchall()]
                logger.error(
                    "V-3: %d PKs with overlapping SCD2 intervals in %s. Samples: %s",
                    result.overlapping_intervals, bronze_table, result.sample_overlapping_pks,
                )

            # --- Check 2: Zero active PKs ---
            # PKs where all versions are expired (no UdmActiveFlag=1 row).
            zero_active_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_group}
                    FROM {q_bronze}
                    GROUP BY {pk_group}
                    HAVING SUM(CASE WHEN UdmActiveFlag = 1 THEN 1 ELSE 0 END) = 0
                ) AS zero_active
            """
            cursor.execute(zero_active_sql)
            result.zero_active_pks = cursor.fetchone()[0]

            if result.zero_active_pks > 0:
                sample_sql = f"""
                    SELECT TOP 5 {pk_group}
                    FROM {q_bronze}
                    GROUP BY {pk_group}
                    HAVING SUM(CASE WHEN UdmActiveFlag = 1 THEN 1 ELSE 0 END) = 0
                """
                cursor.execute(sample_sql)
                result.sample_zero_active_pks = [str(row) for row in cursor.fetchall()]
                logger.error(
                    "V-3: %d PKs with zero active rows in %s. Samples: %s",
                    result.zero_active_pks, bronze_table, result.sample_zero_active_pks,
                )

            # --- Check 3: Gaps between consecutive versions ---
            # Use LAG(UdmEndDateTime) partitioned by PK, ordered by EffectiveDateTime.
            # A gap exists when previous EndDateTime < current EffectiveDateTime
            # with more than 1-second tolerance.
            gap_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_group}
                    FROM (
                        SELECT {pk_group},
                            UdmEffectiveDateTime,
                            LAG(ISNULL(UdmEndDateTime, '9999-12-31'))
                                OVER (PARTITION BY {pk_partition} ORDER BY UdmEffectiveDateTime)
                                AS prev_end
                        FROM {q_bronze}
                    ) versioned
                    WHERE prev_end IS NOT NULL
                        AND prev_end < '9999-12-31'
                        AND DATEDIFF(SECOND, prev_end, UdmEffectiveDateTime) > 1
                    GROUP BY {pk_group}
                ) AS gaps
            """
            cursor.execute(gap_sql)
            result.version_gaps = cursor.fetchone()[0]

            if result.version_gaps > 0:
                sample_sql = f"""
                    SELECT TOP 5 {pk_group}
                    FROM (
                        SELECT {pk_group},
                            UdmEffectiveDateTime,
                            LAG(ISNULL(UdmEndDateTime, '9999-12-31'))
                                OVER (PARTITION BY {pk_partition} ORDER BY UdmEffectiveDateTime)
                                AS prev_end
                        FROM {q_bronze}
                    ) versioned
                    WHERE prev_end IS NOT NULL
                        AND prev_end < '9999-12-31'
                        AND DATEDIFF(SECOND, prev_end, UdmEffectiveDateTime) > 1
                    GROUP BY {pk_group}
                """
                cursor.execute(sample_sql)
                result.sample_gap_pks = [str(row) for row in cursor.fetchall()]
                logger.error(
                    "V-3: %d PKs with version gaps in %s. Samples: %s",
                    result.version_gaps, bronze_table, result.sample_gap_pks,
                )

        # Log summary
        if result.is_clean:
            logger.info(
                "V-3: SCD2 integrity PASSED for %s.%s",
                table_config.source_name, table_config.source_object_name,
            )
        else:
            logger.warning(
                "V-3: SCD2 integrity FAILED for %s.%s: overlaps=%d, zero_active=%d, gaps=%d",
                table_config.source_name, table_config.source_object_name,
                result.overlapping_intervals, result.zero_active_pks, result.version_gaps,
            )

    except Exception as e:
        result.errors.append(str(e))
        logger.exception(
            "V-3: SCD2 integrity validation error for %s.%s",
            table_config.source_name, table_config.source_object_name,
        )

    return result

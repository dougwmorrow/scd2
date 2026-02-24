"""Polars SCD2: Bronze comparison, UPDATES via staging, INSERTs via BCP.

Provides two modes:
  - run_scd2(): Full Bronze comparison (small tables). Reads all active rows.
  - run_scd2_targeted(): PK-targeted Bronze comparison (large tables). Reads only
    Bronze rows matching the PKs in df_current via staging table join.

Optimized 2-step process (P0-8: INSERT-first for crash safety):
  1. INSERT: single batch for new rows + new versions (append-only, never truncate Bronze)
  2. UPDATE: single batch for closes/deletes (UdmActiveFlag=0, UdmEndDateTime=now)

_scd2_key (IDENTITY) is excluded from all INSERT DataFrames and BCP column lists.
UdmHash = _row_hash copied from CDC.

W-16 TODO — SQL Server 2022 temporal tables evaluation:
  SQL Server 2022 temporal tables provide automatic SCD2-like versioning with
  native FOR SYSTEM_TIME AS OF query syntax. For stable dimension tables where
  column-level change tracking granularity isn't needed, temporal tables would
  eliminate custom SCD2 code entirely. Limitations: any column update creates a
  new version (no column-level selectivity), and the history table must be in
  the same database. Evaluation: identify candidate dimension tables that are
  low-to-moderate change volume, don't require column-level change tracking,
  and prototype conversion of one such table. Compare behavior with current
  SCD2 implementation before deciding on broader adoption.

C-4 NOTE — Bronze isolation level:
  ConnectorX reads Bronze using SQL Server's default READ COMMITTED isolation.
  Under standard READ COMMITTED, concurrent DML (non-pipeline writers) can cause
  inconsistent reads. Recommended: enable READ_COMMITTED_SNAPSHOT on the Bronze
  database for consistent snapshot reads without blocking. The table lock (P1-2)
  prevents concurrent pipeline runs but does not protect against non-pipeline
  writers (downstream ETL, reporting queries).

E-8 NOTE — RCSI transient SCD2 inconsistency window:
  Under RCSI, INSERT and UPDATE execute as separate statements, each with its own
  snapshot. Between INSERT commit and UPDATE commit, a concurrent reader may see:
    - Two active versions for updated PKs (new version inserted, old not yet closed)
    - One active version for new PKs (INSERT committed, no prior version to close)
  This transient window is typically milliseconds to seconds. For most analytics
  use cases this is acceptable. For real-time consumers querying Bronze during
  SCD2 promotion, use the dedup-safe query pattern:
    ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC)
    WHERE rn = 1
  instead of WHERE UdmActiveFlag = 1 alone. The V-4 post-SCD2 duplicate check
  and P1-16 dedup recovery handle the crash case where the window persists.

B-5 AUDIT (2026-02-23) — Polars join validation bug (#19624):
  Audited all .join() calls in this module. None use the `validate` parameter.
  All joins use only `on`, `how`, and `suffix`. Polars #19624 (false errors when
  `validate` is used with NULL keys) does not apply. CDC's `_filter_null_pks()`
  already removes NULL PKs before any data reaches SCD2 joins. No action required.

B-14 NOTE — INSERT-first zero-active-row window:
  The 3-step SCD2 pattern (INSERT with Flag=0 → UPDATE to close old → UPDATE to
  activate new) creates a transient window where queries filtering on
  UdmActiveFlag=1 see ZERO active rows for affected PKs. This occurs between
  the close-old UPDATE commit and the activate-new UPDATE commit.

  Timeline for a PK being updated:
    1. INSERT new version with UdmActiveFlag=0      → old=1, new=0 (readers see old)
    2. UPDATE old version: UdmActiveFlag=0           → old=0, new=0 (ZERO ACTIVE)
    3. UPDATE new version: UdmActiveFlag=1           → old=0, new=1 (readers see new)

  Under RCSI, readers with snapshots from before step 2 continue seeing the
  pre-operation state (consistent). New readers during step 2-3 see the gap.
  The window is typically milliseconds (single UPDATE statement execution time).

  For critical consumers, use the defensive query pattern that handles both
  the zero-active-row window AND the RCSI inconsistency window (E-8):
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC
      ) AS rn
      FROM Bronze.table
    ) t WHERE rn = 1

  Minimization: Steps 2 and 3 execute as close together as possible within
  _execute_bronze_updates() followed immediately by _activate_new_versions().
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

import config
import connections
from data_load import bcp_loader
from data_load.bcp_csv import validate_schema_before_concat, write_bcp_csv
from data_load.sanitize import cast_bit_columns, reorder_columns_for_bcp, sanitize_strings
from data_load.schema_utils import align_pk_dtypes, get_column_types
from extract.udm_connectorx_extractor import (
    read_bronze_for_pks,
    read_bronze_table,
    table_exists,
)
from cdc.engine import _safe_concat, _validate_concat_columns

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


@dataclass
class SCD2Result:
    """Results from SCD2 promotion."""

    inserts: int = 0
    new_versions: int = 0
    closes: int = 0
    unchanged: int = 0


def run_scd2(
    table_config: TableConfig,
    df_current: pl.DataFrame,
    pk_columns: list[str],
    output_dir: str | Path,
) -> SCD2Result:
    """Run SCD2 promotion: compare CDC current vs Bronze active.

    Args:
        table_config: Table configuration.
        df_current: Current CDC rows (from CDC result, _cdc_is_current=1).
        pk_columns: Primary key columns for SCD2 business key.
        output_dir: Directory for staging CSV files.

    Returns:
        SCD2Result with counts.
    """
    result = SCD2Result()
    now = datetime.now(timezone.utc)
    bronze_table = table_config.bronze_full_table_name

    if not pk_columns:
        logger.warning("No PK columns for %s — skipping SCD2", table_config.source_object_name)
        return result

    if df_current is None or len(df_current) == 0:
        logger.info("No current CDC rows for %s — skipping SCD2", table_config.source_object_name)
        return result

    # Source columns: everything that's not a CDC or SCD2 internal column
    internal_cols = {
        "_row_hash", "_extracted_at",
        "_cdc_operation", "_cdc_valid_from", "_cdc_valid_to",
        "_cdc_is_current", "_cdc_batch_id",
        "_scd2_key",
        "UdmHash", "UdmEffectiveDateTime", "UdmEndDateTime",
        "UdmActiveFlag", "UdmScd2Operation",
    }
    source_cols = [c for c in df_current.columns if c not in internal_cols]

    # First run: Bronze doesn't exist yet — all rows are inserts
    if not table_exists(bronze_table):
        logger.info("Bronze table %s doesn't exist — all %d rows are new inserts", bronze_table, len(df_current))
        result.inserts = len(df_current)

        df_insert = _build_scd2_insert(df_current, source_cols, pk_columns, now, "I")
        _write_and_load_bronze(df_insert, bronze_table, output_dir, table_config, "inserts")
        return result

    # B-4: Clean up orphaned inactive rows from prior crash recovery
    _cleanup_orphaned_inactive_rows(bronze_table, table_config)

    # Read active Bronze rows
    df_bronze = read_bronze_table(bronze_table)
    logger.info("Active Bronze rows: %d", len(df_bronze))

    if len(df_bronze) == 0:
        result.inserts = len(df_current)
        df_insert = _build_scd2_insert(df_current, source_cols, pk_columns, now, "I")
        _write_and_load_bronze(df_insert, bronze_table, output_dir, table_config, "inserts")
        return result

    # P1-16: Deduplicate active Bronze rows — crash recovery may leave
    # duplicate active rows (P0-8 INSERT-first design). Keep only the
    # row with the latest UdmEffectiveDateTime per PK.
    df_bronze = _dedup_bronze_active(df_bronze, pk_columns, table_config)

    # --- P0-12: Align PK dtypes before joins ---
    df_current, df_bronze = align_pk_dtypes(
        df_current, df_bronze, pk_columns, context="SCD2 run_scd2",
    )

    # --- Detect changes ---

    # Get hash from CDC current (_row_hash) and Bronze (UdmHash)
    df_cdc_keys = df_current.select(pk_columns + ["_row_hash"])
    df_bronze_keys = df_bronze.select(pk_columns + ["UdmHash"])

    # NEW INSERTS: in CDC but not in Bronze (anti-join on PKs)
    df_new = df_current.join(df_bronze_keys, on=pk_columns, how="anti")
    result.inserts = len(df_new)

    # CLOSES (deletes from source): in Bronze but not in CDC
    df_closed = df_bronze_keys.join(df_cdc_keys, on=pk_columns, how="anti")
    result.closes += len(df_closed)

    # E-6/E-18: Resurrection detection — verify no PKs appear in both
    # df_new (new inserts) and df_closed (deletes). By construction this
    # shouldn't happen (they're anti-joins in opposite directions), but a
    # PK appearing in both would cause the close UPDATE to close the just-inserted
    # new version. Resurrected PKs get UdmScd2Operation='R' for audit trail.
    resurrection_pks: pl.DataFrame | None = None
    if len(df_new) > 0 and len(df_closed) > 0:
        resurrection_check = df_new.select(pk_columns).join(
            df_closed.select(pk_columns), on=pk_columns, how="semi"
        )
        if len(resurrection_check) > 0:
            logger.info(
                "E-6/E-18: %d resurrected PKs in %s (in both new and closed sets). "
                "These will be inserted with UdmScd2Operation='R' for audit trail.",
                len(resurrection_check), table_config.source_object_name,
            )
            resurrection_pks = resurrection_check
            # Remove from both sets — resurrected PKs handled separately below
            df_new = df_new.join(resurrection_check, on=pk_columns, how="anti")
            df_closed = df_closed.join(resurrection_check, on=pk_columns, how="anti")
            result.inserts = len(df_new)
            result.closes = len(df_closed)

    # CHANGED vs UNCHANGED: inner join, compare hashes
    df_matched = df_cdc_keys.join(
        df_bronze_keys,
        on=pk_columns,
        how="inner",
        suffix="_bronze",
    )

    # P0-10: NULL hash guards — treat NULL hashes as "changed" to prevent
    # silent misclassification when hashes are missing.
    changed_mask = (
        (pl.col("_row_hash") != pl.col("UdmHash"))
        | pl.col("_row_hash").is_null()
        | pl.col("UdmHash").is_null()
    )
    df_changed_pks = df_matched.filter(changed_mask).select(pk_columns)
    df_unchanged_pks = df_matched.filter(~changed_mask).select(pk_columns)

    result.new_versions = len(df_changed_pks)
    result.closes += len(df_changed_pks)  # old versions get closed
    result.unchanged = len(df_unchanged_pks)

    # P0-12: Count validation — CDC current rows must be fully accounted for
    accounted = result.inserts + result.new_versions + result.unchanged
    cdc_unique_pks = len(df_cdc_keys)
    if accounted != cdc_unique_pks:
        logger.error(
            "P0-12 COUNT MISMATCH in SCD2 %s: inserts(%d) + new_versions(%d) + unchanged(%d) = %d, "
            "but CDC has %d unique PKs. Possible PK dtype mismatch.",
            table_config.source_object_name,
            result.inserts, result.new_versions, result.unchanged, accounted, cdc_unique_pks,
        )

    logger.info(
        "SCD2 %s: inserts=%d, new_versions=%d, closes=%d, unchanged=%d",
        table_config.source_object_name,
        result.inserts, result.new_versions, result.closes, result.unchanged,
    )

    # P0-8: INSERT first, THEN UPDATE. A crash after insert but before update
    # leaves duplicate active rows (recoverable via next run's comparison),
    # instead of zero active rows (data loss requiring manual recovery).

    # --- Step 1: INSERT — new rows + new versions + resurrections ---
    insert_parts: list[pl.DataFrame] = []
    if result.inserts > 0:
        insert_parts.append(_build_scd2_insert(df_new, source_cols, pk_columns, now, "I"))
    if result.new_versions > 0:
        df_new_ver = df_current.join(df_changed_pks, on=pk_columns, how="semi")
        insert_parts.append(_build_scd2_insert(df_new_ver, source_cols, pk_columns, now, "U"))
    # E-18: Resurrected PKs get UdmScd2Operation='R' — distinct audit trail
    # for rows that were previously deleted and reappeared in source.
    if resurrection_pks is not None and len(resurrection_pks) > 0:
        df_resurrected = df_current.join(resurrection_pks, on=pk_columns, how="semi")
        insert_parts.append(_build_scd2_insert(df_resurrected, source_cols, pk_columns, now, "R"))
        result.inserts += len(df_resurrected)

    if insert_parts:
        # T-2: Use _safe_concat to avoid diagonal_relaxed crash bugs.
        df_all_inserts = _safe_concat(insert_parts)
        # P1-17 + C-3: Validate NULLs and fix dtype drift.
        df_all_inserts = _validate_concat_columns(df_all_inserts, df_current, table_config)
        _write_and_load_bronze(df_all_inserts, bronze_table, output_dir, table_config, "inserts")

    # --- Step 2: UPDATE — close old versions + deletes ---
    pks_to_close_parts: list[pl.DataFrame] = []
    if len(df_closed) > 0:
        pks_to_close_parts.append(df_closed.select(pk_columns))
    if len(df_changed_pks) > 0:
        pks_to_close_parts.append(df_changed_pks)
    # E-18: Close the deleted versions for resurrected PKs too (the old
    # "D" or inactive version needs its EndDateTime set).
    if resurrection_pks is not None and len(resurrection_pks) > 0:
        pks_to_close_parts.append(resurrection_pks)

    if pks_to_close_parts:
        # W-7: Validate PK schemas match before concat.
        validate_schema_before_concat(
            pks_to_close_parts, f"SCD2 close PKs for {table_config.source_object_name}")
        # E-5: Dedup concatenated close PKs — a PK can appear in both df_closed
        # (deleted from source) and df_changed_pks (hash changed). Without dedup,
        # the staging table would have duplicate keys.
        pks_to_close = pl.concat(pks_to_close_parts).unique(subset=pk_columns)
        _execute_bronze_updates(pks_to_close, pk_columns, bronze_table, now, output_dir, table_config)

    # --- Step 3: ACTIVATE — flip new versions to active (E-2/E-18) ---
    # New versions (operation="U"/"R") were inserted with UdmActiveFlag=0 to avoid
    # conflicting with the filtered unique index. Now that old versions are closed,
    # activate the new versions.
    if result.new_versions > 0 or (resurrection_pks is not None and len(resurrection_pks) > 0):
        _activate_new_versions(bronze_table, now)

    # V-4: Post-SCD2 duplicate active row check (non-blocking diagnostic).
    _check_duplicate_active_rows(bronze_table, pk_columns, table_config)

    return result


def _check_duplicate_active_rows(
    bronze_table: str,
    pk_columns: list[str],
    table_config: TableConfig,
) -> int:
    """V-4: Post-SCD2 diagnostic — check for duplicate active rows in Bronze.

    Queries Bronze for PKs with more than one UdmActiveFlag=1 row.
    Logs WARNING with count if found. Non-blocking diagnostic.

    Returns:
        Number of PKs with duplicate active rows.
    """
    if not pk_columns:
        return 0

    if not table_exists(bronze_table):
        return 0

    pk_group = ", ".join(f"[{c}]" for c in pk_columns)

    db = bronze_table.split(".")[0]
    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_group}
                FROM {bronze_table}
                WHERE UdmActiveFlag = 1
                GROUP BY {pk_group}
                HAVING COUNT(*) > 1
            ) AS dup
        """)
        dup_count = cursor.fetchone()[0]
        cursor.close()

        if dup_count > 0:
            logger.warning(
                "V-4: %d PKs with duplicate active rows in %s after SCD2 promotion. "
                "Downstream queries should use: "
                "ROW_NUMBER() OVER (PARTITION BY %s ORDER BY UdmEffectiveDateTime DESC) "
                "WHERE rn = 1 — instead of WHERE UdmActiveFlag = 1 alone.",
                dup_count, bronze_table, ", ".join(pk_columns),
            )
        return dup_count

    except Exception:
        logger.debug(
            "V-4: Could not check duplicate active rows for %s — continuing",
            bronze_table, exc_info=True,
        )
        return 0
    finally:
        conn.close()


def _build_scd2_insert(
    df: pl.DataFrame,
    source_cols: list[str],
    pk_columns: list[str],
    effective_dt: datetime,
    operation: str,
) -> pl.DataFrame:
    """Build SCD2 INSERT DataFrame with UDM columns. Excludes _scd2_key (IDENTITY).

    E-2: For operation="U" (new versions of existing PKs), UdmActiveFlag is set
    to 0 initially. The activation UPDATE step (after closing old versions) flips
    it to 1. This prevents conflicts with the filtered unique index
    (ensure_bronze_unique_active_index) which rejects duplicate active rows per PK.
    For operation="I" (brand new PKs), UdmActiveFlag is set to 1 directly since
    there is no existing active row to conflict with.

    E-18: operation="R" (reactivation) behaves like "U" — starts inactive since
    there is an existing version (the deleted row) that needs to be closed first.
    The "R" operation type provides a distinct audit trail for resurrected PKs:
    active period → deleted period → reactivated period.
    """
    # Select only source columns that exist in the DataFrame
    available_cols = [c for c in source_cols if c in df.columns]

    # E-2/E-18: New versions ("U") and reactivations ("R") start inactive to
    # avoid unique index conflict. New inserts ("I") start active since no
    # prior active row exists.
    active_flag = 0 if operation in ("U", "R") else 1

    # B-1: UdmHash is now VARCHAR(64) (full SHA-256 hex string), mapped from _row_hash (Utf8).
    df_out = df.select(available_cols).with_columns(
        df["_row_hash"].alias("UdmHash") if "_row_hash" in df.columns else pl.lit(None).cast(pl.Utf8).alias("UdmHash"),
        pl.lit(effective_dt).alias("UdmEffectiveDateTime"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("UdmEndDateTime"),
        pl.lit(active_flag).cast(pl.Int8).alias("UdmActiveFlag"),
        pl.lit(operation).alias("UdmScd2Operation"),
    )

    return df_out


def _write_and_load_bronze(
    df: pl.DataFrame,
    bronze_table: str,
    output_dir: str | Path,
    table_config: TableConfig,
    label: str,
) -> None:
    """Write SCD2 INSERT rows to CSV and BCP load into Bronze table."""
    df = sanitize_strings(df)
    df = cast_bit_columns(df)

    # P0-1: Reorder columns to match target table positional order
    # Exclude _scd2_key (IDENTITY) — not in our DataFrame, not in BCP column list
    if table_exists(bronze_table):
        df = reorder_columns_for_bcp(df, bronze_table, exclude_columns={"_scd2_key"})

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table_config.source_object_name}_scd2_{label}.csv",
    )
    # E-3: Bronze SCD2 loads use atomic=True (default) — the entire INSERT must be
    # a single transaction. A partial load (some new versions inserted, others not)
    # breaks SCD2 atomicity and creates inconsistent active row state.
    bcp_loader.bcp_load(str(csv_path), bronze_table, expected_row_count=len(df))


def _check_log_space(db: str, bronze_table: str, estimated_log_gb: float) -> None:
    """E-10: Pre-flight check of available transaction log space.

    Queries sys.dm_db_log_space_usage to verify sufficient log space exists
    before large SCD2 UPDATE operations. Logs WARNING if available space
    appears insufficient.
    """
    try:
        conn = connections.get_connection(db)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT total_log_size_in_bytes / 1073741824.0, "
                "       used_log_space_in_bytes / 1073741824.0 "
                "FROM sys.dm_db_log_space_usage"
            )
            row = cursor.fetchone()
            cursor.close()

            if row:
                total_gb, used_gb = float(row[0]), float(row[1])
                available_gb = total_gb - used_gb

                if available_gb < estimated_log_gb * 1.5:
                    logger.warning(
                        "E-10: Transaction log space may be insufficient for %s. "
                        "Available: %.1f GB, estimated need: %.1f GB (total: %.1f GB, "
                        "used: %.1f GB). Ensure frequent log backups (every 15-30 min) "
                        "or increase log file size before proceeding.",
                        bronze_table, available_gb, estimated_log_gb,
                        total_gb, used_gb,
                    )
                else:
                    logger.debug(
                        "E-10: Log space check for %s — available: %.1f GB, "
                        "estimated need: %.1f GB",
                        bronze_table, available_gb, estimated_log_gb,
                    )
        finally:
            conn.close()
    except Exception:
        logger.debug(
            "E-10: Could not check log space for %s — continuing",
            bronze_table, exc_info=True,
        )


def _execute_bronze_updates(
    pks_to_close: pl.DataFrame,
    pk_columns: list[str],
    bronze_table: str,
    end_dt: datetime,
    output_dir: str | Path,
    table_config: TableConfig,
) -> None:
    """Close old versions via staging table + UPDATE JOIN.

    SCD-4 NOTE: All UPDATE operations use cursor.execute() (single statement),
    NOT cursor.executemany(). pyodbc issue #481 confirms rowcount returns -1
    after executemany(), which would break P2-14 rowcount validation.

    L-3 NOTE — Bronze growth and partitioning:
      Bronze is append-only by design. For a 3B-row source with 5% monthly
      churn, Bronze grows ~1.8B rows/year. After 3 years the table reaches
      ~8.4B rows. The targeted read filters on UdmActiveFlag=1, but index
      structures span all rows (active + historical).
      Recommended partitioning strategy:
        - Partition Bronze on UdmActiveFlag or UdmEndDateTime so historical
          rows live in separate filegroups.
        - Periodic archival of UdmActiveFlag=0 rows older than N months to
          an archive table.
        - Use get_active_row_count() (filtered partition stats) instead of
          get_table_row_count() for INDEX_REBUILD_THRESHOLD calculations.
    """
    db = bronze_table.split(".")[0]
    schema = bronze_table.split(".")[1]
    staging_table = f"{db}.{schema}._staging_scd2_{table_config.source_object_name}"

    # P0-3: Use actual PK column types from target table instead of NVARCHAR(MAX)
    pk_types = get_column_types(bronze_table, pk_columns)

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        pk_col_defs = ", ".join(f"[{c}] {pk_types[c]}" for c in pk_columns)
        cursor.execute(f"""
            IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE {staging_table};
            CREATE TABLE {staging_table} ({pk_col_defs})
        """)
        cursor.close()
    finally:
        conn.close()

    # E-5: Dedup staging PKs — defense-in-depth against duplicate business keys.
    # The UPDATE FROM JOIN pattern is nondeterministic with duplicate keys.
    # Currently safe (SET values are constants), but this prevents issues if
    # the pattern is ever extended to SET values from the staging table.
    pre_dedup = len(pks_to_close)
    pks_to_close = pks_to_close.unique(subset=pk_columns)
    if len(pks_to_close) < pre_dedup:
        logger.debug(
            "E-5: Deduplicated %d duplicate PKs from close staging set for %s",
            pre_dedup - len(pks_to_close), bronze_table,
        )

    try:
        # BCP load PKs into staging — keep native dtypes (don't cast to Utf8)
        pks_clean = sanitize_strings(pks_to_close)
        csv_path = write_bcp_csv(
            pks_clean,
            Path(output_dir) / f"{table_config.source_name}_{table_config.source_object_name}_scd2_close_pks.csv",
        )
        # E-3: Staging tables are ephemeral — atomic=False for performance.
        bcp_loader.bcp_load(str(csv_path), staging_table, atomic=False)

        # P2-5: Index staging table for efficient JOIN against large Bronze tables
        bcp_loader.create_staging_index(staging_table, pk_columns, row_count=len(pks_to_close))

        # P2-11/E-10: Warn about large UPDATE JOINs that generate significant
        # transaction log. UPDATEs are always fully logged regardless of recovery
        # model. Each row generates before+after images in the log.
        if len(pks_to_close) > 1_000_000:
            # E-10: Estimate transaction log usage (~400 bytes avg row × 2 for
            # before+after images in log records)
            estimated_log_gb = (len(pks_to_close) * 400 * 2) / (1024**3)
            logger.warning(
                "P2-11/E-10: Large Bronze UPDATE JOIN: %d rows in %s. "
                "Estimated transaction log: %.1f GB. "
                "This is fully logged regardless of BULK_LOGGED recovery model. "
                "Monitor: SELECT * FROM sys.dm_db_log_space_usage",
                len(pks_to_close), bronze_table, estimated_log_gb,
            )

            # E-10: Pre-flight log space check
            _check_log_space(db, bronze_table, estimated_log_gb)

        # SCD-3/B-2: Batch the UPDATE JOIN to avoid lock escalation.
        # SQL Server escalates row locks to table-level exclusive locks at ~5,000
        # locks. Table-level exclusive locks override RCSI, blocking all readers.
        # Keep batch size below 5,000 to maintain row-level locking.
        join_condition = " AND ".join(
            f"t.[{c}] = s.[{c}]" for c in pk_columns
        )

        _SCD3_BATCH_SIZE = config.SCD2_UPDATE_BATCH_SIZE
        total_affected = 0

        if len(pks_to_close) <= _SCD3_BATCH_SIZE:
            # Small enough for a single UPDATE — no batching needed
            conn = connections.get_connection(db)
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    UPDATE t
                    SET t.UdmActiveFlag = 0, t.UdmEndDateTime = ?
                    FROM {bronze_table} t
                    INNER JOIN {staging_table} s ON {join_condition}
                    WHERE t.UdmActiveFlag = 1
                """, end_dt)
                total_affected = cursor.rowcount
                cursor.close()
            finally:
                conn.close()
        else:
            # SCD-3: Batch by adding a row-number column to staging and
            # processing _SCD3_BATCH_SIZE rows at a time via TOP + DELETE.
            # The staging table already has an index from create_staging_index().
            logger.info(
                "SCD-3/B-2: Batching Bronze UPDATE JOIN for %s — %d PKs in batches of %d "
                "(below 5K lock escalation threshold)",
                bronze_table, len(pks_to_close), _SCD3_BATCH_SIZE,
            )
            conn = connections.get_connection(db)
            try:
                batch_num = 0
                while True:
                    batch_num += 1
                    cursor = conn.cursor()
                    # UPDATE TOP(N) processes a bounded batch each iteration.
                    # The WHERE UdmActiveFlag=1 filter ensures already-processed
                    # rows aren't touched again, providing natural convergence.
                    cursor.execute(f"""
                        UPDATE TOP ({_SCD3_BATCH_SIZE}) t
                        SET t.UdmActiveFlag = 0, t.UdmEndDateTime = ?
                        FROM {bronze_table} t
                        INNER JOIN {staging_table} s ON {join_condition}
                        WHERE t.UdmActiveFlag = 1
                    """, end_dt)
                    batch_affected = cursor.rowcount
                    cursor.close()
                    total_affected += batch_affected

                    if batch_affected > 0:
                        logger.info(
                            "SCD-3: Batch %d closed %d Bronze rows in %s (%d total)",
                            batch_num, batch_affected, bronze_table, total_affected,
                        )

                    if batch_affected < _SCD3_BATCH_SIZE:
                        break  # All rows processed
            finally:
                conn.close()

        # P2-14: Verify actual vs expected UPDATE row count
        if total_affected != len(pks_to_close):
            if total_affected < len(pks_to_close):
                # C-2: Expected during retries — WHERE UdmActiveFlag=1 makes
                # this idempotent. Already-closed rows won't be touched.
                logger.info(
                    "P2-14: Bronze UPDATE affected %d rows (expected %d) in %s — "
                    "delta of %d (idempotent retry: already-closed rows from prior run)",
                    total_affected, len(pks_to_close), bronze_table,
                    len(pks_to_close) - total_affected,
                )
            else:
                logger.error(
                    "P2-14: Bronze UPDATE affected %d rows (expected %d) in %s — "
                    "MORE rows affected than expected. Investigate join conditions.",
                    total_affected, len(pks_to_close), bronze_table,
                )
        else:
            logger.info("Closed %d Bronze rows in %s", total_affected, bronze_table)
    finally:
        # Always drop staging table
        conn = connections.get_connection(db)
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            cursor.close()
        finally:
            conn.close()


def _activate_new_versions(
    bronze_table: str,
    effective_dt: datetime,
) -> int:
    """E-2/E-18: Activate newly-inserted SCD2 versions after closing old versions.

    New versions are inserted with UdmActiveFlag=0 (operation='U' or 'R') to avoid
    conflicting with the filtered unique index during the INSERT phase. After
    the UPDATE step closes old active versions, this step flips the new versions
    to active.

    Targets rows that are: UdmActiveFlag=0 AND UdmEndDateTime IS NULL
    (inserted but not yet activated, and not closed by another process).
    Matches both 'U' (updates) and 'R' (reactivations) operations.

    A crash after INSERT but before this activation leaves rows with
    UdmActiveFlag=0 and UdmEndDateTime IS NULL — detectable and recoverable
    on the next pipeline run.

    Args:
        bronze_table: Fully qualified Bronze table name.
        effective_dt: The effective datetime used for this batch's inserts.

    Returns:
        Number of rows activated.
    """
    db = bronze_table.split(".")[0]
    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE {bronze_table}
            SET UdmActiveFlag = 1
            WHERE UdmActiveFlag = 0
              AND UdmEndDateTime IS NULL
              AND UdmScd2Operation IN ('U', 'R')
              AND UdmEffectiveDateTime = ?
        """, effective_dt)
        activated = cursor.rowcount
        cursor.close()

        if activated > 0:
            logger.info(
                "E-2: Activated %d new SCD2 versions in %s",
                activated, bronze_table,
            )
        return activated
    except Exception:
        logger.exception(
            "E-2: Failed to activate new versions in %s — "
            "rows remain with UdmActiveFlag=0, UdmEndDateTime IS NULL. "
            "Will be recovered on next run.",
            bronze_table,
        )
        return 0
    finally:
        conn.close()


def _cleanup_orphaned_inactive_rows(
    bronze_table: str,
    table_config: TableConfig,
) -> int:
    """B-4: Clean up orphaned Flag=0 rows from prior crash recovery.

    After a crash between SCD2 INSERT (UdmActiveFlag=0) and activation,
    orphaned rows persist with UdmActiveFlag=0, UdmEndDateTime IS NULL,
    UdmScd2Operation IN ('U','R'). _activate_new_versions() only targets
    the current batch's UdmEffectiveDateTime, so orphaned rows from prior
    crashed runs are never activated.

    Safe to DELETE because:
    - Flag=0 rows are invisible to downstream consumers (WHERE UdmActiveFlag=1)
    - The current run will re-insert correct versions via normal SCD2 flow
    - Orphaned rows have no UdmEndDateTime (never closed), so they serve no
      historical purpose

    Args:
        bronze_table: Fully qualified Bronze table name.
        table_config: Table configuration for logging context.

    Returns:
        Number of orphaned rows deleted.
    """
    if not table_exists(bronze_table):
        return 0

    db = bronze_table.split(".")[0]

    try:
        # Check for orphaned rows
        conn = connections.get_connection(db)
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) FROM {bronze_table}
                WHERE UdmActiveFlag = 0
                  AND UdmEndDateTime IS NULL
                  AND UdmScd2Operation IN ('U', 'R')
            """)
            orphan_count = cursor.fetchone()[0]
            cursor.close()
        finally:
            conn.close()

        if orphan_count == 0:
            return 0

        logger.warning(
            "B-4: Found %d orphaned inactive rows in %s (UdmActiveFlag=0, "
            "UdmEndDateTime IS NULL, operation U/R). Likely from a prior crash "
            "between INSERT and activation. Deleting — current run will "
            "re-insert correct versions.",
            orphan_count, bronze_table,
        )

        # Delete in batches to avoid lock escalation (reuse B-2 batch size)
        batch_size = config.SCD2_UPDATE_BATCH_SIZE
        total_deleted = 0
        conn = connections.get_connection(db)
        try:
            while True:
                cursor = conn.cursor()
                cursor.execute(f"""
                    DELETE TOP ({batch_size}) FROM {bronze_table}
                    WHERE UdmActiveFlag = 0
                      AND UdmEndDateTime IS NULL
                      AND UdmScd2Operation IN ('U', 'R')
                """)
                deleted = cursor.rowcount
                cursor.close()
                total_deleted += deleted

                if deleted < batch_size:
                    break
        finally:
            conn.close()

        logger.info(
            "B-4: Deleted %d orphaned inactive rows from %s",
            total_deleted, bronze_table,
        )
        return total_deleted

    except Exception:
        logger.warning(
            "B-4: Could not clean up orphaned inactive rows in %s — "
            "continuing with normal SCD2 flow (non-fatal)",
            bronze_table, exc_info=True,
        )
        return 0


def _dedup_bronze_active(
    df_bronze: pl.DataFrame,
    pk_columns: list[str],
    table_config: TableConfig,
) -> pl.DataFrame:
    """P1-16: Deduplicate active Bronze rows per PK.

    After a crash in the INSERT-first SCD2 design (P0-8), a PK may have
    multiple active rows (both old and new version with UdmActiveFlag=1).
    Keep only the row with the latest UdmEffectiveDateTime per PK to
    prevent inflated join results in the comparison.
    """
    before_count = len(df_bronze)

    if "UdmEffectiveDateTime" in df_bronze.columns:
        df_bronze = (
            df_bronze
            .sort("UdmEffectiveDateTime", descending=True)
            .unique(subset=pk_columns, keep="first")
        )
    else:
        df_bronze = df_bronze.unique(subset=pk_columns, keep="first")

    dedup_count = before_count - len(df_bronze)
    if dedup_count > 0:
        logger.warning(
            "P1-16: Found %d duplicate active Bronze rows for %s.%s "
            "(likely from prior crash recovery). Deduplicated to %d rows.",
            dedup_count, table_config.source_name,
            table_config.source_object_name, len(df_bronze),
        )

    return df_bronze


# ---------------------------------------------------------------------------
# P1-3: Targeted SCD2 for large tables
# ---------------------------------------------------------------------------

def run_scd2_targeted(
    table_config: TableConfig,
    df_current: pl.DataFrame,
    pk_columns: list[str],
    output_dir: str | Path,
    deleted_pks: pl.DataFrame | None = None,
) -> SCD2Result:
    """Run SCD2 promotion with PK-targeted Bronze read (large tables).

    Instead of reading all active Bronze rows (impossible at 3B scale),
    this loads only the Bronze rows whose PKs exist in df_current. Uses
    a staging table + INNER JOIN for the targeted read.

    The comparison logic is identical to run_scd2() — only the Bronze
    read is different.

    P0-11: Accepts deleted_pks from windowed CDC. Without this, windowed
    deletes would never propagate to Bronze because the PK-targeted read
    only looks up PKs in df_current (which excludes deleted rows).

    Args:
        table_config: Table configuration.
        df_current: Current CDC rows (from windowed CDC result).
        pk_columns: Primary key columns for SCD2 business key.
        output_dir: Directory for staging CSV files.
        deleted_pks: PKs deleted in windowed CDC (from CDCResult.deleted_pks).

    Returns:
        SCD2Result with counts.
    """
    result = SCD2Result()
    now = datetime.now(timezone.utc)
    bronze_table = table_config.bronze_full_table_name

    if not pk_columns:
        logger.warning("No PK columns for %s — skipping targeted SCD2", table_config.source_object_name)
        return result

    if df_current is None or len(df_current) == 0:
        logger.info("No current CDC rows for %s — skipping targeted SCD2", table_config.source_object_name)
        return result

    # Source columns
    internal_cols = {
        "_row_hash", "_extracted_at",
        "_cdc_operation", "_cdc_valid_from", "_cdc_valid_to",
        "_cdc_is_current", "_cdc_batch_id",
        "_scd2_key",
        "UdmHash", "UdmEffectiveDateTime", "UdmEndDateTime",
        "UdmActiveFlag", "UdmScd2Operation",
    }
    source_cols = [c for c in df_current.columns if c not in internal_cols]

    # First run: Bronze doesn't exist yet — all rows are inserts
    if not table_exists(bronze_table):
        logger.info(
            "Bronze table %s doesn't exist — all %d rows are new inserts (targeted)",
            bronze_table, len(df_current),
        )
        result.inserts = len(df_current)
        df_insert = _build_scd2_insert(df_current, source_cols, pk_columns, now, "I")
        _write_and_load_bronze(df_insert, bronze_table, output_dir, table_config, "inserts")
        return result

    # B-4: Clean up orphaned inactive rows from prior crash recovery
    _cleanup_orphaned_inactive_rows(bronze_table, table_config)

    # Targeted Bronze read: only rows matching PKs in df_current
    pk_df = df_current.select(pk_columns).unique()
    df_bronze = read_bronze_for_pks(
        bronze_table, pk_columns, pk_df, output_dir, table_config,
    )
    logger.info("Targeted Bronze rows matching %d PKs: %d", len(pk_df), len(df_bronze))

    if len(df_bronze) == 0:
        result.inserts = len(df_current)
        df_insert = _build_scd2_insert(df_current, source_cols, pk_columns, now, "I")
        _write_and_load_bronze(df_insert, bronze_table, output_dir, table_config, "inserts")
        return result

    # P1-16: Deduplicate active Bronze rows (crash recovery)
    df_bronze = _dedup_bronze_active(df_bronze, pk_columns, table_config)

    # --- P0-12: Align PK dtypes before joins ---
    df_current, df_bronze = align_pk_dtypes(
        df_current, df_bronze, pk_columns, context="SCD2 run_scd2_targeted",
    )

    # --- Detect changes (same algorithm as run_scd2) ---

    df_cdc_keys = df_current.select(pk_columns + ["_row_hash"])
    df_bronze_keys = df_bronze.select(pk_columns + ["UdmHash"])

    # NEW INSERTS: in CDC but not in Bronze
    df_new = df_current.join(df_bronze_keys, on=pk_columns, how="anti")
    result.inserts = len(df_new)

    # CLOSES: in Bronze but not in CDC (within the targeted set)
    # For large tables, this only closes rows whose PKs were in the
    # extraction window. Rows outside the window are untouched.
    df_closed = df_bronze_keys.join(df_cdc_keys, on=pk_columns, how="anti")
    # C-5: Don't increment result.closes here — compute from deduplicated set below

    # CHANGED vs UNCHANGED
    df_matched = df_cdc_keys.join(
        df_bronze_keys,
        on=pk_columns,
        how="inner",
        suffix="_bronze",
    )

    # P0-10: NULL hash guards (same as run_scd2)
    changed_mask = (
        (pl.col("_row_hash") != pl.col("UdmHash"))
        | pl.col("_row_hash").is_null()
        | pl.col("UdmHash").is_null()
    )
    df_changed_pks = df_matched.filter(changed_mask).select(pk_columns)
    df_unchanged_pks = df_matched.filter(~changed_mask).select(pk_columns)

    result.new_versions = len(df_changed_pks)
    result.unchanged = len(df_unchanged_pks)

    # P0-12: Count validation
    accounted = result.inserts + result.new_versions + result.unchanged
    cdc_unique_pks = len(df_cdc_keys)
    if accounted != cdc_unique_pks:
        logger.error(
            "P0-12 COUNT MISMATCH in targeted SCD2 %s: "
            "inserts(%d) + new_versions(%d) + unchanged(%d) = %d, "
            "but CDC has %d unique PKs. Possible PK dtype mismatch.",
            table_config.source_object_name,
            result.inserts, result.new_versions, result.unchanged, accounted, cdc_unique_pks,
        )

    # C-5: Summary log moved after close set dedup below for accurate close count.

    # P0-8: INSERT first, THEN UPDATE. A crash after insert but before update
    # leaves duplicate active rows (recoverable via next run's comparison),
    # instead of zero active rows (data loss requiring manual recovery).

    # --- Step 1: INSERT — new rows + new versions + resurrections ---
    insert_parts: list[pl.DataFrame] = []
    targeted_resurrection_pks: pl.DataFrame | None = None  # E-18: set below if resurrections found
    if result.inserts > 0:
        insert_parts.append(_build_scd2_insert(df_new, source_cols, pk_columns, now, "I"))
    if result.new_versions > 0:
        df_new_ver = df_current.join(df_changed_pks, on=pk_columns, how="semi")
        insert_parts.append(_build_scd2_insert(df_new_ver, source_cols, pk_columns, now, "U"))

    # E-18: Resurrection inserts added after deleted_pks processing below
    # (need to detect resurrection PKs first before building inserts)

    # --- Step 2: UPDATE — close old versions + deletes ---
    pks_to_close_parts: list[pl.DataFrame] = []
    if len(df_closed) > 0:
        pks_to_close_parts.append(df_closed.select(pk_columns))
    if len(df_changed_pks) > 0:
        pks_to_close_parts.append(df_changed_pks)

    # P0-11: Include deleted PKs from windowed CDC. These PKs were removed
    # from the source within the extraction window. Without this, they'd stay
    # UdmActiveFlag=1 in Bronze permanently because the PK-targeted read
    # never loads them (they're not in df_current).
    if deleted_pks is not None and len(deleted_pks) > 0:
        # Align dtypes before concat
        if len(pks_to_close_parts) > 0:
            deleted_pks, _ = align_pk_dtypes(
                deleted_pks, pks_to_close_parts[0],
                pk_columns, context="SCD2 deleted_pks alignment",
            )

        # E-6/E-18: Resurrection detection — filter out PKs that appear in both
        # deleted_pks (windowed CDC detected as deleted) and df_current
        # (present in the current extraction). These are resurrected rows —
        # a PK was deleted from source in one window but re-inserted in another.
        # Without this filter, the close UPDATE would close the just-inserted
        # new version, leaving zero active rows for that PK.
        # E-18: Resurrected PKs get UdmScd2Operation='R' for audit trail.
        deleted_pks_filtered = deleted_pks.select(pk_columns)
        targeted_resurrection_pks = deleted_pks_filtered.join(
            df_current.select(pk_columns), on=pk_columns, how="semi"
        )
        if len(targeted_resurrection_pks) > 0:
            logger.info(
                "E-6/E-18: %d resurrected PKs detected in %s (in deleted_pks AND "
                "df_current). Removing from close set — these rows will be "
                "inserted with UdmScd2Operation='R' for audit trail.",
                len(targeted_resurrection_pks), table_config.source_object_name,
            )
            deleted_pks_filtered = deleted_pks_filtered.join(
                targeted_resurrection_pks, on=pk_columns, how="anti"
            )

        if len(deleted_pks_filtered) > 0:
            pks_to_close_parts.append(deleted_pks_filtered)
            logger.info(
                "P0-11: Adding %d deleted PKs from windowed CDC to Bronze close set for %s",
                len(deleted_pks_filtered), table_config.source_object_name,
            )

    # E-18: Build resurrection inserts (after targeted_resurrection_pks is known)
    if targeted_resurrection_pks is not None and len(targeted_resurrection_pks) > 0:
        df_resurrected = df_current.join(targeted_resurrection_pks, on=pk_columns, how="semi")
        insert_parts.append(_build_scd2_insert(df_resurrected, source_cols, pk_columns, now, "R"))
        result.inserts += len(df_resurrected)
        # Close the old versions for resurrected PKs too
        pks_to_close_parts.append(targeted_resurrection_pks)

    # P0-8: INSERT first, THEN UPDATE (moved here after resurrection detection)
    if insert_parts:
        # T-2: Use _safe_concat to avoid diagonal_relaxed crash bugs.
        df_all_inserts = _safe_concat(insert_parts)
        # P1-17 + C-3: Validate NULLs and fix dtype drift.
        df_all_inserts = _validate_concat_columns(df_all_inserts, df_current, table_config)
        _write_and_load_bronze(df_all_inserts, bronze_table, output_dir, table_config, "inserts")

    if pks_to_close_parts:
        # W-7: Validate PK schemas match before concat.
        validate_schema_before_concat(
            pks_to_close_parts, f"targeted SCD2 close PKs for {table_config.source_object_name}")
        pre_dedup_count = sum(len(p) for p in pks_to_close_parts)
        pks_to_close = pl.concat(pks_to_close_parts).unique()
        # C-5: Compute closes from the deduplicated set, not the sum of individual sources
        result.closes = len(pks_to_close)
        dedup_delta = pre_dedup_count - len(pks_to_close)
        if dedup_delta > 0:
            logger.info(
                "C-5: Deduplicated %d overlapping PKs from close set for %s "
                "(appeared in multiple sources: closed+changed+deleted)",
                dedup_delta, table_config.source_object_name,
            )
        _execute_bronze_updates(pks_to_close, pk_columns, bronze_table, now, output_dir, table_config)

    # --- Step 3: ACTIVATE — flip new versions to active (E-2/E-18) ---
    # New versions (operation="U"/"R") were inserted with UdmActiveFlag=0 to avoid
    # conflicting with the filtered unique index. Now that old versions are closed,
    # activate the new versions.
    has_resurrections = targeted_resurrection_pks is not None and len(targeted_resurrection_pks) > 0
    if result.new_versions > 0 or has_resurrections:
        _activate_new_versions(bronze_table, now)

    logger.info(
        "Targeted SCD2 %s: inserts=%d, new_versions=%d, closes=%d, unchanged=%d",
        table_config.source_object_name,
        result.inserts, result.new_versions, result.closes, result.unchanged,
    )

    # V-4: Post-SCD2 duplicate active row check (non-blocking diagnostic).
    _check_duplicate_active_rows(bronze_table, pk_columns, table_config)

    return result

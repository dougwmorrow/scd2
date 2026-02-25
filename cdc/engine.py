"""Polars CDC: hash comparison, detect inserts/updates/deletes.

Provides two modes:
  - run_cdc(): Full comparison (small tables). Detects I/U/D across all rows.
  - run_cdc_windowed(): Date-scoped comparison (large tables). Only compares rows
    within the extraction window. Delete detection scoped to window (P1-4).

Algorithm:
  - Filter rows with NULL PKs (P0-4: prevent duplicate inserts)
  - Anti-join on PKs (inserts): rows in fresh but not in existing
  - Inner-join + hash compare (updates/unchanged): rows in both, hash changed vs same
  - Reverse anti-join (deletes): rows in existing but not in fresh (window-scoped for large)

CDC columns: _cdc_operation (I/U/D), _cdc_valid_from/to, _cdc_is_current, _cdc_batch_id

H-4 NOTE — No-op source updates:
  Oracle no-op updates (UPDATE SET col=col) and SQL Server no-op updates still
  generate redo/undo and change tracking metadata. These rows are extracted and
  compared, but the hash comparison correctly identifies them as unchanged (same
  hash before and after). The extraction work is wasted but harmless — no data
  corruption, no false positives. Accept as-is.

T-3 NOTE — Oracle metadata-only DEFAULT changes:
  Oracle 11g+ metadata-only DEFAULT values are transparent to SELECT * — existing
  rows return the default without physical updates. If the DEFAULT definition
  changes, old rows return the old default, new rows the new one. The pipeline
  correctly detects this as a real data difference (hash changes). Expect a
  one-time CDC update surge after any source DEFAULT change. This is correct
  behavior, not a bug.

C-6 KNOWN LIMITATION — INSERT-first timing gap:
  The P0-9 crash safety design creates a deliberate window where duplicate
  _cdc_is_current=1 rows exist in Stage:
    t0: CDC INSERT new versions (BCP load) — both old and new are current=1
    t1: CDC expire old versions (UPDATE JOIN) — old versions set to current=0
  The window between t0 and t1 is 30-60 seconds for typical loads. Any consumer
  reading Stage during this window sees duplicate current rows per affected PK.
  Downstream consumers should either:
    (a) Wait for pipeline completion (use PipelineEventLog as trigger)
    (b) Query with GROUP BY pk_columns HAVING COUNT(*) = 1
    (c) Tolerate the transient duplicates

B-5 AUDIT (2026-02-23) — Polars join validation bug (#19624):
  Audited all .join() calls in this module. None use the `validate` parameter.
  All joins use only `on`, `how`, and `suffix`. Polars #19624 (false errors when
  `validate` is used with NULL keys) does not apply. Additionally,
  `_filter_null_pks()` runs before all CDC joins, so NULL PKs are never present
  in join inputs. No action required.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import json

import polars as pl

import connections
from connections import cursor_for, quote_identifier, quote_table
from data_load import bcp_loader
from data_load.bcp_csv import validate_schema_before_concat, write_bcp_csv
from data_load.sanitize import cast_bit_columns, reorder_columns_for_bcp, sanitize_strings
from data_load.schema_utils import align_pk_dtypes, get_column_types
from extract.udm_connectorx_extractor import (
    read_stage_table,
    read_stage_table_windowed,
    table_exists,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


@dataclass
class CDCResult:
    """Results from CDC comparison."""

    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    unchanged: int = 0
    null_pk_rows: int = 0
    df_current: pl.DataFrame | None = None
    pk_columns: list[str] | None = None
    # P0-11: Deleted PKs from windowed CDC, needed for targeted SCD2 Bronze close.
    # Only populated by run_cdc_windowed() — run_cdc() doesn't need it because
    # run_scd2() reads ALL active Bronze rows and catches deletes via anti-join.
    deleted_pks: pl.DataFrame | None = None


@dataclass
class CDCContext:
    """Parameterizes differences between full and windowed CDC."""

    read_existing: Callable[[], pl.DataFrame]
    track_deleted_pks: bool
    log_label: str   # "CDC" or "Windowed CDC"
    log_window: str  # "" or " [2025-02-15, 2025-02-16)"


def run_cdc(
    table_config: TableConfig,
    df_fresh: pl.DataFrame,
    batch_id: int,
    output_dir: str | Path,
) -> CDCResult:
    """Run CDC comparison: detect inserts, updates, and deletes.

    Args:
        table_config: Table configuration with PK columns.
        df_fresh: Fresh extraction DataFrame (already has _row_hash, _extracted_at).
        batch_id: Pipeline batch ID for _cdc_batch_id.
        output_dir: Directory for staging CSV files.

    Returns:
        CDCResult with counts and df_current (all current CDC rows after changes applied).
    """
    stage_table = table_config.stage_full_table_name
    ctx = CDCContext(
        read_existing=lambda: read_stage_table(stage_table),
        track_deleted_pks=False,
        log_label="CDC",
        log_window="",
    )
    return _run_cdc_core(table_config, df_fresh, batch_id, output_dir, ctx)


def _run_cdc_core(
    table_config: TableConfig,
    df_fresh: pl.DataFrame,
    batch_id: int,
    output_dir: str | Path,
    ctx: CDCContext,
) -> CDCResult:
    """Shared CDC engine for both full and windowed modes.

    Called by run_cdc() and run_cdc_windowed() — not intended for direct use.
    Differences between full and windowed CDC are parameterized via CDCContext.
    """
    result = CDCResult()
    pk_columns = table_config.pk_columns
    result.pk_columns = pk_columns
    now = datetime.now(timezone.utc)

    stage_table = table_config.stage_full_table_name

    if not pk_columns:
        logger.warning("No PK columns for %s — skipping %s", table_config.source_object_name, ctx.log_label)
        return result

    # S-4: Validate PK columns exist in df_fresh before proceeding
    missing_pks = [c for c in pk_columns if c not in df_fresh.columns]
    if missing_pks:
        logger.error(
            "S-4: PK columns %s not found in extraction for %s.%s. "
            "Available columns: %s. This may indicate a source schema change. "
            "Use --refresh-pks to re-discover PKs from the source.",
            missing_pks, table_config.source_name,
            table_config.source_object_name, df_fresh.columns,
        )
        return result

    # --- P0-4: Filter rows with NULL PKs ---
    df_fresh = _filter_null_pks(df_fresh, pk_columns, table_config, result)

    if len(df_fresh) == 0:
        logger.warning(
            "No rows remaining after NULL PK filter for %s — skipping %s",
            table_config.source_object_name, ctx.log_label,
        )
        return result

    # --- S-1: Source PK duplicate guard ---
    df_fresh = _dedup_source_pks(df_fresh, pk_columns, table_config)

    # First run: table doesn't exist yet — all rows are inserts
    if not table_exists(stage_table):
        logger.info(
            "Stage table %s doesn't exist — all %d rows are inserts%s",
            stage_table, len(df_fresh), ctx.log_window,
        )
        result.inserts = len(df_fresh)

        df_inserts = _add_cdc_columns(df_fresh, "I", now, batch_id)
        result.df_current = df_inserts

        _write_and_load_cdc(df_inserts, stage_table, output_dir, table_config, "inserts")
        return result

    # Read existing current rows from Stage
    df_existing = ctx.read_existing()
    logger.info("Existing CDC current rows%s: %d", ctx.log_window, len(df_existing))

    # L-1: Deduplicate Stage current rows (crash recovery may leave duplicates)
    df_existing = _dedup_stage_current(df_existing, pk_columns, table_config)

    if len(df_existing) == 0:
        # No current rows — all fresh rows are inserts
        result.inserts = len(df_fresh)
        df_inserts = _add_cdc_columns(df_fresh, "I", now, batch_id)
        result.df_current = df_inserts
        _write_and_load_cdc(df_inserts, stage_table, output_dir, table_config, "inserts")
        return result

    # --- P0-12: Align PK dtypes before joins ---
    df_fresh, df_existing = align_pk_dtypes(
        df_fresh, df_existing, pk_columns, context=ctx.log_label,
    )

    # --- Detect changes ---

    # P-6: Use lazy + streaming engine for anti-joins to reduce peak memory.
    # Polars v1.32+ supports native streaming anti-joins (PR #21937).
    # The streaming engine uses morsel-driven parallelism, avoiding
    # materialization of the full join result in memory.

    # INSERTS: rows in fresh but not in existing (anti-join on PKs)
    df_new = (
        df_fresh.lazy()
        .join(df_existing.lazy(), on=pk_columns, how="anti")
        .collect(engine="streaming")
    )
    result.inserts = len(df_new)

    # DELETES: rows in existing but not in fresh (reverse anti-join)
    # P1-4 (windowed): Only detects deletes WITHIN the extraction window,
    # not rows outside the window. Rows older than the window are untouched.
    df_deleted = (
        df_existing.lazy()
        .join(df_fresh.lazy(), on=pk_columns, how="anti")
        .collect(engine="streaming")
    )
    result.deletes = len(df_deleted)

    # P0-11: Capture deleted PKs for targeted SCD2 Bronze close (windowed only).
    # Without this, windowed deletes never propagate to Bronze because
    # run_scd2_targeted() only reads Bronze rows for PKs in df_current
    # (which excludes deleted PKs).
    if ctx.track_deleted_pks and result.deletes > 0:
        result.deleted_pks = df_deleted.select(pk_columns)

    # UPDATES vs UNCHANGED: inner join on PKs, compare _row_hash
    df_matched = df_fresh.join(
        df_existing.select(pk_columns + ["_row_hash"]),
        on=pk_columns,
        how="inner",
        suffix="_existing",
    )

    # P0-10: NULL hash guards — treat NULL hashes as "changed" to prevent
    # silent misclassification when hashes are missing (partial load, non-pipeline path).
    changed_mask = (
        (pl.col("_row_hash") != pl.col("_row_hash_existing"))
        | pl.col("_row_hash").is_null()
        | pl.col("_row_hash_existing").is_null()
    )
    df_updated = df_matched.filter(changed_mask).drop("_row_hash_existing")
    df_unchanged = df_matched.filter(~changed_mask).drop("_row_hash_existing")

    result.updates = len(df_updated)
    result.unchanged = len(df_unchanged)

    # P0-12: Count validation — all fresh rows must be accounted for
    accounted = result.inserts + result.updates + result.unchanged
    if accounted != len(df_fresh):
        logger.error(
            "P0-12 COUNT MISMATCH in %s %s%s: "
            "inserts(%d) + updates(%d) + unchanged(%d) = %d, "
            "but fresh has %d rows. Possible PK dtype mismatch causing silent join failure.",
            ctx.log_label, table_config.source_object_name, ctx.log_window,
            result.inserts, result.updates, result.unchanged, accounted, len(df_fresh),
        )

    logger.info(
        "%s %s%s: inserts=%d, updates=%d, deletes=%d, unchanged=%d",
        ctx.log_label, table_config.source_object_name, ctx.log_window,
        result.inserts, result.updates, result.deletes, result.unchanged,
    )
    # O-2: Structured JSON for downstream alerting without regex parsing.
    logger.info(
        "O-2_CDC: %s",
        json.dumps({
            "signal": "cdc_result",
            "source": table_config.source_name,
            "table": table_config.source_object_name,
            "mode": ctx.log_label.lower().replace(" ", "_"),
            "inserts": result.inserts,
            "updates": result.updates,
            "deletes": result.deletes,
            "unchanged": result.unchanged,
            "null_pk_rows": result.null_pk_rows,
            "total_fresh": len(df_fresh),
        }),
    )

    # --- Apply changes ---
    # P0-9: INSERT first, THEN expire. A crash after insert but before expire
    # leaves duplicate "current" rows (recoverable), instead of zero current
    # rows (data loss that cascades as mass re-insert + SCD2 re-versioning).

    # P2-7: Build CDC-annotated DataFrames once and reuse for both changes
    # list and current_parts to avoid duplicate memory allocation.
    df_insert_cdc = _add_cdc_columns(df_new, "I", now, batch_id) if result.inserts > 0 else None
    df_update_cdc = _add_cdc_columns(df_updated, "U", now, batch_id) if result.updates > 0 else None

    # 1. Insert new CDC rows (inserts + updates) with _cdc_is_current=1
    changes: list[pl.DataFrame] = []
    if df_insert_cdc is not None:
        changes.append(df_insert_cdc)
    if df_update_cdc is not None:
        changes.append(df_update_cdc)

    if changes:
        # W-7: Validate schemas match before concat (prevent silent type coercion).
        validate_schema_before_concat(changes, f"{ctx.log_label} changes for {table_config.source_object_name}")
        df_changes = pl.concat(changes)
        _write_and_load_cdc(df_changes, stage_table, output_dir, table_config, "changes")

    # 2. Mark expired rows (updates + deletes) as _cdc_is_current=0
    if result.updates > 0 or result.deletes > 0:
        if result.updates > 0 and result.deletes > 0:
            pk_parts = [df_updated.select(pk_columns), df_deleted.select(pk_columns)]
            validate_schema_before_concat(pk_parts, f"{ctx.log_label} expire PKs for {table_config.source_object_name}")
            expired_pks = pl.concat(pk_parts)
        else:
            expired_pks = df_updated.select(pk_columns) if result.updates > 0 else df_deleted.select(pk_columns)
        _expire_cdc_rows(expired_pks, pk_columns, stage_table, now, output_dir, table_config)

    # Build df_current: unchanged existing rows + new inserts + updated rows
    current_parts = []
    if result.unchanged > 0:
        # Keep existing CDC columns for unchanged rows
        unchanged_existing = df_existing.join(df_unchanged, on=pk_columns, how="semi")
        current_parts.append(unchanged_existing)
    if df_insert_cdc is not None:
        current_parts.append(df_insert_cdc)
    if df_update_cdc is not None:
        current_parts.append(df_update_cdc)

    if current_parts:
        # T-2: Use _safe_concat to avoid diagonal_relaxed crash bugs
        # (Polars #12543, #18911). Pre-aligns schemas before vertical concat.
        result.df_current = _safe_concat(current_parts)
        # P0-7 + C-3: Validate NULLs and fix dtype drift.
        result.df_current = _validate_concat_columns(result.df_current, df_fresh, table_config)
        # W-12: Release over-allocated memory after filter/join/concat operations.
        if len(result.df_current) > 100_000:
            result.df_current.shrink_to_fit(in_place=True)
    else:
        result.df_current = pl.DataFrame()

    return result


# ---------------------------------------------------------------------------
# P0-4: NULL PK filter
# ---------------------------------------------------------------------------

# V-13: Percentage threshold for NULL PK escalation to ERROR.
# If more than this fraction of extracted rows have NULL PKs, escalate to ERROR.
NULL_PK_ERROR_THRESHOLD = 0.01  # 1%


def _filter_null_pks(
    df: pl.DataFrame,
    pk_columns: list[str],
    table_config: TableConfig,
    result: CDCResult,
) -> pl.DataFrame:
    """Filter out rows with NULL values in PK columns.

    NULL PKs cause Polars anti-join to always classify those rows as inserts
    (NULL != NULL), creating duplicates every run.

    V-13: Escalates to ERROR if NULL PKs exceed 1% of total rows (likely
    source data quality issue rather than occasional NULLs).
    """
    null_mask = pl.lit(False)
    for col in pk_columns:
        null_mask = null_mask | pl.col(col).is_null()

    null_count = df.filter(null_mask).height

    if null_count > 0:
        # P3-12: Per-column NULL breakdown for actionable debugging
        null_breakdown = {}
        for col in pk_columns:
            col_nulls = df[col].null_count()
            if col_nulls > 0:
                null_breakdown[col] = col_nulls

        # V-13: Percentage-based escalation — if NULLs exceed threshold,
        # this is likely a source data quality problem, not occasional NULLs.
        total_rows = len(df)
        null_pct = null_count / total_rows if total_rows > 0 else 0

        if null_pct > NULL_PK_ERROR_THRESHOLD:
            logger.error(
                "V-13: %d rows (%.2f%%) with NULL PK columns in %s.%s — "
                "exceeds %.0f%% threshold. Likely source data quality issue. "
                "Filtering out to prevent duplicate inserts. "
                "PK columns checked: %s. NULL breakdown: %s",
                null_count, null_pct * 100,
                table_config.source_name,
                table_config.source_object_name,
                NULL_PK_ERROR_THRESHOLD * 100,
                pk_columns,
                null_breakdown,
            )
        else:
            logger.warning(
                "Found %d rows (%.2f%%) with NULL PK columns in %s.%s — "
                "filtering out to prevent duplicate inserts. "
                "PK columns checked: %s. NULL breakdown: %s",
                null_count, null_pct * 100,
                table_config.source_name,
                table_config.source_object_name,
                pk_columns,
                null_breakdown,
            )

        # P3-12: Log sample of filtered rows (first 5) for debugging
        df_nulls = df.filter(null_mask)
        if len(df_nulls) > 0:
            sample = df_nulls.head(5).select(pk_columns)
            logger.info(
                "P3-12: Sample NULL PK rows for %s.%s (first %d of %d): %s",
                table_config.source_name, table_config.source_object_name,
                min(5, len(df_nulls)), null_count,
                sample.to_dicts(),
            )

        result.null_pk_rows = null_count
        df = df.filter(~null_mask)

    return df


# ---------------------------------------------------------------------------
# S-1: Source PK duplicate guard
# ---------------------------------------------------------------------------

def _dedup_source_pks(
    df: pl.DataFrame,
    pk_columns: list[str],
    table_config: TableConfig,
) -> pl.DataFrame:
    """S-1: Detect and deduplicate source rows with non-unique PKs.

    Non-unique PKs in source data cause Cartesian products in inner-joins,
    leading to compounding duplicates in Stage and Bronze. Deduplicate by
    keeping the last occurrence (arbitrary but stable).
    """
    before_count = len(df)
    df_deduped = df.unique(subset=pk_columns, keep="last")
    dup_count = before_count - len(df_deduped)

    if dup_count > 0:
        # Sample the duplicate PKs for debugging
        dup_pks = (
            df.group_by(pk_columns)
            .len()
            .filter(pl.col("len") > 1)
        )
        sample = dup_pks.head(5).drop("len").to_dicts()

        logger.error(
            "S-1: Source data for %s.%s has %d duplicate PK rows "
            "(%d unique PKs affected). Deduplicating to prevent compounding "
            "duplicates in Stage/Bronze. Sample duplicate PKs: %s",
            table_config.source_name, table_config.source_object_name,
            dup_count, len(dup_pks), sample,
        )
        return df_deduped

    return df


# ---------------------------------------------------------------------------
# L-1: Stage duplicate current row dedup
# ---------------------------------------------------------------------------

def _dedup_stage_current(
    df_existing: pl.DataFrame,
    pk_columns: list[str],
    table_config: TableConfig,
) -> pl.DataFrame:
    """L-1: Deduplicate Stage _cdc_is_current=1 rows by PK.

    The P0-8/P0-9 INSERT-first crash safety design can leave duplicate
    current rows after a crash (old + new version both _cdc_is_current=1).
    Keep the row with the latest _cdc_valid_from per PK to prevent
    Cartesian products in the inner-join hash comparison.
    """
    before_count = len(df_existing)

    if "_cdc_valid_from" in df_existing.columns:
        df_existing = (
            df_existing
            .sort("_cdc_valid_from", descending=True)
            .unique(subset=pk_columns, keep="first")
        )
    else:
        df_existing = df_existing.unique(subset=pk_columns, keep="first")

    dedup_count = before_count - len(df_existing)
    if dedup_count > 0:
        logger.warning(
            "L-1: Found %d duplicate _cdc_is_current=1 Stage rows for %s.%s "
            "(likely from prior crash recovery — P0-8/P0-9 INSERT-first design). "
            "Deduplicated to %d rows before CDC comparison.",
            dedup_count, table_config.source_name,
            table_config.source_object_name, len(df_existing),
        )

    return df_existing


# ---------------------------------------------------------------------------
# P0-7: Post-concat validation
# ---------------------------------------------------------------------------

def _safe_concat(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    """T-2: Safe concat that pre-aligns schemas to avoid diagonal_relaxed crashes.

    Polars issues #12543 and #18911 document full interpreter crashes (not
    exceptions — process kills) with diagonal_relaxed when DataFrames have
    large column count differences. This is more likely after schema evolution.

    W-7: Logs schema mismatches before alignment (informational, not blocking —
    _safe_concat is designed to handle schema differences gracefully).

    Pre-aligns all DataFrames to the same column set and uses vertical concat
    instead of diagonal_relaxed.
    """
    if len(dfs) == 0:
        return pl.DataFrame()
    if len(dfs) == 1:
        return dfs[0]

    # W-7: Log any schema mismatches before alignment (informational).
    reference = dfs[0].schema
    for i, df in enumerate(dfs[1:], 1):
        if df.schema != reference:
            mismatches = {
                col: (reference.get(col), df.schema.get(col))
                for col in set(reference) | set(df.schema)
                if reference.get(col) != df.schema.get(col)
            }
            logger.warning(
                "W-7: Schema mismatch in _safe_concat at index %d: %s. "
                "Aligning schemas before vertical concat.",
                i, mismatches,
            )

    # Collect all columns and their dtypes (first seen dtype wins)
    all_columns: dict[str, pl.DataType] = {}
    for df in dfs:
        for col in df.columns:
            if col not in all_columns:
                all_columns[col] = df[col].dtype

    # Align each DataFrame to have all columns in consistent order
    col_order = list(all_columns.keys())
    aligned = []
    for df in dfs:
        missing = [
            pl.lit(None).cast(all_columns[col]).alias(col)
            for col in col_order
            if col not in df.columns
        ]
        if missing:
            df = df.with_columns(missing)
        aligned.append(df.select(col_order))

    return pl.concat(aligned)


def _validate_concat_columns(
    df_current: pl.DataFrame,
    df_fresh: pl.DataFrame,
    table_config: TableConfig,
) -> pl.DataFrame:
    """Validate and fix diagonal_relaxed concat side-effects.

    P0-7: Warns if concat introduced unexpected NULLs.
    C-3: Detects and corrects dtype widening that would change hash representations
         (e.g., Int64 widened to Utf8 changes how values are hashed).

    Returns:
        df_current, potentially with dtype corrections applied.
    """
    source_cols = [c for c in df_fresh.columns if not c.startswith("_")]
    dtype_corrections: list[pl.Expr] = []

    for col in source_cols:
        if col not in df_current.columns:
            logger.warning(
                "P0-7: Column [%s] from fresh extraction is missing in df_current "
                "after concat for %s.%s — schema evolution may have caused column mismatch",
                col, table_config.source_name, table_config.source_object_name,
            )
            continue

        # C-3: Check for dtype drift from diagonal_relaxed widening
        fresh_dtype = df_fresh[col].dtype
        current_dtype = df_current[col].dtype
        if fresh_dtype != current_dtype:
            logger.warning(
                "C-3: Column [%s] dtype changed from %s to %s after diagonal_relaxed "
                "concat for %s.%s — casting back to prevent hash representation drift",
                col, fresh_dtype, current_dtype,
                table_config.source_name, table_config.source_object_name,
            )
            dtype_corrections.append(pl.col(col).cast(fresh_dtype, strict=False))

        # P0-7: Check for NULL inflation
        fresh_nulls = df_fresh[col].null_count()
        current_nulls = df_current[col].null_count()

        if current_nulls > fresh_nulls and len(df_current) > 0:
            excess_nulls = current_nulls - fresh_nulls
            if excess_nulls > 0:
                logger.warning(
                    "P0-7: Column [%s] in df_current has %d more NULLs than fresh "
                    "extraction for %s.%s — diagonal_relaxed may have introduced "
                    "NULLs from schema-mismatched rows. This may cause hash "
                    "mismatches and SCD2 re-versioning.",
                    col, excess_nulls,
                    table_config.source_name, table_config.source_object_name,
                )

    # C-3: Apply dtype corrections if any were needed
    if dtype_corrections:
        df_current = df_current.with_columns(dtype_corrections)
        logger.info(
            "C-3: Corrected %d column dtypes after diagonal_relaxed concat for %s.%s",
            len(dtype_corrections), table_config.source_name,
            table_config.source_object_name,
        )

    return df_current


# ---------------------------------------------------------------------------
# CDC column helpers
# ---------------------------------------------------------------------------

def _add_cdc_columns(
    df: pl.DataFrame,
    operation: str,
    valid_from: datetime,
    batch_id: int,
) -> pl.DataFrame:
    """Add CDC tracking columns to a DataFrame."""
    return df.with_columns(
        pl.lit(operation).alias("_cdc_operation"),
        pl.lit(valid_from).alias("_cdc_valid_from"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("_cdc_valid_to"),
        pl.lit(1).cast(pl.Int8).alias("_cdc_is_current"),
        pl.lit(batch_id).alias("_cdc_batch_id"),
    )


def _write_and_load_cdc(
    df: pl.DataFrame,
    stage_table: str,
    output_dir: str | Path,
    table_config: TableConfig,
    label: str,
) -> None:
    """Write CDC rows to CSV and BCP load into Stage table."""
    df = sanitize_strings(df)
    df = cast_bit_columns(df)

    # P0-1: Reorder columns to match target table positional order
    if table_exists(stage_table):
        df = reorder_columns_for_bcp(df, stage_table)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table_config.source_object_name}_cdc_{label}.csv",
    )
    # E-3: Stage loads use atomic=False — Stage is truncated before each load,
    # so partial loads are harmless and -b batching improves performance.
    bcp_loader.bcp_load(str(csv_path), stage_table, expected_row_count=len(df), atomic=False)


def _expire_cdc_rows(
    expired_pks: pl.DataFrame,
    pk_columns: list[str],
    stage_table: str,
    valid_to: datetime,
    output_dir: str | Path,
    table_config: TableConfig,
) -> None:
    """Expire existing CDC rows by UPDATE via staging table.

    SCD-4 NOTE: Uses cursor.execute() (single statement), NOT executemany().
    pyodbc issue #481 confirms rowcount returns -1 after executemany().
    """
    db = stage_table.split(".")[0]
    schema = stage_table.split(".")[1]
    staging_table = f"{db}.{schema}._staging_expire_{table_config.source_object_name}"
    q_staging = quote_table(staging_table)

    # P0-3: Use actual PK column types from target table instead of NVARCHAR(MAX)
    pk_types = get_column_types(stage_table, pk_columns)

    pk_col_defs = ", ".join(f"{quote_identifier(c)} {pk_types[c]}" for c in pk_columns)
    with cursor_for(db) as cur:
        cur.execute(f"IF OBJECT_ID(?, 'U') IS NOT NULL DROP TABLE {q_staging}", staging_table)
        cur.execute(f"CREATE TABLE {q_staging} ({pk_col_defs})")

    try:
        # Write PKs to CSV — keep native dtypes (don't cast to Utf8)
        expired_pks_clean = sanitize_strings(expired_pks)
        csv_path = write_bcp_csv(
            expired_pks_clean,
            Path(output_dir) / f"{table_config.source_name}_{table_config.source_object_name}_expire_pks.csv",
        )
        # E-3: Staging tables are ephemeral — atomic=False for performance.
        bcp_loader.bcp_load(str(csv_path), staging_table, atomic=False)

        # P2-5: Index staging table for efficient JOIN against large Stage tables
        bcp_loader.create_staging_index(staging_table, pk_columns, row_count=len(expired_pks))

        # UPDATE join to expire rows
        join_condition = " AND ".join(
            f"t.{quote_identifier(c)} = s.{quote_identifier(c)}" for c in pk_columns
        )
        q_stage = quote_table(stage_table)

        with cursor_for(db) as cur:
            cur.execute(f"""
                UPDATE t
                SET t._cdc_is_current = 0, t._cdc_valid_to = ?
                FROM {q_stage} t
                INNER JOIN {q_staging} s ON {join_condition}
                WHERE t._cdc_is_current = 1
            """, valid_to)

            # P2-14: Verify actual vs expected UPDATE row count
            actual_rows = cur.rowcount

        if actual_rows != len(expired_pks):
            if actual_rows < len(expired_pks):
                # C-2: This is expected during retries of partially failed days
                # (checkpoint was EXTRACTED or FAILED_CDC). The WHERE _cdc_is_current=1
                # filter makes the UPDATE idempotent — already-expired rows won't be touched.
                logger.info(
                    "P2-14: CDC expire affected %d rows (expected %d) in %s — "
                    "delta of %d (idempotent retry: already-expired rows from prior run)",
                    actual_rows, len(expired_pks), stage_table,
                    len(expired_pks) - actual_rows,
                )
            else:
                logger.error(
                    "P2-14: CDC expire affected %d rows (expected %d) in %s — "
                    "MORE rows affected than expected. Investigate join conditions.",
                    actual_rows, len(expired_pks), stage_table,
                )
        else:
            logger.info("Expired %d CDC rows in %s", actual_rows, stage_table)
    finally:
        # Always drop staging table — prevents orphaned tables on exception
        with cursor_for(db) as cur:
            cur.execute(f"DROP TABLE IF EXISTS {q_staging}")


# ---------------------------------------------------------------------------
# L-2: Stage purge utility
# ---------------------------------------------------------------------------

# Default retention period for expired CDC rows (days).
_PURGE_RETENTION_DAYS = 30
# Batch size for batched DELETEs to avoid transaction log bloat.
_PURGE_BATCH_SIZE = 100_000


def purge_expired_cdc_rows(
    stage_table: str,
    retention_days: int = _PURGE_RETENTION_DAYS,
    batch_size: int = _PURGE_BATCH_SIZE,
) -> int:
    """L-2: Delete expired CDC rows older than retention_days.

    Expired rows (_cdc_is_current=0) accumulate over time since CDC only
    marks them as expired but never removes them. This utility removes
    old expired rows in batches to avoid transaction log bloat.

    The retention_days must exceed LookbackDays for any table using the
    Stage table to avoid deleting rows that might be needed for gap
    reprocessing.

    Args:
        stage_table: Full Stage table name (e.g., 'UDM_Stage.DNA.ACCT_cdc').
        retention_days: Keep expired rows newer than this many days.
        batch_size: DELETE TOP N per iteration to limit log growth.

    Returns:
        Total number of rows purged.
    """
    db = stage_table.split(".")[0]
    total_purged = 0
    q_stage = quote_table(stage_table)

    # Item-15: Use cursor_for() per batch — each batch gets a fresh connection.
    # Prevents a single dropped connection from failing the entire purge loop.
    while True:
        with cursor_for(db) as cur:
            cur.execute(f"""
                DELETE TOP (?) FROM {q_stage}
                WHERE _cdc_is_current = 0
                  AND _cdc_valid_to < DATEADD(day, -?, GETUTCDATE())
            """, batch_size, retention_days)
            deleted = cur.rowcount
        total_purged += deleted

        if deleted < batch_size:
            break  # No more rows to purge

        logger.info(
            "L-2: Purged %d expired CDC rows from %s (%d total so far)",
            deleted, stage_table, total_purged,
        )

    if total_purged > 0:
        logger.info(
            "L-2: Purge complete for %s — removed %d expired CDC rows "
            "(retention=%d days)",
            stage_table, total_purged, retention_days,
        )

    return total_purged


# ---------------------------------------------------------------------------
# P1-3/P1-4: Windowed CDC for large tables
#
# P3-9 KNOWN LIMITATION: Late-arriving data beyond the lookback window is
# invisible to windowed CDC. If a source system backdates a correction to a
# date older than LookbackDays, the change will not be detected. The periodic
# reconciliation (P3-4 in reconciliation.py) catches this on a weekly basis.
#
# L-5 KNOWN LIMITATION: Date column value migration creates phantom deletes
# and phantom inserts. When a source system changes a row's date column value
# (e.g., order effective_date moved from Jan 1 to Jan 15), windowed CDC sees
# the row disappear from the Jan 1 window (delete) and appear in the Jan 15
# window (insert). This creates an unnecessary close + re-insert cycle in SCD2,
# inflating Bronze version count with misleading revision history.
# Mitigations: (a) Use a stable date column (created_date vs modified_date) as
# SourceAggregateColumnName, (b) accept as inherent to windowed CDC, (c) weekly
# reconciliation (P3-4) provides eventual consistency but cannot restore lineage.
# ---------------------------------------------------------------------------

def run_cdc_windowed(
    table_config: TableConfig,
    df_fresh: pl.DataFrame,
    batch_id: int,
    output_dir: str | Path,
    date_column: str,
    window_start: date,
    window_end: date,
) -> CDCResult:
    """Run CDC comparison scoped to a date window (large tables).

    Unlike run_cdc(), this only compares df_fresh against Stage rows whose
    business date falls within [window_start, window_end). This prevents
    rows outside the window from being classified as deletes (P1-4).

    Args:
        table_config: Table configuration with PK columns.
        df_fresh: Fresh extraction DataFrame for this window.
        batch_id: Pipeline batch ID for _cdc_batch_id.
        output_dir: Directory for staging CSV files.
        date_column: Business date column name (SourceAggregateColumnName).
        window_start: Start of date window (inclusive).
        window_end: End of date window (exclusive).

    Returns:
        CDCResult with counts and df_current.
    """
    stage_table = table_config.stage_full_table_name
    ctx = CDCContext(
        read_existing=lambda: read_stage_table_windowed(
            stage_table, date_column, window_start, window_end,
        ),
        track_deleted_pks=True,
        log_label="Windowed CDC",
        log_window=f" [{window_start}, {window_end})",
    )
    return _run_cdc_core(table_config, df_fresh, batch_id, output_dir, ctx)

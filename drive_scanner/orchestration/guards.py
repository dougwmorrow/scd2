"""Extraction guards — shared guard logic for small and large table pipelines.

G-1: Parameterized guard function accepting baseline counts and thresholds.
G-2: Unifies _check_extraction_guard (small) and _check_daily_extraction_guard
     (large) into a single module.

Guards prevent CDC processing when extraction row counts are suspicious:
  - Drop guard: blocks if row count dropped >90% vs baseline (P1-1)
  - Warn guard: warns if row count dropped >50% vs baseline (V-10)
  - Growth guard: blocks if row count spiked >5× vs baseline (P1-11)
  - First-run ceiling: blocks if no history and count exceeds ceiling (S-2)
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import TYPE_CHECKING

import config
import connections
from connections import cursor_for

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def check_extraction_guard(
    source_name: str,
    table_name: str,
    fresh_count: int,
    baseline_count: int | None,
    *,
    drop_threshold: float = 0.10,
    warn_threshold: float = 0.50,
    growth_threshold: float = 5.0,
    first_run_ceiling: int = 100_000_000,
    context: str = "",
) -> bool:
    """Check if extraction row count is within expected bounds.

    Compares fresh_count against baseline_count using configurable thresholds.
    Returns False to block CDC, True to proceed.

    Args:
        source_name: Source name for logging.
        table_name: Table name for logging.
        fresh_count: Row count from current extraction.
        baseline_count: Median/previous extraction count, or None if no history.
        drop_threshold: Block if fresh < baseline * threshold (default 10%).
            Set to 0 to disable drop blocking (large tables).
        warn_threshold: Warn if fresh < baseline * threshold (default 50%).
        growth_threshold: Block if fresh > baseline * threshold (default 5x).
        first_run_ceiling: Block on first run if fresh > ceiling.
        context: Additional context for log messages (e.g. "date 2025-01-15").

    Returns:
        True if safe to proceed, False if row count is suspicious.
    """
    ctx = f" {context}" if context else ""

    if baseline_count is None or baseline_count == 0:
        # No previous run — enforce absolute ceiling
        if fresh_count > first_run_ceiling:
            logger.error(
                "EXTRACTION GUARD: %s.%s%s first extraction has %d rows "
                "(absolute ceiling=%d). Use --force to override.",
                source_name, table_name, ctx, fresh_count, first_run_ceiling,
            )
            # O-2: Structured JSON for extraction guard signals.
            logger.info("O-2_GUARD: %s", json.dumps({
                "signal": "extraction_guard", "source": source_name,
                "table": table_name, "guard_type": "ceiling",
                "fresh_count": fresh_count, "ceiling": first_run_ceiling,
                "blocked": True,
            }))
            return False
        return True

    # Drop guard — block
    if drop_threshold > 0:
        drop_limit = int(baseline_count * drop_threshold)
        if fresh_count < drop_limit:
            drop_pct = (1 - fresh_count / baseline_count) * 100
            logger.error(
                "EXTRACTION GUARD: %s.%s%s row count dropped from %d to %d "
                "(threshold=%d, %.0f%% drop). Skipping to prevent false mass-delete. "
                "Use --force to override.",
                source_name, table_name, ctx,
                baseline_count, fresh_count, drop_limit, drop_pct,
            )
            logger.info("O-2_GUARD: %s", json.dumps({
                "signal": "extraction_guard", "source": source_name,
                "table": table_name, "guard_type": "drop",
                "baseline_count": baseline_count, "fresh_count": fresh_count,
                "drop_percent": round(drop_pct, 1), "blocked": True,
            }))
            return False

    # Drop guard — warn
    if warn_threshold > 0:
        warn_limit = int(baseline_count * warn_threshold)
        if warn_limit > 0 and fresh_count < warn_limit:
            drop_pct = (1 - fresh_count / baseline_count) * 100
            logger.warning(
                "EXTRACTION WARN: %s.%s%s row count dropped from %d to %d "
                "(warn threshold=%d, %.0f%% drop). Proceeding, but "
                "investigate if this trend continues.",
                source_name, table_name, ctx,
                baseline_count, fresh_count, warn_limit, drop_pct,
            )
            logger.info("O-2_GUARD: %s", json.dumps({
                "signal": "extraction_guard", "source": source_name,
                "table": table_name, "guard_type": "warn",
                "baseline_count": baseline_count, "fresh_count": fresh_count,
                "drop_percent": round(drop_pct, 1), "blocked": False,
            }))

    # Growth guard — block
    growth_limit = int(baseline_count * growth_threshold)
    if fresh_count > growth_limit:
        logger.error(
            "EXTRACTION GUARD: %s.%s%s row count spiked from %d to %d "
            "(threshold=%dx=%d). Possible Cartesian join, missing WHERE clause, "
            "or permission change. Skipping to prevent OOM / runaway load. "
            "Use --force to override.",
            source_name, table_name, ctx,
            baseline_count, fresh_count,
            int(growth_threshold), growth_limit,
        )
        logger.info("O-2_GUARD: %s", json.dumps({
            "signal": "extraction_guard", "source": source_name,
            "table": table_name, "guard_type": "growth",
            "baseline_count": baseline_count, "fresh_count": fresh_count,
            "growth_factor": round(fresh_count / baseline_count, 1),
            "blocked": True,
        }))
        return False

    return True


# ---------------------------------------------------------------------------
# Convenience wrappers (baseline retrieval + guard check)
# ---------------------------------------------------------------------------

def run_extraction_guard(
    table_config: TableConfig,
    fresh_count: int,
    current_batch_id: int,
    **kwargs,
) -> bool:
    """Run full extraction guard for small tables.

    Retrieves baseline from PipelineEventLog, then checks thresholds.
    Handles baseline retrieval failure gracefully (returns True = proceed).

    Args:
        table_config: Table configuration.
        fresh_count: Row count from current extraction.
        current_batch_id: Current batch ID (excluded from baseline lookup).
        **kwargs: Passed to check_extraction_guard (drop_threshold, etc).

    Returns:
        True if safe to proceed, False if row count is suspicious.
    """
    try:
        baseline = get_extract_baseline(
            table_config.source_object_name,
            table_config.source_name,
            current_batch_id,
        )
    except Exception:
        logger.warning(
            "Could not query previous extraction count for %s.%s — proceeding without guard",
            table_config.source_name, table_config.source_object_name,
        )
        return True

    return check_extraction_guard(
        table_config.source_name,
        table_config.source_object_name,
        fresh_count,
        baseline,
        **kwargs,
    )


def run_daily_extraction_guard(
    table_config: TableConfig,
    fresh_count: int,
    current_batch_id: int,
    target_date: date,
    **kwargs,
) -> bool:
    """Run daily extraction guard for large tables.

    Retrieves daily baseline from PipelineEventLog, then checks thresholds.
    Handles baseline retrieval failure gracefully (returns True = proceed).

    Args:
        table_config: Table configuration.
        fresh_count: Row count from current day's extraction.
        current_batch_id: Current batch ID (included in baseline for L-7).
        target_date: The date being processed (for log context).
        **kwargs: Passed to check_extraction_guard (drop_threshold, etc).

    Returns:
        True if safe to proceed, False if row count is suspicious.
    """
    try:
        baseline = get_daily_extract_baseline(
            table_config.source_object_name,
            table_config.source_name,
            current_batch_id,
        )
    except Exception:
        logger.warning(
            "Could not query daily extraction history for %s.%s — proceeding without guard",
            table_config.source_name, table_config.source_object_name,
        )
        return True

    return check_extraction_guard(
        table_config.source_name,
        table_config.source_object_name,
        fresh_count,
        baseline,
        context=f"date {target_date}",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Baseline retrieval strategies
# ---------------------------------------------------------------------------

def get_extract_baseline(
    table_name: str,
    source_name: str,
    exclude_batch_id: int,
) -> int | None:
    """P3-7 + B-9: Day-of-week aware rolling baseline for small table extraction.

    B-9: Uses same-day-of-week extractions from the last 30 days to build a
    baseline that accounts for weekend/holiday volume patterns. Falls back to
    the last 14 runs (any day) if insufficient same-day data exists.

    Args:
        table_name: Table name.
        source_name: Source name.
        exclude_batch_id: Current batch ID (excluded from lookup).

    Returns:
        Median row count or None if no history.
    """
    with cursor_for(config.GENERAL_DB) as cur:
        # P-8: Prevent indefinite hang if General database is under contention.
        cur.execute("SET LOCK_TIMEOUT 5000")

        # B-9: Day-of-week baseline first (last 30 days, same weekday)
        cur.execute(
            "SELECT TOP 14 RowsProcessed "
            "FROM ops.PipelineEventLog "
            "WHERE TableName = ? AND SourceName = ? "
            "AND EventType = 'EXTRACT' AND Status = 'SUCCESS' "
            "AND BatchId != ? "
            "AND DATEPART(dw, CompletedAt) = DATEPART(dw, GETDATE()) "
            "AND CompletedAt > DATEADD(day, -30, GETDATE()) "
            "ORDER BY CompletedAt DESC",
            table_name, source_name, exclude_batch_id,
        )
        rows = cur.fetchall()

        # Fall back to any-day baseline
        if len(rows) < 2:
            cur.execute(
                "SELECT TOP 14 RowsProcessed "
                "FROM ops.PipelineEventLog "
                "WHERE TableName = ? AND SourceName = ? "
                "AND EventType = 'EXTRACT' AND Status = 'SUCCESS' "
                "AND BatchId != ? "
                "ORDER BY CompletedAt DESC",
                table_name, source_name, exclude_batch_id,
            )
            rows = cur.fetchall()

    return _median_from_rows(rows)


def get_daily_extract_baseline(
    table_name: str,
    source_name: str,
    current_batch_id: int,
) -> int | None:
    """B-9 + L-7: Day-of-week aware median for large table daily extraction.

    B-9: Same-day-of-week baseline from last 30 days.
    L-7: Includes successful extractions from the current batch to build a
    baseline during multi-day backfills.

    Args:
        table_name: Table name.
        source_name: Source name.
        current_batch_id: Current batch ID (included for L-7).

    Returns:
        Median daily row count or None if no history.
    """
    with cursor_for(config.GENERAL_DB) as cur:
        # P-8: Prevent indefinite hang if General database is under contention.
        cur.execute("SET LOCK_TIMEOUT 5000")

        # B-9: Day-of-week baseline first (last 30 days, same weekday)
        cur.execute(
            "SELECT TOP 14 RowsProcessed "
            "FROM ops.PipelineEventLog "
            "WHERE TableName = ? AND SourceName = ? "
            "AND EventType = 'EXTRACT' AND Status = 'SUCCESS' "
            "AND DATEPART(dw, CompletedAt) = DATEPART(dw, GETDATE()) "
            "AND CompletedAt > DATEADD(day, -30, GETDATE()) "
            "ORDER BY CompletedAt DESC",
            table_name, source_name,
        )
        rows = cur.fetchall()

        # Fall back to any-day baseline
        if len(rows) < 2:
            cur.execute(
                "SELECT TOP 14 RowsProcessed "
                "FROM ops.PipelineEventLog "
                "WHERE TableName = ? AND SourceName = ? "
                "AND EventType = 'EXTRACT' AND Status = 'SUCCESS' "
                "ORDER BY CompletedAt DESC",
                table_name, source_name,
            )
            rows = cur.fetchall()

    return _median_from_rows(rows)


def _median_from_rows(rows: list) -> int | None:
    """Compute median row count from query result rows."""
    if not rows:
        return None

    counts = sorted(r[0] for r in rows if r[0] is not None)
    if not counts:
        return None

    mid = len(counts) // 2
    if len(counts) % 2 == 0:
        return (counts[mid - 1] + counts[mid]) // 2
    return counts[mid]

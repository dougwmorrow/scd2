"""Extraction state tracking, gap detection, checkpoints for large tables.

Tracks per-date extraction progress in General.ops.PipelineExtractionState.
Enables:
  - Resume from last successful date after failures
  - Gap detection when pipeline misses runs
  - Per-day checkpointing to avoid re-extracting completed dates
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

import connections

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


@dataclass
class ExtractionState:
    """Tracks extraction progress for a single date."""

    table_name: str
    source_name: str
    date_value: date
    status: str  # SUCCESS, FAILED, PENDING
    batch_id: int | None = None
    processed_at: str | None = None


def save_checkpoint(
    table_name: str,
    source_name: str,
    date_value: date,
    status: str,
    batch_id: int,
    failed_step: str | None = None,
) -> None:
    """Save or update extraction checkpoint for a specific date.

    P3-11: Supports finer-grained status values:
      - EXTRACTED: extraction succeeded, CDC/SCD2 pending
      - SUCCESS: all steps completed
      - FAILED: any step failed (failed_step identifies which)

    Upserts into General.ops.PipelineExtractionState.

    Args:
        table_name: Table being processed.
        source_name: Source system name.
        date_value: The date being checkpointed.
        status: Status string (SUCCESS, FAILED, EXTRACTED).
        batch_id: Pipeline batch ID.
        failed_step: P3-11 — Which step failed (e.g., 'CDC', 'SCD2').
    """
    # P3-11: Encode failed step in status for debugging
    effective_status = status
    if failed_step and status == "FAILED":
        effective_status = f"FAILED_{failed_step}"

    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            MERGE INTO ops.PipelineExtractionState AS target
            USING (SELECT ? AS TableName, ? AS SourceName, ? AS DateValue) AS source
            ON target.TableName = source.TableName
               AND target.SourceName = source.SourceName
               AND target.DateValue = source.DateValue
            WHEN MATCHED THEN
                UPDATE SET Status = ?, BatchId = ?, ProcessedAt = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (TableName, SourceName, DateValue, Status, BatchId, ProcessedAt)
                VALUES (?, ?, ?, ?, ?, GETUTCDATE());
            """,
            table_name, source_name, date_value,
            effective_status, batch_id,
            table_name, source_name, date_value, effective_status, batch_id,
        )
        cursor.close()
        logger.debug(
            "Checkpoint saved: %s.%s date=%s status=%s batch=%d",
            source_name, table_name, date_value, effective_status, batch_id,
        )
    finally:
        conn.close()


def get_last_successful_date(
    table_name: str,
    source_name: str,
) -> date | None:
    """Get the most recent successfully processed date for a table.

    Returns:
        The latest date with Status='SUCCESS', or None if no successful runs.
    """
    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(DateValue) "
            "FROM ops.PipelineExtractionState "
            "WHERE TableName = ? AND SourceName = ? AND Status = 'SUCCESS'",
            table_name, source_name,
        )
        row = cursor.fetchone()
        cursor.close()
        if row and row[0]:
            result = row[0]
            # pyodbc may return datetime — convert to date
            if hasattr(result, "date"):
                return result.date()
            return result
        return None
    finally:
        conn.close()


def detect_gaps(
    table_name: str,
    source_name: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    """Find dates in range that were NOT successfully processed.

    Returns a list of dates between start_date and end_date (inclusive)
    that either have no checkpoint or have Status != 'SUCCESS'.
    """
    # Get all successful dates in the range
    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DateValue "
            "FROM ops.PipelineExtractionState "
            "WHERE TableName = ? AND SourceName = ? "
            "AND Status = 'SUCCESS' "
            "AND DateValue >= ? AND DateValue <= ?",
            table_name, source_name, start_date, end_date,
        )
        success_dates = set()
        for row in cursor.fetchall():
            val = row[0]
            if hasattr(val, "date"):
                val = val.date()
            success_dates.add(val)
        cursor.close()
    finally:
        conn.close()

    # Build the full range and find gaps
    gaps = []
    current = start_date
    while current <= end_date:
        if current not in success_dates:
            gaps.append(current)
        current += timedelta(days=1)

    return gaps


def get_dates_to_process(
    table_config: TableConfig,
    batch_id: int,
) -> list[date]:
    """Determine which dates need processing for a large table.

    Strategy:
      1. Start from the last successful date (or FirstLoadDate if no history).
      2. End at today - 1 (yesterday, to avoid partial-day extractions).
      3. Detect gaps within the range — re-process any failed/missing dates.
      4. Add the LookbackDays rolling window on top.
      5. Alert if gap exceeds LookbackDays.

    Returns:
        Sorted list of dates to process (oldest first).
    """
    table_name = table_config.source_object_name
    source_name = table_config.source_name
    yesterday = date.today() - timedelta(days=1)

    # Determine the start boundary
    last_success = get_last_successful_date(table_name, source_name)

    if last_success:
        # Start from the day after last success
        range_start = last_success + timedelta(days=1)
    elif table_config.first_load_date:
        # First run — use FirstLoadDate from config
        range_start = _parse_date(table_config.first_load_date)
    else:
        # No history and no FirstLoadDate — default to lookback window
        lookback = table_config.lookback_days or 30
        range_start = yesterday - timedelta(days=lookback)

    # Apply lookback window: always re-process the last N days to capture changes
    lookback_days = table_config.lookback_days or 30
    lookback_start = yesterday - timedelta(days=lookback_days)

    # The effective start is the earlier of range_start and lookback_start
    effective_start = min(range_start, lookback_start)

    # Don't go before FirstLoadDate
    if table_config.first_load_date:
        first_load = _parse_date(table_config.first_load_date)
        effective_start = max(effective_start, first_load)

    # Detect gaps and alert
    if last_success:
        gap_days = (yesterday - last_success).days
        if gap_days > lookback_days:
            logger.critical(
                "GAP ALERT: %s.%s has a %d-day gap (last_success=%s, lookback=%d). "
                "Dates beyond the lookback window may have been missed. "
                "Consider a backfill from %s.",
                source_name, table_name, gap_days, last_success,
                lookback_days, last_success,
            )

    # Find all dates that need processing
    gaps = detect_gaps(table_name, source_name, effective_start, yesterday)

    if gaps:
        logger.info(
            "Dates to process for %s.%s: %d dates from %s to %s",
            source_name, table_name, len(gaps), gaps[0], gaps[-1],
        )

    return sorted(gaps)


def get_failed_dates(
    table_name: str,
    source_name: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    """Find dates that were attempted but failed (Status='FAILED').

    Unlike detect_gaps(), this only returns dates with explicit FAILED status,
    not dates that were never attempted.
    """
    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DateValue "
            "FROM ops.PipelineExtractionState "
            "WHERE TableName = ? AND SourceName = ? "
            "AND Status = 'FAILED' "
            "AND DateValue >= ? AND DateValue <= ?",
            table_name, source_name, start_date, end_date,
        )
        failed = []
        for row in cursor.fetchall():
            val = row[0]
            if hasattr(val, "date"):
                val = val.date()
            failed.append(val)
        cursor.close()
        return sorted(failed)
    finally:
        conn.close()


def _parse_date(date_str: str) -> date:
    """Parse a date string from config (various formats)."""
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str}")

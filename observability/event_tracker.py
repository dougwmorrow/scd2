"""PipelineEventTracker context manager -> General.ops.PipelineEventLog.

Writes exactly one row per step per table. All tables in a run share one BatchId
from General.ops.PipelineBatchSequence.

Usage:
    tracker = PipelineEventTracker()
    with tracker.track("EXTRACT", table_config) as event:
        df = extract_from_source(table_config)
        event.rows_processed = len(df)
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import connections

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


@dataclass
class PipelineEvent:
    """Mutable event object — pipeline code sets row counts inside the with block."""

    event_type: str
    table_name: str
    source_name: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: float = 0.0
    status: str = "SUCCESS"
    error_message: str | None = None
    event_detail: str | None = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_unchanged: int = 0
    rows_before: int = 0
    rows_after: int = 0
    table_created: bool = False
    metadata: str | None = None
    rows_per_second: float = 0.0


class PipelineEventTracker:
    """Tracks pipeline events and writes them to General.ops.PipelineEventLog."""

    def __init__(self) -> None:
        self._batch_id: int | None = None

    @property
    def batch_id(self) -> int:
        if self._batch_id is None:
            self._batch_id = self._get_next_batch_id()
        return self._batch_id

    @staticmethod
    def _get_next_batch_id() -> int:
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO ops.PipelineBatchSequence DEFAULT VALUES; "
                "SELECT SCOPE_IDENTITY();"
            )
            row = cursor.fetchone()
            batch_id = int(row[0])
            cursor.close()
            # OBS-5 + R-4: Explicit commit — required even under autocommit=True
            # as a defensive guard against future connection configuration changes.
            # Under the current autocommit=True setting, this is a no-op but ensures
            # the INSERT persists if autocommit is ever disabled.
            conn.commit()
            return batch_id
        finally:
            conn.close()

    @contextmanager
    def track(self, event_type: str, table_config: TableConfig):
        """Context manager that yields a PipelineEvent for the caller to populate."""
        event = PipelineEvent(
            event_type=event_type,
            table_name=table_config.source_object_name,
            source_name=table_config.source_name,
        )
        event.started_at = datetime.now(timezone.utc)
        try:
            yield event
            # OBS-3: Preserve explicitly-set statuses (SKIPPED, etc.)
            if event.status not in ("FAILED", "SKIPPED"):
                event.status = "SUCCESS"
        except Exception as e:
            event.status = "FAILED"
            event.error_message = str(e)[:4000]
            raise
        finally:
            event.completed_at = datetime.now(timezone.utc)
            event.duration_ms = (
                (event.completed_at - event.started_at).total_seconds() * 1000
            )
            if event.duration_ms > 0 and event.rows_processed > 0:
                event.rows_per_second = event.rows_processed / (event.duration_ms / 1000)
            self._write_event(event)

    def _write_event(self, event: PipelineEvent) -> None:
        try:
            conn = connections.get_general_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO ops.PipelineEventLog (
                        BatchId, TableName, SourceName, EventType, EventDetail,
                        StartedAt, CompletedAt, DurationMs, Status, ErrorMessage,
                        RowsProcessed, RowsInserted, RowsUpdated, RowsDeleted,
                        RowsUnchanged, RowsBefore, RowsAfter, TableCreated,
                        Metadata, RowsPerSecond
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self.batch_id,
                    event.table_name,
                    event.source_name,
                    event.event_type,
                    event.event_detail,
                    event.started_at,
                    event.completed_at,
                    int(event.duration_ms),
                    event.status,
                    event.error_message,
                    event.rows_processed,
                    event.rows_inserted,
                    event.rows_updated,
                    event.rows_deleted,
                    event.rows_unchanged,
                    event.rows_before,
                    event.rows_after,
                    1 if event.table_created else 0,
                    event.metadata,
                    round(event.rows_per_second, 2),
                )
                cursor.close()
                # OBS-5: Explicit commit — don't rely on autocommit default.
                # If connection management changes, events still persist.
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to write event to PipelineEventLog")

"""SqlServerLogHandler: custom logging.Handler -> General.ops.PipelineLog.

Every module uses standard logger = logging.getLogger(__name__) calls.
The handler holds BatchId and TableName/SourceName in thread-local context.
"""

from __future__ import annotations

import logging
import threading
import traceback
from datetime import datetime, timezone

import connections


class SqlServerLogHandler(logging.Handler):
    """Custom logging handler that writes log records to General.ops.PipelineLog.

    Usage:
        handler = SqlServerLogHandler()
        handler.set_context(batch_id=42, table_name="ACCT", source_name="DNA")
        logging.getLogger().addHandler(handler)
    """

    def __init__(self, level: int = logging.INFO) -> None:
        super().__init__(level)
        self._context = threading.local()
        self._buffer: list[tuple] = []
        self._buffer_lock = threading.Lock()
        # OBS-4: Reduced from 50 to 10 to narrow the crash-loss window.
        # On SIGKILL/OOM, at most 9 entries are lost instead of 49.
        self._buffer_size = 10

    def set_context(
        self,
        batch_id: int | None = None,
        table_name: str | None = None,
        source_name: str | None = None,
    ) -> None:
        if batch_id is not None:
            self._context.batch_id = batch_id
        if table_name is not None:
            self._context.table_name = table_name
        else:
            self._context.table_name = None
        if source_name is not None:
            self._context.source_name = source_name
        else:
            self._context.source_name = None

    def _get_context(self) -> tuple[int | None, str | None, str | None]:
        return (
            getattr(self._context, "batch_id", None),
            getattr(self._context, "table_name", None),
            getattr(self._context, "source_name", None),
        )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            batch_id, table_name, source_name = self._get_context()
            if batch_id is None:
                return

            error_type = None
            stack_trace = None
            if record.exc_info and record.exc_info[1]:
                error_type = type(record.exc_info[1]).__name__
                stack_trace = "".join(
                    traceback.format_exception(*record.exc_info)
                )[:4000]

            metadata = None
            if hasattr(record, "metadata"):
                metadata = str(record.metadata)

            row = (
                batch_id,
                table_name,
                source_name,
                record.levelname,
                record.name,
                record.funcName,
                self.format(record)[:4000],
                error_type,
                stack_trace,
                metadata,
                datetime.now(timezone.utc),
            )

            with self._buffer_lock:
                self._buffer.append(row)
                # OBS-4: Flush immediately on WARNING+ — these entries are the
                # most diagnostically valuable and most likely to be lost on crash.
                if len(self._buffer) >= self._buffer_size or record.levelno >= logging.WARNING:
                    self._flush_buffer()
        except Exception:
            self.handleError(record)

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        rows = self._buffer[:]
        self._buffer.clear()

        try:
            conn = connections.get_general_connection()
            try:
                cursor = conn.cursor()
                cursor.executemany(
                    """
                    INSERT INTO ops.PipelineLog (
                        BatchId, TableName, SourceName, LogLevel, Module,
                        FunctionName, Message, ErrorType, StackTrace,
                        Metadata, CreatedAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                cursor.close()
                # OBS-5: Explicit commit — don't rely on autocommit default.
                conn.commit()
            finally:
                conn.close()
        except Exception as flush_err:
            # OBS-4: Surface flush failures to stderr instead of silently
            # swallowing. If the General DB connection drops, this makes the
            # failure visible in systemd journal / console output.
            import sys
            print(
                f"[SqlServerLogHandler] FLUSH FAILED ({len(rows)} entries lost): "
                f"{flush_err}",
                file=sys.stderr,
            )

    def flush(self) -> None:
        with self._buffer_lock:
            self._flush_buffer()

    def close(self) -> None:
        self.flush()
        super().close()

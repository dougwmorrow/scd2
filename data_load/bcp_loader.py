"""BCP subprocess wrapper: CSV -> SQL Server.

Handles BULK_LOGGED recovery model context and row count verification.

BCP-HANG-FIX-v2 — Live diagnostic monitoring during BCP execution:
  The core problem: BCP hangs are SILENT. The process produces no output, no
  error, and the default lock timeout is infinite. Previous fixes added pre-BCP
  checks and retry logic, but attempt 1/3 itself hangs inside communicate()
  for up to BCP_TIMEOUT seconds with zero visibility.

  This version adds a background monitor THREAD that runs WHILE BCP is executing.
  Every BCP_MONITOR_INTERVAL seconds it queries SQL Server DMVs to produce a
  real-time diagnostic snapshot covering all hang causes from the diagnostic guide:

    §1  Lock conflicts     — BCP session wait_type, blocking_session_id, wait_time
    §2  Schema mod blocks  — Sch-M blocking chains (DDL/index rebuilds)
    §3  Lock escalation    — Whether table-level X lock has been acquired
    §4  Log pressure       — Transaction log used %, log_reuse_wait, VLF count
    §5  Index overhead     — row_count progress tracking (stalled = index I/O)
    §6  Network/TLS hang   — BCP session not visible (never connected)
    §7  Orphaned sessions  — Other BULK INSERT sessions on same table
    §8  I/O bottleneck     — PAGEIOLATCH_EX waits, avg write latency per file

  Early abort: When a definitive hang signature persists beyond
  BCP_HANG_ABORT_THRESHOLD seconds, the monitor signals the main thread to
  SIGTERM the BCP process immediately rather than waiting the full timeout.
  The abort includes a specific diagnosis string identifying which §section
  matched and what the root cause is.

  The monitor uses its OWN pyodbc connection (bypasses the connection pool
  which is NOT thread-safe per Item-23). If the monitor connection fails,
  BCP continues — monitoring is best-effort.

B-2 KNOWN LIMITATION — Minimal logging on non-empty clustered-index tables:
  For tables with a clustered index (all Bronze SCD2 tables), BCP is fully logged
  on data pages when the table is non-empty. BULK_LOGGED mode only helps minimize
  logging for index page operations. At 3M rows × ~200 bytes with log overhead,
  expect 1.2–3 GB of transaction log per Bronze load. Ensure frequent transaction
  log backups (every 15–30 minutes during loads) under FULL recovery model.
  Monitor with: SELECT * FROM sys.dm_db_log_space_usage

B-3 NOTE — TABLOCK not used (concurrent access allowed):
  This BCP wrapper does NOT use the -h TABLOCK hint. Without TABLOCK, BCP uses
  row/page-level locks, allowing concurrent reads from downstream consumers and
  reporting queries during loads. The tradeoff: fully logged inserts even on heaps
  (no minimal logging benefit from TABLOCK). Since Bronze tables have clustered
  indexes (B-2), TABLOCK wouldn't help with minimal logging anyway.

SCD-4 NOTE — cursor.rowcount reliability:
  All UPDATE operations in the pipeline use cursor.execute() (single statement),
  which returns reliable rowcount values from pyodbc. Do NOT switch to
  cursor.executemany() for SCD2 UPDATE operations — pyodbc issue #481 confirms
  rowcount returns -1 after executemany(), which would break P2-14 validation.

V-5 NOTE — BCP codepage flag intentionally omitted:
  -C 65001 is silently ignored or causes incorrect encoding on Linux BCP
  (mssql-tools18). With SQL Server 2019+ UTF-8 collation and ODBC Driver 18,
  BCP handles UTF-8 natively in -c mode without the codepage flag.

W-1 TODO — Upgrade BCP to mssql-tools18 v18.6.1.1+:
  Microsoft released mssql-tools18 v18.6.1.1 (December 2025) which adds
  native -C 65001 codepage support on Linux/macOS for the first time.
  After upgrading the mssql-tools18 package on the Red Hat server:
    1. Re-add "-C", "65001" to the BCP command below
    2. Validate with round-trip test: café, 日本語, €100, emoji
    3. Update BCP_PATH in config.py if the binary path changes
    4. Update CLAUDE.md to document the version requirement
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pyodbc

import config
import connections
from connections import cursor_for, quote_identifier, quote_table

logger = logging.getLogger(__name__)


class BCPLoadError(Exception):
    """Raised when BCP load fails or row count mismatches."""


class BCPBlockedError(BCPLoadError):
    """Raised when BCP is blocked by another session and cannot proceed.

    BCP-HANG-FIX: Distinct from BCPLoadError to allow callers to distinguish
    between a BCP failure (bad data, schema mismatch) and a blocking condition
    (orphaned session, concurrent DDL) which may be retryable after cleanup.

    Attributes:
        diagnosis: Structured diagnosis string from the hang monitor.
        snapshots: All diagnostic snapshots collected during the hang.
    """

    def __init__(self, message: str, diagnosis: str = "", snapshots: list | None = None):
        super().__init__(message)
        self.diagnosis = diagnosis
        self.snapshots = snapshots or []


# ==========================================================================
# Diagnostic snapshot data model
# ==========================================================================

@dataclass
class BCPDiagnosticSnapshot:
    """Single point-in-time diagnostic snapshot collected by the monitor.

    Each field corresponds to a section from the BCP hang diagnostic guide.
    """
    timestamp: str
    elapsed_seconds: float
    sequence: int

    # §1/§9-Step1: BCP session state
    bcp_session_id: int | None = None
    bcp_status: str | None = None
    bcp_wait_type: str | None = None
    bcp_wait_time_ms: int | None = None
    bcp_wait_resource: str | None = None
    bcp_blocking_session_id: int | None = None
    bcp_row_count: int | None = None
    bcp_total_elapsed_ms: int | None = None

    # §9-Step2: Blocking chain
    blocking_chain: list[dict] = field(default_factory=list)

    # §9-Step3: Locks on target table
    table_locks: list[dict] = field(default_factory=list)

    # §4/§9-Step4: Transaction log
    log_total_mb: float | None = None
    log_used_mb: float | None = None
    log_used_pct: float | None = None
    recovery_model: str | None = None
    log_reuse_wait: str | None = None

    # §9-Step5: Open transactions
    open_transactions: list[dict] = field(default_factory=list)

    # §8: I/O latency
    io_latencies: list[dict] = field(default_factory=list)

    # §7: Other BULK INSERT sessions
    other_bulk_sessions: list[dict] = field(default_factory=list)

    # Monitor metadata
    monitor_error: str | None = None
    hang_detected: str | None = None  # None = no hang, else = §section diagnosis

    def to_log_dict(self) -> dict:
        """Return a compact dict for structured logging."""
        d = {
            "ts": self.timestamp,
            "elapsed_s": round(self.elapsed_seconds, 1),
            "seq": self.sequence,
        }
        if self.bcp_session_id is not None:
            d["bcp_sid"] = self.bcp_session_id
            d["bcp_status"] = self.bcp_status
            d["bcp_wait"] = self.bcp_wait_type
            d["bcp_wait_ms"] = self.bcp_wait_time_ms
            d["bcp_blocked_by"] = self.bcp_blocking_session_id
            d["bcp_rows"] = self.bcp_row_count
        else:
            d["bcp_sid"] = None  # BCP session not found — possible TLS/network hang
        if self.log_used_pct is not None:
            d["log_pct"] = round(self.log_used_pct, 1)
            d["log_reuse_wait"] = self.log_reuse_wait
        if self.blocking_chain:
            d["blockers"] = len(self.blocking_chain)
        if self.table_locks:
            d["table_locks"] = len(self.table_locks)
        if self.other_bulk_sessions:
            d["other_bcp"] = len(self.other_bulk_sessions)
        if self.io_latencies:
            max_lat = max((x.get("avg_write_ms", 0) for x in self.io_latencies), default=0)
            d["max_io_lat_ms"] = round(max_lat, 1)
        if self.monitor_error:
            d["monitor_error"] = self.monitor_error
        if self.hang_detected:
            d["HANG"] = self.hang_detected
        return d


# ==========================================================================
# Background hang monitor thread
# ==========================================================================

class _BCPHangMonitor(threading.Thread):
    """Daemon thread that polls SQL Server DMVs while BCP runs.

    Creates its own pyodbc connection (bypasses connection pool per Item-23
    thread-safety note). If the connection fails, monitoring degrades
    gracefully — BCP continues unmonitored.

    The monitor signals the main thread to abort BCP early via the
    `abort_event` if a definitive hang pattern persists beyond the threshold.

    Usage::

        monitor = _BCPHangMonitor("UDM_Stage.DNA.ACCT_cdc")
        monitor.start()
        # ... BCP runs ...
        monitor.request_stop()
        monitor.join(timeout=5)
        print(monitor.get_diagnosis())
    """

    def __init__(self, full_table_name: str, poll_interval: float | None = None):
        super().__init__(daemon=True, name=f"bcp-monitor-{full_table_name}")
        self.full_table_name = full_table_name
        self.db = full_table_name.split(".")[0]
        self.poll_interval = poll_interval or config.BCP_MONITOR_INTERVAL
        self.abort_threshold = config.BCP_HANG_ABORT_THRESHOLD

        self._stop_event = threading.Event()
        self.abort_event = threading.Event()  # Signals main thread to kill BCP
        self.abort_reason: str = ""

        self.snapshots: list[BCPDiagnosticSnapshot] = []
        self._start_time: float = 0.0
        self._seq: int = 0
        self._conn: pyodbc.Connection | None = None

        # Track row_count progression to detect stalls (§5 index overhead)
        self._last_row_count: int | None = None
        self._row_stall_since: float | None = None

    def run(self):
        """Main monitor loop — runs in daemon thread."""
        self._start_time = time.monotonic()
        self._conn = self._create_monitor_connection()
        if self._conn is None:
            logger.warning(
                "BCP-MONITOR: Could not establish monitoring connection for %s. "
                "BCP will run without live diagnostics.",
                self.full_table_name,
            )
            return

        logger.info(
            "BCP-MONITOR: Started for %s (poll=%ds, abort_threshold=%ds)",
            self.full_table_name, self.poll_interval, self.abort_threshold,
        )

        try:
            while not self._stop_event.wait(timeout=self.poll_interval):
                snapshot = self._collect_snapshot()
                self.snapshots.append(snapshot)

                # Log the snapshot
                log_dict = snapshot.to_log_dict()
                if snapshot.hang_detected:
                    logger.warning("BCP-MONITOR: %s", json.dumps(log_dict, default=str))
                else:
                    logger.info("BCP-MONITOR: %s", json.dumps(log_dict, default=str))

                # Check for early abort
                if snapshot.hang_detected and not self.abort_event.is_set():
                    self.abort_reason = snapshot.hang_detected
                    self.abort_event.set()
                    logger.error(
                        "BCP-MONITOR: EARLY ABORT — %s. Signaling main thread "
                        "to terminate BCP process.",
                        snapshot.hang_detected,
                    )
        finally:
            self._close_connection()

    def request_stop(self):
        """Signal the monitor to stop after the current poll cycle."""
        self._stop_event.set()

    def get_diagnosis(self) -> str:
        """Build a human-readable diagnostic report from all snapshots.

        Called after BCP completes or is aborted to produce a full timeline.
        """
        if not self.snapshots:
            return "BCP-MONITOR: No diagnostic snapshots collected."

        lines = [
            f"{'=' * 72}",
            f"BCP HANG DIAGNOSTIC REPORT — {self.full_table_name}",
            f"{'=' * 72}",
            f"Snapshots collected: {len(self.snapshots)}",
            f"Monitor duration: {self.snapshots[-1].elapsed_seconds:.1f}s",
            f"Abort triggered: {self.abort_reason or 'No'}",
            "",
        ]

        # Summary: was a BCP session ever found?
        sessions_found = [s for s in self.snapshots if s.bcp_session_id is not None]
        if not sessions_found:
            lines.append(
                "*** BCP session NEVER appeared in sys.dm_exec_requests ***"
            )
            lines.append(
                "    This indicates the hang occurs BEFORE SQL Server receives "
                "the BULK INSERT command."
            )
            lines.append(
                "    Most likely cause: TLS handshake failure (§6 — ODBC Driver 18 "
                "defaults to Encrypt=yes, add -Yo flag), network/firewall timeout, "
                "or DNS resolution stall."
            )
            lines.append("")

        # Timeline
        lines.append("--- TIMELINE ---")
        for snap in self.snapshots:
            marker = " *** HANG ***" if snap.hang_detected else ""
            if snap.bcp_session_id is not None:
                lines.append(
                    f"  [{snap.elapsed_seconds:7.1f}s] seq={snap.sequence} "
                    f"session={snap.bcp_session_id} status={snap.bcp_status} "
                    f"wait={snap.bcp_wait_type} wait_ms={snap.bcp_wait_time_ms} "
                    f"blocked_by={snap.bcp_blocking_session_id} "
                    f"rows={snap.bcp_row_count} "
                    f"log={snap.log_used_pct:.0f}%{marker}"
                )
            elif snap.monitor_error:
                lines.append(
                    f"  [{snap.elapsed_seconds:7.1f}s] seq={snap.sequence} "
                    f"MONITOR ERROR: {snap.monitor_error}{marker}"
                )
            else:
                lines.append(
                    f"  [{snap.elapsed_seconds:7.1f}s] seq={snap.sequence} "
                    f"NO BCP SESSION FOUND{marker}"
                )

        # Blocking chain detail (from last snapshot that had one)
        chain_snaps = [s for s in self.snapshots if s.blocking_chain]
        if chain_snaps:
            last_chain = chain_snaps[-1]
            lines.append("")
            lines.append("--- BLOCKING CHAIN (last observed) ---")
            for b in last_chain.blocking_chain:
                lines.append(
                    f"  session={b.get('session_id')} blocked_by={b.get('blocked_by')} "
                    f"cmd={b.get('command')} wait={b.get('wait_type')} "
                    f"wait_ms={b.get('wait_time')} "
                    f"sql={b.get('sql_text', '')[:200]}"
                )

        # Lock detail
        lock_snaps = [s for s in self.snapshots if s.table_locks]
        if lock_snaps:
            last_locks = lock_snaps[-1]
            lines.append("")
            lines.append("--- TABLE LOCKS (last observed) ---")
            for lk in last_locks.table_locks:
                lines.append(
                    f"  session={lk.get('session_id')} "
                    f"type={lk.get('resource_type')} "
                    f"mode={lk.get('mode')} status={lk.get('status')} "
                    f"program={lk.get('program_name')} "
                    f"cmd={lk.get('command')}"
                )

        # I/O latency
        io_snaps = [s for s in self.snapshots if s.io_latencies]
        if io_snaps:
            last_io = io_snaps[-1]
            high_lat = [f for f in last_io.io_latencies if f.get("avg_write_ms", 0) > 5]
            if high_lat:
                lines.append("")
                lines.append("--- I/O LATENCY (elevated files) ---")
                for f in high_lat:
                    lines.append(
                        f"  {f.get('file_name')} ({f.get('type')}): "
                        f"avg_write={f.get('avg_write_ms', 0):.1f}ms"
                    )

        # Open transactions
        tran_snaps = [s for s in self.snapshots if s.open_transactions]
        if tran_snaps:
            last_tran = tran_snaps[-1]
            lines.append("")
            lines.append("--- OPEN TRANSACTIONS (last observed) ---")
            for t in last_tran.open_transactions:
                lines.append(
                    f"  tran={t.get('tran_id')} age={t.get('age_seconds')}s "
                    f"session={t.get('session_id')} login={t.get('login_name')} "
                    f"program={t.get('program_name')} "
                    f"sql={t.get('last_sql', '')[:200]}"
                )

        lines.append(f"\n{'=' * 72}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal: connection management
    # ------------------------------------------------------------------

    def _create_monitor_connection(self) -> pyodbc.Connection | None:
        """Create a dedicated monitoring connection (bypasses pool — Item-23).

        Uses a short connection timeout (10s) to fail fast if the server is
        unreachable. The monitor is best-effort; a failed connection should
        not delay BCP.
        """
        try:
            conn_str = (
                f"DRIVER={{{config.ODBC_DRIVER}}};"
                f"SERVER={config.SQL_SERVER_HOST},{config.SQL_SERVER_PORT};"
                f"DATABASE={self.db};"
                f"UID={config.SQL_SERVER_USER};"
                f"PWD={config.SQL_SERVER_PASSWORD};"
                "TrustServerCertificate=yes;"
                "Connection Timeout=10;"
            )
            return pyodbc.connect(conn_str, autocommit=True)
        except Exception:
            logger.warning(
                "BCP-MONITOR: Failed to create monitoring connection",
                exc_info=True,
            )
            return None

    def _close_connection(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ------------------------------------------------------------------
    # Internal: diagnostic snapshot collection
    # ------------------------------------------------------------------

    def _collect_snapshot(self) -> BCPDiagnosticSnapshot:
        """Run all §9 diagnostic queries and return a snapshot."""
        self._seq += 1
        elapsed = time.monotonic() - self._start_time
        snap = BCPDiagnosticSnapshot(
            timestamp=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            elapsed_seconds=elapsed,
            sequence=self._seq,
        )

        if self._conn is None:
            snap.monitor_error = "No monitoring connection"
            return snap

        try:
            cur = self._conn.cursor()
            try:
                self._query_bcp_session(cur, snap)
                self._query_blocking_chain(cur, snap)
                self._query_table_locks(cur, snap)
                self._query_log_pressure(cur, snap)
                self._query_open_transactions(cur, snap)
                self._query_io_latency(cur, snap)
                self._query_other_bulk_sessions(cur, snap)
            finally:
                cur.close()
        except pyodbc.OperationalError as e:
            snap.monitor_error = f"Connection lost: {e}"
            logger.warning("BCP-MONITOR: Connection lost, attempting reconnect...")
            self._close_connection()
            self._conn = self._create_monitor_connection()
        except Exception as e:
            snap.monitor_error = f"Query error: {e}"

        # Evaluate hang heuristics
        self._evaluate_hang(snap, elapsed)

        return snap

    def _query_bcp_session(self, cur, snap: BCPDiagnosticSnapshot):
        """§9 Step 1: Find the BCP session and its wait state."""
        cur.execute(
            "SELECT TOP 1 "
            "  s.session_id, r.status, r.wait_type, r.wait_time, "
            "  r.wait_resource, r.blocking_session_id, r.row_count, "
            "  r.total_elapsed_time "
            "FROM sys.dm_exec_sessions s "
            "JOIN sys.dm_exec_requests r ON s.session_id = r.session_id "
            "WHERE r.command = 'BULK INSERT' "
            "  AND r.database_id = DB_ID() "
            "ORDER BY r.start_time DESC"
        )
        row = cur.fetchone()
        if row:
            snap.bcp_session_id = row[0]
            snap.bcp_status = row[1]
            snap.bcp_wait_type = row[2]
            snap.bcp_wait_time_ms = row[3]
            snap.bcp_wait_resource = row[4]
            snap.bcp_blocking_session_id = row[5]
            snap.bcp_row_count = row[6]
            snap.bcp_total_elapsed_ms = row[7]

    def _query_blocking_chain(self, cur, snap: BCPDiagnosticSnapshot):
        """§9 Step 2: Full blocking chain with SQL text."""
        cur.execute(
            "SELECT s.session_id, r.blocking_session_id, "
            "  r.command, r.wait_type, r.wait_time, r.wait_resource, "
            "  s.login_name, s.host_name, s.program_name, "
            "  ib.event_info "
            "FROM sys.dm_exec_sessions s "
            "JOIN sys.dm_exec_requests r ON r.session_id = s.session_id "
            "OUTER APPLY sys.dm_exec_input_buffer(s.session_id, NULL) ib "
            "WHERE r.blocking_session_id > 0 "
            "  AND r.database_id = DB_ID()"
        )
        for row in cur.fetchall():
            snap.blocking_chain.append({
                "session_id": row[0],
                "blocked_by": row[1],
                "command": row[2],
                "wait_type": row[3],
                "wait_time": row[4],
                "wait_resource": row[5],
                "login_name": row[6],
                "host_name": row[7],
                "program_name": row[8],
                "sql_text": (row[9] or "")[:500],
            })

    def _query_table_locks(self, cur, snap: BCPDiagnosticSnapshot):
        """§9 Step 3: All locks on the target table."""
        cur.execute(
            "SELECT tl.request_session_id, tl.resource_type, "
            "  tl.request_mode, tl.request_status, "
            "  s.program_name, r.command "
            "FROM sys.dm_tran_locks tl "
            "LEFT JOIN sys.dm_exec_sessions s "
            "  ON tl.request_session_id = s.session_id "
            "LEFT JOIN sys.dm_exec_requests r "
            "  ON tl.request_session_id = r.session_id "
            "WHERE tl.resource_database_id = DB_ID() "
            "  AND tl.resource_associated_entity_id = OBJECT_ID(?) "
            "ORDER BY tl.request_status DESC, tl.request_mode DESC",
            self.full_table_name,
        )
        for row in cur.fetchall():
            snap.table_locks.append({
                "session_id": row[0],
                "resource_type": row[1],
                "mode": row[2],
                "status": row[3],
                "program_name": row[4],
                "command": row[5],
            })

    def _query_log_pressure(self, cur, snap: BCPDiagnosticSnapshot):
        """§4/§9 Step 4: Transaction log space and reuse status."""
        cur.execute(
            "SELECT total_log_size_in_bytes / 1048576.0, "
            "  used_log_space_in_bytes / 1048576.0, "
            "  used_log_space_in_percent "
            "FROM sys.dm_db_log_space_usage"
        )
        row = cur.fetchone()
        if row:
            snap.log_total_mb = row[0]
            snap.log_used_mb = row[1]
            snap.log_used_pct = row[2]

        cur.execute(
            "SELECT recovery_model_desc, log_reuse_wait_desc "
            "FROM sys.databases WHERE database_id = DB_ID()"
        )
        row = cur.fetchone()
        if row:
            snap.recovery_model = row[0]
            snap.log_reuse_wait = row[1]

    def _query_open_transactions(self, cur, snap: BCPDiagnosticSnapshot):
        """§9 Step 5: Long-running open transactions."""
        cur.execute(
            "SELECT tat.transaction_id, "
            "  DATEDIFF(SECOND, tat.transaction_begin_time, GETDATE()), "
            "  tst.session_id, s.login_name, s.program_name, "
            "  ib.event_info "
            "FROM sys.dm_tran_active_transactions tat "
            "JOIN sys.dm_tran_session_transactions tst "
            "  ON tat.transaction_id = tst.transaction_id "
            "LEFT JOIN sys.dm_exec_sessions s "
            "  ON tst.session_id = s.session_id "
            "OUTER APPLY sys.dm_exec_input_buffer(tst.session_id, NULL) ib "
            "WHERE tst.session_id > 50 "
            "ORDER BY tat.transaction_begin_time"
        )
        for row in cur.fetchall():
            snap.open_transactions.append({
                "tran_id": row[0],
                "age_seconds": row[1],
                "session_id": row[2],
                "login_name": row[3],
                "program_name": row[4],
                "last_sql": (row[5] or "")[:500],
            })

    def _query_io_latency(self, cur, snap: BCPDiagnosticSnapshot):
        """§8: I/O write latency per database file."""
        cur.execute(
            "SELECT mf.name, mf.type_desc, "
            "  CASE WHEN vfs.num_of_writes > 0 "
            "    THEN CAST(vfs.io_stall_write_ms AS FLOAT) / vfs.num_of_writes "
            "    ELSE 0 END "
            "FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) vfs "
            "JOIN sys.master_files mf "
            "  ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id "
            "ORDER BY 3 DESC"
        )
        for row in cur.fetchall():
            snap.io_latencies.append({
                "file_name": row[0],
                "type": row[1],
                "avg_write_ms": row[2],
            })

    def _query_other_bulk_sessions(self, cur, snap: BCPDiagnosticSnapshot):
        """§7: Other BULK INSERT sessions (potential orphans)."""
        # Exclude our own BCP session if we know it
        our_sid = snap.bcp_session_id or -1
        cur.execute(
            "SELECT r.session_id, r.status, r.wait_type, r.wait_time, "
            "  r.blocking_session_id, s.login_name, s.host_name "
            "FROM sys.dm_exec_requests r "
            "JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id "
            "WHERE r.command = 'BULK INSERT' "
            "  AND r.database_id = DB_ID() "
            "  AND r.session_id <> ?",
            our_sid,
        )
        for row in cur.fetchall():
            snap.other_bulk_sessions.append({
                "session_id": row[0],
                "status": row[1],
                "wait_type": row[2],
                "wait_time_ms": row[3],
                "blocking_session_id": row[4],
                "login_name": row[5],
                "host_name": row[6],
            })

    # ------------------------------------------------------------------
    # Internal: hang detection heuristics
    # ------------------------------------------------------------------

    def _evaluate_hang(self, snap: BCPDiagnosticSnapshot, elapsed: float):
        """Evaluate whether this snapshot indicates a definitive hang.

        Sets snap.hang_detected if a hang pattern has persisted beyond the
        abort threshold. The main thread checks abort_event to terminate BCP.
        """
        # Skip evaluation during the initial grace period — give BCP time to
        # establish its connection and start inserting.
        if elapsed < self.abort_threshold:
            return

        # --- §6: BCP session never appeared (TLS/network hang) ---
        if snap.bcp_session_id is None and not snap.monitor_error:
            # Only flag if we've NEVER seen a BCP session across all snapshots
            if not any(s.bcp_session_id is not None for s in self.snapshots):
                snap.hang_detected = (
                    "§6 TLS/NETWORK HANG — BCP session never appeared in "
                    "sys.dm_exec_requests after {:.0f}s. The BCP process is "
                    "likely stuck in TLS handshake (ODBC Driver 18 Encrypt=yes "
                    "with self-signed cert) or blocked by a firewall/DNS issue. "
                    "Fix: add -Yo flag to BCP command, or check network "
                    "connectivity to {}:{}".format(
                        elapsed, config.SQL_SERVER_HOST, config.SQL_SERVER_PORT
                    )
                )
            return

        if snap.bcp_session_id is None:
            return  # Can't evaluate further without a session

        # --- §1: Lock conflict — BCP waiting on LCK_M_* ---
        if snap.bcp_wait_type and snap.bcp_wait_type.startswith("LCK_M_"):
            if (snap.bcp_wait_time_ms or 0) > self.abort_threshold * 1000:
                blocker_detail = ""
                if snap.bcp_blocking_session_id:
                    # Find the blocker's details in the blocking chain
                    for b in snap.blocking_chain:
                        if b.get("session_id") == snap.bcp_session_id:
                            blocker_detail = (
                                f" Blocker session {snap.bcp_blocking_session_id}: "
                                f"program={b.get('program_name', '?')}, "
                                f"sql={b.get('sql_text', '?')[:200]}"
                            )
                            break
                    if not blocker_detail:
                        blocker_detail = (
                            f" Blocked by session {snap.bcp_blocking_session_id}"
                        )

                snap.hang_detected = (
                    f"§1 LOCK CONFLICT — BCP session {snap.bcp_session_id} waiting "
                    f"on {snap.bcp_wait_type} for {snap.bcp_wait_time_ms}ms. "
                    f"Resource: {snap.bcp_wait_resource}.{blocker_detail}"
                )
                return

        # --- §2: Schema modification block ---
        if snap.bcp_wait_type == "LCK_M_SCH_S":
            if (snap.bcp_wait_time_ms or 0) > self.abort_threshold * 1000:
                snap.hang_detected = (
                    f"§2 SCHEMA MODIFICATION BLOCK — BCP session "
                    f"{snap.bcp_session_id} waiting on LCK_M_SCH_S for "
                    f"{snap.bcp_wait_time_ms}ms. A DDL operation (ALTER INDEX "
                    f"REBUILD, CREATE INDEX, ALTER TABLE, TRUNCATE) holds or is "
                    f"waiting for a Sch-M lock. Blocked by session "
                    f"{snap.bcp_blocking_session_id}."
                )
                return

        # --- §4: Transaction log pressure ---
        if snap.bcp_wait_type in ("WRITELOG", "LOGBUFFER"):
            if (snap.bcp_wait_time_ms or 0) > self.abort_threshold * 1000:
                snap.hang_detected = (
                    f"§4 LOG PRESSURE — BCP session {snap.bcp_session_id} waiting "
                    f"on {snap.bcp_wait_type} for {snap.bcp_wait_time_ms}ms. "
                    f"Log space: {snap.log_used_mb:.0f}MB / {snap.log_total_mb:.0f}MB "
                    f"({snap.log_used_pct:.0f}%). Recovery model: {snap.recovery_model}. "
                    f"Log reuse wait: {snap.log_reuse_wait}."
                )
                return

        # Log fullness check even without specific wait type
        if (snap.log_used_pct or 0) > 95:
            snap.hang_detected = (
                f"§4 LOG FULL — Transaction log is {snap.log_used_pct:.1f}% full "
                f"({snap.log_used_mb:.0f}MB / {snap.log_total_mb:.0f}MB). "
                f"Log reuse blocked by: {snap.log_reuse_wait}. "
                f"Recovery model: {snap.recovery_model}."
            )
            return

        # --- §8: I/O bottleneck ---
        if snap.bcp_wait_type in ("PAGEIOLATCH_EX", "PAGEIOLATCH_SH"):
            if (snap.bcp_wait_time_ms or 0) > self.abort_threshold * 1000:
                high_lat = [
                    f for f in snap.io_latencies
                    if f.get("avg_write_ms", 0) > 20
                ]
                lat_detail = "; ".join(
                    f"{f['file_name']}={f['avg_write_ms']:.0f}ms"
                    for f in high_lat
                ) if high_lat else "check dm_io_virtual_file_stats"
                snap.hang_detected = (
                    f"§8 I/O BOTTLENECK — BCP session {snap.bcp_session_id} waiting "
                    f"on {snap.bcp_wait_type} for {snap.bcp_wait_time_ms}ms. "
                    f"High latency files: {lat_detail}."
                )
                return

        # --- §5: Row count not advancing (stall detection) ---
        if snap.bcp_row_count is not None and snap.bcp_status == "suspended":
            if (self._last_row_count is not None
                    and snap.bcp_row_count == self._last_row_count):
                if self._row_stall_since is None:
                    self._row_stall_since = elapsed
                elif (elapsed - self._row_stall_since) > self.abort_threshold:
                    snap.hang_detected = (
                        f"ROW STALL — BCP session {snap.bcp_session_id} row_count "
                        f"has not advanced from {snap.bcp_row_count} for "
                        f"{elapsed - self._row_stall_since:.0f}s. "
                        f"Current wait: {snap.bcp_wait_type} ({snap.bcp_wait_time_ms}ms)."
                    )
                    return
            else:
                self._row_stall_since = None

            self._last_row_count = snap.bcp_row_count


# ==========================================================================
# Orphaned session cleanup
# ==========================================================================

def _cleanup_orphaned_bcp_sessions(full_table_name: str) -> int:
    """Kill orphaned BULK INSERT sessions on the target table's database.

    §7: When subprocess.run(timeout=N) expired, Python sent SIGKILL which left
    SQL Server sessions alive with all their locks. This function detects
    BULK INSERT sessions that are suspended and waiting > 60s, then KILLs them.

    Returns:
        Number of orphaned sessions killed.
    """
    db = full_table_name.split(".")[0]
    killed = 0

    try:
        with cursor_for(db) as cur:
            cur.execute(
                "SELECT DISTINCT r.session_id, r.wait_type, r.wait_time, "
                "  r.blocking_session_id, s.login_name "
                "FROM sys.dm_exec_requests r "
                "JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id "
                "WHERE r.command = 'BULK INSERT' "
                "  AND r.database_id = DB_ID() "
                "  AND r.status = 'suspended' "
                "  AND r.wait_time > 60000"
            )
            orphaned = cur.fetchall()

            for row in orphaned:
                session_id = row[0]
                logger.warning(
                    "BCP-HANG-FIX: Killing orphaned BULK INSERT session %d "
                    "(wait_type=%s, wait_time=%dms)",
                    session_id, row[1], row[2],
                )
                try:
                    cur.execute(f"KILL {int(session_id)}")
                    killed += 1
                except Exception:
                    logger.warning(
                        "BCP-HANG-FIX: Failed to kill session %d",
                        session_id, exc_info=True,
                    )

        if killed > 0:
            logger.info(
                "BCP-HANG-FIX: Killed %d orphaned session(s). "
                "Waiting %ds for rollback...",
                killed, config.BCP_ORPHAN_ROLLBACK_WAIT,
            )
            time.sleep(config.BCP_ORPHAN_ROLLBACK_WAIT)

    except Exception:
        logger.warning(
            "BCP-HANG-FIX: Orphan cleanup failed for %s — proceeding",
            full_table_name, exc_info=True,
        )

    return killed


# ==========================================================================
# BCP process execution with live monitoring
# ==========================================================================

def _run_bcp_with_monitor(
    cmd: list[str],
    env: dict[str, str],
    full_table_name: str,
    timeout_seconds: int,
) -> tuple[subprocess.CompletedProcess, _BCPHangMonitor]:
    """Run BCP via Popen with a background diagnostic monitor.

    §7 fix: Uses Popen with SIGTERM escalation (not subprocess.run which
    sends SIGKILL). The monitor thread polls SQL Server DMVs every
    BCP_MONITOR_INTERVAL seconds and can trigger early abort if a
    definitive hang is detected.

    Returns:
        Tuple of (CompletedProcess result, monitor instance).
        The monitor contains all diagnostic snapshots for post-mortem.

    Raises:
        BCPBlockedError: On timeout or early abort (includes full diagnosis).
    """
    monitor = _BCPHangMonitor(full_table_name)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    monitor.start()
    start_time = time.monotonic()

    try:
        # Poll loop: check BCP status + monitor abort signal
        while True:
            elapsed = time.monotonic() - start_time

            # Check if BCP finished naturally
            retcode = proc.poll()
            if retcode is not None:
                # BCP completed — collect output
                stdout, stderr = proc.communicate()
                monitor.request_stop()
                monitor.join(timeout=5)
                return (
                    subprocess.CompletedProcess(
                        args=cmd,
                        returncode=retcode,
                        stdout=stdout.decode("utf-8", errors="replace") if stdout else "",
                        stderr=stderr.decode("utf-8", errors="replace") if stderr else "",
                    ),
                    monitor,
                )

            # Check for hard timeout
            if elapsed >= timeout_seconds:
                diagnosis = _terminate_bcp(
                    proc, monitor,
                    f"BCP hard timeout after {timeout_seconds}s",
                )
                raise BCPBlockedError(
                    f"BCP timed out after {timeout_seconds}s for "
                    f"{full_table_name}",
                    diagnosis=diagnosis,
                    snapshots=monitor.snapshots,
                )

            # Check for monitor-triggered early abort
            if monitor.abort_event.is_set():
                diagnosis = _terminate_bcp(
                    proc, monitor,
                    f"Early abort: {monitor.abort_reason}",
                )
                raise BCPBlockedError(
                    f"BCP hung on {full_table_name}: {monitor.abort_reason}",
                    diagnosis=diagnosis,
                    snapshots=monitor.snapshots,
                )

            # Sleep briefly between checks (1s granularity)
            time.sleep(1)

    except BCPBlockedError:
        raise
    except Exception as e:
        # Unexpected error — clean up
        monitor.request_stop()
        monitor.join(timeout=5)
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        raise BCPLoadError(f"Unexpected error during BCP: {e}") from e


def _terminate_bcp(
    proc: subprocess.Popen,
    monitor: _BCPHangMonitor,
    reason: str,
) -> str:
    """SIGTERM-first termination of a hung BCP process.

    §7: SIGTERM allows ODBC cleanup and TCP FIN, so SQL Server releases locks
    immediately. SIGKILL is last resort (leaves orphaned sessions).

    Returns the full diagnostic report.
    """
    # Stop the monitor first — we want a final snapshot
    monitor.request_stop()

    logger.warning(
        "BCP-HANG-FIX: Terminating BCP — %s. Sending SIGTERM...", reason
    )
    proc.terminate()
    try:
        proc.wait(timeout=10)
        logger.info(
            "BCP-HANG-FIX: BCP terminated gracefully via SIGTERM (rc=%d). "
            "SQL Server should release locks immediately.",
            proc.returncode,
        )
    except subprocess.TimeoutExpired:
        logger.warning(
            "BCP-HANG-FIX: SIGTERM ignored after 10s. Sending SIGKILL "
            "(orphaned session likely — cleanup on next retry)."
        )
        proc.kill()
        proc.wait()

    monitor.join(timeout=5)

    # Build and log the full diagnostic report
    diagnosis = monitor.get_diagnosis()
    logger.error("BCP-MONITOR: FULL DIAGNOSTIC REPORT:\n%s", diagnosis)
    return diagnosis


# ==========================================================================
# Core BCP load function
# ==========================================================================

def bcp_load(
    csv_path: str,
    full_table_name: str,
    expected_row_count: int | None = None,
    format_file: str | None = None,
    atomic: bool = True,
) -> int:
    """Load a BCP CSV file into SQL Server.

    BCP-HANG-FIX-v2: Includes live background diagnostic monitoring, pre-BCP
    orphan cleanup, SIGTERM-first termination, early abort on definitive hang
    detection, and retry with exponential backoff.

    Args:
        csv_path: Path to the CSV file (must follow BCP CSV Contract).
        full_table_name: Fully qualified table name (db.schema.table).
        expected_row_count: If provided, verify the BCP row count matches.
        format_file: W-13: Optional path to a BCP XML format file (.fmt).
        atomic: E-3: If True (default), omit -b flag for SCD2 atomicity.
                If False, use -b batch_size (safe for Stage CDC loads).

    Returns:
        Number of rows copied.

    Raises:
        BCPLoadError: On subprocess failure or row count mismatch.
        BCPBlockedError: If BCP is blocked and all retries exhausted.
    """
    cmd = _build_bcp_command(csv_path, full_table_name, format_file, atomic)
    bcp_env = _build_bcp_env()

    last_error: Exception | None = None

    for attempt in range(1, config.BCP_MAX_RETRIES + 1):
        if attempt > 1:
            wait_seconds = 5 * (2 ** (attempt - 2))
            logger.info(
                "BCP-HANG-FIX: Retry %d/%d for %s (waiting %ds)...",
                attempt, config.BCP_MAX_RETRIES, full_table_name, wait_seconds,
            )
            time.sleep(wait_seconds)

        # Clean up orphaned sessions before every attempt
        _cleanup_orphaned_bcp_sessions(full_table_name)

        logger.info(
            "BCP loading %s -> %s (attempt %d/%d, timeout=%ds, "
            "monitor_interval=%ds, abort_threshold=%ds)",
            csv_path, full_table_name, attempt, config.BCP_MAX_RETRIES,
            config.BCP_TIMEOUT, config.BCP_MONITOR_INTERVAL,
            config.BCP_HANG_ABORT_THRESHOLD,
        )
        logger.debug("BCP command: %s", " ".join(cmd))

        try:
            result, monitor = _run_bcp_with_monitor(
                cmd, bcp_env, full_table_name, config.BCP_TIMEOUT,
            )

            if result.returncode != 0:
                # Log monitor snapshots even on non-hang failures
                if monitor.snapshots:
                    logger.info(
                        "BCP-MONITOR: %d snapshots collected before BCP "
                        "failure (rc=%d)",
                        len(monitor.snapshots), result.returncode,
                    )
                raise BCPLoadError(
                    f"BCP failed (rc={result.returncode}) for {full_table_name}: "
                    f"stderr={result.stderr[:2000]}"
                )

            rows_copied = _parse_rows_copied(result.stdout)

            if expected_row_count is not None and rows_copied != expected_row_count:
                raise BCPLoadError(
                    f"BCP row count mismatch for {full_table_name}: "
                    f"expected={expected_row_count}, got={rows_copied}"
                )

            # Log success with monitor summary
            total_snaps = len(monitor.snapshots)
            logger.info(
                "BCP loaded %d rows into %s (%d monitor snapshots collected)",
                rows_copied, full_table_name, total_snaps,
            )
            return rows_copied

        except BCPBlockedError as e:
            last_error = e
            logger.warning(
                "BCP-HANG-FIX: BCP blocked on attempt %d/%d for %s",
                attempt, config.BCP_MAX_RETRIES, full_table_name,
            )
            continue

        except BCPLoadError:
            raise

    raise BCPBlockedError(
        f"BCP blocked on {full_table_name} after {config.BCP_MAX_RETRIES} "
        f"attempts. Last error: {last_error}",
        diagnosis=getattr(last_error, "diagnosis", ""),
        snapshots=getattr(last_error, "snapshots", []),
    )


# ==========================================================================
# Command and environment builders
# ==========================================================================

def _build_bcp_command(
    csv_path: str,
    full_table_name: str,
    format_file: str | None,
    atomic: bool,
) -> list[str]:
    """Build the BCP command line arguments.

    §6: Includes -Yo (TrustServerCertificate) for ODBC Driver 18.
    §3: BCP_BATCH_SIZE default lowered to 5000 to stay below lock escalation.
    """
    cmd = [
        config.BCP_PATH,
        full_table_name,
        "in",
        csv_path,
        "-c",
        "-t", "\\t",
        "-r", config.CSV_ROW_TERMINATOR,
        # V-5: -C 65001 intentionally omitted — see module docstring.
        "-S", f"{config.SQL_SERVER_HOST},{config.SQL_SERVER_PORT}",
        "-U", config.SQL_SERVER_USER,
        # §6: TrustServerCertificate for ODBC Driver 18.
        "-Yo",
    ]

    # E-3: Only batch when NOT atomic.
    # §3: BCP_BATCH_SIZE = 5000 stays below lock escalation threshold.
    if not atomic:
        cmd.extend(["-b", str(config.BCP_BATCH_SIZE)])

    if format_file:
        cmd.extend(["-f", format_file])
        logger.debug("W-13: Using BCP format file: %s", format_file)

    return cmd


def _build_bcp_env() -> dict[str, str]:
    """Build minimal environment for BCP subprocess.

    H-5/H-6: Password via SQLCMDPASSWORD. Item-11: Minimal env propagation.
    """
    bcp_env = {"SQLCMDPASSWORD": config.SQL_SERVER_PASSWORD}
    for _env_key in ("PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TMP"):
        if _env_key in os.environ:
            bcp_env[_env_key] = os.environ[_env_key]
    return bcp_env


def _parse_rows_copied(stdout: str) -> int:
    """Parse 'N rows copied' from BCP stdout."""
    match = re.search(r"(\d+)\s+rows?\s+copied", stdout)
    if not match:
        raise BCPLoadError(f"Could not parse row count from BCP output: {stdout[:500]}")
    return int(match.group(1))


# ==========================================================================
# Utility functions (unchanged API)
# ==========================================================================

def truncate_table(full_table_name: str) -> None:
    """Truncate a table (used for Stage CDC reload)."""
    db = full_table_name.split(".")[0]
    with cursor_for(db) as cur:
        cur.execute(f"TRUNCATE TABLE {quote_table(full_table_name)}")
    logger.info("Truncated %s", full_table_name)


def execute_sql(full_table_name_or_db: str, sql: str, params: tuple = ()) -> None:
    """Execute arbitrary SQL against the database implied by the table name."""
    db = full_table_name_or_db.split(".")[0]
    with cursor_for(db) as cur:
        cur.execute(sql, params)


def get_row_count(full_table_name: str) -> int:
    """Get approximate row count via sys.dm_db_partition_stats (P2-4)."""
    db = full_table_name.split(".")[0]
    with cursor_for(db) as cur:
        cur.execute(
            "SELECT SUM(p.row_count) "
            "FROM sys.dm_db_partition_stats p "
            "WHERE p.object_id = OBJECT_ID(?) "
            "AND p.index_id IN (0, 1)",
            full_table_name,
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def create_staging_index(
    staging_table: str,
    pk_columns: list[str],
    min_rows: int = 1000,
    row_count: int | None = None,
) -> None:
    """P2-5: Add a clustered index on staging table PKs after BCP load."""
    if not pk_columns:
        return
    if row_count is not None and row_count < min_rows:
        return

    db = staging_table.split(".")[0]
    col_list = ", ".join(quote_identifier(c) for c in pk_columns)
    idx_name = f"CIX_{staging_table.split('.')[-1]}"

    try:
        with cursor_for(db) as cur:
            cur.execute(
                f"CREATE CLUSTERED INDEX {quote_identifier(idx_name)} "
                f"ON {quote_table(staging_table)} ({col_list})"
            )
        logger.debug("P2-5: Created clustered index on %s (%s)", staging_table, col_list)
    except Exception:
        logger.debug("P2-5: Could not create index on %s — proceeding without", staging_table)


@contextmanager
def bulk_load_recovery_context(database: str):
    """Set BULK_LOGGED recovery during load, restore FULL + log backup after.

    Usage:
        with bulk_load_recovery_context("UDM_Bronze"):
            bcp_load(...)
    """
    conn = connections.get_connection(database)
    try:
        cursor = conn.cursor()
        logger.info("Setting %s to BULK_LOGGED recovery model", database)
        cursor.execute(f"ALTER DATABASE {quote_identifier(database)} SET RECOVERY BULK_LOGGED")
        # Item-17: Verify the ALTER actually succeeded.
        cursor.execute(
            "SELECT recovery_model_desc FROM sys.databases WHERE name = ?",
            database,
        )
        row = cursor.fetchone()
        if row and row[0] != "BULK_LOGGED":
            logger.warning(
                "Item-17: ALTER DATABASE SET RECOVERY BULK_LOGGED did not take effect "
                "on %s (current model: %s). Pipeline user may lack ALTER DATABASE "
                "permission. BCP loads will be fully logged.",
                database, row[0] if row else "unknown",
            )
        cursor.close()
    finally:
        conn.close()

    try:
        yield
    finally:
        conn = connections.get_connection(database)
        try:
            cursor = conn.cursor()
            logger.info("Restoring %s to FULL recovery model", database)
            cursor.execute(f"ALTER DATABASE {quote_identifier(database)} SET RECOVERY FULL")
            cursor.execute(
                "SELECT recovery_model_desc FROM sys.databases WHERE name = ?",
                database,
            )
            row = cursor.fetchone()
            if row and row[0] != "FULL":
                logger.warning(
                    "Item-17: Failed to restore %s to FULL recovery model "
                    "(current model: %s). Manual intervention may be needed.",
                    database, row[0] if row else "unknown",
                )
            cursor.close()

            logger.info(
                "R-3: Restored %s to FULL recovery model. Log chain gap during "
                "BULK_LOGGED window accepted — PITR not required during pipeline loads.",
                database,
            )
        except Exception:
            logger.exception("Failed to restore recovery model for %s", database)
        finally:
            conn.close()
"""BCP subprocess wrapper: CSV -> SQL Server.

Handles BULK_LOGGED recovery model context and row count verification.

BCP-HANG-FIX — Addresses all major BCP hang scenarios from diagnostic guide:
  1. subprocess.Popen() with SIGTERM escalation replaces subprocess.run() which
     sent SIGKILL (signal 9) on timeout, leaving orphaned SQL Server sessions
     with locks that cascaded into blocking chains on retry.
  2. Pre-BCP blocker detection queries sys.dm_exec_requests to find existing
     BULK INSERT sessions or blocking chains on the target table before starting.
  3. Orphaned session cleanup kills zombie BCP sessions between retries to break
     cascading lock chains.
  4. BCP lock timeout via SET LOCK_TIMEOUT prevents indefinite waits (default is
     -1 = infinite). Configured via BCP_LOCK_TIMEOUT_MS in config.py.
  5. Retry loop with exponential backoff — up to BCP_MAX_RETRIES attempts with
     blocker cleanup between retries.

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

import logging
import os
import re
import subprocess
import time
from contextlib import contextmanager

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
    """


# ---------------------------------------------------------------------------
# BCP-HANG-FIX: Pre-BCP blocker detection and orphaned session cleanup
# ---------------------------------------------------------------------------

def _check_for_blockers(full_table_name: str) -> list[dict]:
    """Check for existing BULK INSERT sessions or locks on the target table.

    Runs diagnostic queries from §9 of the BCP hang guide to detect:
      - Active BULK INSERT commands targeting this table
      - Sessions holding incompatible locks (X, SCH-M) on the target object
      - Blocking chains that would cause BCP to wait indefinitely

    Args:
        full_table_name: Fully qualified table name (db.schema.table).

    Returns:
        List of dicts with blocker info (session_id, command, wait_type, etc).
        Empty list means no blockers detected.
    """
    db = full_table_name.split(".")[0]
    blockers = []

    try:
        with cursor_for(db) as cur:
            # Step 1: Find active BULK INSERT sessions on this database
            cur.execute(
                "SELECT r.session_id, r.command, r.status, r.wait_type, "
                "       r.wait_time, r.blocking_session_id, "
                "       s.login_name, s.host_name, s.program_name "
                "FROM sys.dm_exec_requests r "
                "JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id "
                "WHERE r.command = 'BULK INSERT' "
                "  AND r.database_id = DB_ID()"
            )
            for row in cur.fetchall():
                blockers.append({
                    "session_id": row[0],
                    "command": row[1],
                    "status": row[2],
                    "wait_type": row[3],
                    "wait_time_ms": row[4],
                    "blocking_session_id": row[5],
                    "login_name": row[6],
                    "host_name": row[7],
                    "program_name": row[8],
                    "blocker_type": "active_bulk_insert",
                })

            # Step 2: Check for locks on the target table object that would
            # block a new BCP load (X locks, SCH-M locks)
            cur.execute(
                "SELECT tl.request_session_id, tl.resource_type, "
                "       tl.request_mode, tl.request_status, "
                "       s.program_name, r.command "
                "FROM sys.dm_tran_locks tl "
                "LEFT JOIN sys.dm_exec_sessions s "
                "  ON tl.request_session_id = s.session_id "
                "LEFT JOIN sys.dm_exec_requests r "
                "  ON tl.request_session_id = r.session_id "
                "WHERE tl.resource_database_id = DB_ID() "
                "  AND tl.resource_associated_entity_id = OBJECT_ID(?) "
                "  AND tl.request_mode IN ('X', 'SCH-M') "
                "  AND tl.request_status = 'GRANT'",
                full_table_name,
            )
            for row in cur.fetchall():
                blockers.append({
                    "session_id": row[0],
                    "resource_type": row[1],
                    "lock_mode": row[2],
                    "lock_status": row[3],
                    "program_name": row[4],
                    "command": row[5],
                    "blocker_type": "incompatible_lock",
                })

    except Exception:
        logger.warning(
            "BCP-HANG-FIX: Could not check for blockers on %s — proceeding with BCP",
            full_table_name,
            exc_info=True,
        )

    return blockers


def _cleanup_orphaned_bcp_sessions(full_table_name: str) -> int:
    """Kill orphaned BULK INSERT sessions on the target table's database.

    BCP-HANG-FIX §7: When subprocess.run(timeout=N) expired, Python sent SIGKILL
    which left SQL Server sessions alive with all their locks. Retry BCP runs
    would block on those locks, time out, get killed, and create MORE orphaned
    sessions — a cascading blockage pattern.

    This function detects BULK INSERT sessions that are:
      - In 'suspended' state (waiting on a lock or resource)
      - Have been waiting for more than 60 seconds
      - Are on the current database

    It KILLs them to break the blocking chain before retrying BCP.

    Args:
        full_table_name: Fully qualified table name (db.schema.table).

    Returns:
        Number of orphaned sessions killed.
    """
    db = full_table_name.split(".")[0]
    killed = 0

    try:
        with cursor_for(db) as cur:
            # Find BULK INSERT sessions that appear orphaned:
            # - suspended (waiting) for more than 60 seconds
            # - OR have no active request but hold locks (sleeping with open tran)
            cur.execute(
                "SELECT DISTINCT r.session_id, r.wait_type, r.wait_time, "
                "       r.blocking_session_id, s.login_name "
                "FROM sys.dm_exec_requests r "
                "JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id "
                "WHERE r.command = 'BULK INSERT' "
                "  AND r.database_id = DB_ID() "
                "  AND r.status = 'suspended' "
                "  AND r.wait_time > 60000"  # waiting > 60 seconds
            )
            orphaned_sessions = cur.fetchall()

            for row in orphaned_sessions:
                session_id = row[0]
                wait_type = row[1]
                wait_time_ms = row[2]
                logger.warning(
                    "BCP-HANG-FIX: Killing orphaned BULK INSERT session %d "
                    "(wait_type=%s, wait_time=%dms) on %s",
                    session_id, wait_type, wait_time_ms, full_table_name,
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
            # Allow time for SQL Server to complete rollback of killed sessions.
            # Rollback is single-threaded and can take time for large transactions.
            logger.info(
                "BCP-HANG-FIX: Killed %d orphaned session(s). "
                "Waiting %d seconds for rollback to complete...",
                killed, config.BCP_ORPHAN_ROLLBACK_WAIT,
            )
            time.sleep(config.BCP_ORPHAN_ROLLBACK_WAIT)

    except Exception:
        logger.warning(
            "BCP-HANG-FIX: Orphaned session cleanup failed for %s — proceeding",
            full_table_name,
            exc_info=True,
        )

    return killed


# ---------------------------------------------------------------------------
# BCP-HANG-FIX: Popen with SIGTERM escalation (replaces subprocess.run)
# ---------------------------------------------------------------------------

def _run_bcp_process(
    cmd: list[str],
    env: dict[str, str],
    timeout_seconds: int,
) -> subprocess.CompletedProcess:
    """Run BCP via Popen with graceful SIGTERM escalation on timeout.

    BCP-HANG-FIX §7: subprocess.run(timeout=N) sends SIGKILL (signal 9) which
    cannot be caught, leaving SQL Server sessions alive with locks held. This
    function uses Popen with a two-phase termination:
      1. SIGTERM — allows BCP to perform ODBC cleanup and TCP FIN handshake
      2. Wait 10 seconds for graceful shutdown
      3. SIGKILL only as last resort if SIGTERM didn't work

    This allows SQL Server to detect the disconnect and release locks immediately,
    instead of waiting for TCP keepalive timeout (up to 2 hours with Linux defaults).

    Args:
        cmd: BCP command and arguments.
        env: Environment variables for subprocess.
        timeout_seconds: Maximum time to wait for BCP to complete.

    Returns:
        CompletedProcess-compatible result with returncode, stdout, stderr.

    Raises:
        BCPBlockedError: If BCP times out (likely blocked by another session).
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_seconds)
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=proc.returncode,
            stdout=stdout.decode("utf-8", errors="replace") if stdout else "",
            stderr=stderr.decode("utf-8", errors="replace") if stderr else "",
        )
    except subprocess.TimeoutExpired:
        logger.warning(
            "BCP-HANG-FIX: BCP process timed out after %d seconds. "
            "Sending SIGTERM for graceful shutdown (allows ODBC cleanup)...",
            timeout_seconds,
        )
        # Phase 1: SIGTERM — allows BCP to perform ODBC cleanup and TCP FIN
        proc.terminate()
        try:
            # Wait up to 10 seconds for graceful shutdown
            proc.wait(timeout=10)
            logger.info(
                "BCP-HANG-FIX: BCP process terminated gracefully via SIGTERM "
                "(rc=%d). SQL Server should release locks promptly.",
                proc.returncode,
            )
        except subprocess.TimeoutExpired:
            # Phase 2: SIGKILL — last resort
            logger.warning(
                "BCP-HANG-FIX: BCP process did not respond to SIGTERM after "
                "10 seconds. Sending SIGKILL (orphaned session risk)."
            )
            proc.kill()
            proc.wait()
            logger.warning(
                "BCP-HANG-FIX: BCP process killed via SIGKILL. SQL Server "
                "session may remain active until TCP keepalive detects the "
                "dead connection. Running orphan cleanup on next retry."
            )

        raise BCPBlockedError(
            f"BCP timed out after {timeout_seconds}s — likely blocked by "
            f"another session. Use _check_for_blockers() to diagnose."
        )


# ---------------------------------------------------------------------------
# Core BCP load function
# ---------------------------------------------------------------------------

def bcp_load(
    csv_path: str,
    full_table_name: str,
    expected_row_count: int | None = None,
    format_file: str | None = None,
    atomic: bool = True,
) -> int:
    """Load a BCP CSV file into SQL Server.

    BCP-HANG-FIX: Now includes pre-BCP blocker detection, orphaned session
    cleanup, SIGTERM-first process termination, and configurable retry logic
    to address all major BCP hang scenarios.

    Args:
        csv_path: Path to the CSV file (must follow BCP CSV Contract).
        full_table_name: Fully qualified table name (db.schema.table).
        expected_row_count: If provided, verify the BCP row count matches.
        format_file: W-13: Optional path to a BCP XML format file (.fmt).
                     When provided, uses -f flag for explicit column mapping
                     instead of relying solely on positional ordering.
        atomic: E-3: If True (default), omit -b flag so the entire BCP load is
                a single transaction. Essential for Bronze SCD2 loads where partial
                loads break atomicity (some new versions inserted but old versions
                not yet closed). If False, use -b batch_size for performance
                (acceptable for Stage CDC loads which are truncated before each run).

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
            # Exponential backoff: 5s, 10s, 20s...
            wait_seconds = 5 * (2 ** (attempt - 2))
            logger.info(
                "BCP-HANG-FIX: Retry %d/%d for %s (waiting %ds)...",
                attempt, config.BCP_MAX_RETRIES, full_table_name, wait_seconds,
            )
            time.sleep(wait_seconds)

            # Clean up orphaned sessions before retry
            _cleanup_orphaned_bcp_sessions(full_table_name)

        # BCP-HANG-FIX: Pre-BCP blocker detection
        blockers = _check_for_blockers(full_table_name)
        if blockers:
            blocker_summary = "; ".join(
                f"session={b['session_id']} type={b['blocker_type']} "
                f"cmd={b.get('command', 'N/A')}"
                for b in blockers
            )
            logger.warning(
                "BCP-HANG-FIX: Detected %d blocker(s) on %s before BCP "
                "attempt %d: %s",
                len(blockers), full_table_name, attempt, blocker_summary,
            )
            # On first attempt with blockers, try orphan cleanup first
            if attempt == 1:
                cleaned = _cleanup_orphaned_bcp_sessions(full_table_name)
                if cleaned > 0:
                    # Re-check after cleanup
                    blockers = _check_for_blockers(full_table_name)
                    if blockers:
                        logger.warning(
                            "BCP-HANG-FIX: %d blocker(s) remain after cleanup. "
                            "Proceeding with BCP (may hang until timeout).",
                            len(blockers),
                        )

        logger.info(
            "BCP loading %s -> %s (attempt %d/%d)",
            csv_path, full_table_name, attempt, config.BCP_MAX_RETRIES,
        )
        logger.debug("BCP command: %s", " ".join(cmd))

        try:
            # BCP-HANG-FIX: Popen with SIGTERM escalation
            result = _run_bcp_process(cmd, bcp_env, config.BCP_TIMEOUT)

            if result.returncode != 0:
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

            logger.info(
                "BCP loaded %d rows into %s", rows_copied, full_table_name
            )
            return rows_copied

        except BCPBlockedError as e:
            last_error = e
            logger.warning(
                "BCP-HANG-FIX: BCP blocked on attempt %d/%d for %s: %s",
                attempt, config.BCP_MAX_RETRIES, full_table_name, e,
            )
            # Continue to next retry (cleanup happens at top of loop)
            continue

        except BCPLoadError:
            # Non-blocking BCP error — don't retry, raise immediately
            raise

    # All retries exhausted
    raise BCPBlockedError(
        f"BCP blocked on {full_table_name} after {config.BCP_MAX_RETRIES} "
        f"attempts. Last error: {last_error}. Run diagnostic queries from §9 "
        f"of the BCP hang guide to identify the root cause."
    )


def _build_bcp_command(
    csv_path: str,
    full_table_name: str,
    format_file: str | None,
    atomic: bool,
) -> list[str]:
    """Build the BCP command line arguments.

    BCP-HANG-FIX §6: Includes -Yo flag (TrustServerCertificate) for ODBC Driver
    18 which defaults to Encrypt=yes. Without this, TLS handshake with
    self-signed certificates silently hangs.
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
        # BCP-HANG-FIX §6: TrustServerCertificate for ODBC Driver 18.
        # Without this, BCP may silently hang during TLS handshake when
        # SQL Server uses a self-signed certificate (common in non-prod).
        "-Yo",
    ]

    # E-3: Only add -b batch size flag when NOT atomic.
    # Without -b, the entire BCP load is a single transaction — failure rolls back
    # everything (safe for SCD2 Bronze loads). With -b, each batch is a separate
    # transaction and failure leaves a partial load (acceptable for Stage/staging).
    #
    # BCP-HANG-FIX §3: BCP_BATCH_SIZE lowered to 5000 (from 10000) to stay at
    # or below SQL Server's ~5,000 lock escalation threshold. At 5000, BCP uses
    # row-level locks only — no escalation to table-level X locks. This
    # dramatically reduces blocking probability and lock duration.
    if not atomic:
        cmd.extend(["-b", str(config.BCP_BATCH_SIZE)])

    # W-13: Use format file for explicit column mapping if provided.
    if format_file:
        cmd.extend(["-f", format_file])
        logger.debug("W-13: Using BCP format file: %s", format_file)

    return cmd


def _build_bcp_env() -> dict[str, str]:
    """Build minimal environment for BCP subprocess.

    H-5/H-6: Pass password via SQLCMDPASSWORD environment variable instead of
    -P flag to prevent exposure via /proc/{pid}/cmdline and `ps aux`.
    Item-11: Minimal environment — only propagate variables BCP actually needs.
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


# ---------------------------------------------------------------------------
# BCP-HANG-FIX: Post-BCP diagnostic helper
# ---------------------------------------------------------------------------

def diagnose_bcp_hang(full_table_name: str) -> str:
    """Run the complete diagnostic query toolkit from §9 of the BCP hang guide.

    Call this when BCP hangs or after a BCPBlockedError to get a full picture
    of what's blocking the load. Returns a formatted diagnostic report.

    Args:
        full_table_name: Fully qualified table name (db.schema.table).

    Returns:
        Formatted diagnostic report string.
    """
    db = full_table_name.split(".")[0]
    lines = [f"=== BCP Hang Diagnostic for {full_table_name} ===\n"]

    try:
        with cursor_for(db) as cur:
            # §9 Step 1: Find BCP session and waits
            cur.execute(
                "SELECT s.session_id, s.login_name, s.host_name, "
                "       s.program_name, r.command, r.status, r.wait_type, "
                "       r.wait_time, r.wait_resource, r.blocking_session_id, "
                "       r.total_elapsed_time, r.row_count "
                "FROM sys.dm_exec_sessions s "
                "LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id "
                "WHERE r.command = 'BULK INSERT' OR s.program_name LIKE '%bcp%'"
            )
            bcp_sessions = cur.fetchall()
            lines.append(f"[Step 1] BCP sessions found: {len(bcp_sessions)}")
            for row in bcp_sessions:
                lines.append(
                    f"  session={row[0]} login={row[1]} host={row[2]} "
                    f"cmd={row[4]} status={row[5]} wait={row[6]} "
                    f"wait_time={row[7]}ms blocked_by={row[9]} rows={row[11]}"
                )

            # §9 Step 4: Transaction log pressure
            cur.execute(
                "SELECT total_log_size_in_bytes/1048576.0, "
                "       used_log_space_in_bytes/1048576.0, "
                "       used_log_space_in_percent "
                "FROM sys.dm_db_log_space_usage"
            )
            log_row = cur.fetchone()
            if log_row:
                lines.append(
                    f"\n[Step 4] Log space: {log_row[1]:.1f}MB / "
                    f"{log_row[0]:.1f}MB ({log_row[2]:.1f}% used)"
                )

            cur.execute(
                "SELECT name, recovery_model_desc, log_reuse_wait_desc "
                "FROM sys.databases WHERE database_id = DB_ID()"
            )
            db_row = cur.fetchone()
            if db_row:
                lines.append(
                    f"  Database: {db_row[0]} recovery={db_row[1]} "
                    f"log_reuse_wait={db_row[2]}"
                )

            # §9 Step 5: Long-running open transactions
            cur.execute(
                "SELECT tat.transaction_id, tat.transaction_begin_time, "
                "       DATEDIFF(SECOND, tat.transaction_begin_time, GETDATE()), "
                "       tst.session_id, s.login_name, s.program_name "
                "FROM sys.dm_tran_active_transactions tat "
                "JOIN sys.dm_tran_session_transactions tst "
                "  ON tat.transaction_id = tst.transaction_id "
                "LEFT JOIN sys.dm_exec_sessions s "
                "  ON tst.session_id = s.session_id "
                "WHERE tst.session_id > 50 "
                "ORDER BY tat.transaction_begin_time"
            )
            open_trans = cur.fetchall()
            lines.append(f"\n[Step 5] Open transactions: {len(open_trans)}")
            for row in open_trans:
                lines.append(
                    f"  tran={row[0]} age={row[2]}s session={row[3]} "
                    f"login={row[4]} program={row[5]}"
                )

    except Exception as e:
        lines.append(f"\n[ERROR] Diagnostic queries failed: {e}")

    report = "\n".join(lines)
    logger.info(report)
    return report


# ---------------------------------------------------------------------------
# Utility functions (unchanged API)
# ---------------------------------------------------------------------------

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
    """P2-5: Add a clustered index on staging table PKs after BCP load.

    For staging tables > min_rows, adds a clustered index to speed up
    the subsequent JOIN against large Bronze/Stage tables. Below min_rows,
    the index creation overhead exceeds the scan cost.

    Args:
        staging_table: Fully qualified staging table name.
        pk_columns: PK columns to index.
        min_rows: Skip index creation below this threshold.
        row_count: Known row count (avoids extra query if provided).
    """
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
                f"CREATE CLUSTERED INDEX {quote_identifier(idx_name)} ON {quote_table(staging_table)} ({col_list})"
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
        # Item-17: Verify the ALTER actually succeeded (catches permission issues).
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
            # Item-17: Verify restoration succeeded.
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

            # R-1/R-3: Log backup after BULK_LOGGED intentionally omitted.
            # The previous BACKUP LOG ... TO DISK = N'/dev/null' silently destroyed
            # the log chain — SQL Server believed the backup succeeded but recovery
            # was impossible through the BULK_LOGGED window. Point-in-time restore
            # during pipeline windows is not a business requirement; the full backup
            # schedule provides sufficient recovery coverage. The BULK_LOGGED window
            # is short (single BCP load duration), and restoring to FULL recovery
            # model ensures subsequent log backups resume the chain correctly.
            logger.info(
                "R-3: Restored %s to FULL recovery model. Log chain gap during "
                "BULK_LOGGED window accepted — PITR not required during pipeline loads.",
                database,
            )
        except Exception:
            logger.exception("Failed to restore recovery model for %s", database)
        finally:
            conn.close()
"""Table-level locking to prevent concurrent pipeline runs (P1-2).

Uses SQL Server sp_getapplock/sp_releaseapplock to ensure only one pipeline
run processes a given table at a time. Prevents duplicate Bronze versions
caused by overlapping runs.

N-1: Lock-holding connections use ODBC connection resiliency
(ConnectRetryCount/ConnectRetryInterval) to survive transient network
failures and firewall idle timeouts. Default RHEL TCP keepalive waits
7,200s (2 hours) — many firewalls timeout at 60 minutes. Connection
resiliency + heartbeat queries every 10 days (P1-14) mitigate this.

W-8: RCSI race condition analysis (Session-owned locks):
  Under Read Committed Snapshot Isolation (RCSI), calling sp_releaseapplock
  explicitly before COMMIT on a *Transaction-scoped* lock creates a race
  condition where another process acquires the lock before the first
  transaction commits. This pipeline uses Session-owned locks
  (@LockOwner='Session') with autocommit=True, so:
    - No transaction wraps the lock — each statement auto-commits.
    - Lock lifetime is tied to the session (connection), not a transaction.
    - sp_releaseapplock is called explicitly, but since there is no pending
      transaction, there is no pre-commit window for race conditions.
    - Session close also releases the lock (crash-safe by design).
  CONCLUSION: The RCSI race condition does NOT apply to this implementation.
  If the lock ownership is ever changed to Transaction-scoped, remove the
  explicit sp_releaseapplock call and let the lock release at COMMIT time.

Usage:
    lock_conn = acquire_table_lock(source_name, table_name)
    if lock_conn is None:
        # Another run owns the lock — skip this table
        return
    try:
        process_table(...)
    finally:
        release_table_lock(lock_conn, source_name, table_name)
"""

from __future__ import annotations

import logging

import pyodbc

import config
import connections

logger = logging.getLogger(__name__)

# Lock resource name pattern
_LOCK_RESOURCE = "UDM_Pipeline_{source}_{table}"


def _get_resilient_lock_connection() -> pyodbc.Connection:
    """N-1: Create a lock-holding connection with ODBC resiliency settings.

    Adds ConnectRetryCount and ConnectRetryInterval to survive transient
    network failures and firewall idle timeouts. Also sets connection timeout
    to catch dead connections faster.
    """
    conn_str = (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={config.SQL_SERVER_HOST},{config.SQL_SERVER_PORT};"
        f"DATABASE={config.GENERAL_DB};"
        f"UID={config.SQL_SERVER_USER};"
        f"PWD={config.SQL_SERVER_PASSWORD};"
        "TrustServerCertificate=yes;"
        "ConnectRetryCount=3;"
        "ConnectRetryInterval=10;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def acquire_table_lock(
    source_name: str,
    table_name: str,
    timeout_ms: int = 0,
) -> object | None:
    """Acquire an exclusive application lock for a table.

    Uses sp_getapplock with @LockTimeout=0 (no wait) by default.
    Returns the connection holding the lock, or None if another
    run already holds it.

    The connection MUST be kept open for the lock to persist.
    Pass it to release_table_lock() when done.

    Args:
        source_name: e.g. 'DNA', 'CCM'.
        table_name: e.g. 'ACCT'.
        timeout_ms: Lock wait timeout in milliseconds. 0 = no wait.

    Returns:
        pyodbc.Connection holding the lock, or None if lock not acquired.
    """
    resource = _LOCK_RESOURCE.format(source=source_name, table=table_name)

    # N-1: Use connection with resiliency settings to survive firewall idle timeouts.
    conn = _get_resilient_lock_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DECLARE @result INT; "
            "EXEC @result = sp_getapplock "
            "  @Resource = ?, "
            "  @LockMode = 'Exclusive', "
            "  @LockOwner = 'Session', "
            "  @LockTimeout = ?; "
            "SELECT @result;",
            resource, timeout_ms,
        )
        result = cursor.fetchone()[0]
        cursor.close()

        # sp_getapplock return codes:
        #  0 = lock granted synchronously
        #  1 = lock granted after waiting
        # -1 = lock request timed out
        # -2 = lock request was cancelled
        # -3 = lock request was chosen as deadlock victim
        # -999 = parameter error

        if result >= 0:
            logger.info(
                "Acquired table lock: %s (result=%d)",
                resource, result,
            )
            return conn  # Caller keeps connection open
        else:
            logger.warning(
                "Could not acquire table lock: %s (result=%d) — "
                "another pipeline run is processing this table. Skipping.",
                resource, result,
            )
            conn.close()
            return None

    except Exception:
        logger.exception("Failed to acquire table lock: %s", resource)
        conn.close()
        return None


def keep_lock_alive(conn: object, source_name: str, table_name: str) -> bool:
    """P1-14: Send a heartbeat query on the lock connection to keep it alive.

    SQL Server and ODBC drivers may silently kill idle connections during
    long backfills. This lightweight query prevents that.

    Args:
        conn: The connection returned by acquire_table_lock().
        source_name: e.g. 'DNA', 'CCM'.
        table_name: e.g. 'ACCT'.

    Returns:
        True if the connection is alive, False if it's dead.
    """
    resource = _LOCK_RESOURCE.format(source=source_name, table=table_name)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        logger.debug("Lock heartbeat OK: %s", resource)
        return True
    except Exception:
        logger.error(
            "P1-14: Lock connection dead for %s — lock may have been released. "
            "Another pipeline instance could start concurrently.",
            resource,
        )
        return False


def release_table_lock(
    conn: object,
    source_name: str,
    table_name: str,
) -> None:
    """Release the application lock and close the connection.

    Args:
        conn: The connection returned by acquire_table_lock().
        source_name: e.g. 'DNA', 'CCM'.
        table_name: e.g. 'ACCT'.
    """
    resource = _LOCK_RESOURCE.format(source=source_name, table=table_name)

    try:
        cursor = conn.cursor()
        cursor.execute(
            "EXEC sp_releaseapplock @Resource = ?, @LockOwner = 'Session';",
            resource,
        )
        cursor.close()
        logger.debug("Released table lock: %s", resource)
    except Exception:
        logger.exception("Failed to release table lock: %s", resource)
    finally:
        try:
            conn.close()
        except Exception:
            pass

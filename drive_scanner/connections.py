"""SQL Server target database connections (UDM_Stage, UDM_Bronze, General).

Provides both pyodbc connections (for DDL/DML) and ConnectorX URIs (for reads).
Also provides identifier quoting helpers (H-1) for safe dynamic SQL construction.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from urllib.parse import quote_plus

import pyodbc

import config

logger = logging.getLogger(__name__)

# P-3: Connection overhead measurement — tracks cumulative time spent
# establishing pyodbc connections per pipeline run. Query via
# get_connection_overhead() at pipeline end for observability.
# NOTE: These counters are per-process and NOT thread-safe (Item-23).
# Safe under the current multiprocessing worker model where each worker
# is a separate process with its own module state.
_connection_time_ms: float = 0.0
_connection_count: int = 0

# Item-18: Per-database connection pool — one long-lived connection per database.
# Eliminates TCP/TLS/ODBC handshake overhead for repeated cursor_for() calls.
# Safe because: autocommit=True prevents transaction state leaks, and the pipeline
# uses multiprocessing (not threading) so each process has its own pool.
# Lock-holding connections (table_lock.py) use _get_resilient_lock_connection()
# and bypass this pool entirely.
_connection_pool: dict[str, pyodbc.Connection] = {}

# ---------------------------------------------------------------------------
# H-1: SQL identifier escaping — bracket-escape with ]] doubling
# ---------------------------------------------------------------------------

_MAX_IDENTIFIER_LENGTH = 128  # SQL Server sysname limit (matches QUOTENAME())


def quote_identifier(name: str) -> str:
    """H-1: Bracket-escape a SQL Server identifier (column, index, constraint name).

    Equivalent to T-SQL QUOTENAME(): wraps in brackets and doubles any embedded
    closing brackets. Rejects identifiers longer than 128 characters to match
    the sysname limit that QUOTENAME() enforces server-side.

    Args:
        name: Raw identifier (e.g. column name, index name).

    Returns:
        Bracket-escaped identifier (e.g. ``[my_column]``, ``[tricky]]name]``).

    Raises:
        ValueError: If name exceeds 128 characters or is empty.
    """
    if not name:
        raise ValueError("H-1: Identifier cannot be empty")
    if len(name) > _MAX_IDENTIFIER_LENGTH:
        raise ValueError(
            f"H-1: Identifier exceeds {_MAX_IDENTIFIER_LENGTH} characters "
            f"(len={len(name)}): {name[:50]}..."
        )
    return f"[{name.replace(']', ']]')}]"


def quote_table(full_table_name: str) -> str:
    """H-1: Bracket-escape a fully qualified table name (db.schema.table).

    Splits on '.' and bracket-escapes each part individually.

    Args:
        full_table_name: e.g. ``UDM_Stage.DNA.ACCT_cdc``

    Returns:
        e.g. ``[UDM_Stage].[DNA].[ACCT_cdc]``

    Raises:
        ValueError: If not exactly 3 parts, or any part exceeds 128 chars.
    """
    parts = full_table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"H-1: Expected 3-part table name (db.schema.table), "
            f"got {len(parts)} parts: {full_table_name}"
        )
    return ".".join(quote_identifier(p) for p in parts)


def resolve_schema_name(database: str, schema_name: str) -> str:
    """Resolve actual schema casing from sys.schemas.

    SQL Server schema names are case-insensitive for queries but BCP uses
    the exact casing provided. If the target database has schema 'dna' but
    SourceName is 'DNA', BCP hangs on the mismatched/ambiguous schema.

    Returns the actual stored casing if the schema exists, or the input
    as-is if no match is found (first run — table_creator.py will create it).
    """
    try:
        with cursor_for(database) as cur:
            cur.execute(
                "SELECT name FROM sys.schemas WHERE LOWER(name) = LOWER(?)",
                schema_name,
            )
            row = cur.fetchone()
            if row is not None:
                resolved = row[0]
                if resolved != schema_name:
                    logger.info(
                        "Schema casing resolved: %s.%s -> %s.%s",
                        database, schema_name, database, resolved,
                    )
                return resolved
    except Exception:
        logger.warning(
            "Could not resolve schema casing for %s.%s — using as-is",
            database, schema_name, exc_info=True,
        )
    return schema_name


def _pyodbc_connection_string(database: str) -> str:
    return (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={config.SQL_SERVER_HOST},{config.SQL_SERVER_PORT};"
        f"DATABASE={database};"
        f"UID={config.SQL_SERVER_USER};"
        f"PWD={config.SQL_SERVER_PASSWORD};"
        "TrustServerCertificate=yes;"
    )


def _connectorx_uri(database: str) -> str:
    usr = quote_plus(config.SQL_SERVER_USER)
    pwd = quote_plus(config.SQL_SERVER_PASSWORD)
    return (
        f"mssql://{usr}:{pwd}@{config.SQL_SERVER_HOST}:{config.SQL_SERVER_PORT}"
        f"/{database}?TrustServerCertificate=true"
    )


# --- pyodbc Connections ---

def get_stage_connection() -> pyodbc.Connection:
    return get_connection(config.STAGE_DB)


def get_bronze_connection() -> pyodbc.Connection:
    return get_connection(config.BRONZE_DB)


def get_general_connection() -> pyodbc.Connection:
    return get_connection(config.GENERAL_DB)


def get_connection(database: str) -> pyodbc.Connection:
    """Create a fresh pyodbc connection (not pooled).

    Item-23: The overhead counters (_connection_time_ms, _connection_count) are
    per-process globals incremented without locking. Safe under the current
    multiprocessing worker model; would race under threading.
    """
    global _connection_time_ms, _connection_count
    start = time.monotonic()
    conn = pyodbc.connect(_pyodbc_connection_string(database), autocommit=True)
    elapsed = (time.monotonic() - start) * 1000
    _connection_time_ms += elapsed
    _connection_count += 1
    return conn


def get_connection_overhead() -> tuple[float, int]:
    """P-3: Return cumulative connection overhead (total_ms, connection_count).

    Call at pipeline end to log total time spent establishing DB connections.
    """
    return _connection_time_ms, _connection_count


def reset_connection_overhead() -> None:
    """P-3: Reset connection overhead counters (e.g. at pipeline start)."""
    global _connection_time_ms, _connection_count
    _connection_time_ms = 0.0
    _connection_count = 0


# --- ConnectorX URIs ---

def stage_connectorx_uri() -> str:
    return _connectorx_uri(config.STAGE_DB)


def bronze_connectorx_uri() -> str:
    return _connectorx_uri(config.BRONZE_DB)


def general_connectorx_uri() -> str:
    return _connectorx_uri(config.GENERAL_DB)


# --- Context Managers ---

@contextmanager
def cursor_for(database: str):
    """X-1: Context manager yielding a cursor for the given database.

    Item-18: Uses a per-database connection pool to avoid repeated TCP/TLS/ODBC
    handshake overhead. The connection stays in the pool after the cursor is closed.
    On pyodbc.OperationalError (connection dropped, server restart), the stale
    connection is evicted and the error propagates to the caller.

    Usage::

        with cursor_for("General") as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
    """
    conn = _connection_pool.get(database)
    if conn is None:
        conn = get_connection(database)
        _connection_pool[database] = conn

    cursor = conn.cursor()
    try:
        yield cursor
    except pyodbc.OperationalError:
        # Connection-level failure — evict stale connection from pool.
        _connection_pool.pop(database, None)
        try:
            conn.close()
        except Exception:
            pass
        raise
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def close_connection_pool() -> None:
    """Item-18: Close all pooled connections. Call at pipeline shutdown."""
    count = len(_connection_pool)
    for db in list(_connection_pool):
        try:
            _connection_pool[db].close()
        except Exception:
            pass
    _connection_pool.clear()
    if count > 0:
        logger.debug("Item-18: Connection pool closed (%d connections)", count)


# --- Startup Validation ---

def verify_rcsi_enabled() -> None:
    """E-9: Verify READ_COMMITTED_SNAPSHOT is enabled on Bronze database.

    Without RCSI, the SCD2 batched UPDATE TOP(N) triggers lock escalation
    to a table-level exclusive lock once N exceeds ~5,000 rows, blocking all
    readers for the duration of each batch. Current batch size is controlled by
    config.SCD2_UPDATE_BATCH_SIZE (default 4,000 — deliberately below the
    escalation threshold). With RCSI, readers see pre-update snapshots and
    are not blocked.

    Logs WARNING if RCSI is not enabled — the pipeline continues but SCD2
    UPDATEs will block concurrent readers.
    """
    try:
        with cursor_for(config.BRONZE_DB) as cur:
            cur.execute(
                "SELECT is_read_committed_snapshot_on "
                "FROM sys.databases WHERE name = ?",
                config.BRONZE_DB,
            )
            row = cur.fetchone()

            if row is None:
                logger.warning(
                    "E-9: Could not find database %s in sys.databases — "
                    "unable to verify RCSI status",
                    config.BRONZE_DB,
                )
            elif not row[0]:
                logger.warning(
                    "E-9: READ_COMMITTED_SNAPSHOT is NOT enabled on %s. "
                    "SCD2 batch UPDATEs (TOP %d) will escalate to table-level "
                    "exclusive locks, blocking all readers. Enable with: "
                    "ALTER DATABASE [%s] SET READ_COMMITTED_SNAPSHOT ON",
                    config.BRONZE_DB, config.SCD2_UPDATE_BATCH_SIZE,
                    config.BRONZE_DB,
                )
            else:
                logger.info("E-9: RCSI verified enabled on %s", config.BRONZE_DB)
    except Exception:
        logger.warning(
            "E-9: Could not verify RCSI on %s — continuing without verification",
            config.BRONZE_DB, exc_info=True,
        )

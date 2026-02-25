"""BCP subprocess wrapper: CSV -> SQL Server.

Handles BULK_LOGGED recovery model context and row count verification.

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
from contextlib import contextmanager

import config
import connections
from connections import cursor_for, quote_identifier, quote_table

logger = logging.getLogger(__name__)


class BCPLoadError(Exception):
    """Raised when BCP load fails or row count mismatches."""


def bcp_load(
    csv_path: str,
    full_table_name: str,
    expected_row_count: int | None = None,
    format_file: str | None = None,
    atomic: bool = True,
) -> int:
    """Load a BCP CSV file into SQL Server.

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
    """
    cmd = [
        config.BCP_PATH,
        full_table_name,
        "in",
        csv_path,
        "-c",
        "-t", "\\t",
        "-r", config.CSV_ROW_TERMINATOR,
        # V-5: -C 65001 intentionally omitted. The codepage flag is silently ignored
        # or causes incorrect encoding on Linux BCP (mssql-tools18). With SQL Server
        # 2019+ UTF-8 collation and ODBC Driver 18, BCP handles UTF-8 natively in
        # -c mode without the codepage flag.
        "-S", f"{config.SQL_SERVER_HOST},{config.SQL_SERVER_PORT}",
        "-U", config.SQL_SERVER_USER,
    ]

    # H-5/H-6: Pass password via SQLCMDPASSWORD environment variable instead of
    # -P flag to prevent exposure via /proc/{pid}/cmdline and `ps aux`.
    # SQLCMDPASSWORD is documented for sqlcmd and works with BCP in mssql-tools18.
    # Falls back to -P if SQLCMDPASSWORD is explicitly disabled via config.
    #
    # Item-11: Minimal environment — only propagate variables BCP actually needs.
    # Prevents leaking other secrets (Oracle passwords, API keys, tokens) to the
    # BCP subprocess via /proc/{pid}/environ.
    bcp_env = {"SQLCMDPASSWORD": config.SQL_SERVER_PASSWORD}
    for _env_key in ("PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TMP"):
        if _env_key in os.environ:
            bcp_env[_env_key] = os.environ[_env_key]

    # E-3: Only add -b batch size flag when NOT atomic.
    # Without -b, the entire BCP load is a single transaction — failure rolls back
    # everything (safe for SCD2 Bronze loads). With -b, each batch is a separate
    # transaction and failure leaves a partial load (acceptable for Stage/staging).
    if not atomic:
        cmd.extend(["-b", str(config.BCP_BATCH_SIZE)])

    # W-13: Use format file for explicit column mapping if provided.
    if format_file:
        cmd.extend(["-f", format_file])
        logger.debug("W-13: Using BCP format file: %s", format_file)

    logger.info("BCP loading %s -> %s", csv_path, full_table_name)
    logger.debug("BCP command: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=config.BCP_TIMEOUT,
        env=bcp_env,
    )

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


def _parse_rows_copied(stdout: str) -> int:
    """Parse 'N rows copied' from BCP stdout."""
    match = re.search(r"(\d+)\s+rows?\s+copied", stdout)
    if not match:
        raise BCPLoadError(f"Could not parse row count from BCP output: {stdout[:500]}")
    return int(match.group(1))


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

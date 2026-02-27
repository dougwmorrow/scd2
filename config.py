"""Environment variables, BCP constants, paths, and CSV contract constants."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from /debi/.env (NOT project root)
load_dotenv("/debi/.env")

# --- Database Connection Vars ---
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "")
SQL_SERVER_PORT = int(os.getenv("SQL_SERVER_PORT", "1433"))
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD", "")

# Target database names
STAGE_DB = os.getenv("STAGE_DB", "UDM_Stage")
BRONZE_DB = os.getenv("BRONZE_DB", "UDM_Bronze")
GENERAL_DB = os.getenv("GENERAL_DB", "General")

# Oracle connection vars
ORACLE_USER = os.getenv("ORACLE_USER", "")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_HOST = os.getenv("ORACLE_HOST", "")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "")

# Oracle Instant Client path (for oracledb thick mode)
ORACLE_CLIENT_DIR = os.getenv("ORACLE_CLIENT_DIR", "/usr/lib/oracle/19.25/client64/lib")

# --- BCP Configuration ---
BCP_PATH = os.getenv("BCP_PATH", "/opt/mssql-tools18/bin/bcp")

# ---------------------------------------------------------------------------
# BCP throughput-optimized batch sizes (per table type)
# ---------------------------------------------------------------------------
# Stage tables (heaps, TABLOCK via sp_tableoption):
#   With BU locks, no row/page-level locks exist — no escalation concern.
#   Each batch commit triggers a log flush; fewer larger batches = fewer flushes.
#   100K–500K optimal. If chunks are ~256 MB, omitting -b entirely works too.
BCP_STAGE_BATCH_SIZE = int(os.getenv("BCP_STAGE_BATCH_SIZE", "100000"))
#
# Bronze tables (clustered index, no TABLOCK, concurrent readers required):
#   Without TABLOCK, BCP uses row-level locks. Lock escalation is checked at
#   ~2,500 acquisitions and attempted at ~5,000 per HoBt. Staying at 800
#   prevents escalation to table-level X locks that block concurrent readers.
BCP_BRONZE_BATCH_SIZE = int(os.getenv("BCP_BRONZE_BATCH_SIZE", "800"))
#
# Legacy fallback — used when caller doesn't specify stage vs bronze.
BCP_BATCH_SIZE = int(os.getenv("BCP_BATCH_SIZE", "5000"))

# TDS packet size: -a flag, fully supported on Linux.
# Default 4096 is dramatically undersized for bulk ops. 32768 (32 KB) matches
# SQL Server's extent write size and benchmarks show 10–20% throughput gain.
# Going to 65535 provides diminishing returns and may negotiate down anyway.
BCP_PACKET_SIZE = int(os.getenv("BCP_PACKET_SIZE", "32768"))

# ---------------------------------------------------------------------------
# Parallel BCP streams (Stage heaps only)
# ---------------------------------------------------------------------------
# A single BCP stream typically achieves 100K–300K rows/sec. Parallel streams
# with sp_tableoption TABLOCK (BU locks are compatible) multiply throughput.
# 8 streams is a good starting point for Stage heaps with 10G networking.
# Bronze tables should NOT use parallel streams with TABLOCK (exclusive lock).
BCP_PARALLEL_STREAMS = int(os.getenv("BCP_PARALLEL_STREAMS", "8"))

# Minimum rows to trigger parallel BCP. Below this, single stream is fine.
BCP_PARALLEL_THRESHOLD = int(os.getenv("BCP_PARALLEL_THRESHOLD", "1000000"))

# ---------------------------------------------------------------------------
# Small table routing: pyodbc fast_executemany for tiny tables
# ---------------------------------------------------------------------------
# BCP subprocess startup costs ~1–2s (spawn, TDS handshake, lock acquisition).
# For tables < threshold, pyodbc fast_executemany completes in milliseconds.
BCP_SMALL_TABLE_THRESHOLD = int(os.getenv("BCP_SMALL_TABLE_THRESHOLD", "1000"))

# ---------------------------------------------------------------------------
# tmpfs for BCP CSV staging
# ---------------------------------------------------------------------------
# Writing CSVs to /dev/shm (tmpfs) eliminates source-side disk I/O.
# Reads/writes at memory bandwidth (~10–50 GB/s) vs NVMe (~3–7 GB/s).
# Falls back to CSV_OUTPUT_DIR if /dev/shm is unavailable or too small.
CSV_TMPFS_DIR = Path(os.getenv("CSV_TMPFS_DIR", "/dev/shm/udm_bcp"))
CSV_TMPFS_ENABLED = os.getenv("CSV_TMPFS_ENABLED", "true").lower() == "true"
# P1-7: BCP timeout in seconds — higher for large tables / initial backfills
BCP_TIMEOUT = int(os.getenv("BCP_TIMEOUT", "7200"))

# BCP-HANG-FIX: Retry configuration for blocked BCP loads.
# When BCP times out (likely blocked by another session), retry with
# exponential backoff after cleaning up orphaned sessions. Default 3 retries
# gives attempts at 0s, 5s, 10s — total max wait ~15s before final failure.
BCP_MAX_RETRIES = int(os.getenv("BCP_MAX_RETRIES", "3"))

# BCP-HANG-FIX §7: Wait time in seconds after killing orphaned BULK INSERT
# sessions, to allow SQL Server to complete single-threaded rollback before
# retrying. For a 300K-row orphaned transaction, rollback typically takes 5-15s.
BCP_ORPHAN_ROLLBACK_WAIT = int(os.getenv("BCP_ORPHAN_ROLLBACK_WAIT", "15"))

# --- BCP Live Monitor Configuration ---
# BCP-HANG-FIX-v2: Background monitor thread polls SQL Server DMVs while BCP
# runs. These settings control how aggressively the monitor detects hangs.

# How often (seconds) the monitor queries SQL Server for diagnostics.
# Lower = more granular timeline but more load on SQL Server.
# 15s is a good balance: captures hang causes within 15s of onset.
BCP_MONITOR_INTERVAL = int(os.getenv("BCP_MONITOR_INTERVAL", "15"))

# How long (seconds) a definitive hang signal (e.g. LCK_M_IX wait, log full)
# must persist before the monitor triggers early abort. This is ALSO the
# initial grace period — no hang evaluation occurs before this many seconds.
# 120s is enough for BCP to establish connection and start inserting, while
# still catching hangs ~30x faster than waiting the full 7200s BCP_TIMEOUT.
BCP_HANG_ABORT_THRESHOLD = int(os.getenv("BCP_HANG_ABORT_THRESHOLD", "120"))

# --- BCP CSV Contract (Single Source of Truth) ---
CSV_SEPARATOR = "\t"
CSV_ROW_TERMINATOR = "0x0A"  # LF only, passed to BCP -r flag
CSV_HAS_HEADER = False
CSV_QUOTE_STYLE = "never"
CSV_NULL_VALUE = ""
CSV_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%3f"
CSV_BATCH_SIZE = 4096  # Polars write_csv batch_size to avoid memory spikes

# --- File Paths ---
CSV_OUTPUT_DIR = Path(os.getenv("CSV_OUTPUT_DIR", "/debi/udm_csv"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# --- ODBC Driver ---
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# --- SQL Server Source Connections (CCM, EPICOR, etc.) ---
# These can be extended per-source; for now they follow the same host or separate hosts
CCM_SERVER_HOST = os.getenv("CCM_SERVER_HOST", SQL_SERVER_HOST)
CCM_SERVER_PORT = int(os.getenv("CCM_SERVER_PORT", str(SQL_SERVER_PORT)))
CCM_SERVER_USER = os.getenv("CCM_SERVER_USER", SQL_SERVER_USER)
CCM_SERVER_PASSWORD = os.getenv("CCM_SERVER_PASSWORD", SQL_SERVER_PASSWORD)

# V-7: Overlap minutes for large table windowed extraction.
# Extends each day's window backward to capture cross-midnight transactions.
# 0 = disabled (default). CDC handles duplicates idempotently via hash match.
OVERLAP_MINUTES = int(os.getenv("OVERLAP_MINUTES", "0"))

# B-2: SCD2 UPDATE batch size. Stay below 5,000 to prevent SQL Server
# lock escalation from row locks to table-level exclusive locks.
# Table-level exclusive locks override RCSI, blocking all concurrent readers.
SCD2_UPDATE_BATCH_SIZE = int(os.getenv("SCD2_UPDATE_BATCH_SIZE", "4000"))

# B-8: RSS memory ceiling in GB. Pipeline logs WARNING at 85% of this limit
# and ERROR at the limit. Polars + glibc arena fragmentation can cause RSS
# to grow monotonically during multi-table runs (Polars issue #23128).
# Combine with MALLOC_ARENA_MAX=2 (W-4) for best results.
MAX_RSS_GB = float(os.getenv("MAX_RSS_GB", "48.0"))

EPICOR_SERVER_HOST = os.getenv("EPICOR_SERVER_HOST", SQL_SERVER_HOST)
EPICOR_SERVER_PORT = int(os.getenv("EPICOR_SERVER_PORT", str(SQL_SERVER_PORT)))
EPICOR_SERVER_USER = os.getenv("EPICOR_SERVER_USER", SQL_SERVER_USER)
EPICOR_SERVER_PASSWORD = os.getenv("EPICOR_SERVER_PASSWORD", SQL_SERVER_PASSWORD)
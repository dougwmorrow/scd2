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
BCP_BATCH_SIZE = int(os.getenv("BCP_BATCH_SIZE", "10000"))
BCP_PACKET_SIZE = int(os.getenv("BCP_PACKET_SIZE", "4096"))
# P1-7: BCP timeout in seconds — higher for large tables / initial backfills
BCP_TIMEOUT = int(os.getenv("BCP_TIMEOUT", "7200"))

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

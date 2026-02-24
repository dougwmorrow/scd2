"""B-1 Migration: ALTER hash columns from BIGINT to VARCHAR(64).

One-time migration script to upgrade _row_hash (Stage) and UdmHash (Bronze)
from BIGINT to VARCHAR(64) for full SHA-256 hex string storage.

Previous: SHA-256 truncated to 64-bit Int64 (~24% collision probability at 3B rows).
After:    Full 256-bit SHA-256 as 64-char hex string (effectively zero collision risk).

IMPORTANT:
  - Run during a maintenance window BEFORE the first pipeline run with new code.
  - The first pipeline run after migration rehashes all rows — all appear as "updated"
    in CDC (one-time event). This is expected and correct.
  - Supports --dry-run mode to preview changes without executing.
  - Existing BIGINT hash values become invalid after ALTER — the pipeline will
    recompute all hashes on the next run.

Usage:
  python3 migrations/b1_hash_varchar64.py --dry-run
  python3 migrations/b1_hash_varchar64.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import connections

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _get_tables_to_migrate(db: str, column_name: str) -> list[tuple[str, str, str]]:
    """Find tables where the hash column is still BIGINT.

    Returns list of (schema, table, full_name) tuples.
    """
    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT TABLE_SCHEMA, TABLE_NAME "
            f"FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE COLUMN_NAME = ? AND DATA_TYPE = 'bigint'",
            column_name,
        )
        rows = cursor.fetchall()
        cursor.close()
        return [
            (row[0], row[1], f"{db}.{row[0]}.{row[1]}")
            for row in rows
        ]
    finally:
        conn.close()


def _alter_column(full_table_name: str, column_name: str, dry_run: bool) -> bool:
    """ALTER a single column from BIGINT to VARCHAR(64).

    Returns True if successful (or dry-run), False on error.
    """
    db = full_table_name.split(".")[0]
    alter_sql = (
        f"ALTER TABLE {full_table_name} "
        f"ALTER COLUMN [{column_name}] VARCHAR(64) NULL"
    )

    if dry_run:
        logger.info("[DRY RUN] Would execute: %s", alter_sql)
        return True

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(alter_sql)
        cursor.close()
        logger.info("Altered %s.[%s] from BIGINT to VARCHAR(64)", full_table_name, column_name)
        return True
    except Exception:
        logger.exception("Failed to alter %s.[%s]", full_table_name, column_name)
        return False
    finally:
        conn.close()


def migrate(dry_run: bool = False) -> None:
    """Run the B-1 hash migration across all Stage and Bronze tables."""
    logger.info("=" * 60)
    logger.info("B-1 Hash Migration: BIGINT -> VARCHAR(64)")
    logger.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("=" * 60)

    # Stage tables: _row_hash column
    stage_tables = _get_tables_to_migrate("UDM_Stage", "_row_hash")
    logger.info("Found %d Stage tables with BIGINT _row_hash", len(stage_tables))

    # Bronze tables: UdmHash column
    bronze_tables = _get_tables_to_migrate("UDM_Bronze", "UdmHash")
    logger.info("Found %d Bronze tables with BIGINT UdmHash", len(bronze_tables))

    if not stage_tables and not bronze_tables:
        logger.info("No tables need migration — all hash columns are already VARCHAR(64) or don't exist.")
        return

    success = 0
    failed = 0

    for schema, table, full_name in stage_tables:
        if _alter_column(full_name, "_row_hash", dry_run):
            success += 1
        else:
            failed += 1

    for schema, table, full_name in bronze_tables:
        if _alter_column(full_name, "UdmHash", dry_run):
            success += 1
        else:
            failed += 1

    logger.info("=" * 60)
    logger.info(
        "Migration %s: %d succeeded, %d failed",
        "preview" if dry_run else "complete",
        success, failed,
    )
    if failed > 0:
        logger.error("Some ALTER operations failed — review errors above before running pipeline.")
    elif not dry_run:
        logger.info(
            "All hash columns migrated to VARCHAR(64). "
            "The next pipeline run will rehash all rows (one-time full CDC update wave)."
        )
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B-1: Migrate hash columns from BIGINT to VARCHAR(64)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview ALTER statements without executing them",
    )
    args = parser.parse_args()

    migrate(dry_run=args.dry_run)

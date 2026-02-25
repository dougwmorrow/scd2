"""Orphaned staging table cleanup (P3-3).

CDC and SCD2 create temporary staging tables (_staging_expire_*, _staging_scd2_*,
_staging_bronze_lookup_*). If the process crashes between CREATE and the finally
DROP, these tables persist.

This module provides a cleanup function to run at pipeline start. It finds and
drops any _staging_* tables in the UDM Stage and Bronze schemas.
"""

from __future__ import annotations

import logging

import config
import connections
from connections import quote_table

logger = logging.getLogger(__name__)

# Staging table prefixes used by the pipeline
_STAGING_PREFIXES = (
    "_staging_expire_",
    "_staging_scd2_",
    "_staging_bronze_lookup_",
)


def cleanup_orphaned_staging_tables() -> int:
    """Drop any orphaned staging tables in UDM_Stage and UDM_Bronze.

    Scans all schemas in both databases for tables matching known
    staging prefixes.

    Returns:
        Number of staging tables dropped.
    """
    total_dropped = 0

    for database in (config.STAGE_DB, config.BRONZE_DB):
        dropped = _cleanup_database(database)
        total_dropped += dropped

    if total_dropped > 0:
        logger.info(
            "P3-3: Cleaned up %d orphaned staging table(s)", total_dropped,
        )
    else:
        logger.debug("P3-3: No orphaned staging tables found")

    return total_dropped


def _cleanup_database(database: str) -> int:
    """Find and drop staging tables in a specific database."""
    conn = connections.get_connection(database)
    dropped = 0

    try:
        cursor = conn.cursor()

        # Find all staging tables
        cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' "
            "AND (TABLE_NAME LIKE '_staging_%')"
        )
        staging_tables = cursor.fetchall()
        cursor.close()

        for schema, table_name in staging_tables:
            # Verify it matches our known prefixes
            if not any(table_name.startswith(prefix) for prefix in _STAGING_PREFIXES):
                continue

            full_name = f"{database}.{schema}.{table_name}"
            try:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {quote_table(full_name)}")
                cursor.close()
                logger.info("P3-3: Dropped orphaned staging table: %s", full_name)
                dropped += 1
            except Exception:
                logger.warning(
                    "P3-3: Failed to drop staging table: %s", full_name,
                )
    finally:
        conn.close()

    return dropped

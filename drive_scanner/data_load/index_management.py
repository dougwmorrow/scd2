"""Index management: disable/rebuild indexes around BCP loads.

Uses metadata from TableConfig.index_configs (sourced from UdmTablesColumnsList).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import connections
from connections import quote_identifier, quote_table

if TYPE_CHECKING:
    from orchestration.table_config import ColumnConfig

logger = logging.getLogger(__name__)


def disable_indexes(full_table_name: str, index_configs: list[ColumnConfig]) -> list[str]:
    """Disable indexes on a table before BCP load.

    Args:
        full_table_name: Fully qualified table name (db.schema.table).
        index_configs: Column configs where is_index=True.

    Returns:
        List of index names that were disabled.
    """
    if not index_configs:
        return []

    db = full_table_name.split(".")[0]
    disabled = []

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        for col_cfg in index_configs:
            if not col_cfg.index_name:
                continue
            try:
                cursor.execute(
                    f"ALTER INDEX {quote_identifier(col_cfg.index_name)} ON {quote_table(full_table_name)} DISABLE"
                )
                disabled.append(col_cfg.index_name)
                logger.info(
                    "Disabled index %s on %s", col_cfg.index_name, full_table_name
                )
            except Exception:
                logger.warning(
                    "Failed to disable index %s on %s",
                    col_cfg.index_name,
                    full_table_name,
                    exc_info=True,
                )
        cursor.close()
    finally:
        conn.close()

    return disabled


def rebuild_indexes(full_table_name: str, index_names: list[str]) -> None:
    """Rebuild previously disabled indexes after BCP load.

    BCP-HANG-FIX §2: Uses ONLINE rebuild with WAIT_AT_LOW_PRIORITY to prevent
    Schema Modification (Sch-M) locks from creating blocking chains. Without
    this, an index rebuild holds Sch-M for its entire duration which blocks
    ALL subsequent Sch-S requests (including BCP loads on other sessions).
    The cascading effect is severe: even a *waiting* Sch-M request blocks
    subsequent Sch-S requests, creating multi-session blocking chains.

    WAIT_AT_LOW_PRIORITY(MAX_DURATION=1, ABORT_AFTER_WAIT=SELF) causes the
    rebuild to abort itself rather than block other operations. On failure,
    falls back to standard offline REBUILD which is guaranteed to succeed
    but may briefly block concurrent operations.

    Args:
        full_table_name: Fully qualified table name (db.schema.table).
        index_names: Index names to rebuild.
    """
    if not index_names:
        return

    db = full_table_name.split(".")[0]

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        for idx_name in index_names:
            try:
                # §2: Try ONLINE rebuild with WAIT_AT_LOW_PRIORITY first.
                # Holds Sch-M only briefly at start/end, and aborts itself
                # after 1 minute rather than creating a blocking chain.
                cursor.execute(
                    f"ALTER INDEX {quote_identifier(idx_name)} "
                    f"ON {quote_table(full_table_name)} REBUILD "
                    f"WITH (ONLINE = ON (WAIT_AT_LOW_PRIORITY "
                    f"(MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF)))"
                )
                logger.info(
                    "BCP-HANG-FIX: Rebuilt index %s on %s "
                    "(ONLINE + WAIT_AT_LOW_PRIORITY)",
                    idx_name, full_table_name,
                )
            except Exception:
                # Fallback: ONLINE rebuild not supported (Standard Edition,
                # spatial/XML indexes, etc). Use standard offline REBUILD.
                logger.debug(
                    "BCP-HANG-FIX: ONLINE rebuild failed for %s on %s — "
                    "falling back to offline REBUILD",
                    idx_name, full_table_name,
                )
                try:
                    cursor.execute(
                        f"ALTER INDEX {quote_identifier(idx_name)} "
                        f"ON {quote_table(full_table_name)} REBUILD"
                    )
                    logger.info(
                        "Rebuilt index %s on %s (offline)",
                        idx_name, full_table_name,
                    )
                except Exception:
                    logger.warning(
                        "Failed to rebuild index %s on %s",
                        idx_name,
                        full_table_name,
                        exc_info=True,
                    )
        cursor.close()
    finally:
        conn.close()
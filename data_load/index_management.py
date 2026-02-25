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
                cursor.execute(
                    f"ALTER INDEX {quote_identifier(idx_name)} ON {quote_table(full_table_name)} REBUILD"
                )
                logger.info(
                    "Rebuilt index %s on %s", idx_name, full_table_name
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

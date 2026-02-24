"""Extraction router — single source of truth for extraction routing logic.

Routes extraction to the correct extractor based on table configuration:
  - Oracle + SourceIndexHint -> oracledb (INDEX hints)
  - Oracle + PartitionOn -> ConnectorX Oracle with partition_on
  - Oracle + neither -> ConnectorX Oracle bulk (FULL scan)
  - SQL Server + PartitionOn -> ConnectorX SQL Server with partition_on
  - SQL Server + neither -> ConnectorX SQL Server bulk

E-3: Centralizes routing to prevent logic drift between small and large
table orchestrators.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import config
from extract.connectorx_oracle_extractor import (
    extract_oracle_connectorx,
    extract_oracle_connectorx_windowed,
)
from extract.connectorx_sqlserver_extractor import (
    extract_sqlserver_connectorx,
    extract_sqlserver_connectorx_windowed,
)
from extract.oracle_extractor import extract_oracle_oracledb_day, extract_oracle_oracledb_full

if TYPE_CHECKING:
    import polars as pl
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def extract_full(
    table_config: TableConfig,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Route full extraction based on table config (small tables).

    Routing rules:
        Oracle + SourceIndexHint (+ PartitionOn irrelevant) -> oracledb full-scan with INDEX hints
        Oracle + no IndexHint + PartitionOn -> ConnectorX Oracle with partition_on
        Oracle + no IndexHint + no PartitionOn -> ConnectorX Oracle bulk (FULL scan)
        SQL Server + PartitionOn -> ConnectorX SQL Server with partition_on
        SQL Server + no PartitionOn -> ConnectorX SQL Server bulk

    Args:
        table_config: Table configuration.
        output_dir: Directory for temp CSV files.

    Returns:
        Tuple of (DataFrame, CSV path).
    """
    output_dir = Path(output_dir)

    if table_config.is_oracle:
        if table_config.source_index_hint:
            # P1-12: Full-scan mode for small tables (no date range filter).
            logger.info("Routing %s to oracledb full-scan (INDEX hint: %s)",
                        table_config.source_object_name, table_config.source_index_hint)
            return extract_oracle_oracledb_full(table_config, output_dir=output_dir)
        elif table_config.partition_on:
            logger.info("Routing %s to ConnectorX Oracle (partition_on=%s)",
                        table_config.source_object_name, table_config.partition_on)
            return extract_oracle_connectorx(
                table_config, output_dir, partition_on=table_config.partition_on
            )
        else:
            logger.info("Routing %s to ConnectorX Oracle (FULL scan)",
                        table_config.source_object_name)
            return extract_oracle_connectorx(table_config, output_dir)

    else:
        if table_config.partition_on:
            logger.info("Routing %s to ConnectorX SQL Server (partition_on=%s)",
                        table_config.source_object_name, table_config.partition_on)
            return extract_sqlserver_connectorx(
                table_config, output_dir, partition_on=table_config.partition_on
            )
        else:
            logger.info("Routing %s to ConnectorX SQL Server (bulk)",
                        table_config.source_object_name)
            return extract_sqlserver_connectorx(table_config, output_dir)


def extract_windowed(
    table_config: TableConfig,
    target_date: date,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Route single-day windowed extraction based on table config (large tables).

    V-7: When OVERLAP_MINUTES >= 1440 (full day), shifts target_date back by
    1+ days to capture cross-midnight transactions. Sub-day overlap logs INFO;
    datetime-precision WHERE clauses require extractor enhancement.

    Routing rules:
        Oracle + SourceIndexHint -> oracledb single-day (INDEX hints)
        Oracle + no IndexHint -> ConnectorX Oracle windowed
        SQL Server -> ConnectorX SQL Server windowed

    Args:
        table_config: Table configuration.
        target_date: The date to extract.
        output_dir: Directory for temp CSV files.

    Returns:
        Tuple of (DataFrame, CSV path).
    """
    output_dir = Path(output_dir)

    # V-7: Apply overlap window for cross-midnight transaction capture.
    if config.OVERLAP_MINUTES > 0:
        if config.OVERLAP_MINUTES >= 1440:
            overlap_days = config.OVERLAP_MINUTES // 1440
            target_date = target_date - timedelta(days=overlap_days)
            logger.info(
                "V-7: OVERLAP_MINUTES=%d — shifted extraction start back %d day(s) to %s for %s",
                config.OVERLAP_MINUTES, overlap_days, target_date, table_config.source_object_name,
            )
        else:
            logger.info(
                "V-7: OVERLAP_MINUTES=%d active for %s. Sub-day overlap relies on "
                "LookbackDays as primary mechanism; datetime-precision WHERE clauses "
                "require extractor enhancement.",
                config.OVERLAP_MINUTES, table_config.source_object_name,
            )

    next_date = target_date + timedelta(days=1)

    if table_config.is_oracle:
        if table_config.source_index_hint:
            logger.debug(
                "Routing %s date %s to oracledb (INDEX hint: %s)",
                table_config.source_object_name, target_date, table_config.source_index_hint,
            )
            return extract_oracle_oracledb_day(table_config, target_date, output_dir)
        else:
            logger.debug(
                "Routing %s date %s to ConnectorX Oracle windowed",
                table_config.source_object_name, target_date,
            )
            return extract_oracle_connectorx_windowed(
                table_config, output_dir, target_date, next_date,
                partition_on=table_config.partition_on,
            )
    else:
        logger.debug(
            "Routing %s date %s to ConnectorX SQL Server windowed",
            table_config.source_object_name, target_date,
        )
        return extract_sqlserver_connectorx_windowed(
            table_config, output_dir, target_date, next_date,
            partition_on=table_config.partition_on,
        )

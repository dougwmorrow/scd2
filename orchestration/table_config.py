"""TableConfig + TableConfigLoader from General.dbo.UdmTablesList metadata.

Drives extraction routing, table naming, and column/PK configuration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import connectorx as cx
import polars as pl

import config
import connections

logger = logging.getLogger(__name__)


@dataclass
class ColumnConfig:
    """Column metadata from General.dbo.UdmTablesColumnsList."""

    source_name: str
    table_name: str
    column_name: str
    ordinal_position: int
    is_primary_key: bool
    layer: str
    is_index: bool = False
    index_name: str | None = None
    index_type: str | None = None


@dataclass
class TableConfig:
    """Configuration for a single table in the pipeline.

    Populated from General.dbo.UdmTablesList + UdmTablesColumnsList.
    """

    source_object_name: str
    source_server: str
    source_database: str
    source_schema_name: str
    source_name: str
    stage_table_name: str | None = None
    bronze_table_name: str | None = None
    source_aggregate_column_name: str | None = None
    source_aggregate_column_type: str | None = None
    source_index_hint: str | None = None
    partition_on: str | None = None
    first_load_date: str | None = None
    lookback_days: int | None = None
    stage_load_tool: str | None = None
    columns: list[ColumnConfig] = field(default_factory=list)

    @property
    def effective_stage_name(self) -> str:
        return self.stage_table_name or self.source_object_name

    @property
    def effective_bronze_name(self) -> str:
        return self.bronze_table_name or self.source_object_name

    @property
    def stage_full_table_name(self) -> str:
        return f"{config.STAGE_DB}.{self.source_name}.{self.effective_stage_name}_cdc"

    @property
    def bronze_full_table_name(self) -> str:
        return f"{config.BRONZE_DB}.{self.source_name}.{self.effective_bronze_name}_scd2_python"

    @property
    def source_full_table_name(self) -> str:
        return f"{self.source_database}.{self.source_schema_name}.{self.source_object_name}"

    @property
    def is_large_table(self) -> bool:
        return self.source_aggregate_column_name is not None

    @property
    def pk_columns(self) -> list[str]:
        return [
            c.column_name
            for c in self.columns
            if c.is_primary_key and c.layer == "Stage"
        ]

    @property
    def index_configs(self) -> list[ColumnConfig]:
        return [c for c in self.columns if c.is_index]

    @property
    def uses_oracledb(self) -> bool:
        """Oracle + SourceIndexHint populated -> oracledb with INDEX hints."""
        return self.source_index_hint is not None

    @property
    def is_oracle(self) -> bool:
        from sources import SourceType, get_source
        return get_source(self.source_name).source_type == SourceType.ORACLE

    @property
    def is_sql_server(self) -> bool:
        from sources import SourceType, get_source
        return get_source(self.source_name).source_type == SourceType.SQL_SERVER


class TableConfigLoader:
    """Loads table configs from General.dbo.UdmTablesList via ConnectorX."""

    def __init__(self) -> None:
        self._uri = connections.general_connectorx_uri()

    def _load_tables_df(self, where_clause: str = "") -> pl.DataFrame:
        query = (
            "SELECT SourceObjectName, SourceServer, SourceDatabase, "
            "SourceSchemaName, SourceName, StageTableName, BronzeTableName, "
            "SourceAggregateColumnName, SourceAggregateColumnType, "
            "SourceIndexHint, PartitionOn, FirstLoadDate, LookbackDays, "
            "StageLoadTool "
            "FROM dbo.UdmTablesList"
        )
        if where_clause:
            query += f" WHERE {where_clause}"
        return cx.read_sql(self._uri, query, return_type="polars")

    def _load_columns_df(self) -> pl.DataFrame:
        query = (
            "SELECT SourceName, TableName, ColumnName, OrdinalPosition, "
            "IsPrimaryKey, Layer, IsIndex, IndexName, IndexType "
            "FROM dbo.UdmTablesColumnsList"
        )
        return cx.read_sql(self._uri, query, return_type="polars")

    def _build_configs(self, tables_df: pl.DataFrame, columns_df: pl.DataFrame) -> list[TableConfig]:
        configs = []
        for row in tables_df.iter_rows(named=True):
            tc = TableConfig(
                source_object_name=row["SourceObjectName"],
                source_server=row["SourceServer"] or "",
                source_database=row["SourceDatabase"] or "",
                source_schema_name=row["SourceSchemaName"] or "",
                source_name=row["SourceName"],
                stage_table_name=row.get("StageTableName"),
                bronze_table_name=row.get("BronzeTableName"),
                source_aggregate_column_name=row.get("SourceAggregateColumnName"),
                source_aggregate_column_type=row.get("SourceAggregateColumnType"),
                source_index_hint=row.get("SourceIndexHint"),
                partition_on=row.get("PartitionOn"),
                first_load_date=str(row["FirstLoadDate"]) if row.get("FirstLoadDate") else None,
                lookback_days=int(row["LookbackDays"]) if row.get("LookbackDays") else None,
                stage_load_tool=row.get("StageLoadTool"),
            )

            table_name = tc.effective_stage_name
            source_name = tc.source_name

            table_cols = columns_df.filter(
                (pl.col("SourceName") == source_name)
                & (pl.col("TableName") == table_name)
            )

            for col_row in table_cols.iter_rows(named=True):
                tc.columns.append(
                    ColumnConfig(
                        source_name=col_row["SourceName"],
                        table_name=col_row["TableName"],
                        column_name=col_row["ColumnName"],
                        ordinal_position=int(col_row["OrdinalPosition"]) if col_row["OrdinalPosition"] is not None else 0,
                        is_primary_key=bool(col_row["IsPrimaryKey"]),
                        layer=col_row["Layer"] or "",
                        is_index=bool(col_row.get("IsIndex")),
                        index_name=col_row.get("IndexName"),
                        index_type=col_row.get("IndexType"),
                    )
                )

            configs.append(tc)
        return configs

    def load_small_tables(self, source_name: str | None = None, table_name: str | None = None) -> list[TableConfig]:
        conditions = [
            "SourceAggregateColumnName IS NULL",
            "StageLoadTool = 'Python'",
        ]
        if source_name:
            conditions.append(f"SourceName = '{source_name}'")
        if table_name:
            conditions.append(f"SourceObjectName = '{table_name}'")

        where = " AND ".join(conditions)
        tables_df = self._load_tables_df(where)
        columns_df = self._load_columns_df()
        configs = self._build_configs(tables_df, columns_df)
        logger.info("Loaded %d small table configs", len(configs))
        return configs

    def load_large_tables(self, source_name: str | None = None, table_name: str | None = None) -> list[TableConfig]:
        conditions = [
            "SourceAggregateColumnName IS NOT NULL",
            "StageLoadTool = 'Python'",
        ]
        if source_name:
            conditions.append(f"SourceName = '{source_name}'")
        if table_name:
            conditions.append(f"SourceObjectName = '{table_name}'")

        where = " AND ".join(conditions)
        tables_df = self._load_tables_df(where)
        columns_df = self._load_columns_df()
        configs = self._build_configs(tables_df, columns_df)
        logger.info("Loaded %d large table configs", len(configs))
        return configs

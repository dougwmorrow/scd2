"""FileConfig + FileConfigLoader from General.dbo.FileExtract metadata.

Duck-type compatible with TableConfig — provides the same property interface
(source_object_name, stage_full_table_name, bronze_full_table_name, pk_columns,
index_configs, etc.) so all shared pipeline functions work without modification.

FileConfig sources are Excel/CSV files from network drives, not databases.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

import config
import connections

logger = logging.getLogger(__name__)


@dataclass
class FileConfig:
    """Configuration for a file-based pipeline source.

    Duck-types the TableConfig interface so shared pipeline functions
    (run_cdc_promotion, run_scd2_promotion, ensure_stage_table, etc.)
    work without modification.
    """

    # --- Core identity ---
    source_name: str
    table_name: str

    # --- File location ---
    base_path: str
    file_pattern: str
    file_type: str  # 'xlsx', 'xls', 'csv', 'txt', 'json', 'ndjson'

    # --- File reading options ---
    sheet_name: str | None = None
    header_row: int = 0
    skip_rows: int = 0
    delimiter: str | None = None
    encoding: str = "utf-8"

    # --- Column mapping / selection ---
    column_mapping: dict[str, str] | None = None
    columns_to_extract: list[str] | None = None

    # --- Table naming overrides ---
    stage_table_name: str | None = None
    bronze_table_name: str | None = None

    # --- Primary keys (from FileExtract.PrimaryKeyColumns) ---
    pk_column_names: list[str] = field(default_factory=list)

    # --- Change detection mode ---
    change_mode: str = "full_replace"  # 'full_replace' or 'append_only'

    # --- Validation ---
    expected_frequency: str | None = None
    expected_min_rows: int | None = 1
    expected_columns: list[str] | None = None

    # --- Column metadata (from UdmTablesColumnsList, same as TableConfig) ---
    columns: list = field(default_factory=list)  # list[ColumnConfig]

    # --- Resolved schema casing from sys.schemas ---
    _resolved_stage_schema: str | None = None
    _resolved_bronze_schema: str | None = None

    # -----------------------------------------------------------------------
    # Duck-type properties matching TableConfig interface
    # -----------------------------------------------------------------------

    @property
    def source_object_name(self) -> str:
        """TableConfig compat: source object name used for logging, CSV naming, etc."""
        return self.table_name

    @property
    def effective_stage_name(self) -> str:
        return self.stage_table_name or self.table_name

    @property
    def effective_bronze_name(self) -> str:
        return self.bronze_table_name or self.table_name

    @property
    def stage_schema(self) -> str:
        return self._resolved_stage_schema or self.source_name

    @property
    def bronze_schema(self) -> str:
        return self._resolved_bronze_schema or self.source_name

    @property
    def stage_full_table_name(self) -> str:
        return f"{config.STAGE_DB}.{self.stage_schema}.{self.effective_stage_name}_cdc"

    @property
    def bronze_full_table_name(self) -> str:
        return f"{config.BRONZE_DB}.{self.bronze_schema}.{self.effective_bronze_name}_scd2_python"

    @property
    def source_full_table_name(self) -> str:
        """Not applicable for file sources — returns a descriptive string."""
        return f"FILE:{self.base_path}/{self.file_pattern}"

    @property
    def is_large_table(self) -> bool:
        return False

    @property
    def pk_columns(self) -> list[str]:
        """PKs from UdmTablesColumnsList (after column sync), or fallback to pk_column_names."""
        from orchestration.table_config import ColumnConfig
        synced_pks = [
            c.column_name
            for c in self.columns
            if isinstance(c, ColumnConfig) and c.is_primary_key and c.layer == "Stage"
        ]
        if synced_pks:
            return synced_pks
        # Fallback: use pk_column_names from FileExtract before column sync runs
        return list(self.pk_column_names)

    @property
    def index_configs(self) -> list:
        """Index configs from UdmTablesColumnsList columns."""
        return [c for c in self.columns if hasattr(c, "is_index") and c.is_index]

    @property
    def is_oracle(self) -> bool:
        return False

    @property
    def is_sql_server(self) -> bool:
        return False

    @property
    def source_server(self) -> str:
        return ""

    @property
    def source_database(self) -> str:
        return ""

    @property
    def source_schema_name(self) -> str:
        return ""

    @property
    def uses_oracledb(self) -> bool:
        return False

    @property
    def source_index_hint(self) -> str | None:
        return None

    @property
    def partition_on(self) -> str | None:
        return None

    @property
    def source_aggregate_column_name(self) -> str | None:
        return None

    @property
    def source_aggregate_column_type(self) -> str | None:
        return None

    @property
    def first_load_date(self) -> str | None:
        return None

    @property
    def lookback_days(self) -> int | None:
        return None

    @property
    def stage_load_tool(self) -> str | None:
        return "Python"


class FileConfigLoader:
    """Loads file configs from General.dbo.FileExtract.

    Uses pyodbc for all queries (H-3: parameterized queries prevent SQL injection).
    """

    def load_file_configs(
        self,
        source_name: str | None = None,
        table_name: str | None = None,
    ) -> list[FileConfig]:
        """Load file configurations from General.dbo.FileExtract.

        Args:
            source_name: Optional filter by SourceName.
            table_name: Optional filter by TableName.

        Returns:
            List of FileConfig instances for active file sources.
        """
        conditions = ["IsActive = 1", "StageLoadTool = 'Python'"]
        params: list = []

        if source_name:
            conditions.append("SourceName = ?")
            params.append(source_name)
        if table_name:
            conditions.append("TableName = ?")
            params.append(table_name)

        where = " WHERE " + " AND ".join(conditions)
        query = (
            "SELECT FileExtractId, SourceName, TableName, BasePath, FilePattern, "
            "FileType, SheetName, HeaderRow, SkipRows, Delimiter, Encoding, "
            "ColumnMapping, ColumnsToExtract, StageTableName, BronzeTableName, "
            "PrimaryKeyColumns, ChangeMode, ExpectedFrequency, ExpectedMinRows, "
            "ExpectedColumns "
            "FROM dbo.FileExtract"
            + where
        )

        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, *params)
            col_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()

        if not rows:
            logger.info("No file configs found matching filters")
            return []

        # Load column metadata from UdmTablesColumnsList
        columns_df = self._load_columns()

        # Resolve schema casing
        unique_sources = {dict(zip(col_names, row))["SourceName"] for row in rows}
        schema_map = self._resolve_schemas(unique_sources)

        configs = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            fc = self._build_file_config(row_dict, schema_map)
            self._attach_columns(fc, columns_df)
            configs.append(fc)

        logger.info("Loaded %d file configs", len(configs))
        return configs

    def get_known_sources(self) -> set[str]:
        """H-4: Get all known source names from FileExtract for CLI validation."""
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT SourceName FROM dbo.FileExtract WHERE IsActive = 1")
            sources = {row[0] for row in cursor.fetchall()}
            cursor.close()
            return sources
        finally:
            conn.close()

    def get_known_tables(self) -> set[str]:
        """H-4: Get all known table names from FileExtract for CLI validation."""
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT TableName FROM dbo.FileExtract WHERE IsActive = 1")
            tables = {row[0] for row in cursor.fetchall()}
            cursor.close()
            return tables
        finally:
            conn.close()

    def _build_file_config(
        self,
        row: dict,
        schema_map: dict[tuple[str, str], str],
    ) -> FileConfig:
        """Build a FileConfig from a database row."""
        source_name = row["SourceName"]

        # Parse JSON columns safely
        column_mapping = _parse_json_dict(row.get("ColumnMapping"))
        columns_to_extract = _parse_json_list(row.get("ColumnsToExtract"))
        expected_columns = _parse_json_list(row.get("ExpectedColumns"))

        # Parse PrimaryKeyColumns as comma-separated list
        pk_raw = row.get("PrimaryKeyColumns", "")
        pk_column_names = [c.strip() for c in pk_raw.split(",") if c.strip()] if pk_raw else []

        fc = FileConfig(
            source_name=source_name,
            table_name=row["TableName"],
            base_path=row["BasePath"],
            file_pattern=row["FilePattern"],
            file_type=(row["FileType"] or "csv").lower(),
            sheet_name=row.get("SheetName"),
            header_row=int(row.get("HeaderRow") or 0),
            skip_rows=int(row.get("SkipRows") or 0),
            delimiter=row.get("Delimiter"),
            encoding=row.get("Encoding") or "utf-8",
            column_mapping=column_mapping,
            columns_to_extract=columns_to_extract,
            stage_table_name=row.get("StageTableName"),
            bronze_table_name=row.get("BronzeTableName"),
            pk_column_names=pk_column_names,
            change_mode=row.get("ChangeMode") or "full_replace",
            expected_frequency=row.get("ExpectedFrequency"),
            expected_min_rows=int(row["ExpectedMinRows"]) if row.get("ExpectedMinRows") is not None else 1,
            expected_columns=expected_columns,
        )

        # Set resolved schema casing
        fc._resolved_stage_schema = schema_map.get((config.STAGE_DB, source_name))
        fc._resolved_bronze_schema = schema_map.get((config.BRONZE_DB, source_name))

        return fc

    def _load_columns(self) -> list[tuple]:
        """Load all column metadata from UdmTablesColumnsList."""
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT SourceName, TableName, ColumnName, OrdinalPosition, "
                "IsPrimaryKey, Layer, IsIndex, IndexName, IndexType "
                "FROM dbo.UdmTablesColumnsList"
            )
            rows = cursor.fetchall()
            cursor.close()
            return rows
        finally:
            conn.close()

    def _attach_columns(self, fc: FileConfig, columns_rows: list[tuple]) -> None:
        """Attach matching ColumnConfig entries from UdmTablesColumnsList."""
        from orchestration.table_config import ColumnConfig

        table_name = fc.effective_stage_name
        source_name = fc.source_name

        for row in columns_rows:
            if row[0] == source_name and row[1] == table_name:
                fc.columns.append(
                    ColumnConfig(
                        source_name=row[0],
                        table_name=row[1],
                        column_name=row[2],
                        ordinal_position=int(row[3]) if row[3] is not None else 0,
                        is_primary_key=bool(row[4]),
                        layer=row[5] or "",
                        is_index=bool(row[6]) if row[6] is not None else False,
                        index_name=row[7],
                        index_type=row[8],
                    )
                )

    def _resolve_schemas(self, source_names: set[str]) -> dict[tuple[str, str], str]:
        """Resolve actual schema casing for each (database, source_name) pair."""
        resolved: dict[tuple[str, str], str] = {}
        for source_name in source_names:
            for database in (config.STAGE_DB, config.BRONZE_DB):
                key = (database, source_name)
                if key not in resolved:
                    resolved[key] = connections.resolve_schema_name(database, source_name)
        return resolved


# ---------------------------------------------------------------------------
# JSON parsing helpers
# ---------------------------------------------------------------------------

def _parse_json_dict(value: str | None) -> dict[str, str] | None:
    """Parse a JSON string as a dict, returning None if empty or invalid."""
    if not value:
        return None
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError):
        logger.warning("Invalid JSON dict in FileExtract: %s", value[:200])
    return None


def _parse_json_list(value: str | None) -> list[str] | None:
    """Parse a JSON string as a list, returning None if empty or invalid."""
    if not value:
        return None
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        logger.warning("Invalid JSON list in FileExtract: %s", value[:200])
    return None

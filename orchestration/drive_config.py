"""DriveConfig + DriveConfigLoader from General.dbo.NetworkDriveConfig metadata.

Duck-type compatible with TableConfig — provides the same property interface
so all shared pipeline functions (CDC, SCD2, schema evolution, column sync)
work without modification.

Each row in NetworkDriveConfig represents one network drive mount to scan.
The scanner collects file metadata (not contents) via os.walk() + os.stat().
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

import config
import connections

logger = logging.getLogger(__name__)


@dataclass
class DriveConfig:
    """Configuration for a network drive metadata scan.

    Duck-types the TableConfig interface so shared pipeline functions
    (run_cdc_promotion, run_scd2_promotion, ensure_stage_table, etc.)
    work without modification.
    """

    # --- Core identity ---
    source_name: str  # e.g. "NETWORK_DRIVES" (shared across drives)
    table_name: str   # e.g. "FINANCE_FILES" (per-drive logical name)

    # --- Drive location ---
    drive_name: str       # Logical drive name: "FINANCE_SHARE"
    mount_path: str       # Linux mount point: "/mnt/finance"
    unc_path: str | None = None  # Windows UNC for reference: "\\\\server\\finance$"

    # --- Scan options ---
    exclude_patterns: list[str] | None = None  # Glob patterns to skip: ["*.tmp", "~$*"]
    include_patterns: list[str] | None = None  # Glob patterns to include (None = all)
    max_depth: int | None = None               # Max directory recursion depth (None = unlimited)
    follow_symlinks: bool = False              # Whether os.walk follows symlinks

    # --- Table naming overrides ---
    stage_table_name: str | None = None
    bronze_table_name: str | None = None

    # --- Primary keys (always drive_name + full_file_path) ---
    pk_column_names: list[str] = field(
        default_factory=lambda: ["drive_name", "full_file_path"]
    )

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
        return f"DRIVE:{self.mount_path}"

    @property
    def is_large_table(self) -> bool:
        return False

    @property
    def pk_columns(self) -> list[str]:
        from orchestration.table_config import ColumnConfig
        synced_pks = [
            c.column_name
            for c in self.columns
            if isinstance(c, ColumnConfig) and c.is_primary_key and c.layer == "Stage"
        ]
        if synced_pks:
            return synced_pks
        return list(self.pk_column_names)

    @property
    def index_configs(self) -> list:
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


class DriveConfigLoader:
    """Loads drive configs from General.dbo.NetworkDriveConfig.

    Uses pyodbc for all queries (H-3: parameterized queries prevent SQL injection).
    """

    def load_drive_configs(
        self,
        drive_name: str | None = None,
    ) -> list[DriveConfig]:
        """Load drive configurations from General.dbo.NetworkDriveConfig.

        Args:
            drive_name: Optional filter by DriveName.

        Returns:
            List of DriveConfig instances for active, enabled drives.
        """
        conditions = ["IsActive = 1", "ScanEnabled = 1", "StageLoadTool = 'Python'"]
        params: list = []

        if drive_name:
            conditions.append("DriveName = ?")
            params.append(drive_name)

        where = " WHERE " + " AND ".join(conditions)
        query = (
            "SELECT NetworkDriveConfigId, DriveName, MountPath, UNCPath, "
            "SourceName, TableName, StageTableName, BronzeTableName, "
            "ExcludePatterns, IncludePatterns, MaxDepth, FollowSymlinks, "
            "PrimaryKeyColumns "
            "FROM dbo.NetworkDriveConfig"
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
            logger.info("No drive configs found matching filters")
            return []

        columns_rows = self._load_columns()
        unique_sources = {dict(zip(col_names, row))["SourceName"] for row in rows}
        schema_map = self._resolve_schemas(unique_sources)

        configs = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            dc = self._build_drive_config(row_dict, schema_map)
            self._attach_columns(dc, columns_rows)
            configs.append(dc)

        logger.info("Loaded %d drive configs", len(configs))
        return configs

    def get_known_drives(self) -> set[str]:
        """H-4: Get all known drive names for CLI validation."""
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT DriveName FROM dbo.NetworkDriveConfig "
                "WHERE IsActive = 1 AND ScanEnabled = 1"
            )
            drives = {row[0] for row in cursor.fetchall()}
            cursor.close()
            return drives
        finally:
            conn.close()

    def _build_drive_config(
        self,
        row: dict,
        schema_map: dict[tuple[str, str], str],
    ) -> DriveConfig:
        source_name = row["SourceName"]

        exclude_patterns = _parse_json_list(row.get("ExcludePatterns"))
        include_patterns = _parse_json_list(row.get("IncludePatterns"))

        pk_raw = row.get("PrimaryKeyColumns", "drive_name,full_file_path")
        pk_column_names = [c.strip() for c in pk_raw.split(",") if c.strip()]

        dc = DriveConfig(
            source_name=source_name,
            table_name=row["TableName"],
            drive_name=row["DriveName"],
            mount_path=row["MountPath"],
            unc_path=row.get("UNCPath"),
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns,
            max_depth=int(row["MaxDepth"]) if row.get("MaxDepth") is not None else None,
            follow_symlinks=bool(row.get("FollowSymlinks", 0)),
            stage_table_name=row.get("StageTableName"),
            bronze_table_name=row.get("BronzeTableName"),
            pk_column_names=pk_column_names,
        )

        dc._resolved_stage_schema = schema_map.get((config.STAGE_DB, source_name))
        dc._resolved_bronze_schema = schema_map.get((config.BRONZE_DB, source_name))

        return dc

    def _load_columns(self) -> list[tuple]:
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

    def _attach_columns(self, dc: DriveConfig, columns_rows: list[tuple]) -> None:
        from orchestration.table_config import ColumnConfig

        table_name = dc.effective_stage_name
        source_name = dc.source_name

        for row in columns_rows:
            if row[0] == source_name and row[1] == table_name:
                dc.columns.append(
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
        resolved: dict[tuple[str, str], str] = {}
        for source_name in source_names:
            for database in (config.STAGE_DB, config.BRONZE_DB):
                key = (database, source_name)
                if key not in resolved:
                    resolved[key] = connections.resolve_schema_name(database, source_name)
        return resolved


def _parse_json_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        logger.warning("Invalid JSON list in NetworkDriveConfig: %s", value[:200])
    return None

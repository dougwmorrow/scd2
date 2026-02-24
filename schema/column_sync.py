"""Auto-populate UdmTablesColumnsList for new tables.

Syncs column metadata from INFORMATION_SCHEMA on newly created Stage/Bronze tables,
then discovers primary keys from the source system (Oracle or SQL Server).

Runs once per table — skips entirely if UdmTablesColumnsList already has rows for
the given SourceName + TableName combination.

Oracle views without discoverable PKs log a warning; IsPrimaryKey must be set manually.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyodbc

import config
import connections
from sources import SourceType, get_source

if TYPE_CHECKING:
    from orchestration.table_config import ColumnConfig, TableConfig

logger = logging.getLogger(__name__)


def sync_columns(table_config: TableConfig, refresh_pks: bool = False) -> bool:
    """Sync column metadata into UdmTablesColumnsList for a new table.

    1. Check if rows already exist — skip if so (idempotent).
    2. Read INFORMATION_SCHEMA.COLUMNS from the Stage and Bronze tables.
    3. Insert rows into UdmTablesColumnsList for both layers.
    4. Discover PKs from the source system.
    5. UPDATE IsPrimaryKey for discovered PKs.
    6. Reload columns into table_config so CDC/SCD2 work on the first run.

    Args:
        table_config: Table configuration (tables must already exist in UDM).
        refresh_pks: P1-10 — If True, re-discover PKs even if columns already exist.

    Returns:
        True if columns were synced (new table), False if already populated.
    """
    table_name = table_config.effective_stage_name
    source_name = table_config.source_name

    if _columns_exist(source_name, table_name):
        if refresh_pks:
            # P1-10: Re-discover PKs from source and update UdmTablesColumnsList
            logger.info(
                "P1-10: Refreshing PKs for %s.%s (--refresh-pks)",
                source_name, table_name,
            )
            _refresh_pk_flags(table_config, source_name, table_name)
            _reload_columns_into_config(table_config)
            return False

        logger.debug(
            "UdmTablesColumnsList already populated for %s.%s — skipping sync",
            source_name, table_name,
        )
        return False

    logger.info(
        "Syncing columns for %s.%s into UdmTablesColumnsList",
        source_name, table_name,
    )

    # Step 1: Insert column metadata from INFORMATION_SCHEMA
    stage_count = _insert_columns_from_info_schema(
        table_config, table_name, source_name, layer="Stage",
    )
    bronze_count = _insert_columns_from_info_schema(
        table_config, table_name, source_name, layer="Bronze",
    )
    logger.info(
        "Inserted %d Stage columns and %d Bronze columns for %s.%s",
        stage_count, bronze_count, source_name, table_name,
    )

    # Step 2: Discover PKs from source system
    pk_columns = _discover_pks(table_config)

    if pk_columns:
        _update_pk_flags(source_name, table_name, pk_columns)
        logger.info(
            "Discovered and set PKs for %s.%s: %s",
            source_name, table_name, pk_columns,
        )
    else:
        logger.warning(
            "No PKs discovered for %s.%s — IsPrimaryKey must be set manually "
            "in General.dbo.UdmTablesColumnsList before CDC/SCD2 will run",
            source_name, table_name,
        )

    # Step 3: Reload columns into in-memory table_config for this run
    _reload_columns_into_config(table_config)

    return True


# ---------------------------------------------------------------------------
# Check if columns already exist
# ---------------------------------------------------------------------------

def _columns_exist(source_name: str, table_name: str) -> bool:
    """Check if UdmTablesColumnsList already has rows for this source + table."""
    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM dbo.UdmTablesColumnsList "
            "WHERE SourceName = ? AND TableName = ?",
            source_name, table_name,
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Insert column metadata from INFORMATION_SCHEMA
# ---------------------------------------------------------------------------

def _insert_columns_from_info_schema(
    table_config: TableConfig,
    table_name: str,
    source_name: str,
    layer: str,
) -> int:
    """Read INFORMATION_SCHEMA.COLUMNS for a UDM table and insert into UdmTablesColumnsList.

    Args:
        table_config: Table configuration.
        table_name: The effective table name used in UdmTablesColumnsList.
        source_name: The source name (DNA, CCM, etc.).
        layer: 'Stage' or 'Bronze'.

    Returns:
        Number of columns inserted.
    """
    if layer == "Stage":
        full_table_name = table_config.stage_full_table_name
    else:
        full_table_name = table_config.bronze_full_table_name

    parts = full_table_name.split(".")
    db, schema, tbl = parts[0], parts[1], parts[2]

    # Read columns from the actual created table
    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COLUMN_NAME, ORDINAL_POSITION "
            f"FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            f"ORDER BY ORDINAL_POSITION",
            schema, tbl,
        )
        columns = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    if not columns:
        logger.warning("No columns found in INFORMATION_SCHEMA for %s", full_table_name)
        return 0

    # Insert into UdmTablesColumnsList
    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO dbo.UdmTablesColumnsList
                (SourceName, TableName, ColumnName, OrdinalPosition,
                 IsPrimaryKey, Layer, IsIndex, IndexName, IndexType)
            VALUES (?, ?, ?, ?, 0, ?, 0, NULL, NULL)
            """,
            [
                (source_name, table_name, col_name, ordinal, layer)
                for col_name, ordinal in columns
            ],
        )
        cursor.close()
    finally:
        conn.close()

    return len(columns)


# ---------------------------------------------------------------------------
# PK discovery — routes to Oracle or SQL Server
# ---------------------------------------------------------------------------

def _discover_pks(table_config: TableConfig) -> list[str]:
    """Discover primary key columns from the source system.

    Routing:
        Oracle -> ALL_CONSTRAINTS (tables) -> ALL_INDEXES unique (views) -> warn
        SQL Server -> sys.indexes PK -> unique index fallback -> warn

    Returns:
        List of PK column names, or empty list if none discovered.
    """
    source = get_source(table_config.source_name)

    try:
        if source.source_type == SourceType.ORACLE:
            return _discover_oracle_pks(table_config, source)
        else:
            return _discover_sqlserver_pks(table_config, source)
    except Exception:
        logger.exception(
            "PK discovery failed for %s.%s — columns synced but PKs unknown",
            table_config.source_name, table_config.source_object_name,
        )
        return []


def _discover_oracle_pks(table_config: TableConfig, source) -> list[str]:
    """Discover PKs from Oracle: constraints first, then unique indexes as fallback.

    Oracle views don't expose PKs via ALL_CONSTRAINTS, so we try ALL_INDEXES
    for unique indexes on the view. If neither returns results, the pipeline
    logs a warning and IsPrimaryKey must be set manually.
    """
    import oracledb
    from extract.oracle_extractor import _ensure_thick_mode

    _ensure_thick_mode()
    connect_params = source.oracledb_connect_params()

    schema = table_config.source_schema_name.upper()
    table = table_config.source_object_name.upper()

    conn = oracledb.connect(**connect_params)
    try:
        # Attempt 1: Primary key constraint (works for tables, not views)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT acc.COLUMN_NAME
            FROM ALL_CONSTRAINTS ac
            JOIN ALL_CONS_COLUMNS acc
                ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                AND ac.OWNER = acc.OWNER
            WHERE ac.OWNER = :schema
              AND ac.TABLE_NAME = :table_name
              AND ac.CONSTRAINT_TYPE = 'P'
            ORDER BY acc.POSITION
            """,
            schema=schema, table_name=table,
        )
        pk_cols = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if pk_cols:
            logger.info(
                "Oracle PK constraint discovered for %s.%s: %s",
                schema, table, pk_cols,
            )
            return pk_cols

        # Attempt 2: Unique index (works for some views and tables without PK constraints)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT aic.COLUMN_NAME
            FROM ALL_INDEXES ai
            JOIN ALL_IND_COLUMNS aic
                ON ai.INDEX_NAME = aic.INDEX_NAME
                AND ai.TABLE_OWNER = aic.INDEX_OWNER
            WHERE ai.TABLE_OWNER = :schema
              AND ai.TABLE_NAME = :table_name
              AND ai.UNIQUENESS = 'UNIQUE'
            ORDER BY aic.COLUMN_POSITION
            """,
            schema=schema, table_name=table,
        )
        unique_cols = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if unique_cols:
            logger.info(
                "Oracle unique index discovered for %s.%s (using as PK): %s",
                schema, table, unique_cols,
            )
            return unique_cols

        # Nothing found — likely an Oracle view without indexes
        logger.warning(
            "No PK constraint or unique index found for Oracle source %s.%s — "
            "this is expected for views. Set IsPrimaryKey manually.",
            schema, table,
        )
        return []
    finally:
        conn.close()


def _discover_sqlserver_pks(table_config: TableConfig, source) -> list[str]:
    """Discover PKs from SQL Server: primary key index first, then unique index fallback."""
    conn_str = (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={source.host},{source.port};"
        f"DATABASE={source.service_or_database};"
        f"UID={source.user};"
        f"PWD={source.password};"
        "TrustServerCertificate=yes;"
    )

    schema = table_config.source_schema_name
    table = table_config.source_object_name

    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        # Attempt 1: Primary key index
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COL_NAME(ic.object_id, ic.column_id) AS ColumnName
            FROM sys.indexes i
            JOIN sys.index_columns ic
                ON i.object_id = ic.object_id
                AND i.index_id = ic.index_id
            WHERE i.is_primary_key = 1
              AND i.object_id = OBJECT_ID(? + '.' + ?)
            ORDER BY ic.key_ordinal
            """,
            schema, table,
        )
        pk_cols = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if pk_cols:
            logger.info(
                "SQL Server PK discovered for %s.%s: %s",
                schema, table, pk_cols,
            )
            return pk_cols

        # Attempt 2: First unique index (fallback for views with unique clustered index)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COL_NAME(ic.object_id, ic.column_id) AS ColumnName
            FROM sys.indexes i
            JOIN sys.index_columns ic
                ON i.object_id = ic.object_id
                AND i.index_id = ic.index_id
            WHERE i.is_unique = 1
              AND i.is_primary_key = 0
              AND i.object_id = OBJECT_ID(? + '.' + ?)
              AND i.index_id = (
                  SELECT MIN(i2.index_id)
                  FROM sys.indexes i2
                  WHERE i2.object_id = OBJECT_ID(? + '.' + ?)
                    AND i2.is_unique = 1
                    AND i2.is_primary_key = 0
              )
            ORDER BY ic.key_ordinal
            """,
            schema, table, schema, table,
        )
        unique_cols = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if unique_cols:
            logger.info(
                "SQL Server unique index discovered for %s.%s (using as PK): %s",
                schema, table, unique_cols,
            )
            return unique_cols

        logger.warning(
            "No PK or unique index found for SQL Server source %s.%s — "
            "set IsPrimaryKey manually.",
            schema, table,
        )
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# P1-10: Refresh PK flags from source (for --refresh-pks)
# ---------------------------------------------------------------------------

def _refresh_pk_flags(
    table_config: TableConfig,
    source_name: str,
    table_name: str,
) -> None:
    """Re-discover PKs from source and update UdmTablesColumnsList.

    Resets all IsPrimaryKey to 0, re-discovers from source, then sets new PKs to 1.
    """
    # Reset all existing PK flags
    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE dbo.UdmTablesColumnsList SET IsPrimaryKey = 0 "
            "WHERE SourceName = ? AND TableName = ?",
            source_name, table_name,
        )
        cursor.close()
    finally:
        conn.close()

    # Re-discover from source
    pk_columns = _discover_pks(table_config)

    if pk_columns:
        _update_pk_flags(source_name, table_name, pk_columns)
        logger.info(
            "P1-10: Refreshed PKs for %s.%s: %s",
            source_name, table_name, pk_columns,
        )
    else:
        logger.warning(
            "P1-10: No PKs discovered during refresh for %s.%s — "
            "IsPrimaryKey must be set manually",
            source_name, table_name,
        )


# ---------------------------------------------------------------------------
# Update PK flags in UdmTablesColumnsList
# ---------------------------------------------------------------------------

def _update_pk_flags(
    source_name: str,
    table_name: str,
    pk_columns: list[str],
) -> None:
    """Set IsPrimaryKey = 1 for discovered PK columns in both Stage and Bronze layers."""
    if not pk_columns:
        return

    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        placeholders = ", ".join("?" for _ in pk_columns)
        cursor.execute(
            f"""
            UPDATE dbo.UdmTablesColumnsList
            SET IsPrimaryKey = 1
            WHERE SourceName = ?
              AND TableName = ?
              AND ColumnName IN ({placeholders})
            """,
            source_name, table_name, *pk_columns,
        )
        cursor.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Reload columns into in-memory TableConfig
# ---------------------------------------------------------------------------

def _reload_columns_into_config(table_config: TableConfig) -> None:
    """Re-read UdmTablesColumnsList and update table_config.columns in place.

    This ensures pk_columns and index_configs reflect the newly synced metadata
    so CDC/SCD2 work on the very first pipeline run.
    """
    from orchestration.table_config import ColumnConfig

    table_name = table_config.effective_stage_name
    source_name = table_config.source_name

    conn = connections.get_general_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT SourceName, TableName, ColumnName, OrdinalPosition, "
            "IsPrimaryKey, Layer, IsIndex, IndexName, IndexType "
            "FROM dbo.UdmTablesColumnsList "
            "WHERE SourceName = ? AND TableName = ?",
            source_name, table_name,
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    table_config.columns.clear()
    for row in rows:
        table_config.columns.append(
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

    pk_count = len(table_config.pk_columns)
    logger.info(
        "Reloaded %d columns for %s.%s (%d PKs)",
        len(table_config.columns), source_name, table_name, pk_count,
    )


# ---------------------------------------------------------------------------
# E-16: Cross-platform type drift detection
# ---------------------------------------------------------------------------

def detect_source_type_drift(table_config: TableConfig) -> list[str]:
    """E-16: Detect source column type/precision/scale changes.

    Compares current source column metadata against the existing UDM Stage
    table's INFORMATION_SCHEMA. Detects precision/scale changes that could
    affect hash computation (e.g., NUMBER(10,2) → NUMBER(15,4) causing
    silent precision differences).

    Does NOT query Oracle or SQL Server source metadata directly — that
    would add a dependency on source availability. Instead, compares the
    current extraction DataFrame types (already available) against the
    target table types. The schema/evolution.py module handles the actual
    ALTER TABLE changes; this function provides early warning logging.

    Args:
        table_config: Table configuration.

    Returns:
        List of warning messages for detected type drifts. Empty if clean.
    """
    stage_table = table_config.stage_full_table_name
    from extract.udm_connectorx_extractor import table_exists

    if not table_exists(stage_table):
        return []

    source = get_source(table_config.source_name)
    warnings = []

    try:
        if source.source_type == SourceType.ORACLE:
            warnings = _check_oracle_type_drift(table_config, source)
        else:
            warnings = _check_sqlserver_type_drift(table_config, source)
    except Exception:
        logger.debug(
            "E-16: Type drift detection failed for %s.%s — continuing",
            table_config.source_name, table_config.source_object_name,
            exc_info=True,
        )

    if warnings:
        logger.warning(
            "E-16: Source type drift detected for %s.%s: %s",
            table_config.source_name, table_config.source_object_name,
            warnings,
        )

    return warnings


def _check_oracle_type_drift(table_config: TableConfig, source) -> list[str]:
    """E-16: Check Oracle source for precision/scale changes."""
    import oracledb
    from extract.oracle_extractor import _ensure_thick_mode

    _ensure_thick_mode()
    connect_params = source.oracledb_connect_params()

    schema = table_config.source_schema_name.upper()
    table = table_config.source_object_name.upper()

    warnings = []
    conn = oracledb.connect(**connect_params)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, DATA_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
            """,
            schema=schema, table_name=table,
        )
        source_cols = {
            row[0]: {
                "type": row[1],
                "precision": row[2],
                "scale": row[3],
                "length": row[4],
            }
            for row in cursor.fetchall()
        }
        cursor.close()
    finally:
        conn.close()

    # Compare against Stage INFORMATION_SCHEMA
    stage_parts = table_config.stage_full_table_name.split(".")
    db, stage_schema, stage_tbl = stage_parts[0], stage_parts[1], stage_parts[2]

    stage_conn = connections.get_connection(db)
    try:
        cursor = stage_conn.cursor()
        cursor.execute(
            f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, "
            f"CHARACTER_MAXIMUM_LENGTH "
            f"FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            stage_schema, stage_tbl,
        )
        stage_cols = {
            row[0]: {
                "type": row[1],
                "precision": row[2],
                "scale": row[3],
                "length": row[4],
            }
            for row in cursor.fetchall()
        }
        cursor.close()
    finally:
        stage_conn.close()

    # Check numeric precision/scale changes for common columns
    for col_name, src in source_cols.items():
        if col_name not in stage_cols:
            continue
        if src["type"] == "NUMBER" and src["precision"] is not None:
            stg = stage_cols[col_name]
            if stg["precision"] is not None:
                if src["precision"] != stg["precision"] or src["scale"] != stg["scale"]:
                    warnings.append(
                        f"{col_name}: Oracle NUMBER({src['precision']},{src['scale']}) "
                        f"vs Stage ({stg['type']} precision={stg['precision']}, "
                        f"scale={stg['scale']})"
                    )

    return warnings


def _check_sqlserver_type_drift(table_config: TableConfig, source) -> list[str]:
    """E-16: Check SQL Server source for precision/scale changes."""
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    conn_str = (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={source.host},{source.port};"
        f"DATABASE={source.service_or_database};"
        f"UID={source.user};"
        f"PWD={source.password};"
        "TrustServerCertificate=yes;"
    )

    warnings = []
    source_conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        cursor = source_conn.cursor()
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, "
            "CHARACTER_MAXIMUM_LENGTH "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            schema, table,
        )
        source_cols = {
            row[0]: {
                "type": row[1],
                "precision": row[2],
                "scale": row[3],
                "length": row[4],
            }
            for row in cursor.fetchall()
        }
        cursor.close()
    finally:
        source_conn.close()

    # Compare against Stage INFORMATION_SCHEMA
    stage_parts = table_config.stage_full_table_name.split(".")
    db, stage_schema, stage_tbl = stage_parts[0], stage_parts[1], stage_parts[2]

    stage_conn = connections.get_connection(db)
    try:
        cursor = stage_conn.cursor()
        cursor.execute(
            f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, "
            f"CHARACTER_MAXIMUM_LENGTH "
            f"FROM [{db}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            stage_schema, stage_tbl,
        )
        stage_cols = {
            row[0]: {
                "type": row[1],
                "precision": row[2],
                "scale": row[3],
                "length": row[4],
            }
            for row in cursor.fetchall()
        }
        cursor.close()
    finally:
        stage_conn.close()

    # Check for precision/scale changes in common numeric columns
    for col_name, src in source_cols.items():
        if col_name not in stage_cols:
            continue
        stg = stage_cols[col_name]
        if src["precision"] is not None and stg["precision"] is not None:
            if src["precision"] != stg["precision"] or src["scale"] != stg["scale"]:
                warnings.append(
                    f"{col_name}: source ({src['type']} precision={src['precision']}, "
                    f"scale={src['scale']}) vs Stage ({stg['type']} "
                    f"precision={stg['precision']}, scale={stg['scale']})"
                )
        if src["length"] is not None and stg["length"] is not None:
            if src["length"] != stg["length"]:
                warnings.append(
                    f"{col_name}: source length={src['length']} "
                    f"vs Stage length={stg['length']}"
                )

    return warnings
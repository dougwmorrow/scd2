"""Auto-create Stage (CDC) and Bronze (SCD2) tables from DataFrame dtypes.

Creates tables if they don't exist, with the correct schema for CDC and SCD2 columns.

W-10 NOTE — Ordered clustered columnstore indexes (SQL Server 2022):
  For billion-row Bronze tables (100M+), SQL Server 2022 ordered clustered
  columnstore indexes (ORDER on BusinessKey, EffectiveDateTime) enable segment
  elimination for point-in-time queries without sacrificing columnstore
  compression benefits. ensure_bronze_columnstore_index() provides this as
  an opt-in migration for specific large tables. Since columnstore replaces
  the clustered B-tree (IDENTITY PK), the PK must be recreated as nonclustered.
  Benchmark query performance before and after on production data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

import connections
from connections import quote_identifier, quote_table
from extract.udm_connectorx_extractor import table_exists

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)

# Polars dtype -> SQL Server type mapping
_DTYPE_MAP: dict[type, str] = {
    pl.Int8: "TINYINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INT",
    pl.Int64: "BIGINT",
    pl.UInt8: "TINYINT",
    pl.UInt16: "SMALLINT",
    pl.UInt32: "INT",
    pl.UInt64: "BIGINT",
    pl.Float32: "FLOAT",
    pl.Float64: "FLOAT",
    pl.Boolean: "BIT",
    pl.Utf8: "NVARCHAR(MAX)",
    pl.String: "NVARCHAR(MAX)",
    pl.Date: "DATE",
    pl.Datetime: "DATETIME2",
    pl.Time: "TIME",
    pl.Duration: "BIGINT",
}


def _polars_dtype_to_sql(dtype: pl.DataType) -> str:
    """Map a Polars dtype to SQL Server column type."""
    for pl_type, sql_type in _DTYPE_MAP.items():
        if isinstance(dtype, pl_type):
            return sql_type
    if hasattr(dtype, "base_type"):
        base = type(dtype)
        if base in _DTYPE_MAP:
            return _DTYPE_MAP[base]
    return "NVARCHAR(MAX)"


def _build_source_columns_ddl(df: pl.DataFrame, exclude_cols: set[str] | None = None) -> list[str]:
    """Build DDL column definitions from DataFrame, excluding internal columns."""
    if exclude_cols is None:
        exclude_cols = set()
    cols = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        if col_name.startswith("_") or col_name in exclude_cols:
            continue
        sql_type = _polars_dtype_to_sql(dtype)
        cols.append(f"    [{col_name}] {sql_type} NULL")
    return cols


def ensure_stage_table(table_config: TableConfig, df: pl.DataFrame) -> bool:
    """Create Stage (CDC) table if it doesn't exist.

    Schema:
        - All source columns (from df, excluding _ prefixed)
        - _row_hash VARCHAR(64) (B-1: full SHA-256 hex string)
        - _extracted_at DATETIME2
        - _cdc_operation NVARCHAR(1)  (I/U/D)
        - _cdc_valid_from DATETIME2
        - _cdc_valid_to DATETIME2 NULL
        - _cdc_is_current BIT
        - _cdc_batch_id BIGINT

    Returns:
        True if the table was created, False if it already existed.
    """
    full_name = table_config.stage_full_table_name

    if table_exists(full_name):
        logger.info("Stage table %s already exists", full_name)
        return False

    source_cols = _build_source_columns_ddl(df)

    cdc_cols = [
        "    [_row_hash] VARCHAR(64) NULL",
        "    [_extracted_at] DATETIME2 NULL",
        "    [_cdc_operation] NVARCHAR(1) NULL",
        "    [_cdc_valid_from] DATETIME2 NULL",
        "    [_cdc_valid_to] DATETIME2 NULL",
        "    [_cdc_is_current] BIT NULL",
        "    [_cdc_batch_id] BIGINT NULL",
    ]

    all_cols = source_cols + cdc_cols
    col_ddl = ",\n".join(all_cols)

    parts = full_name.split(".")
    db, schema, table = parts[0], parts[1], parts[2]
    q_db = quote_identifier(db)
    q_schema = quote_identifier(schema)
    q_full = quote_table(full_name)

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        # Check if table already exists (parameterized)
        cursor.execute(
            f"SELECT 1 FROM {q_db}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            schema, table,
        )
        if cursor.fetchone():
            cursor.close()
            conn.close()
            logger.info("Created Stage table: %s (already existed)", full_name)
            return True

        # Ensure schema exists
        cursor.execute(
            f"SELECT 1 FROM {q_db}.sys.schemas WHERE name = ?", schema,
        )
        if not cursor.fetchone():
            cursor.execute(f"EXEC({q_db}..sp_executesql N'CREATE SCHEMA {q_schema}')")

        # Create table
        cursor.execute(f"CREATE TABLE {q_full} (\n{col_ddl}\n)")
        cursor.close()
        logger.info("Created Stage table: %s", full_name)
    finally:
        conn.close()

    return True


def ensure_bronze_table(table_config: TableConfig, df: pl.DataFrame) -> bool:
    """Create Bronze (SCD2) table if it doesn't exist.

    Schema:
        - _scd2_key BIGINT IDENTITY(1,1) PRIMARY KEY
        - All source columns (from df, excluding _ prefixed)
        - UdmHash VARCHAR(64) (B-1: full SHA-256 hex string)
        - UdmEffectiveDateTime DATETIME2
        - UdmEndDateTime DATETIME2 NULL
        - UdmActiveFlag BIT
        - UdmScd2Operation NVARCHAR(10)

    Returns:
        True if the table was created, False if it already existed.
    """
    full_name = table_config.bronze_full_table_name

    if table_exists(full_name):
        logger.info("Bronze table %s already exists", full_name)
        return False

    source_cols = _build_source_columns_ddl(df)

    # SCD-2: BIGINT IDENTITY — max 9.2 quintillion. At 3M inserts/day,
    # lasts 8.4 trillion years. INT would overflow in ~716 days.
    identity_col = "    [_scd2_key] BIGINT IDENTITY(1,1) PRIMARY KEY"

    scd2_cols = [
        "    [UdmHash] VARCHAR(64) NULL",
        "    [UdmEffectiveDateTime] DATETIME2 NULL",
        "    [UdmEndDateTime] DATETIME2 NULL",
        "    [UdmActiveFlag] BIT NULL",
        "    [UdmScd2Operation] NVARCHAR(10) NULL",
    ]

    all_cols = [identity_col] + source_cols + scd2_cols
    col_ddl = ",\n".join(all_cols)

    parts = full_name.split(".")
    db, schema, table = parts[0], parts[1], parts[2]
    q_db = quote_identifier(db)
    q_schema = quote_identifier(schema)
    q_full = quote_table(full_name)

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        # Check if table already exists (parameterized)
        cursor.execute(
            f"SELECT 1 FROM {q_db}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            schema, table,
        )
        if cursor.fetchone():
            cursor.close()
            conn.close()
            logger.info("Created Bronze table: %s (already existed)", full_name)
            return True

        # Ensure schema exists
        cursor.execute(
            f"SELECT 1 FROM {q_db}.sys.schemas WHERE name = ?", schema,
        )
        if not cursor.fetchone():
            cursor.execute(f"EXEC({q_db}..sp_executesql N'CREATE SCHEMA {q_schema}')")

        # Create table
        cursor.execute(f"CREATE TABLE {q_full} (\n{col_ddl}\n)")
        cursor.close()
        logger.info("Created Bronze table: %s", full_name)
    finally:
        conn.close()

    return True


def ensure_bronze_unique_active_index(
    table_config: TableConfig,
    pk_columns: list[str],
) -> bool:
    """SCD-1: Create unique filtered index on Bronze to prevent duplicate active rows.

    The INSERT-first SCD2 design (P0-8) can leave duplicate active rows on retry
    (crash after INSERT but before UPDATE). This index prevents the second INSERT
    from succeeding — the retry gets a constraint violation (detectable, recoverable)
    instead of silently creating duplicate active records.

    Index: CREATE UNIQUE NONCLUSTERED INDEX UX_Active_<table>
           ON Bronze(pk1, pk2, ...) WHERE UdmActiveFlag = 1

    Args:
        table_config: Table configuration (Bronze table must already exist).
        pk_columns: Primary key columns for the uniqueness constraint.

    Returns:
        True if the index was created, False if it already existed or no PKs.
    """
    if not pk_columns:
        logger.debug("SCD-1: No PK columns for %s — skipping unique active index",
                      table_config.source_object_name)
        return False

    full_name = table_config.bronze_full_table_name
    if not table_exists(full_name):
        return False

    parts = full_name.split(".")
    db, schema, table = parts[0], parts[1], parts[2]

    index_name = f"UX_Active_{table_config.source_object_name}"
    pk_col_list = ", ".join(quote_identifier(c) for c in pk_columns)

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        # Check if index already exists
        cursor.execute(
            "SELECT COUNT(*) FROM sys.indexes "
            "WHERE object_id = OBJECT_ID(?) AND name = ?",
            full_name, index_name,
        )
        exists = cursor.fetchone()[0] > 0

        if exists:
            logger.debug("SCD-1: Unique active index %s already exists on %s",
                         index_name, full_name)
            cursor.close()
            return False

        # Create unique filtered index
        q_full = quote_table(full_name)
        ddl = (
            f"CREATE UNIQUE NONCLUSTERED INDEX {quote_identifier(index_name)} "
            f"ON {q_full} ({pk_col_list}) "
            f"WHERE [UdmActiveFlag] = 1"
        )
        cursor.execute(ddl)
        cursor.close()
        logger.info("SCD-1: Created unique active index %s on %s (%s)",
                     index_name, full_name, pk_col_list)
        return True
    except Exception:
        logger.warning(
            "SCD-1: Failed to create unique active index on %s — "
            "duplicate active row prevention unavailable",
            full_name, exc_info=True,
        )
        return False
    finally:
        conn.close()


def ensure_bronze_point_in_time_index(
    table_config: TableConfig,
    pk_columns: list[str],
) -> bool:
    """V-9: Create point-in-time lookup index on Bronze table.

    Index: CREATE NONCLUSTERED INDEX IX_PIT_<table>
           ON Bronze(pk1, pk2, ..., UdmEffectiveDateTime DESC)

    Supports historical queries like "what was the value of this record on date X"
    without full table scans. At 3B+ rows, this is critical for query performance.

    Args:
        table_config: Table configuration (Bronze table must already exist).
        pk_columns: Primary key columns for the index prefix.

    Returns:
        True if the index was created, False if it already existed or no PKs.
    """
    if not pk_columns:
        return False

    full_name = table_config.bronze_full_table_name
    if not table_exists(full_name):
        return False

    parts = full_name.split(".")
    db = parts[0]

    index_name = f"IX_PIT_{table_config.source_object_name}"
    pk_col_list = ", ".join(quote_identifier(c) for c in pk_columns)

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        # Check if index already exists
        cursor.execute(
            "SELECT COUNT(*) FROM sys.indexes "
            "WHERE object_id = OBJECT_ID(?) AND name = ?",
            full_name, index_name,
        )
        exists = cursor.fetchone()[0] > 0

        if exists:
            logger.debug("V-9: Point-in-time index %s already exists on %s",
                         index_name, full_name)
            cursor.close()
            return False

        q_full = quote_table(full_name)
        ddl = (
            f"CREATE NONCLUSTERED INDEX {quote_identifier(index_name)} "
            f"ON {q_full} ({pk_col_list}, [UdmEffectiveDateTime] DESC)"
        )
        cursor.execute(ddl)
        cursor.close()
        logger.info("V-9: Created point-in-time index %s on %s (%s, UdmEffectiveDateTime DESC)",
                     index_name, full_name, pk_col_list)
        return True
    except Exception:
        logger.warning(
            "V-9: Failed to create point-in-time index on %s — "
            "historical lookups will use full scans",
            full_name, exc_info=True,
        )
        return False
    finally:
        conn.close()


# W-10: Minimum row count threshold for columnstore index migration.
_COLUMNSTORE_ROW_THRESHOLD = 100_000_000


def ensure_bronze_columnstore_index(
    table_config: TableConfig,
    pk_columns: list[str],
    row_count: int | None = None,
    min_rows: int = _COLUMNSTORE_ROW_THRESHOLD,
) -> bool:
    """W-10: Create ordered clustered columnstore index on large Bronze tables.

    SQL Server 2022 ordered clustered columnstore indexes enable segment
    elimination for point-in-time queries while providing columnstore
    compression for billion-row tables.

    WARNING: This is a DESTRUCTIVE schema migration. The existing clustered
    PK index (_scd2_key) is dropped and recreated as nonclustered to make
    room for the clustered columnstore index. Run during a maintenance window.

    Prerequisites:
      - SQL Server 2022 or later
      - Bronze table must already exist
      - Table should have > min_rows to justify the overhead

    Args:
        table_config: Table configuration (Bronze table must exist).
        pk_columns: PK columns for the ORDER clause.
        row_count: Known row count (avoids extra query). If None, queries it.
        min_rows: Skip below this threshold (default 100M).

    Returns:
        True if the columnstore index was created, False otherwise.
    """
    if not pk_columns:
        logger.debug("W-10: No PK columns for %s — skipping columnstore index",
                      table_config.source_object_name)
        return False

    full_name = table_config.bronze_full_table_name
    if not table_exists(full_name):
        return False

    parts = full_name.split(".")
    db = parts[0]

    # Check row count threshold
    if row_count is None:
        from extract.udm_connectorx_extractor import get_table_row_count
        row_count = get_table_row_count(full_name)

    if row_count < min_rows:
        logger.debug(
            "W-10: %s has %d rows (threshold=%d) — skipping columnstore index",
            full_name, row_count, min_rows,
        )
        return False

    cci_name = f"CCI_{table_config.source_object_name}"
    order_cols = ", ".join(quote_identifier(c) for c in pk_columns) + ", [UdmEffectiveDateTime]"

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()

        # Check if columnstore index already exists
        cursor.execute(
            "SELECT COUNT(*) FROM sys.indexes "
            "WHERE object_id = OBJECT_ID(?) AND type_desc = 'CLUSTERED COLUMNSTORE'",
            full_name,
        )
        has_cci = cursor.fetchone()[0] > 0
        if has_cci:
            logger.debug("W-10: Clustered columnstore index already exists on %s", full_name)
            cursor.close()
            return False

        # Verify SQL Server 2022+
        cursor.execute("SELECT SERVERPROPERTY('ProductMajorVersion')")
        version_row = cursor.fetchone()
        major_version = int(version_row[0]) if version_row and version_row[0] else 0
        if major_version < 16:
            logger.warning(
                "W-10: SQL Server version %d does not support ordered columnstore indexes "
                "(requires SQL Server 2022 / version 16+). Skipping %s.",
                major_version, full_name,
            )
            cursor.close()
            return False

        q_full = quote_table(full_name)

        # Step 1: Drop existing clustered PK and recreate as nonclustered
        # Find the current PK constraint name
        cursor.execute(
            "SELECT name FROM sys.key_constraints "
            "WHERE parent_object_id = OBJECT_ID(?) AND type = 'PK'",
            full_name,
        )
        pk_row = cursor.fetchone()
        if pk_row:
            pk_name = pk_row[0]
            logger.info(
                "W-10: Dropping clustered PK [%s] on %s to make room for columnstore",
                pk_name, full_name,
            )
            cursor.execute(f"ALTER TABLE {q_full} DROP CONSTRAINT {quote_identifier(pk_name)}")
            # Recreate PK as nonclustered
            cursor.execute(
                f"ALTER TABLE {q_full} ADD CONSTRAINT {quote_identifier(pk_name)} "
                f"PRIMARY KEY NONCLUSTERED ([_scd2_key])"
            )
            logger.info("W-10: Recreated PK [%s] as NONCLUSTERED on %s", pk_name, full_name)

        # Step 2: Create ordered clustered columnstore index
        ddl = (
            f"CREATE CLUSTERED COLUMNSTORE INDEX {quote_identifier(cci_name)} "
            f"ON {q_full} ORDER ({order_cols})"
        )
        cursor.execute(ddl)
        cursor.close()

        logger.info(
            "W-10: Created ordered clustered columnstore index [%s] on %s "
            "ORDER (%s) — %d rows",
            cci_name, full_name, order_cols, row_count,
        )
        return True

    except Exception:
        logger.warning(
            "W-10: Failed to create columnstore index on %s — "
            "continuing with existing B-tree indexes",
            full_name, exc_info=True,
        )
        return False
    finally:
        conn.close()

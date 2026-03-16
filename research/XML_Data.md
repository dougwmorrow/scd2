# Extracting SQL Server XML columns fast in Python

**The most effective solution is not switching libraries—it's keeping ConnectorX and auto-rewriting queries to CAST XML columns to NVARCHAR(MAX).** This workaround preserves ConnectorX's 3–20× speed advantage over pyodbc while fully capturing XML content. Among alternative drivers, **arrow-odbc** (Rust-based ODBC-to-Arrow bridge, now integrated into Polars) and **turbodbc** (C++ ODBC with native Arrow output) offer the best fallback performance when ConnectorX is unsuitable. No high-performance Python driver natively supports SQL Server's XML data type except pytds, which is pure Python and too slow for bulk ETL.

---

## Why ConnectorX chokes on XML and how to fix it

ConnectorX uses the **tiberius** Rust crate for TDS protocol communication. Tiberius *can* read XML data (the `XmlData` type), but ConnectorX's type-mapping layer in `MsSQLTypeSystem` simply never maps it. When the driver encounters an XML column, it hits an unmapped type and panics with `PanicException: not implemented: xml`. No GitHub issue or roadmap item addresses XML support.

The fix is straightforward: **`CAST(xml_column AS NVARCHAR(MAX))`** in the SELECT query. ConnectorX fully supports NVARCHAR(MAX), mapping it to Arrow's `Utf8` (string) type. NVARCHAR(MAX) stores up to **2 GB**—identical to SQL Server's XML type limit—so no data is lost. Edge cases are minimal: SQL Server already strips `<?xml?>` declarations at storage time, and namespace declarations survive intact. For whitespace-sensitive documents, `CONVERT(NVARCHAR(MAX), xml_col, 1)` preserves insignificant whitespace.

A production-ready pattern auto-detects XML columns and rewrites queries transparently:

```python
import connectorx as cx
import polars as pl

def extract_table(uri: str, table: str, schema: str = "dbo") -> pl.DataFrame:
    # Detect XML columns via metadata (this query itself has no XML columns)
    meta_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
    """
    meta = cx.read_sql(uri, meta_query, return_type="polars")
    
    # Build SELECT with CAST for any XML columns
    cols = []
    for row in meta.iter_rows(named=True):
        if row["DATA_TYPE"] == "xml":
            cols.append(f"CAST([{row['COLUMN_NAME']}] AS NVARCHAR(MAX)) AS [{row['COLUMN_NAME']}]")
        else:
            cols.append(f"[{row['COLUMN_NAME']}]")
    
    query = f"SELECT {', '.join(cols)} FROM [{schema}].[{table}]"
    return pl.read_database_uri(query, uri, engine="connectorx")
```

This approach keeps the pipeline on ConnectorX for **all** tables—including those with XML columns—and requires no additional driver dependencies. The server-side CAST adds negligible overhead since it is essentially serialization of SQL Server's internal XML representation.

---

## How every candidate driver actually handles XML

The SQL Server XML type uses TDS type token **`0xF1` (241)** and transmits via PLP (Partially Length-Prefixed) chunked streaming, introduced in TDS 7.2 (SQL Server 2005). Drivers fail not because the protocol is exotic, but because their type-mapping layers don't recognize `SQL_SS_XML` (ODBC type `-152`) or TDS token `0xF1`.

**pytds is the only Python driver with native XML support.** It implements a dedicated `XmlSerializer` class (type ID 241) that reads PLP-chunked XML and returns Python Unicode strings. However, pytds is a pure-Python TDS implementation with no Arrow output—meaning data must flow through Python objects to reach Polars, making it unsuitable for bulk ETL despite its correct type handling.

**pyodbc handles XML transparently** because the Microsoft ODBC Driver maps XML to wide strings internally. XML values arrive as Python `str` objects. This works but forces row-wise fetching—each row creates Python objects before Polars can build columnar arrays, which is why pyodbc extractions run **3–20× slower** than ConnectorX for large tables.

The remaining drivers all fail on raw XML columns:

| Driver | XML behavior | Root cause | CAST workaround |
|---|---|---|---|
| **ConnectorX 0.4.5** | `PanicException: not implemented: xml` | Type enum missing XML mapping | ✅ Works perfectly |
| **turbodbc 5.2.0** | `RuntimeError: Unsupported type identifier` | `SQL_SS_XML` (-152) not handled | ✅ Works perfectly |
| **pymssql 2.3.13** | Fails / returns None | FreeTDS DB-Library predates XML type | ✅ Works perfectly |
| **mssql-python 1.4.0** | Unconfirmed (likely works like pyodbc) | Uses same ODBC driver internally | Likely unnecessary |
| **arrow-odbc 9.3.3** | Likely returns as string via ODBC | Maps through ODBC driver's type handling | Probably unnecessary |

---

## Performance tiers for getting SQL Server data into Polars

The speed gap between drivers is enormous—**up to 20× between the fastest and slowest paths**—and the difference comes down to whether data stays in columnar Arrow format or passes through Python row objects.

**Tier 1: ConnectorX** remains the fastest option. Its Rust core reads TDS data directly into pre-allocated Arrow buffers with zero intermediate copies. Partition-based parallelism (`partition_on="id", partition_num=10`) splits the query into concurrent range reads, scaling near-linearly with cores. The ConnectorX VLDB 2022 paper demonstrated **21× faster** than Dask and **13× faster** than pandas `read_sql` on an 8.6 GB TPC-H dataset, with **3× less memory**. For SQL Server specifically, `pl.read_database_uri("mssql://user:pass@host/db", query, engine="connectorx")` provides the zero-copy Polars path.

**Tier 2: arrow-odbc and turbodbc** both produce Arrow tables from ODBC, bypassing Python row objects. arrow-odbc (Rust-based, v9.3.3) is now **natively integrated into Polars**—passing an ODBC connection string to `pl.read_database()` auto-invokes it. turbodbc's `fetchallarrow()` returns a PyArrow table that converts to Polars via `pl.from_arrow()` in a zero-copy operation. Both deliver roughly **3–10× faster** bulk reads than pyodbc for large result sets. turbodbc requires `prefer_unicode=True` and `use_async_io=True` for optimal SQL Server performance; it installs best via conda on Linux.

**Tier 3: mssql-python** (Microsoft's new GA driver, v1.4.0) is **2–4× faster** than pyodbc for fetch operations per Microsoft's benchmarks, thanks to its C++/PyBind11 architecture bypassing the ODBC Driver Manager. However, it returns data row-wise with no Arrow output—a feature tracked in GitHub Issue #130 as "under development." Until Arrow support ships, mssql-python offers no advantage over pyodbc for Polars-centric pipelines.

**Tier 4: pyodbc and pymssql** fetch rows as Python tuples, requiring materialization into Python objects before conversion to Polars. This path involves double data movement and GIL contention. pyodbc remains the most battle-tested fallback with the broadest type support.

---

## The recommended pipeline architecture

The optimal design uses ConnectorX as the primary engine with query rewriting for XML columns, and arrow-odbc as a secondary engine for edge cases:

```python
import polars as pl
import connectorx as cx
import logging

logger = logging.getLogger(__name__)

UNSUPPORTED_TYPES = {"xml", "geography", "geometry", "hierarchyid", "sql_variant"}

def extract(uri: str, odbc_str: str, table: str, schema: str = "dbo",
            partition_on: str = None, partition_num: int = 4) -> pl.DataFrame:
    """Extract from SQL Server with ConnectorX primary, arrow-odbc fallback."""
    
    # Step 1: Detect problematic columns
    meta = cx.read_sql(uri, f"""
        SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
        ORDER BY ORDINAL_POSITION""", return_type="polars")
    
    problem_cols = meta.filter(pl.col("DATA_TYPE").is_in(UNSUPPORTED_TYPES))
    
    # Step 2: Build safe query with CASTs
    cols = []
    for row in meta.iter_rows(named=True):
        if row["DATA_TYPE"] in UNSUPPORTED_TYPES:
            cols.append(f"CAST([{row['COLUMN_NAME']}] AS NVARCHAR(MAX)) AS [{row['COLUMN_NAME']}]")
        else:
            cols.append(f"[{row['COLUMN_NAME']}]")
    query = f"SELECT {', '.join(cols)} FROM [{schema}].[{table}]"
    
    # Step 3: Execute with ConnectorX (fastest path)
    try:
        kwargs = {"query": query, "conn": uri, "return_type": "polars"}
        if partition_on:
            kwargs["partition_on"] = partition_on
            kwargs["partition_num"] = partition_num
        return cx.read_sql(**kwargs)
    except Exception as e:
        logger.warning(f"ConnectorX failed: {e}. Falling back to arrow-odbc.")
        # Fallback: arrow-odbc via Polars native ODBC string detection
        return pl.read_database(query=query, connection=odbc_str)
```

This architecture handles XML columns (and other unsupported types like geography/geometry) transparently via CAST, keeps ConnectorX's full performance including parallel partitioning, and falls back to arrow-odbc's Arrow-native ODBC path only when ConnectorX fails for other reasons. The arrow-odbc fallback still avoids the slow row-wise pyodbc path.

---

## Linux Red Hat compatibility and installation

All recommended components run on Red Hat Enterprise Linux with Python 3.12. **ConnectorX** provides `manylinux` wheels on PyPI (`pip install connectorx`). **arrow-odbc** ships Rust-compiled `manylinux` wheels (`pip install arrow-odbc`). **turbodbc** offers PyPI wheels for Python 3.11–3.14 but also requires unixODBC and the Microsoft ODBC Driver 18 installed at the system level (`sudo dnf install msodbcsql18 unixODBC-devel`). **mssql-python** provides RHEL-specific `manylinux_2_28` wheels and needs only `dnf install libtool-ltdl krb5-libs` as system prerequisites—no ODBC Driver Manager required, which simplifies deployment.

For the recommended stack, the only system dependency beyond Python is the Microsoft ODBC Driver 18 (needed for the arrow-odbc fallback path). ConnectorX uses its own built-in Rust TDS client and requires no system ODBC installation.

---

## Conclusion

**The CAST workaround eliminates the need to replace ConnectorX.** Auto-detecting XML columns via `INFORMATION_SCHEMA.COLUMNS` and rewriting `SELECT *` to include `CAST([xml_col] AS NVARCHAR(MAX))` lets ConnectorX handle every table in the pipeline at full speed, with zero data loss and negligible server-side overhead. This is a 15-line code change, not a library migration.

For pipelines that cannot modify queries (e.g., user-supplied SQL), **arrow-odbc** is the strongest alternative—it produces Arrow data natively, integrates into Polars with a single ODBC connection string, runs on RHEL, and handles XML columns through the Microsoft ODBC Driver's transparent string mapping. turbodbc offers similar Arrow performance but requires the same CAST workaround as ConnectorX for XML columns, adding no XML-specific advantage.

The emerging **mssql-python** from Microsoft is worth tracking. Once Arrow output lands (GitHub Issue #130, marked "under development"), it could become a compelling single-driver solution with native XML handling, built-in connection pooling, and no ODBC Driver Manager dependency. Until then, the ConnectorX + query-rewriting approach remains the highest-performance path for XML-containing SQL Server tables in Polars pipelines.









router.py
"""Router integration for turbodbc XML-capable extraction.

This file shows the ADDITIONS to extract/router.py. The existing Oracle
routing logic is untouched — only the SQL Server branches gain XML detection.

Design principle: ConnectorX remains the primary path for ALL SQL Server
tables. turbodbc is only invoked when a table actually contains XML columns
that would crash ConnectorX. The XML column check is cached per-table per
pipeline run to avoid repeated INFORMATION_SCHEMA queries.
"""

# ===========================================================================
# NEW IMPORTS (add to top of router.py)
# ===========================================================================

# from extract.turbodbc_sqlserver_extractor import (
#     extract_sqlserver_turbodbc,
#     extract_sqlserver_turbodbc_windowed,
#     table_has_xml_columns,
# )

# ===========================================================================
# NEW: Per-run cache for XML column detection (module-level in router.py)
# ===========================================================================

# Avoids hitting INFORMATION_SCHEMA on every extraction call for the same table.
# Key: (source_name, schema, table) -> bool
# _xml_column_cache: dict[tuple[str, str, str], bool] = {}
#
# def _has_xml_columns(table_config: TableConfig) -> bool:
#     """Cached XML column check for routing decisions."""
#     key = (
#         table_config.source_name,
#         table_config.source_schema_name,
#         table_config.source_object_name,
#     )
#     if key not in _xml_column_cache:
#         _xml_column_cache[key] = table_has_xml_columns(*key)
#         if _xml_column_cache[key]:
#             logger.info(
#                 "E-XML: %s.%s contains XML columns — routing to turbodbc",
#                 table_config.source_name, table_config.source_object_name,
#             )
#     return _xml_column_cache[key]


# ===========================================================================
# MODIFIED: extract_full() — SQL Server branch (replace existing else block)
# ===========================================================================

# Original:
#     else:
#         if table_config.partition_on:
#             logger.info("Routing %s to ConnectorX SQL Server (partition_on=%s)", ...)
#             return extract_sqlserver_connectorx(table_config, output_dir, partition_on=...)
#         else:
#             logger.info("Routing %s to ConnectorX SQL Server (bulk)", ...)
#             return extract_sqlserver_connectorx(table_config, output_dir)

# New:
#     else:
#         # E-XML: Check for XML columns that would crash ConnectorX.
#         if _has_xml_columns(table_config):
#             logger.info(
#                 "Routing %s to turbodbc (XML columns detected)",
#                 table_config.source_object_name,
#             )
#             return extract_sqlserver_turbodbc(table_config, output_dir)
#         elif table_config.partition_on:
#             logger.info(
#                 "Routing %s to ConnectorX SQL Server (partition_on=%s)",
#                 table_config.source_object_name, table_config.partition_on,
#             )
#             return extract_sqlserver_connectorx(
#                 table_config, output_dir, partition_on=table_config.partition_on,
#             )
#         else:
#             logger.info(
#                 "Routing %s to ConnectorX SQL Server (bulk)",
#                 table_config.source_object_name,
#             )
#             return extract_sqlserver_connectorx(table_config, output_dir)


# ===========================================================================
# MODIFIED: extract_windowed() — SQL Server branch (replace existing else)
# ===========================================================================

# Original:
#     else:
#         logger.debug("Routing %s date %s to ConnectorX SQL Server windowed", ...)
#         return extract_sqlserver_connectorx_windowed(table_config, output_dir, ...)

# New:
#     else:
#         if _has_xml_columns(table_config):
#             logger.debug(
#                 "Routing %s date %s to turbodbc windowed (XML columns)",
#                 table_config.source_object_name, target_date,
#             )
#             return extract_sqlserver_turbodbc_windowed(
#                 table_config, output_dir, target_date, next_date,
#             )
#         else:
#             logger.debug(
#                 "Routing %s date %s to ConnectorX SQL Server windowed",
#                 table_config.source_object_name, target_date,
#             )
#             return extract_sqlserver_connectorx_windowed(
#                 table_config, output_dir, target_date, next_date,
#                 partition_on=table_config.partition_on,
#             )






turbobdc.py
"""turbodbc SQL Server extraction -> Arrow -> Polars DataFrame -> BCP CSV.

XML-capable fallback extractor for SQL Server sources (CCM, EPICOR, etc.)
when ConnectorX cannot handle tables containing XML data type columns.

turbodbc uses ODBC Driver 18 with Apache Arrow columnar fetching
(fetchallarrow()), providing ~3-10x faster bulk reads than pyodbc's
row-wise cursor. Arrow tables convert to Polars via zero-copy pl.from_arrow().

Routing: router.py sends tables here when INFORMATION_SCHEMA detects XML
columns that would crash ConnectorX (PanicException: not implemented: xml).

XML handling strategy:
  - turbodbc also cannot fetch raw XML columns (SQL_SS_XML type -152).
  - All XML columns are auto-detected via INFORMATION_SCHEMA.COLUMNS and
    CAST to NVARCHAR(MAX) in the SELECT, which is lossless (both types
    store up to 2 GB). This happens transparently — callers see string
    columns where XML columns existed in the source.

Provides two modes matching ConnectorX extractor signatures:
  - extract_sqlserver_turbodbc(): Full table scan (small tables).
  - extract_sqlserver_turbodbc_windowed(): Date-windowed extraction (large tables).

Dependencies:
  - turbodbc (pip install turbodbc or conda install -c conda-forge turbodbc)
  - pyarrow (transitive dependency of turbodbc Arrow fetching)
  - ODBC Driver 18 for SQL Server (system-level, already installed for BCP/pyodbc)
  - unixODBC-devel (system-level: dnf install unixODBC-devel)
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

import config
from data_load.bcp_csv import prepare_dataframe_for_bcp, write_bcp_csv
from sources import get_source

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)

# SQL Server data types that turbodbc/ConnectorX cannot handle natively.
# All are CAST to NVARCHAR(MAX) in extraction queries.
_UNSUPPORTED_TYPES = frozenset({
    "xml",
    "geography",
    "geometry",
    "hierarchyid",
    "sql_variant",
})


# ---------------------------------------------------------------------------
# ODBC connection factory
# ---------------------------------------------------------------------------

def _build_odbc_connection_string(source_name: str) -> str:
    """Build an ODBC connection string for a SQL Server source.

    Uses the same ODBC Driver 18 already installed for BCP and pyodbc.
    """
    source = get_source(source_name)
    return (
        f"Driver={{{config.ODBC_DRIVER}}};"
        f"Server={source.host},{source.port};"
        f"Database={source.service_or_database};"
        f"UID={source.user};"
        f"PWD={source.password};"
        "TrustServerCertificate=yes;"
    )


def _get_turbodbc_connection(source_name: str):
    """Create a turbodbc connection with optimal fetch settings.

    Returns a turbodbc connection configured for:
      - prefer_unicode=True: Ensures NVARCHAR columns return proper Unicode.
      - use_async_io=True: Overlaps network I/O with Arrow buffer construction.
      - read_buffer_size: Rows buffered per ODBC fetch call. turbodbc's Arrow
        path works best with large buffers (fewer round-trips).
    """
    import turbodbc

    connection_string = _build_odbc_connection_string(source_name)

    options = turbodbc.make_options(
        prefer_unicode=True,
        use_async_io=True,
        read_buffer_size=turbodbc.Rows(10_000),
    )

    conn = turbodbc.connect(connection_string=connection_string, turbodbc_options=options)
    return conn


# ---------------------------------------------------------------------------
# XML column detection and query rewriting
# ---------------------------------------------------------------------------

def _detect_xml_columns(source_name: str, schema: str, table: str) -> list[str]:
    """Query INFORMATION_SCHEMA.COLUMNS to find XML (and other unsupported) columns.

    Uses pyodbc for this metadata query since it's lightweight and always
    available. Returns list of column names that need CAST to NVARCHAR(MAX).
    """
    import pyodbc

    source = get_source(source_name)
    conn_str = (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={source.host},{source.port};"
        f"DATABASE={source.service_or_database};"
        f"UID={source.user};"
        f"PWD={source.password};"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        # H-3: Parameterized query to prevent SQL injection.
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            schema,
            table,
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    xml_cols = [row[0] for row in rows if row[1].lower() in _UNSUPPORTED_TYPES]

    if xml_cols:
        logger.info(
            "Detected %d unsupported-type columns in %s.%s requiring CAST: %s",
            len(xml_cols), schema, table, xml_cols,
        )

    return xml_cols


def _build_safe_select(
    source_name: str,
    schema: str,
    table: str,
    where_clause: str = "",
) -> tuple[str, list[str]]:
    """Build a SELECT that CASTs XML columns to NVARCHAR(MAX).

    Returns:
        Tuple of (SQL query string, list of XML column names that were CAST).
    """
    import pyodbc

    source = get_source(source_name)
    conn_str = (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={source.host},{source.port};"
        f"DATABASE={source.service_or_database};"
        f"UID={source.user};"
        f"PWD={source.password};"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            schema,
            table,
        )
        all_columns = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    xml_cols = []
    select_parts = []
    for col_name, data_type in all_columns:
        if data_type.lower() in _UNSUPPORTED_TYPES:
            select_parts.append(f"CAST([{col_name}] AS NVARCHAR(MAX)) AS [{col_name}]")
            xml_cols.append(col_name)
        else:
            select_parts.append(f"[{col_name}]")

    query = f"SELECT {', '.join(select_parts)} FROM [{schema}].[{table}]"
    if where_clause:
        query += f" {where_clause}"

    return query, xml_cols


# ---------------------------------------------------------------------------
# Arrow -> Polars conversion
# ---------------------------------------------------------------------------

def _fetch_as_polars(conn, query: str, context: str) -> pl.DataFrame:
    """Execute query via turbodbc and return a Polars DataFrame.

    Uses turbodbc's fetchallarrow() for columnar Arrow output, then
    zero-copy converts to Polars via pl.from_arrow(). This avoids the
    Python-object materialization that makes pyodbc slow for bulk reads.
    """
    cursor = conn.cursor()
    try:
        logger.debug("turbodbc executing: %s", query[:200])
        cursor.execute(query)

        # fetchallarrow() returns a pyarrow.Table in one call.
        # For very large result sets (>10M rows), fetchnumpybatches()
        # would allow streaming, but Arrow tables are the Polars-native path.
        arrow_table = cursor.fetchallarrow()

        # Zero-copy conversion: Polars wraps the Arrow buffers directly.
        df = pl.from_arrow(arrow_table)

        logger.info(
            "turbodbc fetched %d rows, %d columns (%s)",
            len(df), len(df.columns), context,
        )
        return df

    except Exception:
        logger.error("turbodbc fetch failed for %s", context, exc_info=True)
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Public extraction functions (matching ConnectorX extractor signatures)
# ---------------------------------------------------------------------------

def extract_sqlserver_turbodbc(
    table_config: TableConfig,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Extract from SQL Server via turbodbc into a Polars DataFrame and write BCP CSV.

    Full table scan mode for small tables. XML columns are auto-CAST to
    NVARCHAR(MAX). Signature matches extract_sqlserver_connectorx() for
    drop-in routing from router.py.

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    query, xml_cols = _build_safe_select(
        source_name=table_config.source_name,
        schema=schema,
        table=table,
    )

    if xml_cols:
        logger.info(
            "turbodbc extract %s.%s — CAST applied to XML columns: %s",
            schema, table, xml_cols,
        )
    else:
        logger.info("turbodbc extract %s.%s — no XML columns detected", schema, table)

    conn = _get_turbodbc_connection(table_config.source_name)
    try:
        df = _fetch_as_polars(
            conn, query,
            context=f"turbodbc full extract {table_config.source_name}.{table}",
        )
    finally:
        conn.close()

    logger.info(
        "Extracted %d rows, %d columns from %s",
        len(df), len(df.columns), table_config.source_full_table_name,
    )

    # P3-5: Warn about large full-scan extractions
    if len(df) > 5_000_000:
        logger.warning(
            "P3-5: turbodbc full-scan returned %d rows from %s. "
            "Consider date-windowed extraction for better performance.",
            len(df), table_config.source_full_table_name,
        )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=False)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}.csv",
    )

    return df, csv_path


def extract_sqlserver_turbodbc_windowed(
    table_config: TableConfig,
    output_dir: str | Path,
    start_date: date,
    end_date: date,
) -> tuple[pl.DataFrame, Path]:
    """Extract a date window from SQL Server via turbodbc.

    For large tables. Uses SourceAggregateColumnName for the WHERE clause.
    XML columns are auto-CAST to NVARCHAR(MAX). Signature matches
    extract_sqlserver_connectorx_windowed() (minus partition_on/partition_num
    which turbodbc does not support — single-connection only).

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.
        start_date: Start of date range (inclusive).
        end_date: End of date range (exclusive).

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    date_col = table_config.source_aggregate_column_name
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    where_clause = (
        f"WHERE [{date_col}] >= '{start_date}' AND [{date_col}] < '{end_date}'"
    )

    query, xml_cols = _build_safe_select(
        source_name=table_config.source_name,
        schema=schema,
        table=table,
        where_clause=where_clause,
    )

    if xml_cols:
        logger.info(
            "turbodbc windowed %s [%s, %s) — CAST applied to XML columns: %s",
            table, start_date, end_date, xml_cols,
        )
    else:
        logger.info(
            "turbodbc windowed %s [%s, %s) — no XML columns",
            table, start_date, end_date,
        )

    conn = _get_turbodbc_connection(table_config.source_name)
    try:
        df = _fetch_as_polars(
            conn, query,
            context=f"turbodbc windowed {table} [{start_date}, {end_date})",
        )
    finally:
        conn.close()

    logger.info(
        "Extracted %d rows from %s.%s for [%s, %s)",
        len(df), schema, table, start_date, end_date,
    )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}_{start_date}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=False)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}_{start_date}.csv",
    )

    return df, csv_path


# ---------------------------------------------------------------------------
# Utility: check if a table has XML columns (used by router.py)
# ---------------------------------------------------------------------------

def table_has_xml_columns(source_name: str, schema: str, table: str) -> bool:
    """Check if a source table contains XML (or other unsupported) columns.

    Called by router.py to decide between ConnectorX (fast, no XML) and
    turbodbc (Arrow-fast, XML-safe) extraction paths.
    """
    xml_cols = _detect_xml_columns(source_name, schema, table)
    return len(xml_cols) > 0









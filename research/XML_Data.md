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

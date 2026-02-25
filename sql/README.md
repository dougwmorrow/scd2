# PK / Index / INDEX Hint Discovery Scripts

## Purpose

These scripts identify primary keys, indexes, uniqueness constraints, and INDEX hint opportunities across all UDM pipeline source systems. They serve three goals:

1. **Validate existing configuration** — ensure `SourceIndexHint` values reference real Oracle indexes, and `IsPrimaryKey` flags are correctly set in `UdmTablesColumnsList`
2. **Find missing PKs** — especially for Oracle/SQL Server views which don't have constraints, and tables where `column_sync.py` couldn't auto-discover PKs
3. **Identify optimization opportunities** — indexes that could be added as `SourceIndexHint` for large table windowed extraction, or tracked in `UdmTablesColumnsList.IsIndex` for BCP load disable/rebuild cycles

## Script Inventory

| # | Script | Run Against | Purpose |
|---|--------|-------------|---------|
| 01 | `01_oracle_source_discovery.sql` | Oracle DNA | PK constraints, all indexes, view PK candidates, date column coverage, function-based index warnings |
| 02 | `02_sqlserver_source_discovery.sql` | CCM / EPICOR | PK constraints, all indexes, view PK candidates, heap tables, index usage statistics |
| 03 | `03_udm_metadata_crossref.sql` | General (target) | Cross-reference `UdmTablesList` + `UdmTablesColumnsList` vs actual Stage/Bronze indexes; gap analysis |
| 04 | `04_view_pk_profiler.py` | All sources | Automated `COUNT(DISTINCT)` profiling to identify PK candidates for views and PK-less tables |
| 05 | `05_oracle_index_hint_validation.sql` | Oracle DNA | Validates `SourceIndexHint` values, checks date column alignment, stale statistics |

## How They Map to the Pipeline

### INDEX Hints → `oracle_extractor.py`

The pipeline uses `/*+ INDEX(table hint) */` syntax for Oracle extraction when `UdmTablesList.SourceIndexHint` is populated. Scripts 01 (Section 9) and 05 validate that:
- The hinted index actually exists
- It covers the date column used in `WHERE date >= :s AND date < :e`
- It's a standard B-tree (not function-based, which O-1 range predicates can't use)
- Statistics are fresh enough for good query plans

### Primary Keys → `UdmTablesColumnsList.IsPrimaryKey`

PKs drive CDC hash comparison (`cdc/engine.py`) and SCD2 versioning (`scd2/engine.py`). Scripts 01-02 discover PKs from constraints/indexes; Script 04 profiles view columns when no PK is discoverable. Script 03 identifies tables where no PK is configured — these will **fail** CDC/SCD2.

### Target Indexes → `UdmTablesColumnsList.IsIndex` / `IndexName`

`index_management.py` disables/rebuilds indexes during BCP bulk loads. Script 03 (Sections 6-8) shows what indexes exist on Stage/Bronze tables and whether they're tracked in `UdmTablesColumnsList`.

## Execution Order

1. **Start with Script 03** (UDM metadata cross-reference) — this shows you which tables have gaps, which hints are configured, and what needs investigation.
2. **Run Script 01** against Oracle or **Script 02** against SQL Server to discover what actually exists in the source.
3. **Run Script 05** for Oracle sources with `SourceIndexHint` values to validate the hints are correct.
4. **Run Script 04** (`04_view_pk_profiler.py`) for any views or PK-less tables identified by the earlier scripts.

## Script 04 Usage

```bash
# Profile all views across all sources
python 04_view_pk_profiler.py

# Oracle only
python 04_view_pk_profiler.py --source DNA

# SQL Server only
python 04_view_pk_profiler.py --source CCM

# Specific view
python 04_view_pk_profiler.py --source DNA --table MY_VIEW_NAME

# Sample large views (first 100K rows)
python 04_view_pk_profiler.py --max-rows 100000
```

The profiler generates `UPDATE` SQL for `General.dbo.UdmTablesColumnsList` with the recommended PK columns.

## Key Things to Watch For

- **Views without PKs**: The pipeline's `column_sync.py` logs a warning and skips PK assignment. CDC/SCD2 will fail until `IsPrimaryKey` is set manually.
- **Stale INDEX hints**: If an Oracle DBA drops/recreates an index, the hint silently degrades to a full table scan.
- **Function-based indexes on date columns**: `oracle_extractor.py` uses range predicates (`date >= :s AND date < :e`) per O-1 optimization. Function-based indexes like `TRUNC(date_col)` won't be used for range scans.
- **Heap tables (SQL Server)**: Tables without a clustered index — BCP loads are slower and queries against them are less efficient.
- **Unused indexes**: Script 02 Section 10 shows indexes with zero reads — these add write overhead without benefit.
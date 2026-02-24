# Oracle/SQL Server -> SQL Server data pipeline (Python + Polars + BCP)

ETL pipeline extracting from Oracle (DNA) and SQL Server (CCM, EPICOR) sources into a medallion architecture: UDM_Stage (CDC), UDM_Bronze (SCD2). Metadata-driven via General.dbo.UdmTablesList. Runs on Linux RedHat Server.

## Environment & Dependencies
- Python 3.12.11
- Oracle Instant Client 19c (for oracledb thick mode and ConnectorX oracle:// connections)
- ODBC Driver 18 for SQL Server (for BCP, pyodbc, and ConnectorX mssql:// connections)
- Key packages: polars, polars-hash, connectorx, oracledb, pyodbc
- BCP utility (mssql-tools18)
- .env file location: /debi/.env (NOT project root)

## Structure
- config.py - env vars, DB names, BCP thresholds, paths (.env at /debi/.env)
- sources.py - source system registry (Oracle/SQL Server connection factories)
- connections.py - SQL Server target DB connections (Stage/Bronze/General), cursor_for() context manager
- cli_common.py - shared CLI boilerplate (environment setup, logging, startup checks, RSS monitoring)
- main_small_tables.py - CLI entry point for small tables
- main_large_tables.py - CLI entry point for large tables
- extract/ — Source Data Extraction
  - router.py - extraction routing: routes Oracle/SQL Server × ConnectorX/oracledb (E-3)
  - connectorx_oracle_extractor.py - ConnectorX Oracle extraction -> Polars -> BCP CSV
  - udm_connectorx_extractor.py - ConnectorX for internal UDM SQL Server reads
  - oracle_extractor.py - oracledb fallback (date-chunked with INDEX hints)
  - connectorx_sqlserver_extractor.py - ConnectorX SQL Server extraction -> Polars -> BCP CSV
- data_load/ — BCP Loading Infrastructure
  - bcp_loader.py - BCP subprocess wrapper (CSV -> SQL Server)
  - bcp_csv.py - BCP CSV helpers: hashing (polars-hash), sanitization, column reorder (P0-1), CSV write
  - row_hash.py - row hashing (polars-hash SHA-256, fallback hashlib)
  - sanitize.py - DataFrame sanitization (strings, BIT columns, UInt64, Oracle dates)
  - bcp_format.py - BCP XML format file generation
  - schema_utils.py - shared schema metadata queries for column validation and PK type lookup (P0-3)
  - index_management.py - index disable/rebuild around BCP loads
- cdc/ — Change Data Capture
  - engine.py - Polars CDC: hash comparison, detect inserts/updates/deletes, NULL PK filter (P0-4)
  - reconciliation/ - full column-by-column reconciliation subpackage (P3-4)
- scd2/ — Slowly Changing Dimension Type 2
  - engine.py - Polars SCD2: Bronze comparison, UPDATES via typed staging (P0-3), INSERTs via BCP
- orchestration/ — Table Processing Flows
  - small_tables.py - orchestrator for small tables (no date column)
  - large_tables.py - orchestrator for large tables (date-chunked, per-day checkpoint)
  - guards.py - shared extraction guard logic (parameterized thresholds, baseline retrieval)
  - pipeline_steps.py - shared CDC/SCD2 promotion steps
  - table_config.py - TableConfig + TableConfigLoader from General.dbo.UdmTablesList
  - table_lock.py - sp_getapplock table-level locking to prevent concurrent runs (P1-2)
  - pipeline_state.py - extraction state tracking, gap detection, checkpoints
- schema/ — DDL & Schema Management
  - evolution.py - schema drift detection: ADD new cols, WARN removed, ERROR type changes (P0-2)
  - column_sync.py - auto-populate UdmTablesColumnsList + PK discovery from source
  - table_creator.py - auto-create Stage/Bronze tables from DataFrame dtypes
  - staging_cleanup.py - orphaned staging table cleanup at pipeline start (P3-3)
- migrations/ — One-time schema migration scripts
  - b1_hash_varchar64.py - ALTER _row_hash/UdmHash from BIGINT to VARCHAR(64)
- observability/ — Logging & Metrics
  - event_tracker.py - PipelineEventTracker context manager -> General.ops.PipelineEventLog
  - log_handler.py - SqlServerLogHandler (logging.Handler) -> General.ops.PipelineLog

## Known Issues & Backlog
See **TODO.md** for the remaining edge cases and hardening work. All 14 edge cases from the *Validating 14 Edge Cases in a Polars-BCP-CDC-SCD2 Pipeline on RHEL* research have been audited — 12 are fully implemented, 2 have remaining work:
- **P2-1 (polars-hash migration):** `add_row_hash()` now uses polars-hash plugin for deterministic, Rust-native hashing stable across Polars versions and Python sessions. Replaced native `hash_rows()` which was non-deterministic across sessions (GitHub #3966, #7758).
- **P3-2 (ConnectorX Oracle timezone):** oracledb path uses TRUNC() — fully mitigated. ConnectorX windowed path now also uses TRUNC() in WHERE clauses to prevent midnight boundary drift.
- Review TODO.md before starting any new feature work.

## Commands
- Run small tables: python3 main_small_tables.py --workers 4
- Single table: python3 main_small_tables.py --table ACCT --source DNA
- List tables: python3 main_small_tables.py --list-tables
- Extract specific source: python3 main_small_tables.py --workers 4 --source DNA
- Run large tables: python3 main_large_tables.py --workers 6

## BCP CSV Contract (Single Source of Truth)
All CSV writers MUST produce files matching this exact specification. Any deviation will cause BCP load failures or data corruption.
```
Delimiter:          tab (\t)
Row terminator:     LF only (-r 0x0A)
Header:             none
Quoting:            quote_style='never'
NULL representation: empty string
Datetime format:    '%Y-%m-%d %H:%M:%S.%3f'
BIT columns:        Int8 (0/1) — never True/False
Hash columns:       Full SHA-256 hex string, VARCHAR(64) (B-1)
UInt64 (non-hash):  .reinterpret(signed=True) -> Int64
String sanitization: replace \t \n \r \x00 with empty string BEFORE write_csv
Batch size:         write_csv(batch_size=4096) to avoid memory spikes
```

## Table Naming Conventions
Naming is driven by UdmTablesList. Custom names override the default when StageTableName or BronzeTableName is populated.

**Example: ACCT table from DNA source (schema: osibank)**

| Layer | Default Name | Custom Name Override |
|-------|-------------|---------------------|
| Source | `DNA.osibank.ACCT` | n/a |
| Stage (CDC) | `UDM_Stage.DNA.ACCT_cdc` | `UDM_Stage.DNA.{StageTableName}_cdc` |
| Bronze (SCD2) | `UDM_Bronze.DNA.ACCT_scd2_python` | `UDM_Bronze.DNA.{BronzeTableName}_scd2_python` |

Pattern: `{target_db}.{SourceName}.{table_name}_{suffix}`
- Stage suffix: `_cdc`
- Bronze suffix: `_scd2_python`
- The schema in UDM_Stage and UDM_Bronze is the SourceName (e.g., DNA, CCM, EPICOR), NOT the source schema

## Data Flow (per table)

### Small Tables (no date column - full extract each run)
Source (Oracle/SQL Server)
  -> ConnectorX full extract -> Polars DataFrame
  -> add _row_hash (polars-hash, deterministic across sessions) + _extracted_at
  -> Write BCP CSV (per BCP CSV Contract above)
  -> Ensure stage/bronze tables exist in UDM (auto-create from DataFrame dtypes)
  -> Schema evolution: detect new/removed/changed columns (P0-2)
  -> Column sync: auto-populate UdmTablesColumnsList + discover PKs from source
  -> Empty extraction guard: skip CDC if row count drops >90% vs previous run (P1-1)
  -> Table lock: sp_getapplock prevents concurrent runs on the same table (P1-2)
  -> CDC promotion (Polars in-memory comparison with existing CDC table)
        NULL PK filter (P0-4) -> anti-join inserts, hash compare updates, reverse anti-join deletes
        Column reorder to match target INFORMATION_SCHEMA ordinal position (P0-1)
        Staging tables use actual PK types from target (P0-3)
        columns: _cdc_operation (I/U/D), _cdc_valid_from/to, _cdc_is_current, _cdc_batch_id
        -> Capture changes via BCP into staging table
  -> SCD2 promotion (Polars comparison: CDC current vs Bronze active)
        Staging tables use actual PK types from target (P0-3)
        columns: UdmHash, UdmEffectiveDateTime, UdmEndDateTime, UdmActiveFlag, UdmScd2Operation
        -> UPDATEs via BCP staging table + MERGE
        -> INSERTs via BCP (append-only, never truncate Bronze)

### Large Tables (date-chunked - incremental extraction)
Per-day processing pipeline: extract one day at a time → windowed CDC → targeted SCD2 → checkpoint → next day.

Source (Oracle/SQL Server)
  -> Windowed extract (single day via SourceAggregateColumnName) -> Polars DataFrame
  -> add _row_hash (polars-hash) + _extracted_at
  -> Write BCP CSV (per BCP CSV Contract above)
  -> Ensure stage/bronze tables exist (first day only)
  -> Schema evolution (P0-2)
  -> Column sync (first load only)
  -> Table lock: sp_getapplock prevents concurrent runs (P1-2)
  -> Windowed CDC (P1-3/P1-4): compare only within extraction date window
        NULL PK filter (P0-4) -> anti-join inserts, hash compare updates
        Delete detection scoped to extraction window only (P1-4) — rows outside window untouched
        Column reorder (P0-1), typed staging tables (P0-3)
  -> Targeted SCD2 (P1-3): PK-scoped Bronze read via staging table join (not full table load)
  -> Checkpoint date as SUCCESS in PipelineExtractionState (P1-5)
  -> CSV cleanup
  -> Next day...

**Design decisions (resolved):**
- **Memory bounding (P1-3/P2-4):** Processing one day at a time keeps memory bounded. A 3B-row table with 3M rows/day fits comfortably in memory per-day.
- **Checkpoint and gap detection (P1-5):** `orchestration/pipeline_state.py` tracks per-day status in PipelineExtractionState. On restart, the pipeline resumes from the last successful date and fills gaps.
- **Partial extraction recovery (P1-6):** Each completed day is checkpointed. A failure on day 15 of 30 preserves the first 14 days; the next run picks up from day 15.
- **Idempotency:** Windowed CDC comparison handles re-runs safely. Re-extracting the same date produces unchanged hashes for untouched rows.
- **Delete detection (P1-4):** Scoped to extraction window. Rows outside the window are not considered deleted. Pair with periodic full reconciliation for real deletes.
- **Cross-day transaction overlap (V-7):** Source transactions spanning midnight may split across processing windows. Three mitigations exist: (1) `LookbackDays` provides a rolling re-extraction window as the primary mechanism — a lookback of 3 days means each date is re-processed across 3 runs, catching most split transactions. (2) `OVERLAP_MINUTES` env var (default 0) extends each day's window backward; when >= 1440 minutes (full day), shifts target_date back by 1+ days; sub-day precision requires datetime-level WHERE clauses in extractors (future enhancement). (3) Weekly reconciliation (`cdc/reconciliation.py`) catches any remaining discrepancies as the safety net. CDC comparison is idempotent — overlapping extraction windows produce no phantom changes because unchanged rows hash identically.

**Current extraction routing:**
- Oracle + SourceIndexHint populated -> oracledb with per-day date chunks, INDEX hints, TRUNC() boundaries (P3-2), distinct-date pre-query to skip empty days (P2-2)
- Oracle + SourceIndexHint NULL -> ConnectorX windowed with FULL scan hint, TRUNC() boundaries (P3-2)
- SQL Server -> ConnectorX windowed

**What we know works:**
- SourceAggregateColumnName is the date column used for WHERE clause filtering
- LookbackDays provides a rolling window to capture day-over-day changes
- FirstLoadDate defines the earliest date boundary for initial loads
- Multiple runs per day provide natural retry coverage for transient failures

## Key Architecture Decisions

**Extraction routing** is driven by UdmTablesList columns:
- SourceIndexHint: populated -> oracle_extractor (date-chunked INDEX hint)
- SourceIndexHint: NULL + Oracle -> connectorx_extractor (bulk FULL scan, 10-20x faster)
- PartitionOn: If value is not null then use column as ConnectorX partition_on column value else use regular ConnectorX extract. If both SourceIndexHint and PartitionOn are not NULL then proceed with using SourceIndexHint and oracledb for Oracle extracts otherwise proceed with ConnectorX for non-Oracle extracts.
- SourceObjectName: Table name of the source data.
- SourceServer: Linked server or server of source data.
- SourceDatabase: Database of source data.
- SourceSchemaName: Schema name of the source data.
- SourceName: The source name (DNA, CCM, EPICOR, etc) from where the data comes.
- SourceAggregateColumnType: The datatype of the SourceAggregateColumnName.
- SourceAggregateColumnName: The date column for large tables to incrementally extract data from.
- FirstLoadDate: Date in which the large table is to have data extracted from.
- LookbackDays: Number of days from current day which need data extracted for large tables. To capture rolling window and ensure we capture all changes day over day.
- StageTableName: Custom name of the Stage table (overrides SourceObjectName in naming).
- BronzeTableName: Custom name of the Bronze table (overrides SourceObjectName in naming).
- StageLoadTool: If value is Python then extract data. If null then do nothing.
- SQL Server source: NULL + SQL Server -> connectorx_sqlserver_extractor (bulk FULL scan, 10-20x faster)

**Connection string patterns:**
- Oracle (ConnectorX): `oracle://user:pass@host:port/service`
- Oracle (oracledb): thick mode via Oracle Instant Client 19c
- SQL Server (ConnectorX): `mssql://user:pass@host:port/database`
- SQL Server (pyodbc/BCP): ODBC Driver 18 for SQL Server
- Further details TBD after connection testing

**Column Tracking** via General.dbo.UdmTablesColumnsList — used AFTER UDM tables have been created:
- SourceName: The source name of where data originates.
- TableName: Table name in UDM. Similar to SourceObjectName, StageTableName, and BronzeTableName.
- ColumnName: The column name from the table.
- OrdinalPosition: Column position in the table.
- IsPrimaryKey: A boolean flag of 1 or 0. Must be manually set for Oracle views that lack primary keys.
- Layer: The layer of the medallion architecture ie Stage, Bronze, Silver, Gold.
- IsIndex: A boolean flag of 1 or 0 specifically if the UDM table should have an index.
- IndexName: The name of the index.
- IndexType: The type of SQL Server index.

Primary uses:
1. CDC: IsPrimaryKey drives which columns are used for row-level comparison and change detection.
2. SCD2: IsPrimaryKey determines the business key for versioning.
3. Index optimization: IsIndex/IndexName/IndexType used by index_management.py for disable/rebuild around BCP loads.
4. Future: Will be used to optimize Stage and Bronze table structures.

Note: Oracle views do NOT expose primary keys — IsPrimaryKey must be manually populated in UdmTablesColumnsList for any Oracle view source.

**Column Sync** (schema/column_sync.py) — auto-populates UdmTablesColumnsList on first load:

When a new table is added to UdmTablesList, the pipeline automatically syncs column metadata and discovers primary keys. This runs once per table (skips if UdmTablesColumnsList already has rows for the source + table).

Flow:
1. After `ensure_stage_table` / `ensure_bronze_table` create the UDM tables
2. `sync_columns()` checks if UdmTablesColumnsList has rows — if yes, skip
3. Reads `INFORMATION_SCHEMA.COLUMNS` from the newly created Stage and Bronze tables
4. Inserts rows into UdmTablesColumnsList for both Stage and Bronze layers (IsPrimaryKey=0, IsIndex=0)
5. Discovers PKs from the source system:
   - Oracle tables: `ALL_CONSTRAINTS` with `CONSTRAINT_TYPE = 'P'`
   - Oracle views: falls back to `ALL_INDEXES` with `UNIQUENESS = 'UNIQUE'`
   - SQL Server tables: `sys.indexes` with `is_primary_key = 1`
   - SQL Server views: falls back to first unique index
6. Updates `IsPrimaryKey = 1` for discovered columns in both layers
7. Reloads columns into the in-memory `TableConfig` so CDC/SCD2 work on the first run

Optimistic behavior:
- Tables with discoverable PKs: fully automatic, CDC/SCD2 run on the first pipeline execution
- Oracle views without PKs or unique indexes: columns sync, but PK warning is logged and CDC/SCD2 skip until IsPrimaryKey is set manually
- PK discovery failure (connection error, permissions): columns still sync, PK warning logged, non-fatal

**CDC + SCD2 in Polars** (feature flags USE_POLARS_CDC and USE_POLARS_SCD2 in orchestration/small_tables.py). The in-memory path carries df_current and pk_columns forward from CDC to SCD2, eliminating re-extraction from UDM_Stage.

**SCD2 is optimized from 5 steps to 2**: (1) single UPDATE batch for closes/deletes, (2) single INSERT batch for new rows + new versions. Unchanged rows are counted but NOT touched — saves GB of transaction log.

**BULK_LOGGED recovery model** is set on the target DB during the load window, restored to FULL with a log backup after. This is wrapped via '_bulk_load_recovery_context()'.

## Observability: Event Tracking + Pipeline Logs

The pipeline has two complementary observability tables in `General.ops`. Together they answer "what happened and how fast?" (PipelineEventLog) and "why did it happen that way?" (PipelineLog). The join point is `BatchId + TableName + SourceName`.

### General.ops.PipelineEventLog — Runtime Performance Tracking

The primary table for identifying bottlenecks and tracking pipeline health. PipelineEventTracker writes exactly one row per step per table. All tables in a run share one BatchId from General.ops.PipelineBatchSequence.

PipelineEventLog columns:
- BatchId: Pipeline run ID, constant for the entire run. Sourced from General.ops.PipelineBatchSequence.
- TableName: The table being processed (e.g., ACCT).
- SourceName: The data source (DNA, CCM, EPICOR, etc.).
- EventType: EXTRACT, BCP_LOAD, CDC_PROMOTION, SCD2_PROMOTION, CSV_CLEANUP, TABLE_TOTAL.
- EventDetail: Free-text detail about the event. May be removed if not providing value.
- StartedAt: Timestamp when the step began.
- CompletedAt: Timestamp when the step finished.
- DurationMs: Elapsed time in milliseconds (CompletedAt - StartedAt). This is the key metric for bottleneck analysis.
- Status: Success/failure indicator for the step.
- ErrorMessage: Error detail when Status indicates failure.
- RowsProcessed: Total rows handled during the step.
- RowsInserted: Rows inserted (CDC inserts or SCD2 new versions).
- RowsUpdated: Rows updated (CDC updates or SCD2 closes).
- RowsDeleted: Rows marked as deleted (CDC soft deletes).
- RowsUnchanged: Rows that matched and required no action.
- RowsBefore: Row count in the target table before the step ran.
- RowsAfter: Row count in the target table after the step completed.
- TableCreated: BIT (1/0) — whether the UDM table was auto-created during this run.
- Metadata: JSON or free-text field for one-off metrics. May be removed unless specific metrics prove worth tracking.
- RowsPerSecond: Throughput metric derived from RowsProcessed / (DurationMs / 1000).

EventType definitions:
- EXTRACT: Time to pull data from source (Oracle/SQL Server) into a Polars DataFrame and write the BCP CSV.
- BCP_LOAD: Time for the BCP subprocess to load the CSV into the SQL Server staging/CDC table.
- CDC_PROMOTION: Time for the Polars CDC comparison (hash-based insert/update/delete detection) and writing changes.
- SCD2_PROMOTION: Time for the Polars SCD2 comparison against Bronze and executing the UPDATE + INSERT batches.
- CSV_CLEANUP: Time to delete temporary BCP CSV files after load completes.
- TABLE_TOTAL: End-to-end wall time for the entire table pipeline (extract through SCD2), useful for identifying the slowest tables overall. Also used with Status=SKIPPED for lock-blocked tables (OBS-3).

**PipelineEventTracker design**: A context manager that wraps each pipeline step. Captures StartedAt on entry, CompletedAt on exit, computes DurationMs, catches exceptions into ErrorMessage and sets Status to FAILED, then writes the row to PipelineEventLog. Pipeline code sets row counts on the event object as it discovers them. TABLE_TOTAL is an outer context manager around all inner steps for nested timing. If anything inside the `with` block throws, the event still gets recorded — you never lose visibility into failures.

```python
# Usage pattern in pipeline code
with tracker.track("EXTRACT", table_config) as event:
    df = extract_from_source(table_config)
    event.rows_processed = len(df)

with tracker.track("CDC_PROMOTION", table_config) as event:
    cdc_result = run_cdc(df, table_config)
    event.rows_inserted = cdc_result.inserts
    event.rows_updated = cdc_result.updates
    event.rows_deleted = cdc_result.deletes
    event.rows_unchanged = cdc_result.unchanged
```

### General.ops.PipelineLog — Detailed Diagnostic Logs

The investigation table for understanding *why* something was slow, failed, or behaved unexpectedly. Many rows per step — the narrative of what happened inside each pipeline step.

PipelineLog columns:
- BatchId: Same run-level ID as PipelineEventLog, enabling joins.
- TableName: Nullable — some log entries are pipeline-wide, not table-specific.
- SourceName: Nullable for the same reason.
- LogLevel: DEBUG, INFO, WARNING, ERROR, CRITICAL.
- Module: Python module that emitted the log (e.g., `extract.connectorx_oracle_extractor`, `cdc.engine`).
- FunctionName: The specific function (e.g., `extract_with_partition`, `_detect_changes`).
- Message: Human-readable log message.
- ErrorType: Exception class name when applicable (e.g., `ConnectionError`, `PolarsSchemaError`).
- StackTrace: Full traceback for ERROR/CRITICAL entries.
- Metadata: JSON field for structured context (query text, chunk date range, memory usage, intermediate row counts).
- CreatedAt: Timestamp of the log entry.

**SqlServerLogHandler design**: A custom `logging.Handler` subclass so every module uses standard `logger.info()`, `logger.warning()`, `logger.error()` calls. The handler holds the current BatchId and TableName in a thread-local or context variable — individual modules never pass tracking context around. The handler batches log entries and writes them to PipelineLog. Logs below a configurable threshold (e.g., DEBUG in production) are filtered at the handler level to avoid flooding the table.

**Log retention policy**: Keep 30 days of DEBUG/INFO, 90 days of WARNING+, indefinite for ERROR/CRITICAL. A SQL Agent job or pipeline post-step handles cleanup.

### How the Two Tables Work Together

PipelineEventLog is the **dashboard layer** — small, structured, one row per step. Query it to find the 10 slowest tables this week, which EventType is the bottleneck, whether throughput is degrading over time, or if a table's RowsProcessed dropped suddenly (a data quality signal).

PipelineLog is the **investigation layer** — many rows per step, detailed narrative. Once the event log tells you "ACCT SCD2_PROMOTION took 12 minutes on Tuesday's 2PM run," filter PipelineLog by that BatchId + TableName and see: ConnectorX partition count was 4, the DataFrame was 2.3M rows, memory peaked at 6GB, a warning fired about 14 columns requiring dtype casting, and the UPDATE batch hit a lock wait on Bronze.

Typical debugging workflow:
```sql
-- Step 1: Find the slow run
SELECT TableName, EventType, DurationMs, RowsProcessed, Status
FROM General.ops.PipelineEventLog
WHERE BatchId = 1042 AND DurationMs > 30000
ORDER BY DurationMs DESC;

-- Step 2: Dig into the details
SELECT CreatedAt, LogLevel, Module, FunctionName, Message, Metadata
FROM General.ops.PipelineLog
WHERE BatchId = 1042 AND TableName = 'ACCT'
  AND CreatedAt BETWEEN '2025-02-20 14:00:00' AND '2025-02-20 14:15:00'
ORDER BY CreatedAt;
```

Questions these tables answer together:
- "Which tables take the longest?" — PipelineEventLog, TABLE_TOTAL by DurationMs.
- "Is extraction or SCD2 the bottleneck for table X?" — PipelineEventLog, compare EXTRACT vs SCD2_PROMOTION DurationMs.
- "How many rows/second does BCP achieve for large loads?" — PipelineEventLog, RowsPerSecond on BCP_LOAD events.
- "Why did ACCT take 12 minutes today but 2 minutes yesterday?" — PipelineLog, compare Metadata and WARNING entries across BatchIds.
- "Did any step fail and need a retry?" — PipelineEventLog where Status = FAILED, then PipelineLog for StackTrace.
- "Are we seeing data quality drift?" — PipelineEventLog, trend RowsProcessed and RowsInserted/Updated/Deleted over time per table.

## Deployment Requirements
- **MALLOC_ARENA_MAX=2** must be set in the systemd unit file, shell wrapper, or `.bashrc` for the pipeline user BEFORE the Python process starts (W-4). `os.environ.setdefault()` in main_*.py only covers child processes — glibc arena configuration is locked at process start. Without this, Polars/Rust allocations can cause 10x memory bloat from glibc arena fragmentation (Polars issue #23128).
- **mssql-tools18**: Current minimum version is the installed default. When v18.6.1.1+ is available, upgrade to re-enable `-C 65001` for explicit UTF-8 codepage control (W-1).

## Gotchas
- B-1: _row_hash and UdmHash are VARCHAR(64) (full SHA-256 hex string). Previous BIGINT (64-bit truncation) was safe for per-PK CDC (~1.6×10⁻¹⁰ risk) but not for adjacent operations (reconciliation, future surrogate keys) where birthday-paradox collisions reach ~24% at 3B rows. Upgrade was defense-in-depth. Migration script: `migrations/b1_hash_varchar64.py`. First pipeline run after migration rehashes all rows (one-time CDC update wave).
- B-2: SCD2 UPDATE batch size must stay below 5,000 to prevent SQL Server lock escalation from row locks to table-level exclusive locks. Table-level exclusive locks override RCSI, blocking all readers. Controlled by `config.SCD2_UPDATE_BATCH_SIZE` (default 4,000).
- B-3: Schema evolution (column adds) changes all row hashes on the next run. `evolve_schema()` returns `SchemaEvolutionResult` so orchestrators can suppress E-12 false warnings during schema migration runs. Check `PipelineEventLog` metadata for `"schema_migration": true`.
- B-4: Orphaned Flag=0 rows (from crash between SCD2 INSERT and activation) are cleaned up at the start of each SCD2 run via `_cleanup_orphaned_inactive_rows()`. Safe to delete because they are invisible to downstream consumers.
- UInt64 from non-hash sources must be .reinterpret(signed=True) before writing — BCP/SQL Server cannot handle unsigned 64-bit
- String columns with embedded tabs/newlines corrupt BCP CSV when quote_style='never' — sanitize BEFORE write_csv
- ConnectorX returns Oracle DATE as Utf8 sometimes — auto-cast columns where DATE in col.upper()
- _scd2_key is IDENTITY in Bronze — must be excluded from INSERT DataFrames and BCP column lists
- _cdc_is_current and UdmActiveFlag are BIT in SQL Server — cast to Int8 before CSV write (not True/False)
- Polars write_csv with batch_size=4096 avoids memory spikes on large DataFrames
- The .env file lives at /debi/.env not project root
- Oracle views have no primary keys — schema/column_sync.py will attempt unique index discovery, but IsPrimaryKey in UdmTablesColumnsList may still need manual setup for views without unique indexes
- BCP CSV has no header — column mapping is POSITIONAL. `reorder_columns_for_bcp()` enforces deterministic order by reading INFORMATION_SCHEMA ordinal position before every BCP write (P0-1). Never use SELECT * in extraction queries.
- CDC/SCD2 staging tables read actual PK types from the target table via `get_column_types()` (P0-3). Never hardcode NVARCHAR(MAX) for PK columns in staging tables.
- NULL values in PK columns are filtered out before CDC comparison via `_filter_null_pks()` (P0-4). The count is tracked in PipelineEventLog metadata.
- Empty extraction guard (`_check_extraction_guard`) blocks CDC if row count drops >90% vs previous run (P1-1). Use `--force` to override for intentional reloads.
- `sp_getapplock` prevents concurrent pipeline runs on the same table (P1-2). If a run crashes, Session-owned locks auto-release on connection drop.
- Schema evolution runs every extraction (P0-2): new columns are ADDed, removed columns are WARNed (never dropped), type changes raise SchemaEvolutionError and skip the table.
- Large table windowed CDC scopes delete detection to the extraction window (P1-4). Rows outside the window are never marked as deleted.
- `schema/staging_cleanup.py` should run at pipeline start to drop orphaned `_staging_*` tables from crashes (P3-3).
- Weekly reconciliation (`cdc/reconciliation.py`) does full column-by-column comparison (not hash-based) to catch hash collisions and CDC logic bugs (P3-4).
- ConnectorX Oracle windowed extraction uses TRUNC() in WHERE clauses to prevent timezone boundary drift (P3-2). The oracledb path also uses TRUNC().
- oracledb extractor pre-queries for distinct dates to skip empty days (P2-2). ConnectorX large table path does not — empty days return quickly but incur connection overhead.
- ConnectorX partition skew is logged via `_log_partition_skew()` (P2-3). If the partition column has >10% NULLs, a WARNING suggests choosing a different column.
- V-4: After SCD2 promotion, `_check_duplicate_active_rows()` queries Bronze for PKs with >1 active row. Between a crash and recovery, downstream consumers using `WHERE UdmActiveFlag = 1` may see duplicates. Safe current-state access pattern: `ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC) WHERE rn = 1`.
- V-11: If polars-hash becomes incompatible with a future Polars version, `add_row_hash_fallback()` in `bcp_csv.py` provides a pure-Python hashlib SHA-256 fallback. To activate: replace `add_row_hash` with `add_row_hash_fallback` in `prepare_dataframe_for_bcp()`. Performance is slower (~5-10x) but functionally equivalent. Verify hash output matches on a test table before switching.
- W-2: NULL sentinel in hash input uses `\x1FNULL\x1F` (Unit Separator wrapping), NOT `\x00NULL\x00`. Null bytes risk C-string truncation in logging, debugging tools, and FFI boundaries. The `\x1F` sentinel is consistent with the column separator strategy (P0-6). Changing the sentinel changes hash output for every row containing NULLs — requires a one-time CDC update wave on first deployment.
- W-3: Float columns in hash input are normalized for IEEE 754 edge cases: ±0.0 → +0.0, NaN → `\x1FNaN\x1F`, Infinity → `\x1FINF\x1F`, -Infinity → `\x1F-INF\x1F`. Without this, these special values produce platform-dependent string representations causing phantom CDC updates.
- W-7: `validate_schema_before_concat()` in `bcp_csv.py` is called before every `pl.concat()` to catch silent type coercion (e.g., Int64 → Float64 in `diagonal_relaxed` mode). In extractors, schema mismatches are logged as warnings (non-blocking, since ConnectorX may legitimately return different dtypes). In CDC/SCD2, mismatches raise `SchemaValidationError`.
- W-8: `table_lock.py` uses Session-owned locks (`@LockOwner='Session'`) with `autocommit=True`. The RCSI race condition (sp_releaseapplock before COMMIT) does NOT apply to Session-scoped locks. If lock ownership is ever changed to Transaction-scoped, remove the explicit `sp_releaseapplock` call and let locks release at COMMIT time.
- W-10: `ensure_bronze_columnstore_index()` in `table_creator.py` is an opt-in migration for Bronze tables with 100M+ rows. Requires SQL Server 2022. Drops the clustered PK and recreates as nonclustered to make room for the ordered clustered columnstore index. Run during a maintenance window. Benchmark before and after.
- W-11: `reconcile_counts()` in `reconciliation.py` provides lightweight daily count reconciliation (source vs Stage vs Bronze). Designed to catch gross data loss within 24 hours rather than waiting for the weekly full-column reconciliation. Schedule as part of normal pipeline completion.
- W-12: `shrink_to_fit(in_place=True)` is called after large DataFrame operations (>100K rows) in CDC engine, large table extraction, and hash computation. Releases over-allocated memory buffers back to the allocator. Combines with W-4 (MALLOC_ARENA_MAX=2) to minimize memory bloat.
- W-13: `generate_bcp_format_file()` in `bcp_csv.py` produces XML .fmt files from INFORMATION_SCHEMA metadata. `bcp_load()` accepts an optional `format_file` parameter for explicit column mapping. The format file uses character mode with tab delimiters and LF terminators per the BCP CSV Contract. `reorder_columns_for_bcp()` remains as defense-in-depth validation even when format files are used.
- W-17: `General.ops.Quarantine` table stores records that fail schema contracts. `ensure_quarantine_table()`, `quarantine_record()`, and `quarantine_batch()` in `schema/evolution.py` provide the infrastructure. Currently hooked into schema type change errors. Low priority — the existing all-or-nothing table skip is safe, quarantine adds visibility into why records were rejected.
- E-1: Oracle empty string/NULL equivalence — `add_row_hash(source_is_oracle=True)` normalizes empty strings to NULL before hashing for Oracle sources. Oracle treats '' as NULL; SQL Server does not. Without this, every Oracle-sourced row with empty string fields generates phantom CDC updates indefinitely. Applied to the DataFrame itself (not just hash input) so BCP CSV output is also normalized. First deployment triggers a one-time CDC update wave for affected rows.
- E-2: SCD2 INSERT-first now uses a 3-step process: (1) INSERT new versions with UdmActiveFlag=0 for operation='U', (2) UPDATE to close old active versions, (3) UPDATE to activate new versions (`_activate_new_versions()`). This prevents conflicts with the filtered unique index (`ensure_bronze_unique_active_index`). New inserts (operation='I') still use UdmActiveFlag=1 directly. A crash after INSERT but before activation leaves rows with UdmActiveFlag=0, UdmEndDateTime IS NULL — detectable and recoverable on next run.
- E-3: BCP `-b` (batch size) flag is now controlled by `bcp_load(atomic=True/False)`. Bronze SCD2 loads default to atomic=True (no `-b` flag) — the entire INSERT is a single transaction. Stage/staging loads use atomic=False for performance since they are ephemeral. Without atomicity, a BCP failure partway through an SCD2 INSERT creates inconsistent Bronze state (some PKs with two active versions, others with none).
- E-4: All string columns are RTRIM'd before hashing to prevent phantom hash differences from trailing space divergence (Oracle CHAR padding, SQL Server ANSI padding rules). Applied after NFC normalization (V-2) and Oracle empty string normalization (E-1). First deployment triggers a one-time CDC update wave for rows with trailing spaces.
- E-5: `_execute_bronze_updates()` deduplicates `pks_to_close` via `.unique(subset=pk_columns)` before loading into the staging table. Also applied in `run_scd2()` close concat. Defense-in-depth against UPDATE FROM JOIN nondeterminism with duplicate staging keys. Currently safe (SET values are constants) but prevents issues if the pattern is extended.
- E-8: Under RCSI, the SCD2 INSERT and UPDATE are separate statements each with their own snapshot. Between INSERT commit and UPDATE commit, concurrent readers may see two active versions for updated PKs (transient window, typically milliseconds). Downstream consumers should use `ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC) WHERE rn = 1` instead of `WHERE UdmActiveFlag = 1` alone. The V-4 diagnostic and P1-16 dedup recovery handle the crash case.
- E-9: `verify_rcsi_enabled()` in `connections.py` runs at pipeline startup to check `READ_COMMITTED_SNAPSHOT` on the Bronze database. B-2 reduced batch size from 500K to 4K to prevent lock escalation even with RCSI — table-level exclusive locks override RCSI. The check is non-blocking — logs WARNING and continues if RCSI is disabled.
- E-10: `_check_log_space()` in `scd2/engine.py` runs before large UPDATE operations (>1M rows) to verify sufficient transaction log space. UPDATEs are always fully logged (~400 bytes × 2 per row for before/after images). A 5M-row UPDATE may require 5-20 GB of log space. Warns if available space is <1.5× estimated need. Ensure frequent log backups (15-30 min) during pipeline runs.
- E-11: `validate_source_schema()` in `schema/evolution.py` compares extracted DataFrame columns against expected columns from UdmTablesColumnsList. Missing columns (possible rename/drop) are ERROR-level and skip the table. Unexpected columns (new in source) are WARNING-level and allowed through — schema evolution handles the ADD. Only runs when UdmTablesColumnsList has been populated (not on first run).
- E-12: CDC update ratio is tracked in `CDC_PROMOTION` event metadata as `update_ratio`. A ratio >50% with >1000 updates triggers a WARNING for systematic hash mismatch (encoding change, schema drift, normalization bug). Query `PipelineEventLog WHERE EventType='CDC_PROMOTION'` and parse metadata JSON to trend update ratios over time.
- E-13: `check_version_velocity()` in `cdc/reconciliation.py` queries avg/max versions per PK and flags PKs with >10 versions. High version velocity indicates either genuine high-change-rate data or phantom version creation. Schedule as part of periodic reconciliation.
- E-14: Active-to-total ratio is tracked in `SCD2_PROMOTION` event metadata as `active_ratio`. A ratio <1% triggers a WARNING for possible mass incorrect closures. Trend this metric over time — gradual decline is expected as history accumulates.
- E-15: `_log_data_freshness()` checks max `UdmEffectiveDateTime` in Bronze after each SCD2 run. Warns if data is >48 hours stale, indicating silent pipeline failures where runs complete without processing new data.
- E-16: `detect_source_type_drift()` in `schema/column_sync.py` compares source column metadata (type, precision, scale) against Stage INFORMATION_SCHEMA. Detects precision changes (e.g., NUMBER(10,2) → NUMBER(15,4)) that could cause phantom hash mismatches. Call periodically or as part of reconciliation.
- E-17: `reconcile_aggregates()` in `cdc/reconciliation.py` compares SUM/COUNT/MIN/MAX of numeric columns between source and Bronze active rows. Catches value-level corruption that count validation misses. Schedule daily for high-value tables.
- E-18: Resurrected PKs (previously deleted, now reappearing in source) get `UdmScd2Operation='R'` in Bronze for audit trail. The version chain is: active ('I') → closed → deleted ('D') → closed → reactivated ('R'). Both `run_scd2()` and `run_scd2_targeted()` detect resurrections and build inserts with operation='R'. `_activate_new_versions()` targets both 'U' and 'R' operations.
- E-19: The `\x1F` (Unit Separator) between columns in hash concatenation prevents cross-column collisions. Without it, `("AB", "CD")` and `("A", "BCD")` produce the same hash. Documented in `add_row_hash()` and `add_row_hash_fallback()`.
- E-20: Categorical columns in Polars use physical integer encoding internally. polars-hash hashes the physical integer, not the logical string value (Polars Issue #21533). `add_row_hash()` and `add_row_hash_fallback()` detect Categorical columns and cast to Utf8 before hashing.
- E-21: ConnectorX converts Oracle NUMBER to Python float64, which has ~15 significant digits (vs Oracle's 38). Precision is already lost before pipeline processing for high-precision Oracle NUMBER columns. For critical columns with >15 digits, cast to VARCHAR2 in the extraction SQL.
- B-6: `sanitize_strings()` strips extended Unicode line-break characters (`\x0B`, `\x0C`, `\x85`, `\u2028`, `\u2029`) in addition to `\t`, `\n`, `\r`, `\x00`. These rare characters corrupt BCP CSV row boundaries. More likely in internationalized data, CMS content, or legacy system migrations.
- B-7: `cx_read_sql_safe()` in `extract/__init__.py` wraps all ConnectorX calls with Rust panic recovery (catches `BaseException`, not just `Exception`) and exponential backoff retry (3 attempts, 2s base delay). Non-retryable errors (syntax, permissions) fail fast. All extractors route through this wrapper.
- B-8: `_check_rss_memory()` in both `main_small_tables.py` and `main_large_tables.py` monitors RSS between table iterations in sequential mode. WARNING at 85% of `config.MAX_RSS_GB` (default 48), ERROR at limit. Combine with `MALLOC_ARENA_MAX=2` (W-4) for best results. psutil is optional — silently skipped if not installed.
- B-9: Freshness alerting uses two tiers: WARNING at 36 hours (1.5× expected refresh interval), ERROR at 48 hours (2× — two missed cycles). Extraction guard baselines use day-of-week aware queries (last 30 days, same weekday) to reduce false positives on weekends/holidays; falls back to any-day median if insufficient same-day data.
- B-10: `detect_distribution_shift()` in `reconciliation.py` compares numeric column means against a stored baseline using z-score analysis. Alert when shift exceeds 2σ. Stores baselines as `DISTRIBUTION_CHECK` events in PipelineEventLog. Schedule weekly — not every run.
- B-11: `reconcile_transformation_boundary()` in `reconciliation.py` validates row counts, key existence, and NULL rate comparisons between adjacent medallion layers. Implements circuit-breaker pattern — callers can block downstream processing on failure.
- B-12: `check_referential_integrity()` in `reconciliation.py` validates FK relationships across SCD2 tables. Supports both non-temporal (active flag) and temporal (BETWEEN effective/end dates) lookup modes. Detects orphaned FKs and ambiguous FKs (resolving to multiple dimension rows).
- OBS-1: The `BCP_LOAD` event type was removed from `small_tables.py` — it wrapped an empty block. BCP timing is captured within `CDC_PROMOTION` (staging table loads) and `SCD2_PROMOTION` (Bronze INSERT/UPDATE loads). Large tables never had a standalone BCP_LOAD event.
- OBS-2: Large table per-day events (EXTRACT, CDC_PROMOTION, SCD2_PROMOTION, CSV_CLEANUP) set `EventDetail = target_date` for per-day diagnostic filtering. Query example: `WHERE TableName = 'ACCT' AND EventDetail = '2025-02-15'`.
- OBS-3: Lock-skipped tables write a `TABLE_TOTAL` event with `Status = 'SKIPPED'` and `EventDetail = 'Lock held by another run'`. SKIPPED is a valid PipelineEventLog status alongside SUCCESS and FAILED. Monitor lock contention: `WHERE Status = 'SKIPPED'`.
- OBS-4: `SqlServerLogHandler` buffer reduced from 50 to 10 entries to narrow crash-loss window. WARNING+ log entries flush immediately regardless of buffer state. Flush failures print to stderr instead of being silently swallowed.
- OBS-5: `PipelineEventTracker._write_event()` and `SqlServerLogHandler._flush_buffer()` both call explicit `conn.commit()` after writes. Do not remove these — they protect against future autocommit configuration changes silently breaking observability.
- OBS-6: `General.ops.ReconciliationLog` stores reconciliation results for historical trending. Created by `ensure_reconciliation_log_table()` (idempotent). All public reconciliation functions (`reconcile_table`, `reconcile_counts`, `reconcile_active_pks`, `reconcile_bronze`, `reconcile_aggregates`) persist results via `_persist_reconciliation_result()`. CheckType discriminator identifies the reconciliation type.
- OBS-7: `_log_active_ratio()` in both orchestrators uses `json.loads()`/`json.dumps()` merge pattern for SCD2 event metadata. Any future SCD2 metadata extensions must follow this pattern — never overwrite `scd2_event.metadata` directly.
- B-13: Six Oracle→SQL Server type conversion pitfalls: (1) Oracle DATE includes time — `fix_oracle_date_columns()` upcasts `pl.Date` to `pl.Datetime` to prevent truncation. (2) Oracle NUMBER without precision can have >8 decimal places — document and monitor. (3) Oracle FLOAT(126) loses precision vs SQL Server FLOAT(53) — ~15 significant digits max. (4) Oracle RAW(16) GUID uses big-endian vs SQL Server mixed-endian — requires byte reordering if used as join keys. (5) Oracle BLOB/CLOB 4GB vs SQL Server 2GB limit — silent truncation possible. (6) Oracle VARCHAR2 BYTE vs SQL Server VARCHAR character semantics differ for multi-byte.
- B-14: The INSERT-first SCD2 pattern creates a transient zero-active-row window between closing old versions and activating new versions. Under RCSI, new readers during this window see zero active rows for affected PKs. Documented in `scd2/engine.py` module docstring. Defensive query pattern: `ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC) WHERE rn = 1`.

## Do NOT
- Do NOT truncate Bronze tables — SCD2 is append-only by design
- Do NOT use write_csv without batch_size=4096 on large DataFrames
- Do NOT add quoting to BCP CSV output — quote_style must always be 'never'
- Do NOT write True/False for BIT columns — must be 0/1 (Int8)
- Do NOT include _scd2_key (IDENTITY) in INSERT DataFrames or BCP column lists
- Do NOT assume source column order is stable — `reorder_columns_for_bcp()` enforces deterministic order, never bypass it
- Do NOT allow CDC to run if extraction returned 0 rows — the empty extraction guard (`_check_extraction_guard`) handles this; do not remove or weaken it
- Do NOT run full in-memory CDC/SCD2 comparison on large tables (3B+ rows) — use `run_cdc_windowed()` and `run_scd2_targeted()` which are date-partitioned
- Do NOT hardcode NVARCHAR(MAX) for PK columns in staging tables — always use `get_column_types()` from schema_utils.py
- Do NOT run overlapping pipeline instances on the same table — `orchestration/table_lock.py` enforces this via sp_getapplock; do not bypass it
- Do NOT use native Polars `hash_rows()` for row hashing — it is non-deterministic across Python sessions. Use the polars-hash plugin via `add_row_hash()` in bcp_csv.py
- Do NOT drop columns during schema evolution — `schema/evolution.py` logs WARNINGs for removed columns but never drops them. Data preservation is mandatory.
- Do NOT skip `_filter_null_pks()` before CDC comparison — NULL PKs cause duplicate inserts every run due to Polars NULL != NULL anti-join semantics
- Do NOT use `add_row_hash_fallback()` in production without first verifying it produces identical hashes to `add_row_hash()` on a test table — the fallback exists for polars-hash dependency failure scenarios only (V-11)
- Do NOT truncate SHA-256 hash output to Int64 — the full 64-char hex string must be stored as VARCHAR(64). Per-PK CDC comparison was safe at 64-bit (~1.6×10⁻¹⁰ risk — birthday paradox doesn't apply to per-key change detection), but full SHA-256 eliminates risk for adjacent operations (reconciliation, future surrogate keys, deduplication) where birthday-paradox collisions are real at 3B rows (~24%). See project research doc for full analysis (B-1)
- Do NOT set SCD2_UPDATE_BATCH_SIZE above 5,000 — SQL Server escalates to table-level exclusive locks at ~5,000 locks, overriding RCSI and blocking all concurrent readers (B-2)
- Do NOT remove `_cleanup_orphaned_inactive_rows()` from SCD2 entry points — orphaned Flag=0 rows from crash recovery accumulate silently and are never activated by normal flow (B-4)
- Do NOT query Bronze with only `WHERE UdmActiveFlag = 1` without dedup protection — use `ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC) WHERE rn = 1` to handle duplicate active rows from crash recovery windows (V-4)
- Do NOT use `\x00` (null byte) in hash sentinels — use `\x1F` (Unit Separator) instead. Null bytes cause C-string truncation in logging, FFI, and serialization layers, silently corrupting hash inputs (W-2)
- Do NOT use `pl.concat()` with `diagonal_relaxed` or `vertical_relaxed` without calling `validate_schema_before_concat()` first — silent type coercion (e.g., Int64 → Float64) can corrupt precision-sensitive values (W-7)
- Do NOT hash Categorical columns directly via polars-hash — it hashes the physical integer encoding, not the logical string value. Cast to Utf8 first. `add_row_hash()` handles this automatically (E-20)
- Do NOT change `@LockOwner` in table_lock.py from 'Session' to 'Transaction' without removing the explicit `sp_releaseapplock` call — Transaction-scoped locks with explicit release before COMMIT create a race condition under RCSI (W-8)
- Do NOT use `bcp_load(atomic=False)` for Bronze SCD2 loads — partial loads break SCD2 atomicity. Only Stage and ephemeral staging table loads should use `atomic=False` (E-3)
- Do NOT set `UdmActiveFlag=1` directly in `_build_scd2_insert()` for operation='U' — new versions must be inserted with UdmActiveFlag=0 and activated via `_activate_new_versions()` after closing old versions. Otherwise the filtered unique index rejects the INSERT (E-2)
- (Additional rules to be added as we test and discover anti-patterns)

## Autonomous Rules
- Proceed without asking: refactoring shared utilities, adding type hints, fixing lint
- STOP and ask: changing BCP CSV format, modifying CDC/SCD2 comparison logic, altering table naming conventions, changing database/schema names, changing hash algorithm or polars-hash configuration, weakening or removing any edge case safeguard (P0-1 through P3-4)
- If a module fails to import: check sys.path.insert — project uses parent-directory imports
- Always verify BCP CSV format compatibility after ANY change to write functions
- Test with --table <single_table> --source DNA before running full pipeline

## Error Recovery
- BCP row count mismatch -> investigate string sanitization, check for new control characters in source data
- BCP column count mismatch -> source schema changed (new/dropped column). `schema/evolution.py` handles new columns automatically; removed columns log WARNING. Check PipelineLog for SchemaEvolutionError if a type changed.
- ConnectorX connection failure -> verify .env credentials, check Oracle listener, try oracledb fallback
- CDC table auto-create failure -> check schema exists, verify _polars_dtype_to_sql mapping covers the dtype
- SCD2 staging table cleanup -> _execute_bronze_updates drops staging table in finally block; run `schema/staging_cleanup.py` to clean orphans from crashes
- Sudden spike in CDC "updates" for all rows -> likely a Polars upgrade invalidating polars-hash output (check changelog), or column order shifted (check `reorder_columns_for_bcp` warnings in PipelineLog), or schema evolution added a column (check PipelineEventLog metadata for `"schema_migration": true` — B-3)
- Orphaned Flag=0 rows in Bronze (UdmActiveFlag=0, UdmEndDateTime IS NULL) -> `_cleanup_orphaned_inactive_rows()` handles this automatically at the start of each SCD2 run (B-4). If manual cleanup is needed: `DELETE FROM Bronze WHERE UdmActiveFlag = 0 AND UdmEndDateTime IS NULL AND UdmScd2Operation IN ('U','R')`
- Hash migration from BIGINT to VARCHAR(64) -> run `python3 migrations/b1_hash_varchar64.py --dry-run` first, then without --dry-run during maintenance window. First pipeline run after migration rehashes all rows. The migration was driven by defense-in-depth for adjacent operations (reconciliation, future joins) where birthday-paradox risk is real at 3B rows, not by per-PK CDC risk which was safe at 64-bit (B-1)
- Extraction returns 0 rows -> extraction guard blocks CDC automatically. Check Oracle listener, network, permissions. Use `--force` only after confirming the empty extraction is intentional.
- Duplicate rows appearing in Bronze -> check PipelineLog for table lock acquisition failures (another run may have bypassed locking), or check for NULL PKs in PipelineEventLog metadata (null_pk_rows field)
- Large table OOM -> extraction window too wide for available memory. Reduce LookbackDays or verify the per-day processing path is being used (orchestration/large_tables.py processes one day at a time)
- Schema evolution error skipping table -> a source column changed type. Requires manual resolution: verify the type change is intentional, ALTER the target column or re-create the table, then re-run with `--force`
- Table lock not acquired -> another pipeline run is processing the same table. Wait for it to complete, or check for stale sessions if the other run crashed (Session-owned sp_getapplock locks auto-release on disconnect)
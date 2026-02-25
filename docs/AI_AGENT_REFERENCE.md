# UDM Data Pipeline — AI Agent Reference

> **Purpose**: This document is written for AI agents (LLMs, coding assistants, automation tools) that need to understand, modify, debug, or extend this data pipeline. It encodes the architectural decisions, invariants, and failure modes that are not obvious from reading the code alone. Treat every section marked with ⛔ as a hard constraint — violating it causes data corruption or silent failures.

---

## 1. What This Pipeline Does (30-Second Summary)

This is an ETL pipeline that extracts data from **Oracle** (source system: DNA) and **SQL Server** (source systems: CCM, EPICOR) into a SQL Server **medallion architecture**:

```
Source (Oracle / SQL Server)
  → UDM_Stage  (CDC layer — tracks inserts, updates, deletes via hash comparison)
  → UDM_Bronze (SCD2 layer — full version history, append-only)
```

It is written in Python 3.12, uses **Polars** for in-memory data processing, **ConnectorX** for high-speed extraction, and **BCP** (bulk copy program) for loading data into SQL Server. It runs on a Linux RedHat server.

The pipeline is **metadata-driven**: the table `General.dbo.UdmTablesList` controls which tables are extracted, how they are routed, and what naming conventions apply. Column metadata lives in `General.dbo.UdmTablesColumnsList`.

---

## 2. Architecture Overview

### 2.1 Two Pipeline Modes

| Mode | Entry Point | Trigger | Memory Model |
|------|-------------|---------|--------------|
| **Small Tables** | `main_small_tables.py` | `SourceAggregateColumnName IS NULL` | Full table extracted into memory each run |
| **Large Tables** | `main_large_tables.py` | `SourceAggregateColumnName IS NOT NULL` | Per-day windowed extraction, one day at a time |

A table is "small" or "large" based on whether `SourceAggregateColumnName` (a date column) is populated in `UdmTablesList`. There is no row-count threshold.

### 2.2 Per-Table Processing Flow

**Small Tables** (full extract each run):
1. Extract full table via ConnectorX or oracledb → Polars DataFrame
2. Add `_row_hash` (SHA-256 via polars-hash) + `_extracted_at`
3. Write BCP-format CSV (tab-delimited, no header, no quoting)
4. Ensure Stage/Bronze tables exist (auto-create from DataFrame dtypes if missing)
5. Schema evolution: detect new/removed/changed columns
6. Column sync: auto-populate `UdmTablesColumnsList` + discover PKs from source
7. Empty extraction guard: skip CDC if row count drops >90% vs baseline
8. Table lock: `sp_getapplock` prevents concurrent runs on same table
9. **CDC Promotion**: Polars in-memory hash comparison (anti-join inserts, hash-compare updates, reverse anti-join deletes) → write changes to Stage via BCP
10. **SCD2 Promotion**: Compare CDC current rows vs Bronze active rows → UPDATE to close old versions, INSERT new versions via BCP

**Large Tables** (per-day windowed):
- Same as above but extraction is scoped to a single calendar day via `SourceAggregateColumnName`
- Each day is checkpointed to `ops.PipelineExtractionState`
- On failure, the next run resumes from the last successful date
- Delete detection is scoped to the extraction window only — rows outside the window are never marked deleted

### 2.3 Extraction Routing

The router (`extract/router.py`) is the single source of truth for which extractor handles each table:

| Source | SourceIndexHint | PartitionOn | Extractor |
|--------|----------------|-------------|-----------|
| Oracle | Populated | (ignored) | `oracledb` with INDEX hints |
| Oracle | NULL | Populated | ConnectorX Oracle with `partition_on` |
| Oracle | NULL | NULL | ConnectorX Oracle bulk (FULL scan) |
| SQL Server | (ignored) | Populated | ConnectorX SQL Server with `partition_on` |
| SQL Server | (ignored) | NULL | ConnectorX SQL Server bulk |

### 2.4 Database Topology

| Database | Purpose | Schema Pattern |
|----------|---------|----------------|
| `General` | Metadata + observability | `dbo.UdmTablesList`, `dbo.UdmTablesColumnsList`, `ops.*` |
| `UDM_Stage` | CDC layer | `{SourceName}.{TableName}_cdc` |
| `UDM_Bronze` | SCD2 layer | `{SourceName}.{TableName}_scd2_python` |

The schema within UDM_Stage and UDM_Bronze is the **SourceName** (e.g., `DNA`, `CCM`, `EPICOR`), not the source system's schema.

### 2.5 Table Naming Conventions

Naming is driven by `UdmTablesList`. Custom names override the default when `StageTableName` or `BronzeTableName` is populated.

Example for table `ACCT` from source `DNA` (source schema: `osibank`):

| Layer | Default Name | Custom Name Override |
|-------|-------------|---------------------|
| Source | `DNA.osibank.ACCT` | n/a |
| Stage | `UDM_Stage.DNA.ACCT_cdc` | `UDM_Stage.DNA.{StageTableName}_cdc` |
| Bronze | `UDM_Bronze.DNA.ACCT_scd2_python` | `UDM_Bronze.DNA.{BronzeTableName}_scd2_python` |

---

## 3. Data Contracts and Invariants

### 3.1 ⛔ BCP CSV Contract (Single Source of Truth)

Every CSV file written by this pipeline MUST conform to this exact specification. BCP loads will fail or silently corrupt data on any deviation.

```
Delimiter:           tab (\t)
Row terminator:      LF only (-r 0x0A)
Header:              none
Quoting:             quote_style='never'
NULL representation: empty string
Datetime format:     '%Y-%m-%d %H:%M:%S.%3f'
BIT columns:         Int8 (0/1) — NEVER True/False
Hash columns:        Full SHA-256 hex string, VARCHAR(64)
UInt64 (non-hash):   .reinterpret(signed=True) → Int64
String sanitization: replace \t \n \r \x00 with empty string BEFORE write_csv
Batch size:          write_csv(batch_size=4096) to avoid memory spikes
```

**Why no quoting?** BCP character mode with tab delimiters does not support quoted fields. Instead, all control characters that could break field/row boundaries are stripped from string values before writing. This is handled by `sanitize_strings()` in `data_load/sanitize.py`.

**Why no header?** BCP column mapping is **positional**. `reorder_columns_for_bcp()` reads `INFORMATION_SCHEMA` ordinal positions before every write to enforce deterministic column order. Never bypass this function.

### 3.2 ⛔ Row Hashing Contract

Row hashing is the foundation of CDC (change detection). The pipeline uses **polars-hash** (a Polars plugin backed by Rust) for SHA-256 hashing.

Critical rules:
- **polars-hash is pinned to exact version** (`==0.4.5`). A version change can change hash output, causing every row to appear "changed" — a mass-update cascade.
- **Native Polars `hash_rows()` is FORBIDDEN** — it is non-deterministic across Python sessions (Polars GitHub issues #3966, #7758).
- Hash output is the full 64-character SHA-256 hex string stored as `VARCHAR(64)`. Never truncate.
- Column separator in hash input: `\x1F` (Unit Separator) — prevents cross-column collisions where `("AB", "CD")` and `("A", "BCD")` would otherwise hash identically.
- NULL sentinel: `\x1FNULL\x1F` — never use `\x00` (null byte causes C-string truncation in logging/FFI).
- Float normalization: ±0.0 → +0.0, NaN → `\x1FNaN\x1F`, Infinity → `\x1FINF\x1F`, -Infinity → `\x1F-INF\x1F`.
- Categorical columns: cast to `Utf8` before hashing (polars-hash hashes the physical integer encoding, not the logical string value).
- Oracle empty string → NULL normalization: applied before hashing for Oracle sources (Oracle treats `''` as NULL; SQL Server does not).
- All strings: NFC-normalized and RTRIM'd before hashing.

### 3.3 ⛔ CDC (Change Data Capture) Invariants

CDC uses Polars in-memory hash comparison. The CDC engine (`cdc/engine.py`) detects inserts, updates, and deletes:
- **Inserts**: Anti-join on PK columns (rows in source but not in Stage)
- **Updates**: Inner join on PK columns + hash comparison (rows in both, but hash differs)
- **Deletes**: Reverse anti-join (rows in Stage but not in source)

CDC columns added to Stage:
- `_cdc_operation`: `I` (insert), `U` (update), `D` (delete)
- `_cdc_valid_from` / `_cdc_valid_to`: temporal validity
- `_cdc_is_current`: BIT flag (1 = current version)
- `_cdc_batch_id`: BatchId for the run that detected this change

⛔ **NULL PK filter**: `_filter_null_pks()` MUST run before CDC comparison. Polars NULL != NULL in anti-joins, meaning NULL-PK rows would appear as new inserts every single run. The count of filtered NULL-PK rows is tracked in `PipelineEventLog` metadata.

⛔ **Empty extraction guard**: If extraction returns 0 rows or drops >90% vs the historical median baseline, CDC is blocked. This prevents accidental mass-delete detection from transient source issues. Override with `--force` only after confirming the empty extraction is intentional.

⛔ **Large table delete scoping**: For windowed (large table) CDC, delete detection is scoped to the extraction window only. Rows outside the window are never marked as deleted. Use periodic full reconciliation to catch real deletes.

### 3.4 ⛔ SCD2 (Slowly Changing Dimension Type 2) Invariants

SCD2 maintains full version history in Bronze. The SCD2 engine (`scd2/engine.py`) implements a 3-step INSERT-first pattern:

1. **INSERT** new versions with `UdmActiveFlag=0` (for updates/resurrections)
2. **UPDATE** to close old active versions (`UdmActiveFlag=0`, set `UdmEndDateTime`)
3. **UPDATE** to activate new versions (`UdmActiveFlag=1`) via `_activate_new_versions()`

New inserts (operation `I`) use `UdmActiveFlag=1` directly.

SCD2 columns added to Bronze:
- `UdmHash`: SHA-256 row hash (VARCHAR(64))
- `UdmEffectiveDateTime`: When this version became active
- `UdmEndDateTime`: When this version was superseded (NULL if current)
- `UdmActiveFlag`: BIT (1 = current active version)
- `UdmScd2Operation`: `I` (insert), `U` (update), `D` (delete), `R` (resurrection)
- `_scd2_key`: IDENTITY column — **never include in INSERT DataFrames or BCP column lists**

⛔ **Bronze is append-only** — NEVER truncate Bronze tables. SCD2 only closes old versions and inserts new ones.

⛔ **UPDATE batch size must stay below 5,000** (`SCD2_UPDATE_BATCH_SIZE` default: 4,000). SQL Server escalates to table-level exclusive locks at ~5,000 locks, overriding RCSI and blocking all concurrent readers.

⛔ **BCP atomicity for Bronze**: Bronze SCD2 loads MUST use `atomic=True` (single transaction). Only Stage/staging table loads may use `atomic=False`.

⛔ **Transient duplicate active rows**: Between INSERT commit and UPDATE commit, concurrent readers may see two active versions for updated PKs. Safe query pattern:
```sql
ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC)
WHERE rn = 1
```
Do NOT rely solely on `WHERE UdmActiveFlag = 1` without this dedup protection.

⛔ **Orphaned inactive rows**: A crash between INSERT (step 1) and activation (step 3) leaves rows with `UdmActiveFlag=0, UdmEndDateTime IS NULL`. These are automatically cleaned up by `_cleanup_orphaned_inactive_rows()` at the start of each SCD2 run. Do not remove this cleanup.

### 3.5 ⛔ Schema Evolution Rules

`schema/evolution.py` runs every extraction:
- **New columns** in source: `ALTER TABLE ADD` to Stage and Bronze automatically.
- **Removed columns** from source: `WARNING` logged, column is **never dropped**. Data preservation is mandatory.
- **Type changes**: `SchemaEvolutionError` raised, table is **skipped**. Requires manual resolution.

After a schema evolution adds a column, **all row hashes change on the next run** (the new column participates in hashing). This triggers a one-time mass-update wave. The `SchemaEvolutionResult` includes a `schema_migration` flag so monitoring can suppress false E-12 (high update ratio) warnings.

---

## 4. Source System Quirks and Cross-Platform Pitfalls

### 4.1 Oracle-Specific

| Issue | Mitigation | Code Reference |
|-------|-----------|----------------|
| Oracle `DATE` includes time component | `fix_oracle_date_columns()` upcasts `pl.Date` → `pl.Datetime` | `data_load/sanitize.py` |
| Oracle `''` equals `NULL` | `add_row_hash(source_is_oracle=True)` normalizes empty strings to NULL before hashing | `data_load/row_hash.py` (E-1) |
| Oracle CHAR columns have trailing space padding | RTRIM applied before hashing | `data_load/row_hash.py` (E-4) |
| Oracle views have no primary keys | `column_sync.py` attempts unique index discovery; may need manual `IsPrimaryKey` setup | `schema/column_sync.py` |
| ConnectorX returns Oracle DATE as Utf8 sometimes | Auto-cast columns where `DATE` appears in column name | `data_load/sanitize.py` |
| Oracle NUMBER without precision → float64 (loses digits >15) | Document and monitor; cast to VARCHAR2 in extraction SQL for critical columns | (E-21) |
| Oracle FLOAT(126) → SQL Server FLOAT(53): ~15 significant digits max | Precision loss happens before pipeline processing | (B-13) |
| Oracle timezone boundary drift | TRUNC() used in WHERE clauses for both oracledb and ConnectorX paths | (P3-2) |

### 4.2 SQL Server Target-Specific

| Issue | Mitigation | Code Reference |
|-------|-----------|----------------|
| BIT columns accept only 0/1 | Cast to Int8 before CSV write; never write True/False | BCP CSV Contract |
| `_scd2_key` is IDENTITY | Exclude from all INSERT DataFrames and BCP column lists | SCD2 engine |
| Lock escalation at ~5,000 row locks | `SCD2_UPDATE_BATCH_SIZE` defaults to 4,000 | `config.py` (B-2) |
| RCSI can be overridden by table-level exclusive locks | Batch size control is the primary defense | (E-9) |
| Transaction log space for large UPDATEs | `_check_log_space()` warns if space < 1.5× estimate | `scd2/engine.py` (E-10) |
| `BULK_LOGGED` recovery model during loads | `_bulk_load_recovery_context()` sets and restores recovery model | `data_load/bcp_loader.py` |

### 4.3 String Sanitization

`sanitize_strings()` strips these characters before BCP CSV write:
- `\t` (tab — field delimiter)
- `\n` (newline — potential row terminator confusion)
- `\r` (carriage return)
- `\x00` (null byte — C-string truncation)
- `\x0B`, `\x0C`, `\x85`, `\u2028`, `\u2029` (extended Unicode line-break characters that corrupt row boundaries)

---

## 5. Metadata Tables and Configuration

### 5.1 `General.dbo.UdmTablesList` — Pipeline Configuration

This is the central configuration table. Every field influences pipeline behavior:

| Column | Effect |
|--------|--------|
| `SourceObjectName` | Table name in source system |
| `SourceServer` | Linked server or server of source data |
| `SourceDatabase` | Database of source data |
| `SourceSchemaName` | Schema in source system |
| `SourceName` | Source system identifier (`DNA`, `CCM`, `EPICOR`) — becomes the schema in UDM_Stage/Bronze |
| `StageTableName` | Custom Stage table name (overrides `SourceObjectName`) |
| `BronzeTableName` | Custom Bronze table name (overrides `SourceObjectName`) |
| `SourceAggregateColumnName` | Date column for large table windowed extraction. NULL = small table |
| `SourceAggregateColumnType` | Data type of the date column |
| `SourceIndexHint` | Oracle INDEX hint — routes to oracledb extractor |
| `PartitionOn` | ConnectorX `partition_on` column for parallel extraction |
| `FirstLoadDate` | Earliest date boundary for initial large table loads |
| `LookbackDays` | Rolling window in days for large table extraction |
| `StageLoadTool` | Must be `'Python'` for this pipeline to process the table |

### 5.2 `General.dbo.UdmTablesColumnsList` — Column Metadata

| Column | Effect |
|--------|--------|
| `SourceName` | Source system identifier |
| `TableName` | Table name in UDM |
| `ColumnName` | Column name |
| `OrdinalPosition` | Column position (drives BCP positional mapping) |
| `IsPrimaryKey` | BIT — drives CDC join columns and SCD2 business key |
| `Layer` | `Stage` or `Bronze` |
| `IsIndex` | BIT — whether UDM table should have an index |
| `IndexName` | Index name |
| `IndexType` | SQL Server index type |

⛔ **Oracle views**: PK discovery may fail. `IsPrimaryKey` must be manually set for Oracle views without unique indexes.

### 5.3 Observability Tables (in `General.ops`)

| Table | Purpose | Grain |
|-------|---------|-------|
| `PipelineBatchSequence` | Run ID generator (IDENTITY) | One row per pipeline run |
| `PipelineEventLog` | Dashboard layer — performance + health | One row per step per table per run |
| `PipelineLog` | Investigation layer — detailed diagnostics | Many rows per step (Python logging sink) |
| `PipelineExtractionState` | Large table checkpoint/resume | One row per table per date |
| `Quarantine` | Schema contract failure isolation | One row per rejected record |
| `ReconciliationLog` | Reconciliation result persistence | One row per reconciliation check |

**Join key** between EventLog and Log: `BatchId + TableName + SourceName`.

**Key EventLog fields for monitoring**:
- `EventType`: `EXTRACT`, `CDC_PROMOTION`, `SCD2_PROMOTION`, `TABLE_TOTAL`, `CSV_CLEANUP`
- `Status`: `SUCCESS`, `FAILED`, `SKIPPED` (lock held by another run)
- `Metadata` (JSON): Contains `update_ratio`, `active_ratio`, `null_pk_rows`, `schema_migration`, etc.
- `DurationMs`: Primary metric for bottleneck analysis

---

## 6. Concurrency and Safety

### 6.1 Table Locking

`sp_getapplock` (Session-scoped) prevents concurrent pipeline runs on the same table. Implemented in `orchestration/table_lock.py`.

- Lock ownership is `Session` with `autocommit=True`
- If a run crashes, Session-owned locks auto-release on connection drop
- Lock-skipped tables write a `TABLE_TOTAL` event with `Status='SKIPPED'`
- ⛔ Do NOT change `@LockOwner` from `'Session'` to `'Transaction'` without removing the explicit `sp_releaseapplock` call — this creates a race condition under RCSI

### 6.2 CSV Concurrency

`CSV_OUTPUT_DIR` is safe for concurrent `--workers` only because each table's extract→CDC→SCD2→cleanup pipeline is sequential within a single worker. `cleanup_csvs()` uses a trailing underscore in the glob pattern (`{source}_{table}_*.csv`) to prevent cross-table filename collisions.

⛔ Do NOT restructure the pipeline to allow overlapping steps within a worker without adding per-table subdirectories for CSV isolation.

### 6.3 Connection Pooling

`connections.py` implements a per-database connection pool. `cursor_for()` reuses pooled connections. On `pyodbc.OperationalError`, stale connections are evicted. Lock-holding connections bypass the pool via `_get_resilient_lock_connection`.

---

## 7. Memory Management

This pipeline processes tables that can have 3+ billion rows. Memory management is critical.

| Mechanism | Purpose | Reference |
|-----------|---------|-----------|
| `MALLOC_ARENA_MAX=2` | Prevents glibc arena fragmentation (10x memory bloat) | Must be set BEFORE Python starts (W-4) |
| `write_csv(batch_size=4096)` | Prevents memory spikes during CSV serialization | BCP CSV Contract |
| `shrink_to_fit(in_place=True)` | Releases over-allocated Polars buffers after large ops | (W-12) |
| `gc.collect()` after `del df` | Forces GC at extraction→SCD2 transition for large tables | (Item 19) |
| Per-day processing for large tables | Bounds memory to single-day row count | (P1-3) |
| `_check_rss_memory()` | Monitors RSS between table iterations; WARNING at 85% ceiling | (B-8) |
| `MAX_RSS_GB` (default 48) | Configurable RSS ceiling | `config.py` |

⛔ `MALLOC_ARENA_MAX=2` **must** be set in the systemd unit file, shell wrapper, or `.bashrc` BEFORE the Python process starts. `os.environ.setdefault()` only covers child processes — glibc arena configuration is locked at process start.

---

## 8. Error Recovery Playbook

| Symptom | Likely Cause | Resolution |
|---------|-------------|------------|
| BCP row count mismatch | New control characters in source data | Check `sanitize_strings()` coverage |
| BCP column count mismatch | Source schema changed | Schema evolution handles new cols; type changes skip table |
| ConnectorX connection failure | Credentials, Oracle listener, network | Verify `.env`, try oracledb fallback |
| Sudden spike in CDC updates for all rows | polars-hash version change, column order shift, or schema evolution added column | Check PipelineEventLog metadata for `schema_migration: true` |
| Orphaned Flag=0 rows in Bronze | Crash between SCD2 INSERT and activation | `_cleanup_orphaned_inactive_rows()` handles this automatically |
| Extraction returns 0 rows | Source issue or intentional empty table | Extraction guard blocks CDC; use `--force` only if intentional |
| Duplicate rows in Bronze | Table lock failure or NULL PKs | Check PipelineLog for lock failures; check `null_pk_rows` metadata |
| Large table OOM | Extraction window too wide | Reduce `LookbackDays` or verify per-day processing path is used |
| Schema evolution error skipping table | Source column changed type | Manual resolution: verify type change, ALTER target column, re-run with `--force` |
| Table lock not acquired | Another run processing same table | Wait for completion or check for stale sessions |
| Hash migration needed | VARCHAR(64) migration from BIGINT | Run `migrations/b1_hash_varchar64.py --dry-run` first |

---

## 9. Key Files Reference

### Entry Points and Configuration
| File | Purpose |
|------|---------|
| `main_small_tables.py` | CLI entry point for small tables |
| `main_large_tables.py` | CLI entry point for large tables |
| `config.py` | Environment variables, BCP constants, CSV contract constants |
| `sources.py` | Source system registry (Oracle DNA, SQL Server CCM/EPICOR) |
| `connections.py` | SQL Server target connections, `cursor_for()` context manager, connection pool |
| `cli_common.py` | Shared CLI boilerplate (env setup, logging, startup checks, RSS monitoring) |

### Extraction (`extract/`)
| File | Purpose |
|------|---------|
| `router.py` | Single source of truth for extraction routing logic |
| `connectorx_oracle_extractor.py` | ConnectorX Oracle extraction → Polars → BCP CSV |
| `connectorx_sqlserver_extractor.py` | ConnectorX SQL Server extraction → Polars → BCP CSV |
| `oracle_extractor.py` | oracledb fallback (date-chunked with INDEX hints) |
| `udm_connectorx_extractor.py` | ConnectorX for internal UDM SQL Server reads |

### Data Loading (`data_load/`)
| File | Purpose |
|------|---------|
| `bcp_loader.py` | BCP subprocess wrapper (CSV → SQL Server) |
| `bcp_csv.py` | BCP CSV helpers: hashing, sanitization, column reorder, CSV write |
| `row_hash.py` | Row hashing (polars-hash SHA-256, fallback hashlib) |
| `sanitize.py` | DataFrame sanitization (strings, BIT columns, UInt64, Oracle dates) |
| `bcp_format.py` | BCP XML format file generation |
| `schema_utils.py` | Schema metadata queries for column validation and PK type lookup |
| `index_management.py` | Index disable/rebuild around BCP loads |

### CDC (`cdc/`)
| File | Purpose |
|------|---------|
| `engine.py` | Polars CDC: hash comparison, detect inserts/updates/deletes, NULL PK filter |
| `reconciliation/core.py` | Full column-by-column reconciliation |
| `reconciliation/counts.py` | Lightweight count reconciliation |
| `reconciliation/data_quality.py` | Distribution shift, aggregate checks |
| `reconciliation/scd2_integrity.py` | SCD2 structural integrity (overlaps, gaps, zero-active) |
| `reconciliation/velocity.py` | Version velocity monitoring |
| `reconciliation/boundaries.py` | Cross-layer boundary reconciliation |
| `reconciliation/persistence.py` | ReconciliationLog write helper |

### SCD2 (`scd2/`)
| File | Purpose |
|------|---------|
| `engine.py` | Polars SCD2: Bronze comparison, UPDATEs via typed staging, INSERTs via BCP |

### Orchestration (`orchestration/`)
| File | Purpose |
|------|---------|
| `small_tables.py` | Orchestrator for small tables |
| `large_tables.py` | Orchestrator for large tables (per-day checkpoint) |
| `pipeline_steps.py` | Shared CDC/SCD2 promotion steps |
| `table_config.py` | `TableConfig` + `TableConfigLoader` from UdmTablesList |
| `table_lock.py` | `sp_getapplock` table-level locking |
| `pipeline_state.py` | Extraction state tracking, gap detection, checkpoints |
| `guards.py` | Extraction guard logic (threshold-based, baseline retrieval) |

### Schema Management (`schema/`)
| File | Purpose |
|------|---------|
| `evolution.py` | Schema drift detection: ADD new cols, WARN removed, ERROR type changes |
| `column_sync.py` | Auto-populate UdmTablesColumnsList + PK discovery |
| `table_creator.py` | Auto-create Stage/Bronze tables from DataFrame dtypes |
| `staging_cleanup.py` | Orphaned staging table cleanup at pipeline start |

### Observability (`observability/`)
| File | Purpose |
|------|---------|
| `event_tracker.py` | `PipelineEventTracker` context manager → PipelineEventLog |
| `log_handler.py` | `SqlServerLogHandler` (logging.Handler) → PipelineLog |

---

## 10. ⛔ Hard "Do NOT" Rules

These rules exist because each one was discovered through a production incident or a deep analysis of failure modes. Every rule protects against silent data corruption or operational failures.

1. **Do NOT truncate Bronze tables** — SCD2 is append-only by design.
2. **Do NOT use `write_csv` without `batch_size=4096`** on large DataFrames.
3. **Do NOT add quoting to BCP CSV output** — `quote_style` must always be `'never'`.
4. **Do NOT write `True`/`False` for BIT columns** — must be `0`/`1` (`Int8`).
5. **Do NOT include `_scd2_key` (IDENTITY) in INSERT DataFrames or BCP column lists**.
6. **Do NOT assume source column order is stable** — `reorder_columns_for_bcp()` enforces deterministic order.
7. **Do NOT allow CDC to run if extraction returned 0 rows** — the empty extraction guard handles this.
8. **Do NOT run full in-memory CDC/SCD2 on large tables (3B+ rows)** — use windowed/targeted variants.
9. **Do NOT hardcode `NVARCHAR(MAX)` for PK columns in staging tables** — use `get_column_types()`.
10. **Do NOT run overlapping pipeline instances on the same table** — `table_lock.py` enforces this.
11. **Do NOT use native Polars `hash_rows()`** — it is non-deterministic across sessions.
12. **Do NOT drop columns during schema evolution** — data preservation is mandatory.
13. **Do NOT skip `_filter_null_pks()` before CDC comparison** — NULL PKs cause duplicate inserts every run.
14. **Do NOT use `add_row_hash_fallback()` in production without verification** — the fallback exists for dependency failure only.
15. **Do NOT truncate SHA-256 hash to Int64** — full 64-char hex string must be stored as VARCHAR(64).
16. **Do NOT set `SCD2_UPDATE_BATCH_SIZE` above 5,000** — lock escalation overrides RCSI.
17. **Do NOT remove `_cleanup_orphaned_inactive_rows()`** — orphaned Flag=0 rows accumulate silently.
18. **Do NOT query Bronze with only `WHERE UdmActiveFlag = 1`** — use `ROW_NUMBER()` dedup pattern.
19. **Do NOT use `\x00` (null byte) in hash sentinels** — use `\x1F` (Unit Separator).
20. **Do NOT use `pl.concat()` with `diagonal_relaxed` without `validate_schema_before_concat()` first**.
21. **Do NOT hash Categorical columns directly** — cast to Utf8 first.
22. **Do NOT change `@LockOwner` from `'Session'` to `'Transaction'`** without removing explicit release.
23. **Do NOT use `bcp_load(atomic=False)` for Bronze SCD2 loads**.
24. **Do NOT set `UdmActiveFlag=1` directly in `_build_scd2_insert()` for operation='U'**.

---

## 11. Reconciliation Strategy

The pipeline has a layered reconciliation approach:

| Check | Frequency | Scope | Function |
|-------|-----------|-------|----------|
| Count reconciliation (source vs Stage vs Bronze) | Daily | Lightweight | `reconcile_counts()` |
| Aggregate reconciliation (SUM/COUNT/MIN/MAX) | Daily for high-value tables | Value-level | `reconcile_aggregates()` |
| Full column-by-column reconciliation | Weekly | Complete | `reconcile_table()` |
| SCD2 structural integrity (overlaps, gaps) | Weekly | Bronze structure | `validate_scd2_integrity()` |
| Version velocity check | Periodic | SCD2 health | `check_version_velocity()` |
| Distribution shift detection | Weekly | Statistical | `detect_distribution_shift()` |
| Transformation boundary reconciliation | Periodic | Cross-layer | `reconcile_transformation_boundary()` |
| Referential integrity | Periodic | Cross-table FKs | `check_referential_integrity()` |
| Active PK reconciliation | Periodic | Windowed delete drift | `reconcile_active_pks()` |

All results are persisted to `General.ops.ReconciliationLog` for trending.

---

## 12. Environment and Dependencies

### Runtime Environment
- Python 3.12.11
- Oracle Instant Client 19c (for oracledb thick mode)
- ODBC Driver 18 for SQL Server (for BCP, pyodbc, ConnectorX)
- BCP utility (`mssql-tools18`) at `/opt/mssql-tools18/bin/bcp`
- Linux RedHat server
- `.env` file at `/debi/.env` (NOT project root)

### Python Dependencies
```
polars>=1.32.0        # Minimum for str.normalize("NFC") and streaming anti-joins
polars-hash==0.4.5    # EXACT pin — version change changes hash output
connectorx>=0.3.3     # High-speed extraction
oracledb>=2.0.0       # Oracle connectivity (thick mode)
pyodbc>=5.1.0         # SQL Server connectivity
python-dotenv>=1.0.0  # Environment variable loading
psutil>=5.9.0         # RSS memory monitoring
```

### CLI Commands
```bash
# Small tables
python3 main_small_tables.py --workers 4
python3 main_small_tables.py --table ACCT --source DNA
python3 main_small_tables.py --list-tables

# Large tables
python3 main_large_tables.py --workers 6

# Hash migration (if needed)
python3 migrations/b1_hash_varchar64.py --dry-run

# Hash regression test
python3 tests/test_hash_regression.py --self-check
```

---

## 13. Operational Monitoring Queries

```sql
-- Find the 10 slowest tables this week
SELECT TOP 10 TableName, EventType, DurationMs, RowsProcessed, Status
FROM General.ops.PipelineEventLog
WHERE EventType = 'TABLE_TOTAL'
  AND CompletedAt > DATEADD(DAY, -7, GETUTCDATE())
ORDER BY DurationMs DESC;

-- Check for failed steps
SELECT BatchId, TableName, EventType, ErrorMessage, CompletedAt
FROM General.ops.PipelineEventLog
WHERE Status = 'FAILED'
  AND CompletedAt > DATEADD(DAY, -1, GETUTCDATE())
ORDER BY CompletedAt DESC;

-- Monitor extraction guard triggers
SELECT TableName, SourceName, RowsProcessed, Metadata
FROM General.ops.PipelineEventLog
WHERE EventType = 'EXTRACT'
  AND Status = 'SKIPPED'
ORDER BY CompletedAt DESC;

-- Check for high CDC update ratios (possible systematic issue)
SELECT TableName, Metadata
FROM General.ops.PipelineEventLog
WHERE EventType = 'CDC_PROMOTION'
  AND JSON_VALUE(Metadata, '$.update_ratio') > 0.5
  AND CompletedAt > DATEADD(DAY, -7, GETUTCDATE());

-- Check SCD2 active-to-total ratio trends
SELECT TableName,
       JSON_VALUE(Metadata, '$.active_ratio') AS active_ratio,
       CompletedAt
FROM General.ops.PipelineEventLog
WHERE EventType = 'SCD2_PROMOTION'
  AND CompletedAt > DATEADD(DAY, -30, GETUTCDATE())
ORDER BY TableName, CompletedAt;

-- Large table checkpoint status
SELECT TableName, SourceName, DateValue, Status, ProcessedAt
FROM General.ops.PipelineExtractionState
WHERE Status LIKE 'FAILED%'
ORDER BY ProcessedAt DESC;

-- Lock contention monitoring
SELECT TableName, EventDetail, CompletedAt
FROM General.ops.PipelineEventLog
WHERE Status = 'SKIPPED'
ORDER BY CompletedAt DESC;

-- Reconciliation failures
SELECT TableName, CheckType, MismatchedRows, SourceOnlyRows, TargetOnlyRows, RunAt
FROM General.ops.ReconciliationLog
WHERE IsClean = 0
ORDER BY RunAt DESC;
```

---

## 14. Guidance for AI Agents Making Changes

### Before Making Any Change

1. **Read CLAUDE.md** — it is the authoritative source for constraints.
2. **Read TODO.md** — check if the issue is already tracked or resolved.
3. **Check the edge case tags** — references like `P0-1`, `B-2`, `E-3`, `W-8` link to specific documented decisions. Search for these tags in CLAUDE.md before modifying related code.

### Safe to Do Without Asking
- Refactoring shared utilities
- Adding type hints
- Fixing lint issues
- Performance improvements that don't change output

### ⛔ STOP and Ask Before
- Changing BCP CSV format
- Modifying CDC/SCD2 comparison logic
- Altering table naming conventions
- Changing database/schema names
- Changing hash algorithm or polars-hash configuration
- Weakening or removing any edge case safeguard (P0-1 through P3-4)

### Testing Approach
- Always test with `--table <single_table> --source DNA` before running the full pipeline
- Verify BCP CSV format compatibility after ANY change to write functions
- If modifying hash logic, run `tests/test_hash_regression.py --self-check` to verify polars-hash and hashlib produce identical output
- If a module fails to import, check `sys.path.insert` — the project uses parent-directory imports

---

## 15. Glossary

| Term | Definition |
|------|-----------|
| **CDC** | Change Data Capture — hash-based detection of inserts, updates, deletes |
| **SCD2** | Slowly Changing Dimension Type 2 — full version history with effective dates |
| **BCP** | Bulk Copy Program — Microsoft's high-speed bulk data loading utility |
| **ConnectorX** | Rust-based library for fast SQL extraction into DataFrames |
| **polars-hash** | Polars plugin providing deterministic SHA-256 hashing |
| **RCSI** | Read Committed Snapshot Isolation — SQL Server concurrency mode |
| **Stage** | `UDM_Stage` — CDC layer, tracks current state + change history |
| **Bronze** | `UDM_Bronze` — SCD2 layer, full append-only version history |
| **UdmTablesList** | Metadata table driving pipeline configuration |
| **PK** | Primary Key — drives CDC join columns and SCD2 business key |
| **Extraction Guard** | Safety mechanism blocking CDC when extraction row count drops suspiciously |
| **Orphaned inactive rows** | Bronze rows with `UdmActiveFlag=0, UdmEndDateTime IS NULL` from crash recovery |
| **Lock escalation** | SQL Server behavior where many row locks escalate to a table-level lock |
| **Phantom CDC updates** | False hash mismatches caused by normalization, encoding, or precision differences |

---

*Document generated from pipeline source code and CLAUDE.md as of 2026-02-24.*
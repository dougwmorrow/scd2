# Pipeline Architecture — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## Medallion Architecture

The pipeline follows Stage CDC → Bronze SCD2, mapping cleanly to the multi-hop medallion pattern.

**Validated by Databricks:** "The focus in this layer is quick Change Data Capture and the ability to provide a historical archive of source." Azure Databricks documentation explicitly warns: "Do not write to silver tables directly from ingestion."

**Validated by Databricks APPLY CHANGES:** Their own AUTO CDC API implements this exact separation — CDC events land in a streaming source, then a separate statement applies SCD2 semantics to a target.

The staging layer absorbs schema drift and data quality issues before SCD2 processing. CDC detection is logically separated from SCD2 historization with separate databases and concerns.

---

## Schema Evolution (B-3 / P0-2)

The single most commonly cited operational pain point in CDC/SCD2 post-mortems. Debezium DDL parsing failures, Netflix schema propagation overhead, and dbt snapshot bugs with new columns all point to schema evolution as a top-tier failure mode.

### Strategy: Additive-First Evolution
- **New columns:** `ALTER TABLE ADD` with NULL default. All historical rows retain NULL.
- **Removed columns:** Logged as WARNINGs, never dropped. Data preservation is mandatory.
- **Type changes:** Raise `SchemaEvolutionError` and skip the table. Requires manual resolution.
- **Detection:** `evolve_schema()` runs every extraction, comparing DataFrame columns against existing Stage/Bronze tables.

### Hash Interaction
Any schema change that affects the hash column set causes every row to appear as changed on the next run. `evolve_schema()` returns `SchemaEvolutionResult` so orchestrators can suppress false CDC warnings (E-12) during schema migration runs. PipelineEventLog metadata includes `"schema_migration": true`.

### Source Schema Validation (E-11)
`validate_source_schema()` compares extracted DataFrame columns against expected columns from UdmTablesColumnsList. Missing columns (rename/drop) are ERROR-level and skip the table. Unexpected columns (new in source) are WARNING-level and allowed through.

### Source Type Drift (E-16)
`detect_source_type_drift()` compares source column metadata (type, precision, scale) against Stage INFORMATION_SCHEMA. Detects precision changes that could cause phantom hash mismatches.

---

## Concurrent Run Protection

### sp_getapplock (P1-2)
Session-owned locks via `sp_getapplock` prevent concurrent pipeline runs on the same table. `@LockTimeout=0` for non-blocking skip. Session-owned locks auto-release on connection drop (crash-safe). Uses `autocommit=True` to avoid the RCSI race condition with Transaction-scoped locks.

### Resilient Lock Connection (N-1)
`_get_resilient_lock_connection()` adds `ConnectRetryCount=3, ConnectRetryInterval=10` for firewall timeout scenarios.

---

## Checkpoint and Recovery

### Per-Day Checkpointing (P1-5)
`orchestration/pipeline_state.py` tracks per-day status in PipelineExtractionState. On restart, the pipeline resumes from the last successful date and fills gaps.

### Partial Extraction Recovery (P1-6)
Each completed day is checkpointed as SUCCESS. A failure on day 15 of 30 preserves the first 14 days; the next run picks up from day 15.

### Idempotency
Windowed CDC comparison handles re-runs safely. Re-extracting the same date produces unchanged hashes for untouched rows. LookbackDays provides rolling re-extraction coverage. The 2024–2025 consensus: "Idempotency is not about preventing retries — it's about making retries safe."

### Crash Recovery
INSERT-first SCD2 with dedup-on-next-run. Orphaned Flag=0 rows cleaned up automatically. Multiple active records detected and recovered. See SCD2.md for details.

---

## Two Processing Modes

| Mode | Trigger | CDC Variant | SCD2 Variant | Memory Strategy |
|------|---------|-------------|--------------|-----------------|
| Small tables | No `SourceAggregateColumnName` | `run_cdc()` full comparison | `run_scd2()` reads all active Bronze | Entire table fits in memory |
| Large tables | `SourceAggregateColumnName` set | `run_cdc_windowed()` date-scoped | `run_scd2_targeted()` PK-targeted Bronze read | One day at a time, per-day checkpoint |

### Small Table Lifecycle
Extract → add hash + timestamp → BCP CSV → ensure tables → schema evolution → column sync → extraction guards → CDC (NULL PK filter → anti-joins → hash compare → count validation → BCP to Stage) → SCD2 (read active Bronze → dedup → compare → INSERT new → UPDATE close → activate → integrity check) → cleanup

### Large Table Lifecycle (per day)
Lock → null date check → get dates → FOR EACH day: extract windowed → checkpoint EXTRACTED → daily guard → schema evolution → windowed CDC (scoped deletes) → release DataFrame → targeted SCD2 (PK-scoped Bronze read) → checkpoint SUCCESS → cleanup → next day → release lock

---

## Observability

Two complementary tables in `General.ops`:

### PipelineEventLog — Runtime Performance
One row per step per table per run. All tables share one BatchId from PipelineBatchSequence.
- **EventTypes:** EXTRACT, BCP_LOAD, CDC_PROMOTION, SCD2_PROMOTION, CSV_CLEANUP, TABLE_TOTAL
- **Metrics:** RowCount, DurationSeconds, metadata JSON (schema_migration, update_ratio, null_pk_rows, active_ratio, etc.)

### PipelineLog — Diagnostic Investigation
Standard Python logging.Handler writing to SQL Server. WARNING/ERROR/CRITICAL level messages for debugging.
- **Join point:** BatchId + TableName + SourceName

---

## Metadata-Driven Orchestration

### UdmTablesList (General.dbo)
Master configuration driving extraction routing, table naming, and processing mode. Key columns: SourceName, SourceObjectName, SourceServer, SourceDatabase, SourceSchemaName, SourceAggregateColumnName (triggers large table mode), SourceIndexHint (triggers oracledb path), LookbackDays, FirstLoadDate, PartitionOn, StageTableName, BronzeTableName, StageLoadTool.

### UdmTablesColumnsList (General.dbo)
Column metadata driving CDC/SCD2 logic. IsPrimaryKey drives comparison keys. IsIndex drives index management. OrdinalPosition drives column ordering. Layer tracks Stage vs Bronze.

### Column Sync (schema/column_sync.py)
Auto-populates UdmTablesColumnsList on first load. Discovers PKs from source metadata (Oracle: ALL_CONSTRAINTS/ALL_INDEXES; SQL Server: sys.indexes). Optimistic behavior — tables with discoverable PKs are fully automatic.

---

## Memory Guards

| Guard | Threshold | Action |
|-------|-----------|--------|
| MALLOC_ARENA_MAX | 2 arenas | Set before imports |
| POLARS_MAX_THREADS | 1 per worker | Prevents thread oversubscription |
| Worker count | 4 on 64 GB | WARNING when >4 |
| Small table ceiling (S-2) | 50M rows | Cap first-run extractions |
| Memory estimate (ST-1) | 20 GB peak | Skip if exceeded |
| RSS monitor (B-8) | 85% of MAX_RSS_GB | WARNING/ERROR |
| FD monitor (M-3) | 500 warn, 800 error | Track open files |

---

## Quarantine (W-17)

`General.ops.Quarantine` stores records failing schema contracts. `ensure_quarantine_table()`, `quarantine_record()`, and `quarantine_batch()` in `schema/evolution.py`. Currently hooked into schema type change errors. Low priority — existing all-or-nothing table skip is safe; quarantine adds visibility.

---

## Real-World Failures That Validate This Architecture

- **Zalando's Debezium post-mortem** (billions of events): replication slot management is #1 operational issue. Query-based approach avoids this.
- **Retail company lost a week of sales data** from a vendor column rename — `.get('column_name', 0)` defaulted to zero. Schema drift affects 60%+ of pipelines lacking automated contract testing. This pipeline's schema validation defends against this.
- **dbt snapshots silently materialized as regular tables** (pre-1.4), destroying historical data. Only discovered on upgrade. Post-SCD2 integrity checks defend against this class of failure.
- **Databricks CDC MERGE degraded from 20 min to 4+ hours** on 800M rows due to small-file proliferation. For SQL Server, this manifests as index fragmentation and statistics staleness — addressed by index management.

---

## Ecosystem Alternatives Evaluated

| Tool | Status | Notes |
|------|--------|-------|
| pydbzengine | Evaluation pending | Pythonic Debezium interface (Feb 2025); potential Oracle log-based CDC |
| dbt snapshots v1.9+ | Not adopted | Cannot handle out-of-order records or capture intermediate states |
| SQL Server temporal tables | Evaluation pending | No column-level change control; same-database history only |
| Polars streaming engine | Available | Not yet adopted; useful for future out-of-core processing |
| XXH3-128 / BLAKE3 | Evaluation pending | Performance alternatives to SHA-256; current SHA-256 is functional |

---

*Sources: Databricks medallion architecture documentation, Azure Databricks APPLY CHANGES documentation, Debezium/Zalando post-mortems, Netflix DBLog, dbt snapshot documentation, pydbzengine release notes, SQL Server 2022 documentation.*
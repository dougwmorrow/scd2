# UDM Pipeline — Technical Reference (Confluence)

**Companion document to the UDM Pipeline presentation deck.**
This page contains the deep technical detail behind each concept introduced in the slides. Engineers and analysts can use this as a reference when working with or extending the pipeline.

---

## Hash-Based CDC — Detailed Mechanics

### Anti-Join Pattern Explained

The CDC engine uses a **three-way anti-join pattern** in Polars to classify every row:

- **Anti-join (detect INSERTs):** Fresh source data (Table A) anti-joined against existing Stage data (Table B) on primary keys. Any source row whose PK doesn't exist in Stage is new — it's an INSERT. In SQL terms, think `WHERE NOT EXISTS` or `LEFT JOIN ... WHERE B.key IS NULL`.
- **Reverse anti-join (detect DELETEs):** Flip it. Existing Stage data (Table A) anti-joined against fresh source (Table B). Any Stage row whose PK doesn't exist in the source anymore has been deleted.
- **Inner join + hash compare (detect UPDATEs):** For PKs that exist in both, compare SHA-256 hashes. Same hash = unchanged. Different hash = something in that row's non-PK columns changed.

This three-way comparison gives a complete, deterministic classification of every row in a single pass.

### Why Hash-Based Instead of Timestamp-Based

- Works identically across Oracle and SQL Server (no `ROWVERSION`, no trigger dependency)
- Deterministic — same data always produces the same hash
- Catches changes that timestamps miss (bulk updates, timezone bugs, manual corrections)
- Per-key collision risk is negligible at 64-bit: ~1.6×10⁻¹⁰

### Hash Collision Risk — The Math

A hash collision is when two different inputs produce the same hash output. For CDC, the question is: "Could a row's data change without its hash changing?"

We use the first 64 bits of SHA-256 for per-key comparison. The probability of a collision on any single key is **1 in 2⁶⁴ — approximately 1.6×10⁻¹⁰**, or roughly 1 in 6 billion.

To put that in perspective: if you processed every row in our largest table (3 billion rows) every day, you'd statistically expect one false "unchanged" classification roughly once every 2 days across the entire table. In practice, the risk is even lower because we're comparing the same key's hash across two points in time (not comparing hashes across different keys).

**Critical distinction — Birthday Paradox:**

The low per-key risk applies **only** to per-key change detection (comparing the same PK's hash before and after). For operations that compare hashes across many different keys — like reconciliation, deduplication, or future surrogate key generation — the **birthday paradox** applies and collision risk jumps dramatically. At 3 billion rows with 64-bit hashes, birthday paradox collision probability is ~24%. That's why those operations use the **full 256-bit SHA-256 hash**.

### Two Processing Modes

| Mode | Used For | Strategy |
|------|----------|----------|
| Small tables | Tables without a date column | Full extract each run, full CDC comparison |
| Large tables | Tables with a date column (e.g., 3B+ rows) | Extract one day at a time, windowed CDC, per-day checkpoint |

**How does the pipeline decide?** The `SourceAggregateColumnName` column in `General.dbo.UdmTablesList` drives this. If `NULL` → small table (full extraction). If populated (e.g., `TransactionDate`) → large table (date-windowed extraction). This is metadata-driven, not a row-count heuristic.

Safety net: if a "small" table exceeds a size threshold during extraction, the pipeline logs a warning suggesting reclassification. The code lives in `orchestration/table_config.py` — `load_small_tables()` filters `WHERE SourceAggregateColumnName IS NULL` and `load_large_tables()` filters `WHERE SourceAggregateColumnName IS NOT NULL`.

---

## SCD2 — INSERT-First Ordering Deep Dive

### Why INSERT-First Instead of UPDATE-First

The conventional SCD2 pattern is UPDATE-first (close old version), then INSERT (add new version). We do the opposite. Here's the detailed crash analysis:

**Scenario A — UPDATE-first (conventional):**

1. UPDATE old version: set `UdmActiveFlag = 0`, `UdmEndDateTime = now` → **Crash here**
2. INSERT new version: *(never happens)*
3. **Result:** Zero active versions for that PK. Downstream queries filtering `WHERE UdmActiveFlag = 1` return nothing. This is **silent data loss** — no row, no error, no trace. Recovery requires manually identifying which PKs lost their active version and reconstructing the correct state.

**Scenario B — INSERT-first (our approach):**

1. INSERT new version with `UdmActiveFlag = 0` → **Crash here**
2. UPDATE old version: *(never happens)*
3. UPDATE new version to activate: *(never happens)*
4. **Result:** The old active version is still there (`UdmActiveFlag = 1`). There's also an orphaned inactive row (`UdmActiveFlag = 0`, `UdmEndDateTime IS NULL`). On the next run, `_cleanup_orphaned_inactive_rows()` automatically deletes the orphan (B-4), and the pipeline re-processes the change normally. **No data loss, no manual intervention.**

**Key insight:** Duplicate active rows are detectable and self-healing; missing active rows are silent and require manual recovery.

### Defense Layers for INSERT-First

- **Filtered unique index** on Bronze `(pk_columns) WHERE UdmActiveFlag = 1` — prevents creating a second active row (constraint violation instead, which is catchable)
- **Orphan cleanup** (`_cleanup_orphaned_inactive_rows()`) runs at the start of every SCD2 cycle — deletes Flag=0 rows with NULL `UdmEndDateTime`
- **Dedup-safe query pattern** — downstream consumers should use `ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC) WHERE rn = 1` instead of `WHERE UdmActiveFlag = 1` alone
- **P1-16 dedup recovery** — pipeline deduplicates active Bronze rows before comparison on the next run

---

## Performance at Scale — Detailed Mechanics

### Unchanged Rows — Count but Don't Write

Consider a table with 3 million rows where only 5,000 change daily. The SCD2 engine classifies all 3M rows but only writes the 5,000 changes. Without this, every row would need an UPDATE generating ~2.4 GB of transaction log for no data change (`3M rows × 400 bytes × 2 for before/after log images`). Over a week: 16+ GB of wasted log writes.

### UPDATE JOIN with BCP Staging Tables

When SCD2 needs to close 50,000 old versions, it doesn't execute 50,000 individual UPDATE statements. Instead:

1. Write all 50,000 PKs to a BCP CSV
2. Bulk-load into a temp staging table
3. Create an index on the staging table
4. Execute a single `UPDATE t SET ... FROM Bronze t INNER JOIN staging s ON t.pk = s.pk WHERE t.UdmActiveFlag = 1`

Completes in seconds instead of minutes.

### Lock Escalation Prevention (4,000-Row Batch Cap)

SQL Server has an internal threshold: when a single statement acquires ~5,000 row-level locks, it automatically escalates to a **table-level exclusive lock**. This blocks **all** concurrent readers — even under RCSI.

By capping each UPDATE batch at 4,000 rows (safely below ~5,000), we keep row-level locks and concurrent readers are never blocked. The pipeline loops through batches until all rows are processed.

Example — closing 50,000 old versions:
```
Batch 1:  UPDATE TOP(4000) ... → 4,000 rows closed (row locks only)
Batch 2:  UPDATE TOP(4000) ... → 4,000 rows closed (row locks only)
...
Batch 13: UPDATE TOP(4000) ... → 2,000 rows closed (done)
Total: 50,000 rows across 13 batches — no reader ever blocked.
```

### BULK_LOGGED Recovery Model

BCP INSERTs can be minimally logged under BULK_LOGGED, reducing transaction log writes by up to 90% for INSERTs. **Nuance:** for non-empty tables with a clustered index (all Bronze tables), BCP is still fully logged on data pages — BULK_LOGGED only minimizes logging for index page operations.

The pipeline switches to BULK_LOGGED before the load window and restores to FULL with a log backup afterward. For large SCD2 UPDATE operations (>1M rows), the pipeline estimates transaction log usage at ~400 bytes × 2 per row and checks available log space before starting (E-10).

### Transaction Log Monitoring

The pipeline logs a warning when UPDATE JOINs exceed 1M rows, includes estimated log footprint, and recommends monitoring via `SELECT * FROM sys.dm_db_log_space_usage`. Frequent log backups (every 15–30 minutes) are recommended during pipeline runs.

---

## Polars Lazy Evaluation — Why It Matters

Instead of executing every operation immediately (like Pandas), Polars builds a query plan first and optimizes before touching data. Think of it like SQL Server's query optimizer: it sees the full picture, eliminates unnecessary work, reorders operations, and only materializes results when you call `.collect()`.

This means Polars can push filters down before joins, skip reading columns it doesn't need, and avoid creating intermediate DataFrames that waste memory. At 3B+ rows, eager execution would blow out RAM; lazy evaluation lets us express complex transformations and let the engine figure out the most efficient execution path.

---

## Code Prefix Reference (Traceability System)

Throughout the pipeline code, alphanumeric codes trace every safeguard back to its research origin.

### Research-Originated Prefixes

| Prefix | Source Research | What It Covers |
|--------|----------------|----------------|
| **P0- to P3-** | *CDC SCD2 Pipeline Validation Against Production Best Practices* | Priority-tiered edge cases. P0 = critical (data corruption), P1 = high (silent failures), P2 = medium (performance), P3 = low (minor). |
| **B-** | *Validating at Billion-Row Scale* | Hash collision math, lock escalation, transaction log sizing, memory monitoring. |
| **E-** | *Every Edge Case and Failure Mode* | 20+ failure modes from Oracle/SQL Server cross-platform issues to RCSI concurrency. |
| **V-** | *Validating Against 2024-2025 Best Practices* | Float normalization, Unicode NFC, BOM stripping, version integrity checks. |
| **W-** | *Ten Technical Claims Tested* | Null byte risks, schema concat safety, lock scope behavior. |

### Internal Organizational Prefixes

| Prefix | Scope | Examples |
|--------|-------|---------|
| **S-** | Source data safeguards | S-1 (source PK dedup), S-4 (PK column validation) |
| **L-** | Large table specific | L-1 (Stage dedup), L-2 (Stage purge utility) |
| **C-** | Correctness/count validation | C-2 (idempotent retry), C-5 (close count dedup) |
| **T-** | Technical safety | T-2 (safe concat wrapper) |
| **SCD-** | SCD2 design decisions | SCD-1 (unique active index), SCD-3 (batched updates) |
| **ST-** | Small table specific | ST-1 (memory ceiling guard) |
| **OBS-** | Observability enhancements | OBS-2 (event detail), OBS-7 (metadata merge) |
| **R-** | Refactoring tasks | R-1 through R-9 (directory restructure) |
| **D-** | Deduplication tasks | D-1 through D-5 (orchestrator dedup) |

---

## TODO Status — Completed Audit Summary

Across all research audits and code reviews, the pipeline has addressed **150+ edge cases, bugs, and improvements**.

| Audit Stream | Items | Status |
|-------------|-------|--------|
| Refactoring Phases 1–5 (R-, D-, C-, S-, B-, E-, L-, X-, G-series) | 42 | **All Complete** |
| B-series — Billion-row scale validation | 14 | **All Complete** |
| E-series — Every edge case & failure mode | 21 | **All Complete** |
| Fifth audit — Production-scale edge cases | 26 | **All Complete** |
| OBS-series — Observability gaps | 8 | **All Complete** |
| V-series — Initial best practices validation | 12 of 13 | **12 Complete, 1 Deferred** |
| W-series — 2024–2025 best practices | 10 of 18 | **10 Complete, 8 Deferred** |
| Bugs/Issues/MVP review | 23 of 25 | **23 Complete, 2 Remaining** |
| Technical updates (H-, N-, P-, A-, M-, R-, O-series) | 25 of 33 | **25 Complete, 8 Server-Dependent** |

### Remaining — Server-Dependent (Require RHEL/Production Access)

| ID | Description | Dependency |
|----|-------------|------------|
| H-5 | Test `SQLCMDPASSWORD` env var with installed BCP version | RHEL server |
| H-6b | Fallback: Kerberos/trusted connection or dedicated SQL login | RHEL server + H-5 |
| N-4 | Before/after timing of NFC normalization on string-heavy table | DB + string-heavy table |
| P-1 | Check installed unixODBC version for pyodbc connection pooling | RHEL server |
| P-2 | Verify pooling or implement connection pool (SQLAlchemy QueuePool) | RHEL server + P-1 |
| P-4 | Verify Polars ≥ 1.32 for native streaming anti-joins | Pipeline server |
| P-5 | Test polars-hash compatibility with lazy frames + streaming | Pipeline server |
| P-7 | Measure peak memory reduction from streaming anti-joins | Large table + monitoring |
| M-3 | Verify both hash paths produce identical output on test DataFrame | Test DataFrame |

### Remaining — Deferred Evaluations (Future Work)

| ID | Priority | Description |
|----|----------|-------------|
| W-1 | P0 | Upgrade BCP to mssql-tools18 v18.6.1.1 for `-C 65001` UTF-8 codepage on Linux |
| W-5 | P1 | Adopt Polars streaming engine (`collect(engine="streaming")`) for memory-constrained ops |
| W-6 | P1 | ConnectorX Oracle contingency — evaluate pydbzengine and oracledb+PyArrow |
| W-9 | P2 | Evaluate polars-hash upgrade 0.4.5 → 0.5.6 |
| W-14 | P3 | Evaluate pydbzengine for log-based Oracle CDC |
| W-15 | P3 | Monitor ADBC/Columnar ecosystem for Oracle driver availability |
| W-16 | P3 | Evaluate SQL Server 2022 temporal tables for stable dimension tables |
| W-18 | P3 | Evaluate XXH3-128 or BLAKE3 as hash alternatives |

---

## Appendix — Anticipated Questions

**Q: Why not just use SSIS / ADF / dbt?**
SSIS is Windows-only and doesn't handle Oracle extraction natively at this scale. ADF requires cloud infrastructure we may not want. dbt is a transformation tool (Silver/Gold) not an extraction tool — it doesn't replace Source → Stage → Bronze ingestion.

**Q: What happens if the pipeline crashes mid-run?**
Small tables: CDC comparison is idempotent — re-running produces the same result. Large tables: per-day checkpointing means we resume from the last successful date.

**Q: How do we know the data is correct?**
Hash-based CDC gives deterministic change detection. The reconciliation suite includes 12 validation strategies: row count checks, hash collision detection, version velocity monitoring, cross-layer boundary validation, referential integrity checks, and distribution shift analysis.

**Q: What if a source system adds new columns?**
Schema evolution detects new columns automatically and adds them to Stage/Bronze. Removed columns trigger an error and block processing until reviewed — preventing silent data loss.

**Q: How fast is it?**
Depends on the table. Extraction is bottlenecked by source system I/O. BCP loading sustains hundreds of thousands of rows per second. The observability layer tracks rows/second per step per table — real numbers are available from PipelineEventLog.

---

## TODO Source Files Reference

| File | Scope | Total | Remaining |
|------|-------|-------|-----------|
| `TODO.md` | Refactoring phases 1–5 | 42 | 0 |
| `refacotring_todo.md` | Verified refactoring completion | 42 | 0 |
| `hash_todo.md` | Billion-row scale | 14 | 0 |
| `edge_case_todo.md` | 26 production-scale edge cases | 26 | 0 |
| `scd2_cdc_edge_cases_todo.md` | 21 CDC→SCD2 failure modes | 21 | 0 |
| `scd2_cdc_initial_todo.md` | Initial validation | 13 | 1 (V-8 → W-5) |
| `scd2_cdc_best_practices_todo.md` | 2024–2025 best practices | 18 | 8 deferred |
| `observability_todo.md` | Observability gaps | 8 | 0 |
| `bugs_issues_mvp_todo.md` | Bugs, code quality, security | 25 | 2 |
| `pipeline_steps_todo.md` | Pipeline step extraction | 12 | 2 |
| `technical_updates_todo.md` | Security hardening, streaming, hash | 33 | 8 server-dependent |
| `b_series_todo.md` | B-series verification gaps | 2 | 0 |
| `adress_missed_todo.md` | Post-audit verification | 3 | 0 |

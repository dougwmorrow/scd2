# Edge Case Analysis — Fifth Audit

*Context: All prior edge cases (P0-1 through C-7) are fully implemented. This analysis is derived from "Edge Cases in Production-Scale Polars-to-SQL-Server Pipelines: A Systematic Investigation" — a research-backed audit identifying 34 edge cases across the pipeline's extraction, loading, memory management, CDC, and SCD2 layers.*

---

## 1. Extraction (ConnectorX)

### E-1 (P0): ConnectorX partitioned reads produce torn reads on actively-written tables

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`, `udm_connectorx_extractor.py`

**Problem:** When `partition_num > 1`, ConnectorX opens N independent connections, each executing its own range query. On Oracle, each connection gets its own statement-level SCN. On SQL Server under default READ COMMITTED, each sees committed data at slightly different times. If the source table is being actively written to during extraction:

- Rows can be **duplicated**: a row's partition key is updated from range B to A between two connections' snapshots
- Rows can be **missed**: a row moves from range A to B between snapshots

This manifests as phantom CDC changes — rows that appear modified but weren't, or rows that temporarily disappear. For CDC-sensitive tables, this directly corrupts the change detection layer.

**Impact:** Silent data corruption. Phantom inserts/deletes/updates in CDC that don't reflect actual source changes. Compounds across runs if phantom deletes trigger SCD2 closures.

**Fix:**
- For CDC-sensitive tables, use `partition_num=1` to ensure a single consistent snapshot
- On Oracle, use Flashback Query (`AS OF SCN`) to pin all partitions to the same SCN
- On SQL Server, enable SNAPSHOT isolation for extraction connections
- Add a configurable `safe_partitioning` flag per table in UdmTablesList that forces `partition_num=1` for tables with high write concurrency

---

### E-2 (P0): ConnectorX silently drops rows where partition column is NULL

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`

**Problem:** ConnectorX's documentation states partition columns "cannot contain NULL." The generated WHERE clauses use `>` and `<` operators, which never match NULL. No error is raised — rows simply vanish from the result set. For any table where the partition column is nullable, affected rows are silently excluded from every extraction.

**Impact:** Permanent silent data loss for any row with a NULL partition column. No error, no log, no indication.

**Fix:**
- Before partitioned extraction, query `SELECT COUNT(*) FROM source WHERE partition_col IS NULL`
- If count > 0, either:
  - Supplement with a separate `WHERE partition_col IS NULL` query and concat results
  - Wrap the column in `COALESCE(partition_col, <sentinel>)` in the extraction query
  - Fall back to `partition_num=1`
- Log WARNING with count of NULL partition column rows

---

### E-3 (P1): ConnectorX partition skew degrades parallelism silently

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`

**Problem:** ConnectorX partitions by computing `SELECT MIN(col), MAX(col)` and splitting the range evenly. No skew detection is performed. If 90% of values cluster in one range segment (common with monotonically increasing IDs, clustered timestamp inserts, or gapped sequences), one thread processes the vast majority of rows while others handle trivial amounts — providing no parallelism benefit, just connection overhead.

**Impact:** No data corruption. Performance degradation masquerading as parallelism. Extra Oracle/SQL Server sessions consumed with no benefit.

**Fix:**
- Add optional skew detection: query percentiles (`PERCENTILE_DISC`) before partitioning to assess distribution
- If skew ratio exceeds a threshold (e.g., largest partition > 5x smallest), fall back to `partition_num=1` with a log INFO
- Or accept as-is and rely on `partition_num=1` for CDC-sensitive tables (E-1 fix) which sidesteps this entirely

---

### E-4 (P1): Oracle DATE sentinel `9999-12-31` crashes ConnectorX with Rust panic

**Files:** `connectorx_oracle_extractor.py`, `oracle_extractor.py`

**Problem:** ConnectorX GitHub issue #644 documents that reading `DATE '9999-12-31'` from Oracle causes a Rust panic: `PanicException: out of range DateTime` at `arrow_assoc.rs`. This terminates the Python process with no recovery — not an exception, a full process kill.

Sentinel dates like `9999-12-31` are extremely common in SCD2 source tables (end-date for currently active records) and ERP systems.

**Impact:** Unrecoverable process crash. Any table containing a DATE sentinel causes full pipeline termination.

**Fix:**
- Filter sentinel dates in the extraction query: `WHERE date_col < DATE '9999-01-01'` or cast in query: `CASE WHEN date_col = DATE '9999-12-31' THEN NULL ELSE date_col END`
- Add a pre-extraction check for known sentinel values in DATE columns
- Document known-bad Oracle DATE values in CLAUDE.md

---

### E-5 (P2): ConnectorX connection leak at scale exhausts Oracle sessions

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`

**Problem:** ConnectorX opens N+1 connections per `read_sql` call (one for metadata plus one per partition). With 6 partitions across 365 backfill iterations, that's 2,555 Oracle sessions if connections leak on error paths. Oracle's default `SESSIONS` parameter is often 100–300.

**Impact:** Oracle session exhaustion causing `ORA-00018: maximum number of sessions exceeded` — blocks all pipeline extraction until sessions are released.

**Fix:**
- Monitor `V$SESSION` count before and after extraction batches
- Implement explicit connection cleanup on error paths
- Add a session count guard: if current sessions > 80% of max, pause and wait for cleanup
- Consider batching iterations with periodic connection pool resets

---

## 2. BCP Loading

### B-1 (P0): BCP with `quote_style='never'` silently corrupts data containing tabs or newlines

**Files:** `bcp_csv.py`, `bcp_loader.py`

**Problem:** Polars documentation explicitly warns: `quote_style='never'` "never puts quotes around fields, even if that results in invalid CSV data." No escaping is performed. Any source data containing:
- A **tab character** (`\t`) splits a field into two columns
- An **embedded newline** (`\n` or `\r\n`) splits a row into two rows

BCP parses these corrupted files without error — it loads the wrong data into the wrong columns. Embedded newlines are extremely common in production data: addresses, comments, email bodies, and any free-text field.

**Impact:** Silent data corruption. Fields shift columns, rows split, data integrity is destroyed with no error raised.

**Fix:**
- **Best:** Switch to `quote_style='necessary'`, which quotes only fields containing the delimiter, newlines, or the quote character
- **If 'never' is required:** Pre-process all string columns to strip or replace `\t`, `\n`, `\r` before writing CSV
- **Alternative:** Use BCP format files with multi-character delimiters unlikely to appear in data (e.g., `[||]` as column delimiter, `[~~]\n` as row terminator)
- **Detection:** Use BCP's `-e` error file and validate row counts after every load

---

### B-2 (P2): Minimal logging impossible on non-empty clustered-index tables

**Files:** `bcp_loader.py`, operational documentation

**Problem:** For tables with a clustered index, BCP achieves minimal logging on data pages only when the table is empty. For the pipeline's 3B-row tables, every BCP row is fully logged regardless of TABLOCK or recovery model. At 3M rows × ~200 bytes/row with logging overhead, expect 1.2–3 GB of transaction log per load. Without frequent log backups under FULL recovery, the log grows unbounded.

**Impact:** Transaction log bloat. Not data corruption, but can cause disk space exhaustion and slow recovery times.

**Fix:**
- Ensure frequent transaction log backups under FULL recovery model (every 15-30 minutes during loads)
- Document expected log growth per load in operational runbook
- For maximum performance, consider partition switching: load into an empty staging partition, then `ALTER TABLE SWITCH` the partition in
- Monitor log space with `DBCC SQLPERF(LOGSPACE)` during loads

---

### B-3 (P2): TABLOCK blocks all concurrent access during BCP load

**Files:** `bcp_loader.py`

**Problem:** TABLOCK acquires a bulk update lock — a table-level lock that blocks all concurrent reads and writes until BCP completes. For large loads taking minutes, this creates a significant availability window.

**Impact:** Downstream consumers and reporting queries blocked during BCP loads. Not data corruption.

**Fix:**
- On partitioned tables, set `LOCK_ESCALATION = AUTO` to enable partition-level locking
- Document the lock window in operational runbook so downstream consumers can plan accordingly
- Without TABLOCK, BCP uses row/page locks (fully logged but allows concurrent access) — evaluate the tradeoff per table

---

## 3. Memory & System Resources

### M-1 (P0): glibc arena fragmentation causes OOM on long-running backfills

**Files:** `main_large_tables.py`, `main_small_tables.py`, `large_table_orchestrator.py`, system configuration

**Problem:** Polars uses Rust's Rayon thread pool internally, and Rust allocations go through glibc's malloc. On a 6-core RHEL system, glibc creates up to 48 memory arenas (8 × cores). When Polars threads allocate and free large DataFrames across iterations, small long-lived allocations pin entire arenas, preventing memory return to the OS.

Real-world evidence: Polars GitHub issue #23128 (Feb 2025) documents 10 GB starting data growing RSS to 2,766 GB — a 9:1 waste ratio. Issue #21732 (Dec 2024) reports OOM from fragmentation in Polars ≥1.7.0. Critically, `gc.collect()` or `malloc_trim(0)` from Python has **no effect** on Polars/Rust allocations because they use a different heap.

**Impact:** Inevitable OOM-kill after ~50–100 backfill iterations. The single most impactful reliability risk for the pipeline.

**Fix:**
- Set `MALLOC_ARENA_MAX=2` before launching the process — reduces arena count from 48 to 2, with <1% CPU impact (validated by Presto team on issue #8993)
- Alternatively, use jemalloc via `LD_PRELOAD=/path/to/libjemalloc.so`
- For extreme cases, spawn a new subprocess per day-batch to guarantee OS memory reclamation on exit
- Set `POLARS_MAX_THREADS=1` per worker when using multiprocessing to prevent thread oversubscription
- Add to pipeline launch script or systemd unit file

---

### M-2 (P1): Six parallel workers on 64 GB creates impossible memory budget

**Files:** `main_large_tables.py`, `main_small_tables.py`, system configuration

**Problem:** With OS overhead consuming 2–4 GB, the effective per-worker budget is ~10 GB. A 3M × 200 column DataFrame of mixed types occupies 4–8 GB. Polars join operations spike to 2–4× the DataFrame size in peak memory, meaning a single join could demand 16–32 GB — far exceeding the 10 GB budget. The OOM killer sends SIGKILL with no warning and no chance to clean up.

**Impact:** Random worker OOM-kills during CDC or SCD2 join operations. No graceful recovery.

**Fix:**
- Run 3–4 workers instead of 6, budgeting 15–20 GB per worker
- Set `POLARS_MAX_THREADS=1` per worker since parallelism is achieved via multiprocessing
- Use cgroups v2 to limit each worker's memory (`memory.max = 15G`) for controlled OOM targeting
- Critical RHEL settings:
  - `ulimit -n 65536` (open files)
  - `ulimit -u 4096` (max user processes)
  - `MALLOC_ARENA_MAX=2` (arena fragmentation prevention)
  - `POLARS_MAX_THREADS=1` (CPU oversubscription prevention)

---

### M-3 (P1): File descriptor leak across 365+ backfill iterations

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`, system configuration

**Problem:** ConnectorX opens N+1 connections per `read_sql` call. If connections aren't closed on error paths, file descriptors accumulate. Default RHEL `ulimit -n` is 1024. At 7 FDs per iteration, the process hits `OSError: [Errno 24] Too many open files` after ~100–150 iterations.

**Impact:** Process crash after sustained backfill operations. Predictable but not immediately obvious failure mode.

**Fix:**
- Set `ulimit -n 65536` in the pipeline's launch configuration
- Monitor FD count with `ls /proc/<pid>/fd | wc -l` periodically or via PipelineEventLog
- Add explicit connection cleanup in error handlers

---

## 4. Small Tables Scaling

### ST-1 (P1): Small tables crossing 8–10M rows will OOM during full CDC comparison

**Files:** `small_table_orchestrator.py`, `cdc_polars.py`

**Problem:** Memory estimation for a Polars DataFrame with mixed types (30 Int64, 20 Float64, 40 Utf8 averaging 50 chars, 10 DateTime across 100 columns): ~2.8 GB per million rows. An anti-join requires both DataFrames plus a hash table, peaking at ~2.5–3× the DataFrame size.

| Row count | DataFrame size | Anti-join peak RAM | Feasible (64 GB, 6 workers)? |
|-----------|---------------|-------------------|------------------------------|
| 5M        | ~14 GB         | ~42 GB            | Marginal with 1 worker       |
| 10M       | ~28 GB         | ~84 GB            | No                           |

The practical threshold is ~8–10M rows on a 64 GB system with 6 workers processing 100-column mixed-type tables.

**Impact:** OOM for any small table that grows beyond the threshold. No graceful handling.

**Fix:**
- Use `df.estimated_size()` at runtime before attempting CDC comparison
- If estimated peak memory (3× DataFrame size) exceeds available per-worker budget, either:
  - Switch to watermark-based incremental extraction automatically
  - Chunk the comparison by primary key ranges
- Add a hard ceiling guard that logs ERROR and skips if estimated memory exceeds threshold
- Recommend tables exceeding 8–10M rows be reconfigured as large tables with SourceAggregateColumnName

---

## 5. CDC & Hashing

### H-1 (P1): polars-hash concatenation collision — `("ab","c")` equals `("a","bc")`

**Files:** `bcp_csv.py` (`add_row_hash`)

**Problem:** polars-hash hashes concatenated column values. Without a separator, columns `("ab", "c")` and `("a", "bc")` produce identical hashes. This is a structural collision that occurs regardless of hash function quality.

**Impact:** Deterministic hash collisions for any rows where column boundary values happen to concatenate identically. CDC misclassifies changed rows as unchanged.

**Fix:**
- Use a separator character that cannot appear in data (e.g., `\x1f` unit separator) in the concatenation
- Verify the separator doesn't appear in any source string columns (or escape it if found)
- Add a regression test with known collision cases

---

### H-2 (P1): polars-hash version upgrade silently changes hash output

**Files:** `bcp_csv.py`, `requirements.txt`

**Problem:** polars-hash is explicitly designed for cross-version stability, unlike Polars' native `Expr.hash()`. However, polars-hash hashes the **string representation** of values. If a Polars version upgrade changes how temporal types, decimals, or nulls render as strings, hashes silently change. This would cause every row in the pipeline to appear "changed" on the first run after the upgrade — a mass-update cascade.

**Impact:** Mass false-positive CDC updates after any Polars or polars-hash version upgrade. All rows reclassified as changed, creating millions of unnecessary SCD2 versions.

**Fix:**
- Pin the polars-hash version in `requirements.txt`
- Add a regression test that hashes a known DataFrame (with temporal types, decimals, nulls, and edge-case values) and asserts expected outputs
- Run this regression test on every dependency upgrade before deploying
- Document the pinning requirement in CLAUDE.md

---

### H-3 (P2): xxhash64 collision probability significant at 30M+ rows

**Files:** `bcp_csv.py` (`add_row_hash`), documentation

**Problem:** At 30M rows, birthday paradox yields ~0.0024% collision chance per run — roughly 1 collision per 42 runs. At 100M rows: ~0.027%. At 1B rows: ~2.7%. A collision means a changed row keeps the same hash → CDC classifies it as unchanged → update silently lost.

**Impact:** ~1–2 silently lost updates per year across full pipeline for 3B-row tables. Weekly reconciliation provides eventual correction within 7 days.

**Fix:**
- Document the expected collision rate so the team can make an informed risk decision
- Consider extending hash to 128 bits (two BIGINT columns) — reduces collisions to effectively zero
- Or accept current rate and rely on weekly reconciliation (pragmatic approach)

---

### H-4 (P2): No-op source updates generate phantom CDC work

**Files:** `cdc_polars.py`, documentation

**Problem:** Oracle no-op updates (`UPDATE SET col=col`) still generate redo/undo and change `ORA_ROWSCN`. SQL Server increments its change tracking version. These phantom operations waste source system resources. The hash comparison correctly filters them (hash unchanged), but the extraction still pulls the rows and processes them through the comparison pipeline.

**Impact:** Wasted extraction and comparison work. No data corruption. Correctly handled by hash comparison.

**Fix:**
- Document as known behavior
- Accept as-is — hash comparison provides correct filtering

---

## 6. SCD2 Integrity

### SCD-1 (P0): INSERT-first SCD2 design creates duplicate active records on retry

**Files:** `scd2_polars.py`, Bronze table DDL

**Problem:** If the pipeline design is: (1) BCP INSERT new version rows → COMMIT, (2) UPDATE to close old versions → DEADLOCK → ROLLBACK, (3) retry → BCP INSERT again, the result is two active rows per PK in Bronze. SQL Server deadlock detection runs every 5 seconds, rolls back the entire victim transaction (error 1205), and the pipeline cannot distinguish a first attempt from a retry.

**Impact:** Duplicate active records in Bronze. Downstream consumers see double-counted data. Critical data integrity violation.

**Fix:**
- **Strongest:** Create a unique filtered index: `CREATE UNIQUE INDEX UX_Active ON Bronze(pk1, pk2, ..., pk5) WHERE _cdc_is_current = 1` — retry INSERT fails with a constraint violation (detectable and recoverable)
- **Alternative A:** Wrap INSERT + UPDATE in a single transaction so deadlock rolls back both
- **Alternative B:** Make INSERT idempotent with `NOT EXISTS` check
- **Alternative C:** Add post-hoc dedup check using `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _cdc_valid_from DESC)`
- For the UPDATE isolation level: ensure BCP has committed before starting UPDATE. Avoid SNAPSHOT isolation for the SCD2 UPDATE (it would miss BCP-inserted rows)

---

### SCD-2 (P0): INT IDENTITY overflow on 3B+ row SCD2 table in ~716 days

**Files:** `table_creator.py`, Bronze table DDL

**Problem:** INT max is 2,147,483,647. At 3M inserts/day, overflow occurs in ~716 days. The error is `Msg 8115: Arithmetic overflow error converting IDENTITY to data type int` — the INSERT fails entirely. No partial corruption — the entire batch is rejected.

**Impact:** Complete pipeline halt for the affected table. Requires offline `ALTER TABLE ALTER COLUMN` on a 3B-row table (hours of downtime).

**Fix:**
- Use BIGINT from day one for any SCD2 IDENTITY column (max ~9.2 quintillion, lasting 8.4 trillion years at 3M/day)
- BIGINT adds only 4 bytes per row — negligible at scale
- If already at INT, plan a maintenance window for `ALTER TABLE ALTER COLUMN` before the ~716-day deadline
- Add a monitoring query: `SELECT IDENT_CURRENT('table') / 2147483647.0 AS pct_used` to PipelineEventLog

---

### SCD-3 (P1): Composite PK UPDATE JOINs on 3B rows need batching

**Files:** `scd2_polars.py` (`_execute_bronze_updates`)

**Problem:** A 5-column composite key `UPDATE...FROM...JOIN` on 3B rows runs for hours, generates hundreds of GB of transaction log, and escalates to table locks. The current pipeline issues a single UPDATE JOIN for all affected PKs.

**Impact:** Hours-long UPDATE operations. Transaction log exhaustion. Table-level lock escalation blocks all concurrent access.

**Fix:**
- Batch into 100K–500K row chunks using a temp table of changed PKs
- Add a nonclustered index on Bronze leading with composite PK + `_cdc_is_current`: `CREATE NONCLUSTERED INDEX IX_SCD2 ON Bronze(pk1, pk2, pk3, pk4, pk5, _cdc_is_current) INCLUDE (_cdc_valid_to)`
- Log progress per batch for operational visibility

---

### SCD-4 (P2): pyodbc `cursor.rowcount` returns -1 after `executemany()`

**Files:** `scd2_polars.py`, `cdc_polars.py`

**Problem:** pyodbc issue #481 confirms `cursor.rowcount` returns -1 after `executemany()`. For single `cursor.execute()` of `UPDATE...FROM...JOIN`, rowcount is reliable. If the pipeline ever switches to `executemany()` for SCD2 operations, the P2-14 rowcount validation silently breaks.

**Impact:** Loss of rowcount validation if implementation changes to executemany. Current implementation is safe (uses execute).

**Fix:**
- Document in code comments that `execute()` (not `executemany()`) must be used for SCD2 UPDATE operations
- Prefix SQL batches with `SET NOCOUNT ON` to prevent intermediate message interference
- For critical validation, follow up with `SELECT @@ROWCOUNT`

---

## 7. Oracle Extraction Performance

### O-1 (P1): Oracle `TRUNC(date_col)` imposes 50–60× performance penalty

**Files:** `oracle_extractor.py`, `connectorx_oracle_extractor.py`

**Problem:** `WHERE TRUNC(date_col) = :date` cannot use a standard index on `date_col`. Benchmarks show 0.21 seconds with an index range scan vs 12.32 seconds with `TRUNC()` — a 59× penalty. Oracle treats the function-wrapped column as a different expression.

**Impact:** 50–60× slower extraction for every windowed query using TRUNC. Across 365 backfill days, this compounds to hours of unnecessary extraction time.

**Fix:**
- Rewrite all windowed extraction queries as range predicates: `WHERE date_col >= TRUNC(:start) AND date_col < TRUNC(:end) + INTERVAL '1' DAY`
- This uses standard B*Tree indexes via INDEX RANGE SCAN
- Audit all queries in oracle_extractor.py and connectorx_oracle_extractor.py for TRUNC usage

---

### O-2 (P2): Bind variable peeking causes inconsistent Oracle query plans

**Files:** `oracle_extractor.py`, `connectorx_oracle_extractor.py`

**Problem:** When date ranges vary from 1 day (normal) to 365 days (backfill), Oracle's bind variable peeking optimizes the plan for the first execution's cardinality. Adaptive Cursor Sharing (11g+) eventually compensates, but the first ~10 executions may use a suboptimal plan.

**Impact:** Sporadic slow extraction during initial backfill runs. Self-correcting over time.

**Fix:**
- For initial backfill, consider using literals instead of bind variables
- Or establish SQL Plan Management baselines for the extraction queries
- Accept as self-correcting for steady-state operation

---

## 8. Network & Locking

### N-1 (P1): sp_getapplock sessions die silently on firewall idle timeout

**Files:** `table_lock.py`, system configuration

**Problem:** Default RHEL TCP keepalive waits 7,200 seconds (2 hours) before sending the first probe. Many network firewalls have 60-minute idle timeouts. After 60 minutes of inactivity, the firewall silently drops the connection. The next SQL command fails with `[08S01] Communication link failure`, and the sp_getapplock is released server-side — another session could acquire it, breaking the mutual exclusion guarantee.

**Impact:** Silent lock loss. Concurrent pipeline runs possible after firewall drops the lock-holding connection. Potential data corruption from parallel writes.

**Fix:**
- Set `net.ipv4.tcp_keepalive_time=300` via sysctl (5-minute keepalive)
- Implement heartbeat queries every 5 minutes on the lock-holding connection
- Use ODBC Driver 17.2+ connection resiliency: `ConnectRetryCount=3;ConnectRetryInterval=10` in the connection string
- Add connection health check before critical operations

---

## 9. Schema & Type Safety

### T-1 (P1): DST fall-back creates irrecoverable 1-hour ambiguity in Oracle DATE conversions

**Files:** `oracle_extractor.py`, `connectorx_oracle_extractor.py`, `cdc_polars.py`

**Problem:** Oracle DATE stores no timezone. During DST fall-back, 1:30 AM local time could be UTC-4 (EDT) or UTC-5 (EST). The pipeline cannot determine which. If CDC metadata timestamps derive from Oracle DATE values, the 1-hour ambiguity can cause incorrect ordering of changes during the fall-back window.

**Impact:** Potential incorrect CDC ordering during 1-hour DST fall-back windows twice per year. Affects tables with Oracle DATE-based extraction timestamps.

**Fix:**
- Store all CDC metadata timestamps (`_cdc_valid_from`, `_cdc_valid_to`) as UTC using SQL Server's DATETIMEOFFSET type
- Generate these timestamps in the pipeline itself (`datetime.utcnow()`) rather than relying on source system times
- Document the DST ambiguity for any source-derived timestamps

---

### T-2 (P1): Polars `diagonal_relaxed` concat has documented crash bugs

**Files:** `cdc_polars.py`

**Problem:** Polars issue #12543 reports Python crashes (not exceptions — full interpreter termination) when DataFrames have large differences in column count. Issue #18911 documents List column dtype errors in lazy mode. These are full process kills with no recovery.

**Impact:** Unrecoverable crash during CDC concat operations. More likely with schema evolution producing column count differences.

**Fix:**
- Pre-align schemas manually before concat — add missing columns as explicit null columns
- Use eager mode rather than lazy for `diagonal_relaxed` operations on 200+ column DataFrames
- Monitor Polars issue tracker for fixes and pin to a version without known crash bugs

---

### T-3 (P2): Oracle metadata-only DEFAULT changes trigger false CDC changes

**Files:** `cdc_polars.py`, documentation

**Problem:** Oracle's metadata-only DEFAULT values (11g+) are transparent to `SELECT *` — existing rows return the default without physical updates. However, if the default definition is later changed, old rows retain the original metadata default while new rows get the new default. This produces legitimate data differences that the pipeline correctly detects as CDC changes, but they're unexpected from the source system perspective.

**Impact:** Unexpected CDC update volume after an Oracle DEFAULT change. Not incorrect, but operationally surprising.

**Fix:**
- Document as known behavior in CLAUDE.md
- After any source DEFAULT change, expect a one-time CDC update surge for affected rows
- Accept as-is — the pipeline is correctly detecting real data differences

---

## Summary

| ID | Priority | Category | Edge Case | Status |
|----|----------|----------|-----------|--------|
| E-1 | P0 | Extraction | ConnectorX torn reads on active tables | DONE — partition_num forced to 1 in all 4 extractors |
| E-2 | P0 | Extraction | ConnectorX drops NULL partition column rows | DONE — _supplement_null_partition_rows() in both extractors |
| E-3 | P1 | Extraction | ConnectorX partition skew | DONE — mitigated by E-1 (partition_num=1 sidesteps skew) |
| E-4 | P1 | Extraction | Oracle DATE sentinel crashes ConnectorX | DONE — _build_safe_select_with_uri() wraps DATE columns in CASE WHEN to NULL-ify sentinels |
| E-5 | P2 | Extraction | ConnectorX connection leak exhausts sessions | DONE — mitigated by E-1 (2 connections vs 7+); M-3 FD monitoring; documented |
| B-1 | P0 | BCP Loading | quote_style='never' corrupts tabs/newlines | DONE — sanitize_strings() pre-strips; _validate_sanitized() in write_bcp_csv() |
| B-2 | P2 | BCP Loading | Minimal logging impossible on non-empty tables | DONE — documented in bcp_loader.py; bulk_load_recovery_context() provides BULK_LOGGED |
| B-3 | P2 | BCP Loading | TABLOCK blocks concurrent access | DONE — TABLOCK not used (no -h flag); concurrent access already allowed; documented |
| M-1 | P0 | Memory | glibc arena fragmentation causes OOM | DONE — MALLOC_ARENA_MAX=2 + POLARS_MAX_THREADS=1 in both main scripts |
| M-2 | P1 | Memory | 6 workers exceed 64 GB memory budget | DONE — default workers=4; warning when >4 |
| M-3 | P1 | Memory | File descriptor leak across iterations | DONE — psutil FD monitoring in _check_memory_pressure() |
| ST-1 | P1 | Small Tables | OOM at 8–10M rows during full CDC | DONE — _check_small_table_memory() returns False at 20 GB ceiling; process_small_table skips |
| H-1 | P1 | Hashing | Concatenation collision in polars-hash | DONE — already mitigated by P0-6 (\x1f separator) |
| H-2 | P1 | Hashing | polars-hash version upgrade changes output | DONE — pinned polars-hash==0.4.5 in requirements.txt |
| H-3 | P2 | Hashing | xxhash64 collision probability at scale | DONE — collision rate documented in add_row_hash() C-1 comment; weekly reconciliation covers |
| H-4 | P2 | Hashing | No-op source updates create phantom work | DONE — hash comparison correctly filters; documented in cdc_polars.py; accept as-is |
| SCD-1 | P0 | SCD2 | INSERT-first retry creates duplicate active rows | DONE — ensure_bronze_unique_active_index() in both orchestrators |
| SCD-2 | P0 | SCD2 | INT IDENTITY overflow in ~716 days | DONE — already BIGINT IDENTITY(1,1); documented in table_creator.py |
| SCD-3 | P1 | SCD2 | Composite PK UPDATE JOIN needs batching | DONE — _execute_bronze_updates() batches via UPDATE TOP(500K) loop |
| SCD-4 | P2 | SCD2 | pyodbc rowcount -1 after executemany | DONE — code uses execute() (safe); protective comments added to all UPDATE operations |
| O-1 | P1 | Oracle | TRUNC(date_col) 50–60× performance penalty | DONE — all WHERE clauses use range predicates instead of TRUNC(col) |
| O-2 | P2 | Oracle | Bind variable peeking inconsistent plans | DONE — ConnectorX uses literals (no peeking); oracledb self-corrects via ACS; documented |
| N-1 | P1 | Network | sp_getapplock silent loss on firewall timeout | DONE — _get_resilient_lock_connection() with ConnectRetryCount=3 |
| T-1 | P1 | Schema | DST fall-back 1-hour ambiguity | DONE — pipeline uses datetime.now(timezone.utc) for all CDC timestamps |
| T-2 | P1 | Schema | diagonal_relaxed crash bugs | DONE — _safe_concat() pre-aligns schemas; replaces all diagonal_relaxed uses |
| T-3 | P2 | Schema | Oracle DEFAULT changes trigger false CDC | DONE — pipeline correctly detects real differences; documented in cdc_polars.py; accept as-is |

### Top 6 by urgency (ALL DONE):

1. **B-1 (P0):** DONE — sanitize_strings() pre-strips \t\n\r\x00; _validate_sanitized() raises on unsanitized data in write_bcp_csv()
2. **E-1 (P0):** DONE — partition_num forced to 1 in all 4 ConnectorX extractor functions
3. **SCD-1 (P0):** DONE — ensure_bronze_unique_active_index() creates unique filtered index on (pk_columns) WHERE UdmActiveFlag = 1
4. **M-1 (P0):** DONE — MALLOC_ARENA_MAX=2 and POLARS_MAX_THREADS=1 set before any imports in both main scripts
5. **SCD-2 (P0):** DONE — already uses BIGINT IDENTITY(1,1); documented in table_creator.py
6. **E-2 (P0):** DONE — _supplement_null_partition_rows() adds NULL partition rows via supplementary query in both extractors

### All 12 P1 items (ALL DONE):

1. **E-3:** Mitigated by E-1 (partition_num=1 makes skew moot)
2. **E-4:** _build_safe_select_with_uri() wraps DATE columns in CASE WHEN to NULL-ify sentinel dates >= 9999-01-01
3. **M-2:** Default --workers=4; warning when >4 on 64 GB systems
4. **M-3:** psutil FD monitoring (warn at 500, error at 800) in _check_memory_pressure()
5. **ST-1:** _check_small_table_memory() hard ceiling at 20 GB; process_small_table returns False
6. **H-1:** Already mitigated by P0-6 (\x1f unit separator in hash concatenation)
7. **H-2:** polars-hash pinned to ==0.4.5 in requirements.txt
8. **SCD-3:** _execute_bronze_updates() batches via UPDATE TOP(500K) loop with progress logging
9. **O-1:** All Oracle WHERE clauses use range predicates (date_col >= :start AND date_col < :end) instead of TRUNC(date_col)
10. **N-1:** _get_resilient_lock_connection() adds ConnectRetryCount=3, ConnectRetryInterval=10
11. **T-1:** Pipeline uses datetime.now(timezone.utc) for all CDC timestamps (no source-derived times)
12. **T-2:** _safe_concat() pre-aligns schemas before vertical concat; replaces all diagonal_relaxed uses

### All 8 P2 items (ALL DONE):

1. **E-5:** Mitigated by E-1 (partition_num=1 → 2 connections instead of 7+); M-3 FD monitoring provides leak detection
2. **B-2:** SQL Server operational reality — documented in bcp_loader.py; bulk_load_recovery_context() provides BULK_LOGGED
3. **B-3:** TABLOCK not used in BCP command (no `-h` flag); concurrent access already allowed by default
4. **H-3:** Collision rate already documented in add_row_hash() C-1 comment with full math; weekly reconciliation covers
5. **H-4:** Hash comparison correctly filters no-op updates; no code change needed; documented in cdc_polars.py
6. **SCD-4:** Pipeline uses cursor.execute() (reliable rowcount), not executemany(); protective comments added
7. **O-2:** ConnectorX uses literals (no bind peeking); oracledb self-corrects via Adaptive Cursor Sharing; documented
8. **T-3:** Pipeline correctly detects Oracle DEFAULT changes as real data differences; documented in cdc_polars.py

**ALL 26 EDGE CASES FROM FIFTH AUDIT ARE NOW RESOLVED (6 P0 + 12 P1 + 8 P2).**

*Updated: 2026-02-23*
*Source: "Edge Cases in Production-Scale Polars-to-SQL-Server Pipelines: A Systematic Investigation" — research-backed fifth audit*
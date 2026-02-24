# CDC/SCD2 Pipeline — Action Items (Billion-Row Scale Validation Research)

*Context: Derived from "Validating a CDC/SCD2 Pipeline at Billion-Row Scale: Hash Collision Risk, Lock Escalation, and Architectural Best Practices" — a validation of the existing pipeline against production best practices at 3+ billion row scale. Cross-referenced against the current codebase and existing TODOs (W-series, E-series). Items already fully addressed by prior work are excluded. Items that contradict or refine prior assessments are noted explicitly.*

*Numbering: B-series (Billion-row scale research). Priority classification follows the established convention: P0 = data corruption, P1 = production reliability, P2 = performance/operational, P3 = improvements.*

---

## 1. Critical — Data Corruption (P0)

### B-1 (P0): 64-bit hash truncation creates ~24% collision probability at 3 billion rows — SHOWSTOPPER

**Files:** `bcp_csv.py` (`add_row_hash`, `add_row_hash_fallback`), `engine.py` (CDC comparison), Bronze schema

**Problem:** SHA-256 produces 256 bits but the pipeline truncates to Int64 (64 bits), reducing the effective hash space to 2^64 ≈ 1.84 × 10^19 values. The birthday paradox formula gives collision probability ≈ n² / (2 × 2^64). At 3 billion rows: (3 × 10^9)² / (3.69 × 10^19) ≈ **0.244 — a ~24.4% chance of at least one hash collision** across the dataset. At 5 billion rows this reaches 50%.

A hash collision in CDC means two genuinely different rows produce the same hash value, causing a **changed row to be incorrectly classified as unchanged**. This is silent data loss — no error, no alert, just a missed update propagating downstream forever. The reconciliation layer would eventually catch it during weekly column-by-column reconciliation, but daily hash-based CDC remains blind.

**⚠️ This contradicts the prior assessment in the existing TODO's "Validated architecture decisions" section, which states "SHA-256 truncated to 64-bit is safe for per-PK CDC comparison (birthday paradox doesn't apply)." That assessment assumed per-PK comparison never accumulates collision risk across the full dataset. The research from Daniel Lemire, John D. Cook, Preshing birthday probability tables, and the Scalefree Data Vault community confirms the collision probability applies to the total distinct hash values stored across Bronze, not just per-PK comparisons.**

**Impact:** ~1-in-4 chance of at least one silent missed change across the 3B-row dataset. The missed update propagates downstream permanently. This is the single highest-severity issue identified in any research pass.

**Fix — Option A (recommended: full SHA-256 as string):**
- The polars-hash plugin already computes the full 256-bit SHA-256, outputting it as a 64-character hex string
- Store and compare the full hash as `VARCHAR(64)` or `CHAR(64)` in Stage and Bronze instead of `BIGINT`
- Update `add_row_hash()` and `add_row_hash_fallback()` to output the hex string directly instead of casting to Int64
- Update CDC comparison in `engine.py` to compare string hashes instead of integer hashes
- Full SHA-256 at 256 bits is safe up to ~9.2 × 10^25 rows — effectively infinite
- Storage cost: 64 bytes vs 8 bytes per row — negligible against data integrity at this scale

**Fix — Option B (minimum: 128-bit hash):**
- Use the first 32 hex characters of the SHA-256 output, stored as `CHAR(32)` or `BINARY(16)`
- 128-bit hash is safe up to ~325 billion rows at 50% collision probability
- The Scalefree Data Vault community explicitly recommends MD5 (128-bit) as the minimum for production hashing and calls 64-bit hashes "a risky choice"

**Migration considerations:**
- Schema change required on all Stage and Bronze tables (alter `UdmHash` column from `BIGINT` to `VARCHAR(64)` or `CHAR(32)`)
- All existing hash values must be recomputed — a one-time full reload of Bronze hash columns
- CDC comparison logic must change from integer equality to string equality
- Performance impact of string comparison vs integer comparison is minimal at these volumes given index support

---

## 2. High — Production Reliability (P1)

### B-2 (P1): UPDATE TOP(500K) triggers lock escalation, overriding RCSI — REVISES E-9

**Files:** `engine.py` (`_execute_bronze_updates`), `config.py`

**Problem:** SQL Server escalates from row locks to a **table-level exclusive lock when a single statement acquires approximately 5,000 locks**. `UPDATE TOP(500000)` exceeds this threshold by ~100×, guaranteeing lock escalation every batch. The new research clarifies a critical detail: **a table-level exclusive lock overrides RCSI**, making RCSI's non-blocking reader benefits irrelevant during the update. This contradicts the E-9 assessment which concluded "if RCSI is enabled, this is a non-issue for readers."

Multiple SQL Server experts (Brent Ozar, SQLServerCentral, MSSQLTips) converge on **2,000–5,000 rows per batch** to stay below the escalation threshold.

**Impact:** Every SCD2 UPDATE batch acquires a table-level exclusive lock that blocks ALL concurrent readers and writers, even under RCSI, for the duration of each 500K-row batch. For a 3-million-row daily close set, this means 6 batches of prolonged blocking.

**Fix:**
- Reduce `UPDATE TOP` batch size from 500,000 to **4,000 rows** in `_execute_bronze_updates()`
- Update `config.py` batch size constant accordingly
- Use a `WHILE` loop pattern with explicit `COMMIT` between each batch to release locks and allow transaction log reuse
- For 3 million daily rows: ~750 iterations of ~4,000 rows each. Each completes in milliseconds with no lock escalation, vs 6 iterations of 500K with table-level locks
- Optional: add `WAITFOR DELAY '00:00:00.050'` (50ms) between batches to reduce I/O contention during heavy periods
- **Alternative structural fix:** Table partitioning with `LOCK_ESCALATION = AUTO` enables partition-level escalation instead of table-level. This is a better long-term solution for billion-row tables but requires more planning.
- Do **not** use `ALTER TABLE SET (LOCK_ESCALATION = DISABLE)` — this risks memory pressure from millions of individual row locks at billion-row scale

---

### B-3 (P1): Schema evolution strategy is entirely missing — most cited operational pain point

**Files:** `evolution.py`, `column_sync.py`, `engine.py`, orchestrators

**Problem:** The pipeline has no documented strategy for handling schema changes (new columns, dropped columns, changed data types). The research identifies this as the **single most commonly cited operational pain point** in CDC/SCD2 post-mortems. Debezium DDL parsing failures, Netflix schema propagation overhead, and dbt snapshot bugs with new columns all point to schema evolution as a top-tier failure mode.

Critical interaction: For this pipeline's hash-based comparison, **any schema change that affects the hash column set will cause every row to appear as changed** in the next run, potentially triggering the explosion guard. This interaction is currently unhandled.

**Impact:** Schema changes can cause unexpected full-table reprocessing, explosion guard trips, pipeline failures, or silently incorrect CDC comparisons if column sets drift between source and target.

**Fix — Detection:**
- Add a schema comparison check before each CDC run in the orchestrators
- Compare source schema (from `validate_source_schema()` in `evolution.py`) against the expected column set used for hashing
- If source schema differs from expected schema, **block processing** and require explicit acknowledgment before proceeding
- Log the specific differences (added columns, dropped columns, type changes)

**Fix — New columns:**
- Add to Bronze with `NULL` default for all existing rows
- Update hash computation to include the new column
- All historical rows retain NULL for the new column — this is correct SCD2 behavior
- **Critical:** The first CDC run after adding a column will see every row as changed (hash includes new column with NULL vs. old hash without it). This must be handled as a known "schema migration run" that bypasses the explosion guard

**Fix — Dropped source columns:**
- Keep in Bronze for historical accuracy
- New rows carry NULL or a sentinel value for the dropped column
- Never physically drop columns from SCD2 tables

**Fix — Changed data types:**
- Add a new column with the new type rather than altering in place
- Mark the old column as deprecated
- Update hash computation to use the new column

**Fix — Explosion guard interaction:**
- Add a `schema_migration_mode` flag that bypasses the explosion guard for the first run after a schema change
- Log clearly that the full-table update is expected due to schema migration, not data corruption
- Consider pre-computing new hashes in a background process before the official cutover

---

### B-4 (P1): Crash recovery may not match orphaned Flag=0 rows if source data changes

**Files:** `engine.py` (dedup recovery logic)

**Problem:** The INSERT-first SCD2 pattern's crash recovery relies on dedup on the next run to clean up orphaned rows. However, if a crash occurs between INSERT (with `UdmActiveFlag=0`) and activation, and the source data **changes before the recovery run**, the orphaned Flag=0 rows may no longer match the updated source data. The dedup logic should verify it handles this case explicitly.

**Impact:** Orphaned Flag=0 rows from a previous crashed run could conflict with new data in the recovery run, potentially creating incorrect version chains or leaving stale inactive rows in Bronze.

**Fix:**
- Review the dedup recovery logic in `engine.py` to verify it handles the case where:
  1. Run N crashes after INSERT, leaving Flag=0 rows with `UdmEndDateTime IS NULL`
  2. Source data changes between Run N crash and Run N+1
  3. Run N+1 must correctly handle the orphaned Flag=0 rows from Run N alongside the new source data
- The recovery should either: (a) detect and remove orphaned Flag=0 rows before processing new data, or (b) treat orphaned rows as part of the current run's insert set
- Add explicit detection of orphaned Flag=0 rows at the start of each run:
  ```sql
  SELECT COUNT(*) FROM Bronze.table
  WHERE UdmActiveFlag = 0 AND UdmEndDateTime IS NULL
  ```
- If orphaned rows exist, log a warning and handle before proceeding with the normal CDC flow

---

### B-5 (P1): Polars join validation bug with NULL keys — false errors possible

**Files:** `engine.py` (CDC join operations)

**Problem:** Polars GitHub issue #19624 documents a bug where `validate="1:m"` (or other join cardinality validation) **incorrectly fails when NULL values exist in join keys** even with `join_nulls=False`. If the pipeline uses join validation parameters on any of its anti-join or inner-join CDC operations, this bug could cause false errors that halt processing.

**Impact:** Intermittent false errors during CDC processing for tables with any NULL values in join key columns, despite the pipeline correctly filtering NULL PKs before joins.

**Fix:**
- Audit all join operations in `engine.py` for use of `validate` parameter
- If `validate` is used, either:
  - Confirm NULL PKs are filtered before the join (the pipeline already does this, so verify the filter applies before validation)
  - Remove `validate` parameter and rely on the existing count validation invariant check instead
  - Pin to a Polars version where this bug is fixed (check issue #19624 for resolution status)
- Document the known Polars issue in code comments referencing #19624

---

## 3. Performance / Operational (P2)

### B-6 (P2): BCP should sanitize additional Unicode line-break characters

**Files:** `bcp_csv.py` (`sanitize_strings`)

**Problem:** The current string sanitization strips `\t`, `\n`, `\r`, and `\x00` — the primary BCP danger characters. However, BCP can also be disrupted by Unicode line-break characters that the current sanitization misses: vertical tab (`\x0B`), form feed (`\x0C`), NEL (`\x85`), and Unicode line/paragraph separators (`U+2028`, `U+2029`).

**Impact:** Rare but possible data corruption or BCP load failure if source data contains these characters. More likely in internationalized data, CMS-sourced content, or data migrated from legacy systems.

**Fix:**
- Extend `sanitize_strings()` to strip the additional Unicode line-break characters:
  ```python
  # B-6: Extended BCP character sanitization
  # Add to existing \t \n \r \x00 stripping:
  additional_chars = ["\x0B", "\x0C", "\x85", "\u2028", "\u2029"]
  ```
- Also verify that `-C 65001` (UTF-8 encoding) is specified on BCP commands to avoid character set issues during loads

---

### B-7 (P2): ConnectorX errors crash via Rust panics — no graceful recovery

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`, `udm_connectorx_extractor.py`

**Problem:** ConnectorX errors manifest as **Rust thread panics rather than Python exceptions**, meaning a single bad row can crash the entire extraction process without graceful error handling. Additionally, ConnectorX pre-allocates memory based on `SELECT COUNT(*)` before extraction — if source data changes during extraction, row count mismatches can occur.

Known ConnectorX issues:
- Oracle TIMESTAMP columns can be truncated to Date type (losing time components) in certain versions
- SQL Server datetime/datetime2 extraction causes `PanicException` in ConnectorX 0.3.3+ (fixed by reverting to 0.3.2 or using newer alpha versions)

**Impact:** Unhandled Rust panics can crash the pipeline without logging or alerting, making diagnosis difficult. Row count mismatches during extraction can cause memory allocation issues.

**Fix:**
- Wrap ConnectorX calls in **subprocess isolation** so that a Rust panic kills only the subprocess, not the main pipeline process
- Or wrap in explicit try/except that catches `BaseException` (Rust panics may not inherit from `Exception`)
- Add retry logic with exponential backoff for transient ConnectorX failures
- Validate extracted row counts against expected counts after extraction completes
- Document the ConnectorX version requirements and known issues in CLAUDE.md

---

### B-8 (P2): Polars memory not released to OS — gradual exhaustion during backfills

**Files:** All processing modules, `config.py`

**Problem:** Polars has a documented memory management issue (GitHub issue #23128) where Rust's allocator does not release memory back to the OS after heavy DataFrame operations. For a pipeline processing 3 million daily rows on a 64GB server, this could cause **gradual memory exhaustion across processing days during backfills**.

**Impact:** During multi-day backfill operations, RSS memory grows monotonically even as DataFrames are dropped, eventually causing OOM kills.

**Fix:**
- Consider compiling Polars with jemalloc allocator, which handles memory release more aggressively
- Alternatively, run extraction/processing in **separate child processes** (using `multiprocessing` or `subprocess`) so that OS-level memory reclaim occurs when the child process exits
- Add RSS memory monitoring between table processing iterations:
  ```python
  import psutil
  rss_gb = psutil.Process().memory_info().rss / (1024**3)
  if rss_gb > config.MAX_RSS_GB * 0.85:
      logger.warning("B-8: RSS at %.1fGB — approaching limit", rss_gb)
  ```
- For backfill operations specifically, consider processing in batches of N tables per process invocation

---

### B-9 (P2): Alerting thresholds need refinement — dynamic baselines and severity tiers

**Files:** `reconciliation.py`, orchestrators

**Problem:** Two alerting thresholds identified in the research warrant refinement:

1. **48-hour freshness warning should be a critical alert** for daily pipelines — it means two full refresh cycles were missed. The current single threshold doesn't distinguish warning from critical severity.

2. **>90% empty extraction guard is too aggressive** — some tables legitimately have low-volume days (holidays, weekends). Static thresholds produce false positives.

**Impact:** Alert fatigue from false positives on low-volume days; insufficient urgency on genuine freshness violations.

**Fix:**
- **Freshness:** Implement two-tier alerting:
  - Warning at 1.5× expected refresh interval (36 hours for daily tables)
  - Critical at 2× expected refresh interval (48 hours for daily tables)
- **Empty extraction:** Replace static >90% threshold with dynamic thresholds based on a **30-day rolling average with day-of-week adjustment**:
  ```sql
  -- Calculate expected row count baseline
  SELECT AVG(row_count) as baseline
  FROM PipelineEventLog
  WHERE table_name = @table
    AND DATEPART(dw, event_datetime) = DATEPART(dw, GETDATE())
    AND event_datetime > DATEADD(day, -30, GETDATE())
  ```
  - Alert when extraction count is below e.g. 10% of the day-of-week baseline, rather than an absolute percentage

---

## 4. Improvements (P3)

### B-10 (P3): Distribution shift monitoring — catch subtle drift that counts and sums miss

**Files:** `reconciliation.py`

**Problem:** The current reconciliation covers counts, column-by-column comparison, PK reconciliation, aggregates, version velocity, phantom changes, active-to-total ratio, and data freshness. However, it does not detect **distribution shifts** — scenarios where individual values shift but aggregate totals remain stable. Uber's data quality platform (UDQ) specifically highlighted this as a key detection mechanism that catches ~90% of data quality incidents.

**Impact:** Subtle data drift (e.g., a demographic distribution shifting, prices clustering differently) goes undetected until downstream analysis produces incorrect results.

**Fix:**
- Track statistical distributions (mean, standard deviation, P25/P50/P75 percentiles) of key numeric columns over time
- Compare current run statistics against a rolling baseline
- Alert when distributions shift beyond configurable thresholds (e.g., >2 standard deviations from 30-day rolling mean)
- Implement as a periodic reconciliation job, not every-run (weekly is sufficient)

---

### B-11 (P3): Reconciliation at transformation boundaries (Bronze→Silver, Silver→Gold)

**Files:** `reconciliation.py`, Silver/Gold processing modules

**Problem:** The current reconciliation validates source-to-Stage and Stage-to-Bronze, but does not validate **Bronze→Silver and Silver→Gold boundaries**. Issues introduced by transformation logic (joins, aggregations, business rules) are not caught until downstream consumers report problems. DQOps and Acceldata both recommend the "circuit breaker" pattern at every layer boundary, blocking bad data from propagating downstream.

**Impact:** Transformation bugs in Silver/Gold layers propagate undetected to business consumers.

**Fix:**
- Add reconciliation checks at each transformation boundary:
  - Bronze→Silver: row count deltas, key existence checks, NULL rate comparisons
  - Silver→Gold: aggregate validation, referential integrity verification
- Implement a "circuit breaker" pattern: if reconciliation fails at a boundary, block downstream processing until the issue is resolved
- Start with lightweight count-based checks; expand to full statistical validation over time

---

### B-12 (P3): Cross-table referential integrity checks for SCD2 surrogate key lookups

**Files:** `reconciliation.py`

**Problem:** SCD2 surrogate key lookups are a known source of silent incorrect joins. Foreign key relationships between fact and dimension tables must survive the CDC/SCD2 process, but temporal joins (requiring `BETWEEN` logic on effective/end dates) are error-prone. Production post-mortems note: "every join needs that BETWEEN logic; miss one and your results are silently wrong."

**Impact:** Incorrect fact-to-dimension joins produce silently wrong results in downstream analytics.

**Fix:**
- Add cross-table referential integrity checks that verify FK relationships survive SCD2 processing
- For temporal dimension lookups, validate that every fact row's FK resolves to exactly one active or temporally-correct dimension row
- Alert on orphaned FKs (fact rows pointing to no dimension row) and ambiguous FKs (resolving to multiple dimension rows)

---

### B-13 (P3): Cross-platform type conversion gaps — six additional Oracle→SQL Server pitfalls

**Files:** `connectorx_oracle_extractor.py`, `schema_utils.py`, `column_sync.py`

**Problem:** Six additional Oracle-to-SQL Server type conversion pitfalls identified beyond what the pipeline currently handles:

1. **Oracle DATE always includes time** (hours, minutes, seconds). Mapping to SQL Server `date` silently truncates time. Correct mapping: `datetime2(0)`
2. **Oracle NUMBER without specified precision** can hold up to 38 digits. Default mapping to `DECIMAL(38,8)` silently truncates values with more than 8 decimal places
3. **Oracle FLOAT(126)** provides ~38 decimal digits; SQL Server `FLOAT(53)` provides only ~15 — **significant precision loss** for scientific/financial data
4. **Oracle RAW(16) as GUID** uses big-endian byte ordering; SQL Server `UNIQUEIDENTIFIER` uses **mixed-endian**. Direct byte copy produces different GUID string representations, breaking joins
5. **Oracle BLOB/CLOB** supports 4GB; SQL Server `VARBINARY(MAX)`/`VARCHAR(MAX)` supports 2GB. Data exceeding 2GB is silently truncated
6. **Character length semantics differ**: Oracle `VARCHAR2(100 BYTE)` in AL32UTF8 may hold only 33 multi-byte characters, while SQL Server `VARCHAR(100)` holds 100 bytes

**Impact:** Most are rare edge cases, but items 1 (DATE→date truncation) and 4 (GUID byte ordering) can cause silent data corruption in common scenarios.

**Fix:**
- Audit `schema_utils.py` and `column_sync.py` type mapping logic for each of the six pitfalls
- Add explicit Oracle DATE → `datetime2(0)` mapping (never `date`)
- Add Oracle RAW(16) GUID byte-reordering during extraction if GUIDs are used as join keys
- Document Oracle NUMBER precision limitations and BLOB/CLOB size limits in CLAUDE.md
- Add character length validation for multi-byte character columns

---

### B-14 (P3): INSERT-first zero-active-row window — document and mitigate

**Files:** `engine.py`, CLAUDE.md

**Problem:** The INSERT-first SCD2 pattern (insert new versions with `UdmActiveFlag=0`, close old versions, activate new versions) creates a window where — between the close of the old row and the activation of the new row — queries filtering on `UdmActiveFlag=1` will see **zero active rows** for affected PKs. Under RCSI, readers in-flight before the operation started see consistent pre-operation data, but new readers during the window see the gap.

**Impact:** Transient data gaps visible to new queries during the SCD2 update window. Acceptable for batch analytics; potentially problematic for near-real-time consumers.

**Fix:**
- Document this behavior explicitly in CLAUDE.md and engine.py docstrings
- For critical consumers, recommend the defensive query pattern:
  ```sql
  -- Handles both the zero-active-row window and the RCSI inconsistency window
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC
    ) AS rn
    FROM Bronze.table
  ) t WHERE rn = 1
  ```
- Consider minimizing the window by combining the close-old and activate-new steps as tightly as possible

---

## Summary

| ID | Priority | Category | Issue | Status |
|----|----------|----------|-------|--------|
| B-1 | P0 | Hash Integrity | 64-bit hash truncation — ~24% collision at 3B rows | DONE — full SHA-256 VARCHAR(64), migration script in migrations/b1_hash_varchar64.py |
| B-2 | P1 | Lock Escalation | UPDATE TOP(500K) table lock overrides RCSI | DONE — batch size reduced to 4K via config.SCD2_UPDATE_BATCH_SIZE |
| B-3 | P1 | Schema Evolution | No strategy for column adds/drops/type changes | DONE — SchemaEvolutionResult propagated to orchestrators, E-12 warning conditioned |
| B-4 | P1 | Crash Recovery | Orphaned Flag=0 rows with changed source data | DONE — _cleanup_orphaned_inactive_rows() runs before each SCD2 comparison |
| B-5 | P1 | Polars Bug | Join validation false errors with NULL keys (#19624) | DONE — audited: no validate param used, _filter_null_pks() prevents exposure |
| B-6 | P2 | BCP Safety | Missing Unicode line-break character sanitization | DONE — sanitize_strings() extended with \x0B, \x0C, \x85, \u2028, \u2029 |
| B-7 | P2 | Extraction | ConnectorX Rust panics crash without recovery | DONE — cx_read_sql_safe() wrapper in extract/__init__.py with BaseException catch + exponential backoff retry |
| B-8 | P2 | Memory | Polars allocator doesn't release RSS during backfills | DONE — _check_rss_memory() in both main_*.py, config.MAX_RSS_GB (default 48) |
| B-9 | P2 | Monitoring | Alerting thresholds need dynamic baselines | DONE — two-tier freshness (36h warn, 48h error), day-of-week aware extraction guard baselines |
| B-10 | P3 | Reconciliation | Distribution shift monitoring | DONE — detect_distribution_shift() in reconciliation.py with z-score alerting |
| B-11 | P3 | Reconciliation | Transformation boundary checks (Bronze→Silver→Gold) | DONE — reconcile_transformation_boundary() in reconciliation.py with circuit breaker pattern |
| B-12 | P3 | Reconciliation | Cross-table referential integrity for SCD2 joins | DONE — check_referential_integrity() in reconciliation.py with temporal + non-temporal modes |
| B-13 | P3 | Type Safety | Six Oracle→SQL Server type conversion gaps | DONE — pl.Date→pl.Datetime upcast in fix_oracle_date_columns(), type pitfalls documented in CLAUDE.md |
| B-14 | P3 | Documentation | INSERT-first zero-active-row window | DONE — B-14 NOTE block in scd2/engine.py module docstring |

### Top priorities:

1. **B-1 (P0):** Increase hash width to 128+ bits — ~24% silent data loss probability is a showstopper
2. **B-2 (P1):** Reduce UPDATE batch size from 500K to 4,000 — table locks override RCSI (revises E-9 assessment)
3. **B-3 (P1):** Add schema evolution strategy — most cited CDC/SCD2 operational failure mode
4. **B-4 (P1):** Verify crash recovery handles orphaned rows with changed source data
5. **B-5 (P1):** Audit join validation for Polars NULL key bug

### Key revisions to prior assessments:

- **E-9 (UPDATE batch size):** Previously marked DONE with RCSI verification. The new research clarifies that table-level exclusive locks **override RCSI**, making the 500K batch size problematic even with RCSI enabled. B-2 reopens this item with the correct batch size recommendation (4,000 rows).
- **"SHA-256 truncated to 64-bit is safe" (Validated architecture decisions):** This prior assessment is **incorrect at billion-row scale**. The birthday paradox collision probability applies to the full set of stored hashes, not just per-PK comparisons. B-1 addresses this with the hash width upgrade.

### Validated architecture decisions (confirmed by this research):

- Three-way anti-join CDC pattern is textbook-correct
- MERGE avoidance strongly validated (Aaron Bertrand bug catalogue, Paul White February 2025 data corruption bug)
- INSERT-first ordering is the correct crash-recovery pattern for filtered unique index
- NULL PK filtering, PK dtype alignment, count validation invariant — all exceed published standards
- NULL sentinel with `\x1F` wrapping, float normalization, Unicode NFC, Oracle empty-string normalization — more thorough than any published framework
- Nine-element reconciliation strategy is more comprehensive than Uber's UDQ platform
- sp_getapplock with session locks, per-day checkpointing, idempotent operations — all validated

*Source: "Validating a CDC/SCD2 Pipeline at Billion-Row Scale: Hash Collision Risk, Lock Escalation, and Architectural Best Practices" — validated against Daniel Lemire, John D. Cook, Preshing birthday tables, Scalefree Data Vault community, Brent Ozar, SQLServerCentral, MSSQLTips, Uber UDQ, DQOps, Acceldata, Aaron Bertrand, Paul White, Hugo Kornelis, Michael J. Swart, dbt, Databricks, Snowflake, Netflix, and Debezium/Zalando post-mortems.*
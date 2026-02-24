# Validating a CDC/SCD2 pipeline at billion-row scale

**This pipeline is exceptionally well-engineered, with one critical flaw: the 64-bit hash truncation creates a ~24% collision probability at 3 billion rows, meaning there's roughly a 1-in-4 chance of at least one silent missed change.** Beyond that showstopper, only two other issues rise to "must fix" severity — the UPDATE batch size and a handful of cross-platform type conversion gaps. The remaining architecture decisions are strongly validated by current literature, and in several areas (reconciliation depth, crash recovery, NULL/float normalization) this pipeline exceeds published best practices from Kimball, dbt, Databricks, and production post-mortems at Netflix, Uber, and others.

---

## The 64-bit hash truncation is a showstopper at this scale

The most critical finding from this validation is mathematical, not architectural. SHA-256 produces 256 bits of output, but truncating to Int64 (64 bits) reduces the effective hash space to **2^64 ≈ 1.84 × 10^19 possible values**. The birthday paradox formula gives the collision probability as approximately *p ≈ n² / (2 × 2^64)*. With 3 billion rows, that yields *(3 × 10^9)² / (3.69 × 10^19) ≈ 0.244* — a **~24.4% chance of at least one hash collision** across the dataset. At 5 billion rows, you hit 50%. This calculation is confirmed by Daniel Lemire, John D. Cook, the Preshing birthday probability tables, and the Scalefree Data Vault community.

A hash collision in CDC means two genuinely different rows produce the same hash value, causing **a changed row to be incorrectly classified as unchanged**. This is silent data loss — no error, no alert, just a missed update propagating downstream forever. The pipeline's reconciliation layer would eventually catch it during the weekly column-by-column reconciliation, but the daily hash-based CDC would remain blind to the discrepancy.

The fix is straightforward. The polars-hash plugin already computes the full 256-bit SHA-256, outputting it as a 64-character hex string. Rather than truncating to Int64, **store and compare the full hash as a string or binary column, or at minimum use 128 bits** (first 32 hex characters cast to a pair of Int64s or stored as BINARY(16)). MD5 at 128 bits is safe up to ~325 billion rows at 50% collision probability. Full SHA-256 at 256 bits is safe up to ~9.2 × 10^25 rows — effectively infinite. The Scalefree Data Vault community explicitly recommends MD5 as the minimum for production hashing and calls 64-bit hashes "a risky choice." The storage cost difference (8 bytes vs. 16–32 bytes per row) is negligible against data integrity at this scale.

---

## UPDATE TOP(500K) will trigger lock escalation every time

SQL Server escalates from row locks to a **table-level exclusive lock when a single statement acquires approximately 5,000 locks**. An `UPDATE TOP(500000)` exceeds this threshold by roughly 100×, guaranteeing that every batch will escalate to a table lock, blocking all concurrent readers and writers for the duration. Under RCSI, readers normally don't block writers — but a table-level exclusive lock overrides this, making RCSI's benefits irrelevant during the update.

Multiple SQL Server experts (Brent Ozar, SQLServerCentral, MSSQLTips) converge on **2,000–5,000 rows per batch** to stay below the escalation threshold. The recommended pattern is a `WHILE` loop with `UPDATE TOP(4000)`, committing between batches to release locks and allow transaction log reuse. For a 3-million-row daily close set, this means ~750 iterations instead of 6, but each iteration completes in milliseconds rather than seconds, with dramatically lower blocking impact. Adding a `WAITFOR DELAY '00:00:00.050'` between batches can further reduce I/O contention during heavy periods.

An alternative is `ALTER TABLE SET (LOCK_ESCALATION = DISABLE)`, but this risks memory pressure from millions of individual row locks and is generally not recommended at billion-row scale. **Table partitioning with `LOCK_ESCALATION = AUTO`** is a better structural solution, enabling partition-level escalation rather than table-level.

---

## The CDC pattern is textbook — and the edge-case handling exceeds standards

The three-way comparison pattern (LEFT ANTI JOIN for inserts, reverse ANTI JOIN for deletes, INNER JOIN with hash compare for updates) is the canonical hash-based CDC approach documented across SQLServerCentral, DataForge, and Estuary. This pipeline implements every critical edge case that the literature identifies:

**NULL PK filtering** before joins is essential because Polars (since v0.20) treats NULL keys as never matching in joins by default (`join_nulls=False`), matching SQL standard semantics. Rows with NULL PKs would appear as inserts in every anti-join without this filter — the pipeline correctly handles this. However, Polars GitHub issue #19624 documents a bug where join cardinality validation (`validate="1:m"`) incorrectly fails when NULL values exist in join keys even with `join_nulls=False`. If the pipeline uses join validation, this bug could cause false errors.

**PK dtype alignment** before joins is a critical Polars-specific requirement that many pipelines miss. Polars does **not perform implicit type casting** on join keys — an Int32 column joined against an Int64 column will error or produce unexpected results. The pipeline's explicit dtype alignment step addresses a real and documented footgun.

**The NULL sentinel using \x1F wrapping** is a sound approach that goes beyond most implementations. The BimlFlex/Varigence documentation uses a simple "NVL" string as its NULL sentinel; the pipeline's use of a Unit Separator control character as both sentinel wrapper and column separator eliminates the theoretical risk of delimiter collision with actual data values. The count validation after classification (inserts + updates + unchanged = total fresh PKs) provides a mathematical invariant check that catches join logic errors immediately — this is an uncommon but excellent practice.

**Float normalization** (10 decimal places, ±0.0 → +0.0, NaN/Infinity handling) addresses the IEEE 754 pitfalls that the literature identifies as high-risk for cross-platform hashing. The **Unicode NFC normalization** handles the well-documented problem of equivalent but differently-encoded Unicode characters (e.g., ñ as U+00F1 vs. U+006E + U+0303) that would produce different hashes for visually identical data. The **Oracle empty string → NULL normalization** and **trailing space RTRIM** address the two most-cited cross-database hashing inconsistencies in migration literature. These four normalizations together represent a more thorough pre-hash normalization pipeline than any published framework provides out of the box.

---

## MERGE avoidance is strongly validated, and INSERT-first is a reasonable trade-off

The decision to avoid SQL Server's MERGE statement is overwhelmingly supported by the SQL Server expert community. Aaron Bertrand maintains a definitive catalog of MERGE bugs, including **silent wrong results with indexed views** (acknowledged by Microsoft as a critical unfixed issue), filtered index violations, and race conditions requiring HOLDLOCK hints. Paul White continues to discover new MERGE bugs as recently as February 2025 (data corruption through views). Hugo Kornelis's 2023 reassessment — the most charitable recent analysis — still concludes "do not use MERGE with a DELETE action," which would affect SCD2 row expiration. Michael J. Swart's position has only strengthened: "avoid MERGE altogether."

The INSERT-first pattern (insert new versions with UdmActiveFlag=0, close old versions, then activate new versions) is a creative solution to the filtered unique index constraint. Since the unique index enforces one active row per PK, inserting with Flag=1 immediately would violate the constraint before the old version is closed. **The described ordering correctly avoids this.** However, this creates a window where — between the close of the old row and the activation of the new row — queries filtering on `UdmActiveFlag=1` will see **zero active rows** for affected PKs. Under RCSI, readers in-flight before the operation started will see consistent pre-operation data, but new readers during the window see the gap. The pipeline accepts this trade-off in exchange for constraint enforcement.

The crash recovery design (dedup on next run) handles the case where the process crashes between INSERT and activation. However, the Kimball purist approach and all major frameworks (dbt snapshots, Databricks Delta MERGE, Snowflake Streams+MERGE) favor atomic operations — either all changes commit or none do. The pipeline's multi-step approach is justified specifically because SQL Server MERGE is unsafe, but it's worth documenting that **if the source data changes between a crash and the recovery run, orphaned Flag=0 rows might not be correctly matched** to the updated source data. The dedup logic should verify it handles this case explicitly.

---

## Technology-specific risks require awareness, not architecture changes

**ConnectorX** has several documented issues relevant to this pipeline. Oracle TIMESTAMP columns can be truncated to Date type (losing time components) in certain versions. SQL Server datetime/datetime2 extraction causes `PanicException` in ConnectorX 0.3.3+ (fixed by reverting to 0.3.2 or using newer alpha versions). ConnectorX errors manifest as Rust thread panics rather than Python exceptions, meaning **a single bad row can crash the entire extraction process** without graceful error handling. The pipeline should wrap ConnectorX calls in subprocess isolation or explicit exception handling to prevent silent crashes. ConnectorX also pre-allocates memory based on a `SELECT COUNT(*)` before extraction — if source data changes during extraction, row count mismatches can occur.

**Polars** has a documented memory management issue where Rust's allocator does not release memory back to the OS after heavy DataFrame operations (GitHub issue #23128). For a pipeline processing 3 million daily rows on a 64GB server, this could cause gradual memory exhaustion across processing days during backfills. The recommended mitigation is compiling Polars with jemalloc or running extraction in separate processes. Polars v1.0+ also changed `count()` to ignore NULLs (matching SQL `COUNT(col)` semantics) — use `len()` for row counts that include NULLs.

**BCP**'s handling of NULL vs. empty string is notably counterintuitive: BCP represents **empty strings as \x00 (null character) and NULLs as empty strings**, the opposite of typical expectations. The pipeline's string sanitization for \t, \n, \r, and \x00 covers the primary BCP danger characters, but should also address Unicode line-break characters: vertical tab (\x0B), form feed (\x0C), NEL (\x85), and Unicode line/paragraph separators (U+2028, U+2029). The pipeline should also specify `-C 65001` for UTF-8 encoding to avoid character set issues during BCP loads.

---

## Cross-platform type conversion has pitfalls beyond what's currently handled

The pipeline addresses the most commonly cited Oracle-to-SQL-Server issues (empty string/NULL, trailing spaces, float precision, Unicode normalization), but several additional type conversion pitfalls warrant attention:

- **Oracle DATE always includes time** (hours, minutes, seconds). It is not a date-only type. Mapping to SQL Server `date` would silently truncate time components. The correct mapping is `datetime2(0)`.
- **Oracle NUMBER without specified precision** can hold any numeric value up to 38 digits. ConnectorX or the pipeline's extraction layer must choose a SQL Server type — the default mapping is typically DECIMAL(38,8), which silently truncates values with more than 8 decimal places.
- **Oracle FLOAT(126)** provides ~38 decimal digits of precision, but SQL Server FLOAT(53) provides only ~15 decimal digits. This is a **significant precision loss** for high-precision scientific or financial data.
- **Oracle RAW(16) used as GUID** stores bytes in big-endian format, while SQL Server UNIQUEIDENTIFIER uses **mixed-endian** byte ordering. Direct byte copy produces different GUID string representations, breaking joins and lookups.
- **Oracle BLOB/CLOB** supports up to 4GB; SQL Server VARBINARY(MAX)/VARCHAR(MAX) supports up to 2GB. Data exceeding 2GB will be silently truncated.
- **Character length semantics** differ: Oracle VARCHAR2(100 BYTE) in AL32UTF8 may hold only 33 multi-byte characters, while SQL Server VARCHAR(100) holds 100 bytes. Ensure SQL Server column sizes accommodate worst-case character expansion from Oracle's character-semantic columns.

---

## The reconciliation strategy is comprehensive but could add three layers

The nine-element reconciliation strategy (daily counts, weekly column-by-column, Bronze reconciliation, full-PK reconciliation, aggregate columns, version velocity, phantom change alerting, active-to-total ratio, data freshness) is **more thorough than what most production pipelines implement**. Uber's data quality platform (UDQ) — one of the most sophisticated in industry — monitors freshness, completeness, duplicates, outliers, and distribution shifts, and catches ~90% of data quality incidents. This pipeline's reconciliation covers all of those dimensions except distribution shift.

Three additions would meaningfully improve detection speed:

**Distribution shift monitoring** would catch subtle data drift that counts and sums miss. Tracking statistical distributions (mean, standard deviation, percentiles) of key numeric columns over time detects scenarios where individual values shift but aggregate totals remain stable. Uber's UDQ specifically highlighted this as a key detection mechanism.

**Reconciliation at transformation boundaries** (Bronze→Silver and Silver→Gold) would catch issues introduced by transformation logic, not just ingestion. The current strategy validates source-to-Stage and Stage-to-Bronze, but DQOps and Acceldata both recommend the "circuit breaker" pattern at every layer boundary, blocking bad data from propagating downstream.

**Cross-table referential integrity checks** would verify that foreign key relationships between fact and dimension tables survive the CDC/SCD2 process. SCD2 surrogate key lookups are a known source of silent incorrect joins — as noted in production post-mortems, "every join needs that BETWEEN logic; miss one and your results are silently wrong."

The alerting thresholds deserve refinement. The **48-hour freshness warning** should be a **critical alert for daily pipelines** — it means two full refresh cycles were missed. Set warnings at 1.5× the expected refresh interval (36 hours for daily), with 48 hours as critical. The **>90% empty extraction guard** is aggressive; some tables legitimately have low-volume days (holidays, weekends). Dynamic thresholds based on a 30-day rolling average with day-of-week adjustment would reduce false positives while maintaining sensitivity.

---

## Schema evolution is the one major area without a documented strategy

None of the pipeline's implementation details address how schema changes (new columns, dropped columns, changed data types) are handled. This is the **single most commonly cited operational pain point** in CDC/SCD2 post-mortems. Debezium DDL parsing failures, Netflix's schema propagation overhead, and dbt snapshot bugs with new columns all point to schema evolution as a top-tier failure mode.

The recommended approach for SCD2 tables follows the principle that **columns should only ever be added, never removed or altered**:

- **New columns**: Add to Bronze with NULL default for all existing rows. Hash computation must be updated to include the new column. All historical rows retain NULL for the new column.
- **Dropped source columns**: Keep in Bronze for historical accuracy. New rows carry NULL or a sentinel value. Never physically drop columns from SCD2 tables.
- **Changed data types**: Add a new column with the new type rather than altering in place. Mark the old column as deprecated.
- **Detection**: Add a schema comparison check before each CDC run. If source schema differs from expected schema, **block processing** and require explicit acknowledgment before proceeding.

dbt's timestamp strategy handles schema evolution more gracefully than its check strategy because it doesn't need a column list updated when schema changes. For this pipeline, which uses hash-based comparison, **any schema change that affects the hash column set will cause every row to appear as changed** in the next run, potentially triggering the explosion guard. This interaction should be explicitly handled.

---

## Conclusion

This pipeline implements CDC/SCD2 patterns with a level of defensive engineering that surpasses published frameworks from dbt, Databricks, and Snowflake in several dimensions — particularly NULL handling, float normalization, cross-platform type normalization, and reconciliation depth. The three items requiring immediate attention are:

1. **Increase hash width to 128+ bits** (fix the ~24% collision probability at 3B rows)
2. **Reduce UPDATE batch size from 500K to 2,000–5,000 rows** (avoid table-level lock escalation)
3. **Add a schema evolution strategy** with detection, blocking, and documented procedures for column changes

Beyond those, the architecture decisions — MERGE avoidance, INSERT-first ordering, filtered unique index, RCSI, sp_getapplock with session locks, per-day checkpointing, idempotent operations, and the full suite of safety guards — are all validated by current literature and production experience. The pipeline's defensive posture (count validation after classification, empty/explosion/memory guards, dual reconciliation methods, crash recovery via dedup) reflects lessons that major companies learned through painful production incidents. The additional improvements noted throughout this report (ConnectorX isolation, BCP Unicode handling, cross-platform type mapping expansions, dynamic alerting thresholds, distribution shift monitoring) are incremental hardening rather than structural gaps.
# CDC + SCD2 pipeline: every edge case and failure mode

**Your architecture is fundamentally sound — the INSERT-first ordering, MERGE avoidance, filtered unique index, and post-SCD2 integrity checks are all correct design decisions validated by industry practice.** However, this research identified **7 critical gaps, 12 high-severity risks, and ~15 medium-severity edge cases** across the CDC→SCD2 pipeline that could cause silent data corruption or unrecoverable failures in production. The most dangerous finding is that several failure modes are *silent* — the pipeline succeeds while producing wrong data, with typical detection lag of days to weeks.

The research draws from Databricks, Snowflake, and dbt official documentation; Debezium production post-mortems from Zalando; SQL Server internals from Paul White, Aaron Bertrand, and Erland Sommarskog; Polars GitHub issues; and documented production incidents across the data engineering community.

---

## The 7 critical gaps that need immediate attention

**1. Oracle empty string = NULL mismatch.** Oracle treats `''` as `NULL` in VARCHAR2 columns; SQL Server does not. When comparing Oracle source data against a SQL Server target, every row with an empty-string field will hash differently every single run, producing **phantom updates indefinitely**. This is the single most likely source of persistent phantom changes in a cross-platform pipeline. The fix: normalize Oracle NULLs and SQL Server empty strings to a consistent representation before hash computation — either treat `''` as `NULL` everywhere or vice versa.

**2. UPDATE FROM JOIN is nondeterministic with duplicate staging keys.** The `UPDATE target SET ... FROM target JOIN staging ON target.key = staging.key` pattern silently picks an arbitrary matching row when the staging table contains duplicate business keys. No error, no warning — SQL Server documents this as nondeterministic behavior. If CDC produces multiple changes for the same PK within a batch (e.g., rapid updates), the staging table will have duplicates, and the UPDATE will silently apply an arbitrary one. The fix: **always deduplicate the staging table** before the UPDATE JOIN, keeping only the latest change per PK via `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY sequence DESC)`.

**3. INSERT-first ordering conflicts with the filtered unique index.** The filtered unique index on `PKs WHERE UdmActiveFlag=1` will **reject the INSERT of a new active row** if the old active row hasn't been closed yet — which is exactly the state during INSERT-first processing. This means either: (a) the BCP INSERT must set `UdmActiveFlag=0` initially and a second UPDATE flips it to 1 after closing the old row, or (b) the filtered unique index must be dropped/disabled during the INSERT phase. If BCP is inserting rows with `UdmActiveFlag=1` directly, the unique index violation will cause the entire BCP load to fail for any PK that already has an active row. This needs explicit verification in the current implementation — it is the most subtle interaction in the architecture.

**4. BCP tab-delimiter data corruption.** BCP treats the data file as a binary stream and cannot distinguish between a tab character in the data and the field delimiter. If any source column value contains a tab character (`\t`), BCP will silently misalign all subsequent fields in that row, corrupting data. Similarly, embedded `\r\n` in text fields will be interpreted as row terminators. The fix: either sanitize all data before export (replace tabs/newlines with safe characters), or switch to a multi-character delimiter unlikely to appear in data (e.g., `\x01` or `||~||`).

**5. Windowed delete detection is fundamentally incomplete.** In per-day windowed CDC, deletes can only be detected for rows that fall within the extraction window. A row deleted from the source with no timestamp update will simply be absent from the next extraction — but if that row's last-modified timestamp falls outside the current window, **the delete is invisible**. LookbackDays mitigates this but cannot guarantee completeness. The fix: implement periodic full-PK reconciliation (extract all PKs from source, compare against all active PKs in target) on a schedule — weekly at minimum, daily if feasible.

**6. Concurrent pipeline instances can corrupt data.** If two pipeline instances process the same table simultaneously, both may INSERT the same new rows (causing duplicates or PK violations) and both may UPDATE the same existing rows (lost updates). RCSI does not prevent write-write conflicts. The fix: implement **application-level mutual exclusion** via `sp_getapplock` at the start of each table's processing, ensuring only one instance processes a given table at any time.

**7. Trailing space divergence between Oracle and SQL Server.** SQL Server's ANSI padding rules treat `'abc' = 'abc  '` in equality comparisons, but hash computation sees them as different byte sequences. Oracle CHAR columns are blank-padded to declared length; VARCHAR2 columns are not. This causes phantom hash differences for any string field where trailing spaces differ between source and target. The fix: **RTRIM all string values** before hash computation.

---

## Hash-based CDC: complete failure mode taxonomy

The SHA-256 truncated to 64-bit approach is safe for pairwise change detection. The birthday paradox concern (50% collision at ~4.3 billion rows) applies to surrogate key generation but **not** to CDC's per-PK comparison model, where each comparison is independent — the per-row collision probability is **1 in 2^64 (~18.4 quintillion)**, which is effectively zero. The real hash-based CDC risks are all in the inputs to the hash function, not the hash function itself.

**Float normalization must handle four cases.** IEEE 754 defines multiple bit patterns for NaN, and `+0.0` and `-0.0` have different binary representations despite being semantically equal. The architecture's float normalization (round to 10 decimals, `±0.0 → +0.0`, NaN/Infinity sentinels) correctly addresses all four: NaN normalization, signed-zero normalization, precision rounding, and infinity handling. One gap to verify: ensure that Oracle's `NUMBER` type (which supports up to 38 digits of precision) is rounded to the same precision as the SQL Server target type *before* hashing, not after. A value like `123.4567890123456789` in Oracle may round differently depending on whether you round before or after conversion to Python float.

**The NULL sentinel `\x1FNULL\x1F` is safe but verify column delimiting.** The Unit Separator character (`\x1F`) is a non-printable control character with near-zero probability of appearing in production data. However, the concatenation scheme must also prevent cross-column collisions: the values `("AB", "CD")` and `("A", "BCD")` would produce identical concatenated strings `"ABCD"` without delimiters. Verify that columns are separated by `\x1F` in the concatenation, so the above would produce `"AB\x1FCD"` vs `"A\x1FBCD"` — different hashes.

**Unicode NFC normalization handles the common cases.** NFC is idempotent and covers the vast majority of normalization needs (precomposed vs decomposed forms). One edge case: NFC does **not** normalize compatibility equivalents — the ligature `ﬁ` remains distinct from `fi`. NFKC would handle this but is more aggressive and may normalize intentional distinctions. NFC is the correct choice for CDC unless the source data contains compatibility characters that should be treated as equivalent.

**The polars-hash library is the correct choice over Polars built-in hashing.** Polars' built-in `.hash()` method uses random seeds and is explicitly **not stable across sessions** — confirmed by GitHub Issue #3966. Using polars-hash's cryptographic functions (SHA-256) ensures deterministic, reproducible hashes across runs. One caution: do not hash Categorical columns via polars-hash, as it hashes the underlying physical integer encoding rather than the logical string value (Issue #21533), meaning the same string could hash differently depending on construction order.

---

## Anti-join pattern: validated with three caveats

The three-way anti-join pattern (LEFT ANTI JOIN for inserts, reverse for deletes, INNER JOIN + hash comparison for updates) is **logically correct** when three invariants hold: PKs are truly unique in both source and target, PK dtypes match exactly, and no NULLs exist in PK columns.

**NULL PK filtering is essential and correctly implemented.** Since Polars ≥0.20, `join_nulls` defaults to `False`, meaning NULL PK values silently appear in every anti-join result — a row with NULL PK in the source always appears as an insert, and in the target always appears as a delete. The architecture's explicit NULL PK filtering before joins correctly prevents this. With composite PKs, a NULL in *any* column of the composite key causes the entire row to fail matching, so the filter must check all PK columns.

**Dtype alignment is a hard failure, not a silent one.** Polars throws `RuntimeError` on join key dtype mismatches (e.g., Int32 vs Int64). This is good — it fails loudly. But the alignment step must handle cross-platform type differences: Oracle may return `NUMBER` as Python `Decimal` or `float64` depending on the connector, while SQL Server may return `int` or `bigint`. Ensure explicit casting to identical Polars dtypes before all joins.

**The count validation catches gross errors but not row-level misclassification.** The invariant `inserts + updates + unchanged = total fresh PKs` will pass even when individual rows are misclassified — a hash collision marking a changed row as unchanged, or compensating errors where one misclassified insert offsets one misclassified update. This validation is necessary but not sufficient. Supplement with periodic full-table hash-sum reconciliation: compute `SUM(row_hash)` on source and target and compare.

---

## SCD2 failure modes beyond duplicate active rows

**Partial BCP loads depend on the `-b` flag.** Without `-b`, the entire BCP load is a single transaction — failure rolls back everything (safe). With `-b N`, each batch of N rows is a separate transaction, and failure leaves a partial load (dangerous for SCD2 because only some new versions exist). **Verify that BCP is invoked without `-b`** for SCD2 loads to maintain atomicity.

**The same PK in both inserts and closes requires explicit ordering.** If CDC detects a row as both deleted and re-inserted in the same batch (the "resurrection" pattern), the pipeline must process the delete first (close old version), then the insert (create new version). If processed in the wrong order, the close operation could close the newly-inserted version instead of the old one. A documented bug in the dlt library (Issue #1683) shows this exact failure: SCD2 reinsert after retirement fails silently when the system doesn't recognize a previously retired record needs reactivation. The fix: when a PK appears in both `deleted_pks` and `changed_rows`, **the reinsert takes priority** — process deletes first, then inserts, and ensure the close targets only the pre-existing active row (by surrogate key or version number), not the newly-inserted one.

**SCD2 re-inserts should create new version chains.** When a previously-deleted row reappears in the source, the recommended pattern (per Roelant Vos) is to close the current "deleted" version and insert a new active version. This maintains a complete audit trail: active period → deleted period → reactivated period. The pipeline must check whether the current active version is a "deleted" marker and handle reactivation accordingly — simply comparing attributes won't work if the reinserted row has the same values as before deletion.

**Batched UPDATE TOP(500000) is correct for convergence but triggers lock escalation.** `UPDATE TOP(N)` has no ordering guarantee, but for SCD2 closes this doesn't matter — each iteration processes some qualifying rows (WHERE `UdmActiveFlag=1 AND pk IN staging`), and once updated, those rows no longer qualify. The loop converges because the qualifying set shrinks monotonically. However, at **500K rows per batch, lock escalation to a table-level exclusive lock is virtually guaranteed** (threshold is ~5,000 locks). This blocks all readers for the duration of each batch. With RCSI enabled, readers see pre-update snapshots and aren't blocked — but without RCSI, a 500K-row UPDATE will lock the entire table. Consider reducing batch size to **5,000-10,000 rows** if concurrent reads are needed without RCSI, or ensure RCSI is enabled.

---

## SQL Server specifics that affect correctness

**RCSI provides statement-level snapshots, not transaction-level.** Under RCSI, the INSERT and UPDATE in the SCD2 pattern execute as separate statements, each with its own snapshot. A concurrent reader could see the new rows (from INSERT) but not yet the closed old rows (from UPDATE), or vice versa. This creates a **transient inconsistency window** where both old and new versions appear active. For most analytics use cases this is acceptable, but for real-time consumers it could cause incorrect results. If transaction-level consistency is required, use SNAPSHOT isolation instead.

**UPDATEs are always fully logged.** There is no minimally-logged UPDATE in SQL Server, regardless of recovery model. For millions of SCD2 closes, the transaction log will grow by approximately `(row count × row size × 2)` for before-and-after images. A single UPDATE of 5 million rows could require **5-20 GB of transaction log space**. The BCP INSERT phase *can* be minimally logged under SIMPLE/BULK_LOGGED recovery with TABLOCK hint, but the UPDATE phase cannot. Pre-size the transaction log and ensure adequate log backup frequency under FULL recovery.

**The filtered unique index correctly prevents duplicate actives — but this interacts with INSERT-first ordering.** When rows transition from `active=1` to `active=0`, the filtered index performs a delete-then-insert internally (removing the row from the filtered index). After a large SCD2 batch, the filtered index may have elevated fragmentation. Monitor and reorganize periodically. The filtered index also provides excellent performance characteristics at billion-row scale because it only indexes the active subset — on a table with 1 billion historical rows and 50 million active rows, the index covers only 50 million rows.

**MERGE avoidance is emphatically confirmed.** Aaron Bertrand catalogued numerous unfixed MERGE bugs including race conditions causing PK violations, wrong results with indexed views, filtered index violations, assertion errors, and access violations with minimal logging. Paul White, Dan Guzman, Michael Swart, and Hugo Kornelis all recommend separate INSERT/UPDATE/DELETE statements. The Tiger Team blog confirmed access violations with MERGE under SIMPLE recovery requiring trace flag 692 as a workaround. **The separate INSERT + UPDATE approach is definitively safer.**

---

## Industry validation: how major platforms handle these problems

**Databricks explicitly warns against manual MERGE for CDC.** Their documentation states that `MERGE INTO` "can produce incorrect results because of out-of-sequence records." Their recommended approach — AUTO CDC (APPLY CHANGES INTO) with a mandatory `SEQUENCE BY` column — automatically handles out-of-order events. Key lesson for the described architecture: if source data can arrive out of order (common with windowed extraction), the pipeline needs explicit ordering logic. A monotonically-increasing column (timestamp or sequence number) should determine which version wins when the same PK appears multiple times.

**dbt snapshots have documented failure modes that validate several of the architecture's design choices.** dbt's check strategy has a known bug (Issue #8821) where `dbt_scd_id` hash computation includes the timestamp, causing MERGE matching failures. dbt snapshots create duplicate records during concurrent runs (Issue #4661). Hard deletes are silently ignored unless `invalidate_hard_deletes=True`. Non-unique keys cause silent incorrect behavior. The architecture's explicit dedup, integrity checks, and hard-delete handling via `deleted_pks` are all correctly addressing known dbt failure patterns.

**Snowflake Streams have a staleness problem the architecture avoids.** Snowflake streams become stale when their offset falls outside the data retention period — all unconsumed change records are permanently lost. The architecture's query-based CDC approach avoids this entirely since it doesn't depend on database-managed change tracking. However, Snowflake's best practice of "one stream per consumer" reinforces the principle: **never share CDC state between independent consumers**.

**Debezium's production experience at Zalando validates at-least-once semantics.** Debezium provides at-least-once delivery — duplicates occur after crashes or failovers. Zalando's experience processing billions of events showed that replication slot management is the single biggest operational issue at scale. For the described architecture (query-based CDC rather than log-based), the key takeaway is: **design all downstream processing to be idempotent**. The bronze dedup step correctly handles this.

---

## Reconciliation strategy: why count validation alone is insufficient

Count validation (`inserts + updates + unchanged = total`) is a necessary sanity check but cannot detect five categories of errors: hash collisions misclassifying individual rows, compensating errors that net to zero, phantom changes from cross-platform encoding differences, missed deletes outside the extraction window, and row-level data corruption from BCP delimiter issues.

A production-grade reconciliation strategy should layer three levels of validation. **Level 1 (every run):** count validation, post-SCD2 integrity checks (overlapping intervals, zero-active PKs, duplicate actives, version gaps), and `@@ROWCOUNT` validation after UPDATE JOIN. **Level 2 (daily):** column-level aggregate reconciliation — compare `SUM`, `COUNT`, `MIN`, `MAX` of key numeric columns between source and target. **Level 3 (weekly):** full-table PK reconciliation — extract all source PKs and compare against all active target PKs to catch any windowed-CDC drift, including missed deletes.

The architecture's post-SCD2 integrity checks (overlapping intervals, zero-active PKs, version gaps, duplicate active rows) are exactly the right checks — they match the industry standard validation suite documented by Kimball Group, dbt, and multiple data quality frameworks.

---

## Monitoring and alerting for CDC/SCD2 correctness

Beyond the existing integrity checks, production CDC/SCD2 pipelines need these specific monitors:

- **Phantom change rate**: Track the ratio of CDC-detected updates to total rows over time. A sudden spike in update percentage (e.g., from 0.1% to 50%) indicates a systematic hash mismatch, not real data changes. Alert on deviations >2σ from the rolling average.
- **SCD2 version velocity**: Track the average number of versions per PK. A sudden increase indicates either genuine high-change-rate data or a pipeline bug creating spurious versions.
- **Active-to-total ratio**: Monitor `COUNT(*) WHERE active=1 / COUNT(*)` over time. This ratio should decline gradually as history accumulates. A sudden drop indicates mass incorrect closures; a sudden increase indicates mass incorrect activations.
- **Data freshness**: Track the maximum `UdmEffectiveDate` in the target relative to the current time. Alert if freshness exceeds the expected pipeline cadence by >2x.
- **Cross-platform type drift**: Periodically sample source column metadata (Oracle `ALL_TAB_COLUMNS`, SQL Server `INFORMATION_SCHEMA.COLUMNS`) and alert on any datatype, precision, or scale changes that could affect hash computation.

---

## Real-world failures that validate the architecture's design

**Zalando's Debezium post-mortem** (processing hundreds of thousands of CDC events per second) revealed that replication slot management is the #1 operational issue at scale. The architecture's query-based approach avoids this entirely but introduces its own tradeoff: eventual consistency and incomplete delete detection.

**A retail company lost a week of sales data** when a vendor renamed a column and the pipeline's `.get('column_name', 0)` defaulted to zero. Schema drift is the most common silent failure in production pipelines, affecting **60%+ of pipelines** lacking automated contract testing. The architecture should implement schema validation at the extraction layer — compare source column names and types against an expected schema before processing.

**dbt snapshots silently materialized as regular tables** (not SCD2) for users running versions prior to 1.4, destroying all historical data. This was only discovered when upgrading dbt raised a new parsing error. The lesson: **never trust that a pipeline is producing correct output just because it runs without errors**. The architecture's post-SCD2 integrity checks are a direct defense against this class of failure.

**Databricks users reported CDC MERGE pipelines degrading from 20 minutes to 4+ hours** on a 2.3TB, 800M-row table due to small-file proliferation (50,000+ tiny files). While this is specific to Delta Lake, the general lesson applies: SCD2 tables that receive frequent small writes need periodic compaction or maintenance. For SQL Server, this manifests as index fragmentation and statistics staleness rather than small files.

---

## Conclusion: what the architecture gets right and what to fix

The architecture makes several non-obvious correct decisions that are validated by production experience across the industry: INSERT-first ordering (crash-recoverable), MERGE avoidance (documented bugs), filtered unique index (scales to billions), three-way anti-join (logically complete), and polars-hash over built-in Polars hashing (stable across sessions).

The **critical fixes** needed are: (1) Oracle empty-string/NULL normalization and trailing-space trimming before hashing, (2) staging table deduplication before UPDATE JOIN, (3) resolution of the INSERT-first vs filtered-unique-index interaction, (4) BCP delimiter sanitization or multi-character delimiters, (5) periodic full-PK reconciliation for windowed-mode delete detection, (6) `sp_getapplock` for concurrent-instance protection, and (7) verification that BCP runs without the `-b` flag.

The **monitoring additions** needed are: phantom change rate alerting, aggregate column reconciliation (daily), full-PK reconciliation (weekly), schema drift detection, and SCD2 version velocity tracking. These transform the pipeline from "probably correct" to "provably correct with bounded error detection latency."
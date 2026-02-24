# CDC/SCD2 Pipeline — Action Items (Every Edge Case & Failure Mode Research)

*Context: Derived from "CDC + SCD2 Pipeline: Every Edge Case and Failure Mode" — a comprehensive research document identifying 7 critical gaps, 12 high-severity risks, and ~15 medium-severity edge cases across the CDC→SCD2 pipeline. Cross-referenced against the current codebase and existing TODO (W-1 through W-18). Items already fully addressed by prior work are excluded. Items partially addressed are noted with what remains.*

*Numbering: E-series (Edge case research). Priority classification follows the established convention: P0 = data corruption, P1 = production reliability, P2 = performance/operational, P3 = improvements.*

---

## 1. Critical — Data Corruption (P0)

### E-1 (P0): Oracle empty string = NULL mismatch causes phantom updates indefinitely

**Files:** `bcp_csv.py` (`add_row_hash`, `add_row_hash_fallback`), `connectorx_oracle_extractor.py`

**Problem:** Oracle treats `''` as `NULL` in VARCHAR2 columns; SQL Server does not. When comparing Oracle source data against a SQL Server target, every row with an empty-string field hashes differently every single run, producing **phantom updates indefinitely**. The research identifies this as the single most likely source of persistent phantom changes in a cross-platform pipeline.

Currently, `add_row_hash()` distinguishes NULL from empty string via the `_NULL_SENTINEL` (W-2), which is correct for same-platform CDC. But for Oracle sources, a column that is `NULL` in Oracle extracts as `NULL`, while the same value stored in SQL Server Stage may be `''` (or vice versa), causing the hash inputs to diverge permanently.

**Impact:** Every Oracle-sourced table with any empty-string/NULL values generates phantom SCD2 versions every run. At scale, this creates massive, unbounded Bronze table growth with no real data changes.

**Fix:**
- Add a normalization step in `add_row_hash()` and `add_row_hash_fallback()` **before** the hash expression loop, applied to all Utf8/String columns on Oracle-sourced tables:
  ```python
  # E-1: Normalize Oracle empty string/NULL equivalence.
  # Oracle treats '' as NULL; SQL Server does not. Without this,
  # every row with an empty string field hashes differently every run.
  if source_is_oracle:
      for c in string_cols:
          df = df.with_columns(
              pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c)
          )
  ```
- The normalization direction (empty → NULL vs NULL → empty) should be empty → NULL, because: (a) Oracle semantically treats them as identical, (b) the NULL sentinel already handles NULL distinctly in hashing, and (c) this preserves the Oracle semantic intent.
- Pass a `source_type` parameter to `add_row_hash()` from the calling context (TableConfig already knows the source type), or read from `table_config.source_name` to determine if Oracle normalization applies.
- **Verification:** After implementation, run a CDC pass on an Oracle table known to have empty strings. The update count should drop to near-zero if no real changes exist.

---

### E-2 (P0): INSERT-first SCD2 conflicts with filtered unique index — BCP will fail

**Files:** `engine.py` (`_build_scd2_insert`), `table_creator.py` (`ensure_bronze_unique_active_index`)

**Problem:** The filtered unique index on `(PKs) WHERE UdmActiveFlag=1` will **reject the INSERT of a new active row** if the old active row hasn't been closed yet. The INSERT-first design (P0-8) inserts new versions with `UdmActiveFlag=1` (line 311 of `engine.py`) BEFORE the UPDATE step closes the old version. For any PK that already has an active row in Bronze, the BCP INSERT will violate the unique index and the **entire BCP load will fail**.

This is currently mitigated only by the V-4 diagnostic check and P1-16 dedup recovery, but the INSERT itself should be failing. Either: (a) the filtered unique index is not present on all tables, (b) BCP is using batched mode which commits some rows before failing, or (c) there is an interaction we haven't identified. **This needs immediate verification on production tables.**

The research identifies two correct solutions:
1. BCP INSERT sets `UdmActiveFlag=0` initially, then a second UPDATE flips it to `1` after closing the old row
2. Drop/disable the filtered unique index during the INSERT phase

**Impact:** If the filtered unique index exists and BCP loads the full batch as one transaction, every SCD2 update (not just inserts of new PKs) will fail. This is the most subtle interaction in the architecture.

**Fix — Option A (recommended: INSERT with UdmActiveFlag=0, then flip):**
- In `_build_scd2_insert()`, change `pl.lit(1).cast(pl.Int8).alias("UdmActiveFlag")` to `pl.lit(0).cast(pl.Int8).alias("UdmActiveFlag")` for new-version rows (operation="U")
- After the UPDATE step closes old versions, add a second UPDATE to activate the new versions:
  ```sql
  UPDATE t
  SET t.UdmActiveFlag = 1
  FROM {bronze_table} t
  INNER JOIN {staging_table} s ON {join_condition}
  WHERE t.UdmActiveFlag = 0
    AND t.UdmEndDateTime IS NULL
    AND t.UdmEffectiveDateTime = @effective_dt
  ```
- New inserts (operation="I") can remain `UdmActiveFlag=1` since they have no prior active row
- This preserves crash safety: a crash after INSERT but before the activate-UPDATE leaves rows with `UdmActiveFlag=0` and `UdmEndDateTime IS NULL` — detectable and recoverable

**Fix — Option B (simpler but riskier: disable index during INSERT):**
- Disable the filtered unique index before INSERT, re-enable after UPDATE
- Risk: a crash between disable and re-enable leaves the table unprotected

**Verification (IMMEDIATE):**
- Check if `ensure_bronze_unique_active_index()` has actually been called on production Bronze tables
- Run: `SELECT name, filter_definition FROM sys.indexes WHERE is_unique=1 AND filter_definition IS NOT NULL AND object_id = OBJECT_ID('Bronze.table')`
- If the index exists, test a CDC run with known updates and verify whether BCP errors occur

---

### E-3 (P0): BCP `-b` flag enables partial loads — SCD2 atomicity broken

**Files:** `bcp_loader.py` (line 92), `config.py` (`BCP_BATCH_SIZE`)

**Problem:** The BCP command currently includes `-b 10000` (from `config.BCP_BATCH_SIZE`). The research states: "Without `-b`, the entire BCP load is a single transaction — failure rolls back everything (safe). With `-b N`, each batch of N rows is a separate transaction, and failure leaves a partial load." For SCD2 Bronze loads, a partial INSERT means some new versions exist but their corresponding old versions haven't been closed — creating duplicate active rows that the V-4 diagnostic will flag but that **actively corrupt query results until the next run**.

The P0-8 INSERT-first design assumes the INSERT is atomic — either all new versions are inserted or none are. The `-b` flag breaks this assumption.

**Impact:** A BCP failure partway through an SCD2 INSERT load creates inconsistent Bronze state. Some PKs have two active versions; others have the new version only. The UPDATE step then closes old versions for ALL PKs in the staging table, potentially closing the wrong version for partially-loaded PKs.

**Fix:**
- **For Bronze SCD2 loads:** Remove the `-b` flag from the BCP command. This makes the entire INSERT a single transaction — either all rows load or none do.
- **For Stage CDC loads:** The `-b` flag is acceptable because Stage is ephemeral and truncated before each load. Partial Stage loads are harmless since the next run truncates and reloads.
- Modify `bcp_load()` to accept an `atomic` parameter (default `True` for Bronze, `False` for Stage):
  ```python
  def bcp_load(csv_path, full_table_name, expected_row_count=None,
               format_file=None, atomic=True):
      cmd = [...]
      if not atomic:
          cmd.extend(["-b", str(config.BCP_BATCH_SIZE)])
      # ...
  ```
- Update all Bronze BCP load call sites to use `atomic=True` (or rely on the default)
- Update all Stage BCP load call sites to explicitly pass `atomic=False` for the performance benefit
- **Tradeoff:** Without `-b`, very large Bronze loads (millions of rows) will use more transaction log space since the entire load is one transaction. This is already documented in B-2. Monitor transaction log usage after this change.
- Document in CLAUDE.md

---

### E-4 (P0): Trailing space divergence causes phantom hash differences

**Files:** `bcp_csv.py` (`add_row_hash`, `add_row_hash_fallback`)

**Problem:** SQL Server's ANSI padding rules treat `'abc' = 'abc  '` in equality comparisons, but hash computation sees them as different byte sequences. Oracle CHAR columns are blank-padded to declared length; VARCHAR2 columns are not. This causes phantom hash differences for any string field where trailing spaces differ between source and target.

The current `sanitize_strings()` function strips `\t \n \r \x00` but does NOT strip trailing spaces. The hash function receives the raw string values, so `"abc"` and `"abc  "` produce different hashes.

**Impact:** Phantom SCD2 updates for any table with CHAR-type columns (Oracle or SQL Server) or where trailing spaces vary between source and target. Less common than E-1 (Oracle empty string) but can affect any source platform.

**Fix:**
- Add RTRIM to all Utf8/String columns **before hash computation** in `add_row_hash()` and `add_row_hash_fallback()`:
  ```python
  # E-4: RTRIM all string values before hashing to prevent phantom
  # hash differences from trailing space divergence between Oracle
  # CHAR padding and SQL Server ANSI padding rules.
  if string_cols:
      df = df.with_columns([
          pl.col(c).str.rstrip(" ").alias(c)
          for c in string_cols
      ])
  ```
- Apply RTRIM **after** NFC normalization (V-2) and **after** Oracle empty string normalization (E-1), but **before** the hash expression loop
- Note: This changes hash output for any row with trailing spaces. Like W-2, this requires a coordinated migration — the next CDC run will detect a one-time update wave for affected rows. Schedule during a maintenance window.
- **Alternative:** Apply RTRIM only to CHAR-type columns (not VARCHAR) by checking source column metadata. This is more precise but requires column-type awareness in the hash function.

---

### E-5 (P0): UPDATE FROM JOIN nondeterminism — staging table dedup required

**Files:** `engine.py` (`_execute_bronze_updates`), CDC engine (staging table usage)

**Problem:** The `UPDATE target SET ... FROM target JOIN staging ON pk_join` pattern is documented as nondeterministic when the staging table contains duplicate business keys. SQL Server silently picks an arbitrary matching row — no error, no warning.

In the current SCD2 implementation, the staging table contains PKs to close, and the SET values are constants (`UdmActiveFlag=0`, `UdmEndDateTime=@now`), so nondeterminism in row selection doesn't affect the UPDATE result. **However**, the CDC staging tables used for the `read_bronze_for_pks` targeted read (PK-targeted Bronze extraction) could contain duplicate PKs if the extraction window includes rapid updates to the same PK.

The `run_scd2_targeted()` function deduplicates `pks_to_close` (line 723) but the CDC-level staging table used for the targeted Bronze read is loaded directly from `df_current` which should be unique per PK (fresh extract). Verify this assumption.

**Impact:** Low risk for the current SCD2 UPDATE pattern (constant SET values). Higher risk if the pattern is ever extended to SET values from the staging table.

**Fix:**
- Add an explicit `.unique(subset=pk_columns)` call in `_execute_bronze_updates()` at the point where `pks_to_close` enters the function, as a defense-in-depth measure:
  ```python
  pks_to_close = pks_to_close.unique(subset=pk_columns if len(pk_columns) > 1 else None)
  ```
- Add a comment documenting why this is safe today (constant SET values) but necessary for future-proofing
- In `run_scd2()` (non-targeted), add `.unique()` to the `pks_to_close` concat (it's missing, unlike the targeted version which has it)
- Add a DEBUG-level log if deduplication removes any rows, as this would indicate an upstream issue

---

## 2. High Severity — Production Reliability (P1)

### E-6 (P1): Resurrection pattern — PK in both deletes and inserts requires explicit ordering

**Files:** `engine.py` (`run_scd2`, `run_scd2_targeted`)

**Problem:** If CDC detects a row as both deleted and re-inserted in the same batch (the "resurrection" pattern — a row is deleted from source, then re-inserted with the same PK), the pipeline must process the delete first (close old version) then the insert (create new version). If processed in the wrong order, the close operation could close the newly-inserted version instead of the old one.

The current implementation's INSERT-first design (P0-8) inserts the new version FIRST, then the UPDATE closes old versions. When a PK appears in both `df_closed` (deleted from source) and `df_new` (new insert), the INSERT creates a new active row, then the UPDATE closes **all** active rows matching that PK — including the just-inserted one.

The dlt library (Issue #1683) documents this exact failure: SCD2 reinsert after retirement fails silently.

**Impact:** Resurrected rows get their new version immediately closed, resulting in zero active rows for that PK. The next run would re-insert, creating a version chain that incorrectly shows the row as deleted even though it exists in the source.

**Fix:**
- Before executing SCD2, detect PKs that appear in both `df_new` (inserts) and `df_closed` (deletes):
  ```python
  # E-6: Resurrection detection — PK in both inserts and closes.
  resurrection_pks = df_new.join(df_closed, on=pk_columns, how="semi")
  if len(resurrection_pks) > 0:
      logger.info(
          "E-6: %d resurrected PKs detected in %s (deleted then re-inserted). "
          "These will be processed as updates, not close+insert.",
          len(resurrection_pks), table_config.source_object_name,
      )
      # Treat resurrections as updates: close old version, insert new version
      # Remove from df_new (they're not true new inserts)
      # Remove from df_closed (they're not true deletes)
      # Add to df_changed_pks (treat as version updates)
      df_new = df_new.join(resurrection_pks, on=pk_columns, how="anti")
      df_closed = df_closed.join(resurrection_pks, on=pk_columns, how="anti")
      # The resurrection PKs need new versions inserted — add to insert_parts
      # The old versions need closing — add to pks_to_close
  ```
- The key insight: resurrection PKs should be treated identically to "changed" rows — close the old active version, insert a new active version. The INSERT-first ordering then works correctly because the new version is inserted first, and the UPDATE targets only `WHERE UdmActiveFlag=1 AND UdmEffectiveDateTime < @effective_dt` to avoid closing the new version.
- **Alternatively**, modify the UPDATE's WHERE clause to exclude rows inserted in the current batch:
  ```sql
  WHERE t.UdmActiveFlag = 1 AND t.UdmEffectiveDateTime < @effective_dt
  ```
  This ensures the UPDATE only closes versions that existed before the current run.

---

### E-7 (P1): Windowed delete detection requires periodic full-PK reconciliation

**Files:** `reconciliation.py`, `config.py`, scheduling infrastructure

**Problem:** In per-day windowed CDC (large tables), deletes can only be detected for rows that fall within the extraction window. A row deleted from the source with no timestamp update will simply be absent from the next extraction — but if that row's last-modified timestamp falls outside the current window, **the delete is invisible**. `LookbackDays` mitigates this but cannot guarantee completeness.

The existing `reconcile_table()` in `reconciliation.py` performs a full column-by-column comparison, but it's designed for Stage→Source reconciliation, not for Bronze→Source active-PK reconciliation. The W-11 item added `reconcile_counts()` for lightweight daily checks.

What's missing is a **full-PK reconciliation** that extracts all PKs from the source and compares against all active PKs in Bronze to catch windowed-CDC drift, including missed deletes.

**Impact:** Over time, windowed-mode Bronze tables accumulate "ghost" active rows — rows that were deleted from the source but never closed in Bronze. These ghost rows appear in downstream queries and reports.

**Fix:**
- Add a `reconcile_active_pks()` function to `reconciliation.py`:
  ```python
  def reconcile_active_pks(table_config: TableConfig) -> dict:
      """E-7: Full PK reconciliation — detect ghost active rows in Bronze.

      Extracts all PKs from source, compares against all active Bronze PKs.
      Reports: source_only (missing from Bronze), bronze_only (ghost active rows).
      """
      # Extract all source PKs (SELECT pk_columns FROM source — PK-only, low memory)
      # Extract all active Bronze PKs (SELECT pk_columns FROM bronze WHERE UdmActiveFlag=1)
      # Anti-join both directions
      # Return ghost_count, missing_count
  ```
- Schedule weekly for all windowed-mode tables (large tables)
- For ghost active rows detected, optionally auto-close them via the existing `_execute_bronze_updates()` mechanism
- Add the ghost PK count to `PipelineEventLog` for trend monitoring
- Alert if ghost count exceeds a configurable threshold (e.g., >0.1% of active rows)

---

### E-8 (P1): RCSI statement-level snapshots create transient SCD2 inconsistency window

**Files:** `engine.py` (documentation), `CLAUDE.md`

**Problem:** Under RCSI (Read Committed Snapshot Isolation), the INSERT and UPDATE in the SCD2 pattern execute as separate statements, each with its own snapshot. A concurrent reader could see the new rows (from INSERT) but not yet the closed old rows (from UPDATE), or vice versa. This creates a **transient inconsistency window** where both old and new versions appear active.

With the INSERT-first design, the window is: after INSERT commits, before UPDATE commits. During this window, a PK has two active versions — the new one just inserted and the old one not yet closed.

**Impact:** For most analytics use cases this is acceptable. For real-time consumers querying Bronze directly, it could produce incorrect results (double-counting). The V-4 diagnostic already warns about this pattern.

**Fix:**
- **Documentation (immediate):** Add a clear note in CLAUDE.md and engine.py docstrings that RCSI creates a transient inconsistency window during SCD2 promotion. Downstream queries should use `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY UdmEffectiveDateTime DESC) WHERE rn=1` instead of `WHERE UdmActiveFlag=1` alone.
- **For critical consumers (if needed):** Switch to SNAPSHOT isolation for the SCD2 transaction (both INSERT and UPDATE within a single explicit transaction under SNAPSHOT isolation). This provides transaction-level consistency at the cost of increased tempdb usage.
- **Current mitigation:** The V-4 post-SCD2 duplicate active check and P1-16 dedup recovery handle the crash case. The transient window during normal operation is typically milliseconds to seconds.

---

### E-9 (P1): UPDATE TOP(500K) batch size triggers lock escalation

**Files:** `engine.py` (`_execute_bronze_updates`), `config.py`

**Problem:** The batched UPDATE uses `TOP(500000)` per iteration. SQL Server escalates from row/page locks to table-level exclusive locks at approximately 5,000 locks. At 500K rows per batch, lock escalation to a **table-level exclusive lock is virtually guaranteed**. This blocks all readers for the duration of each batch.

With RCSI enabled, readers see pre-update snapshots and aren't blocked — but the research notes this should be verified. Without RCSI, a 500K-row UPDATE will lock the entire table.

**Impact:** If RCSI is enabled (as recommended in C-4), this is a non-issue for readers. If RCSI is not enabled, all downstream queries are blocked during each 500K-row batch.

**Fix:**
- **Verify RCSI is enabled** on all Bronze databases:
  ```sql
  SELECT name, is_read_committed_snapshot_on FROM sys.databases WHERE name LIKE 'UDM_Bronze%'
  ```
- If RCSI is NOT enabled, reduce `_SCD3_BATCH_SIZE` from 500,000 to 5,000-10,000 to prevent lock escalation
- If RCSI IS enabled, the current 500K batch size is acceptable — readers aren't blocked
- Add the RCSI check as a startup validation in `main_small_tables.py` / `main_large_tables.py`:
  ```python
  # E-9: Verify RCSI enabled on Bronze databases for non-blocking SCD2 updates
  cursor.execute("SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = ?", db)
  if not cursor.fetchone()[0]:
      logger.warning("E-9: RCSI not enabled on %s — SCD2 batch UPDATEs will block readers", db)
  ```
- Document the RCSI dependency in CLAUDE.md

---

### E-10 (P1): Transaction log sizing for large SCD2 UPDATEs

**Files:** `engine.py` (`_execute_bronze_updates`), deployment documentation

**Problem:** UPDATEs are **always fully logged** in SQL Server regardless of recovery model. For millions of SCD2 closes, the transaction log grows by approximately `(row_count × row_size × 2)` for before-and-after images. A single UPDATE of 5 million rows could require **5-20 GB of transaction log space**.

The existing P2-11 warning at 1M rows is correct but doesn't include sizing guidance.

**Impact:** Transaction log exhaustion during large SCD2 runs causes the UPDATE to fail, leaving the pipeline in the INSERT-complete-UPDATE-failed state (recoverable but requires manual intervention).

**Fix:**
- Enhance the P2-11 warning with sizing estimates:
  ```python
  if len(pks_to_close) > 1_000_000:
      estimated_log_gb = (len(pks_to_close) * 400 * 2) / (1024**3)  # ~400 bytes avg row
      logger.warning(
          "P2-11/E-10: Large Bronze UPDATE: %d rows in %s. "
          "Estimated transaction log: %.1f GB. "
          "Monitor: SELECT * FROM sys.dm_db_log_space_usage",
          len(pks_to_close), bronze_table, estimated_log_gb,
      )
  ```
- Add pre-flight transaction log space check before large UPDATEs:
  ```sql
  SELECT log_space_in_bytes_since_last_backup / 1073741824.0 AS log_used_gb,
         total_log_size_in_bytes / 1073741824.0 AS log_total_gb
  FROM sys.dm_db_log_space_usage
  ```
- Ensure frequent log backups (every 15-30 minutes) under FULL recovery during pipeline runs
- Document the transaction log requirement in deployment guides

---

### E-11 (P1): Schema validation at extraction layer — detect source column changes before processing

**Files:** `connectorx_oracle_extractor.py`, `connectorx_sqlserver_extractor.py`, `evolution.py`

**Problem:** The research cites a production incident where a vendor renamed a column and the pipeline defaulted to zero, losing a week of sales data. Schema drift is the most common silent failure in production pipelines, affecting 60%+ of pipelines lacking automated contract testing.

The pipeline has `evolution.py` for schema drift handling, but it reacts to drift after extraction rather than validating before processing. A pre-extraction schema check would catch column renames, type changes, and dropped columns before any data processing begins.

**Impact:** Column renames or type changes in the source that aren't caught early can cause silent data corruption, hash mismatches, or pipeline failures deep in the processing chain.

**Fix:**
- Add a `validate_source_schema()` function that compares current source column metadata against the expected schema from `UdmTablesColumnsList`:
  ```python
  def validate_source_schema(table_config: TableConfig) -> list[str]:
      """E-11: Pre-extraction schema validation against expected column contract."""
      expected_cols = set(table_config.column_names)
      actual_cols = set(get_source_column_names(table_config))
      missing = expected_cols - actual_cols
      unexpected = actual_cols - expected_cols
      warnings = []
      if missing:
          warnings.append(f"Missing columns in source: {missing}")
      if unexpected:
          warnings.append(f"Unexpected columns in source: {unexpected}")
      return warnings
  ```
- Call before extraction in `small_tables.py` and `large_tables.py`
- On validation failure: log ERROR, skip the table (existing behavior for evolution failures), and write to `PipelineEventLog`
- Periodically sample source column metadata (Oracle `ALL_TAB_COLUMNS`, SQL Server `INFORMATION_SCHEMA.COLUMNS`) and alert on datatype, precision, or scale changes

---

## 3. Medium Severity — Monitoring & Observability (P2)

### E-12 (P2): Phantom change rate monitoring — detect systematic hash mismatches

**Files:** `event_tracker.py`, `reconciliation.py`

**Problem:** Track the ratio of CDC-detected updates to total rows over time. A sudden spike in update percentage (e.g., from 0.1% to 50%) indicates a systematic hash mismatch (encoding change, schema drift, normalization bug), not real data changes. Without this monitor, a systematic hash bug silently creates millions of phantom SCD2 versions before anyone notices.

**Impact:** Days-to-weeks detection lag for systematic hash bugs. E-1 (Oracle empty string) and E-4 (trailing spaces) would both manifest as phantom change spikes.

**Fix:**
- Log the CDC update ratio in `PipelineEventLog` after each CDC run:
  ```python
  update_ratio = cdc_result.updates / max(cdc_result.total, 1)
  log_event("CDC_UPDATE_RATIO", table_name, value=update_ratio)
  ```
- Add a monitoring query that alerts when the update ratio deviates >2σ from the rolling 30-day average for a table
- Create a dashboard panel showing update ratio trends per table
- Target detection latency: same-day for sudden spikes (>10× normal rate)

---

### E-13 (P2): SCD2 version velocity monitoring — detect spurious version creation

**Files:** `event_tracker.py`, `reconciliation.py`

**Problem:** Track the average number of versions per PK in Bronze. A sudden increase indicates either genuine high-change-rate data or a pipeline bug creating spurious versions. Combined with E-12, this distinguishes between "many rows changed once" (hash bug) and "same rows changing repeatedly" (version velocity issue).

**Impact:** Complements E-12 for root cause analysis.

**Fix:**
- Add a periodic check (daily, as part of reconciliation):
  ```sql
  SELECT AVG(version_count * 1.0) AS avg_versions,
         MAX(version_count) AS max_versions
  FROM (
      SELECT {pk_columns}, COUNT(*) AS version_count
      FROM {bronze_table}
      GROUP BY {pk_columns}
  ) v
  ```
- Log to `PipelineEventLog` and alert if average versions per PK increases >20% week-over-week
- Identify PKs with unusually high version counts (>10 versions) for investigation

---

### E-14 (P2): Active-to-total ratio monitoring — detect mass incorrect closures/activations

**Files:** `event_tracker.py`, `reconciliation.py`

**Problem:** Monitor `COUNT(*) WHERE UdmActiveFlag=1 / COUNT(*)` over time. This ratio should decline gradually as history accumulates. A sudden drop indicates mass incorrect closures; a sudden increase indicates mass incorrect activations.

**Impact:** Catches bulk SCD2 processing errors that count validation alone misses (compensating errors can net to zero).

**Fix:**
- Log after each SCD2 run:
  ```python
  active_count = get_active_row_count(bronze_table)
  total_count = get_total_row_count(bronze_table)
  active_ratio = active_count / max(total_count, 1)
  log_event("BRONZE_ACTIVE_RATIO", table_name, value=active_ratio)
  ```
- Alert if ratio changes by >5% in a single run (indicates bulk error)
- Track trend over time — gradual decline is expected and healthy

---

### E-15 (P2): Data freshness monitoring — alert on stale Bronze data

**Files:** `event_tracker.py`, pipeline scheduling

**Problem:** Track the maximum `UdmEffectiveDateTime` in Bronze relative to the current time. Alert if freshness exceeds the expected pipeline cadence by >2×.

**Impact:** Catches silent pipeline failures where the run completes without processing new data (empty extraction, skipped tables, lock contention).

**Fix:**
- After each SCD2 run, log the max effective datetime:
  ```sql
  SELECT MAX(UdmEffectiveDateTime) FROM {bronze_table} WHERE UdmActiveFlag = 1
  ```
- Alert if `(NOW - max_effective_dt) > 2 × expected_cadence`
- For daily pipelines: alert if data is >48 hours stale
- For near-real-time pipelines: alert if data is >2× the scheduled interval

---

### E-16 (P2): Cross-platform type drift detection

**Files:** `column_sync.py`, `event_tracker.py`

**Problem:** Periodically sample source column metadata (Oracle `ALL_TAB_COLUMNS`, SQL Server `INFORMATION_SCHEMA.COLUMNS`) and alert on any datatype, precision, or scale changes that could affect hash computation. A source column changing from `NUMBER(10,2)` to `NUMBER(15,4)` could cause silent precision differences in hash inputs.

**Impact:** Preemptive detection of type changes that would cause phantom hash mismatches.

**Fix:**
- Extend `column_sync.py` to store source column metadata snapshots (type, precision, scale)
- On each sync, compare current source metadata against stored snapshot
- Alert on any changes to numeric precision/scale, string length, or type changes
- Log changes to `PipelineEventLog` for audit trail

---

### E-17 (P2): Level 2 aggregate reconciliation — daily column-level validation

**Files:** `reconciliation.py`

**Problem:** The existing reconciliation performs either count-only (W-11) or full column-by-column (P3-4) comparison. The research recommends a middle tier: daily column-level **aggregate reconciliation** — compare `SUM`, `COUNT`, `MIN`, `MAX` of key numeric columns between source and target. This catches data corruption that count validation misses (e.g., wrong values in the right number of rows) without the cost of full row-by-row comparison.

**Impact:** Catches value-level corruption within 24 hours instead of waiting for weekly full reconciliation.

**Fix:**
- Add a `reconcile_aggregates()` function:
  ```python
  def reconcile_aggregates(table_config: TableConfig) -> dict:
      """E-17: Column-level aggregate reconciliation.

      Compares SUM, COUNT, MIN, MAX of key numeric columns between
      source and Bronze active rows.
      """
      # Identify numeric columns from table_config
      # Run aggregate queries on source and Bronze
      # Compare results, report discrepancies
  ```
- Schedule daily for high-value tables
- Focus on columns most likely to contain meaningful numeric data (monetary amounts, quantities, IDs)
- Alert on any aggregate mismatch

---

## 4. Low Severity — Improvements & Hardening (P3)

### E-18 (P3): SCD2 re-insert version chain — maintain complete audit trail

**Files:** `engine.py`

**Problem:** When a previously-deleted row reappears in the source (resurrection pattern), the recommended approach (per Roelant Vos) is to close the current "deleted" version and insert a new active version. This maintains a complete audit trail: active period → deleted period → reactivated period.

This is related to E-6 but focuses on the version chain correctness rather than the processing order.

**Fix:**
- When E-6 resurrection detection identifies a resurrected PK, verify the version chain is correct:
  - Old active version → closed with UdmEndDateTime = deletion time
  - Deletion marker (UdmActiveFlag=0, UdmScd2Operation='D') → closed with UdmEndDateTime = reactivation time
  - New active version (UdmActiveFlag=1, UdmScd2Operation='R' for reactivation)
- Add UdmScd2Operation='R' as a new operation type for reactivations

---

### E-19 (P3): NULL sentinel column delimiter verification

**Files:** `bcp_csv.py` (verification only)

**Problem:** The research notes that the concatenation scheme must prevent cross-column collisions: `("AB", "CD")` and `("A", "BCD")` must produce different concatenated strings. The current implementation uses `\x1F` as the separator in `pl.concat_str(hash_exprs, separator="\x1f")`, which correctly prevents this.

**Status:** Already addressed. This item exists for documentation completeness.

**Fix:**
- Add a comment in `add_row_hash()` explicitly noting the cross-column collision prevention
- Add a unit test verifying that `("AB", "CD")` and `("A", "BCD")` produce different hashes

---

### E-20 (P3): Categorical column hash safety — verify polars-hash behavior

**Files:** `bcp_csv.py` (`add_row_hash`)

**Problem:** The research warns: do not hash Categorical columns via polars-hash, as it hashes the underlying physical integer encoding rather than the logical string value (Polars Issue #21533). The same string could hash differently depending on construction order.

The pipeline casts all columns to Utf8 before hashing, which should avoid this. Verify explicitly.

**Fix:**
- Add an assertion in `add_row_hash()` that no source columns have Categorical dtype:
  ```python
  cat_cols = [c for c in source_cols if df[c].dtype == pl.Categorical]
  if cat_cols:
      logger.warning("E-20: Categorical columns found before hashing: %s. "
                      "Casting to Utf8 to prevent physical encoding hash.", cat_cols)
      df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cat_cols])
  ```

---

### E-21 (P3): Oracle NUMBER precision — round before Python float conversion

**Files:** `connectorx_oracle_extractor.py`, `bcp_csv.py`

**Problem:** Oracle's `NUMBER` type supports up to 38 digits of precision. A value like `123.4567890123456789` in Oracle may round differently depending on whether it's rounded before or after conversion to Python float (which has ~15 digits of precision). If ConnectorX converts Oracle NUMBER to Python float64 before the pipeline rounds for hashing, precision is already lost.

**Impact:** Rare — only affects Oracle NUMBER columns with >15 significant digits. Most business data uses fewer digits.

**Fix:**
- Verify how ConnectorX converts Oracle NUMBER: does it produce `float64` or `Decimal`?
- If `float64`: precision is already lost for large-precision numbers. Document this limitation.
- If `Decimal`: the pipeline's `.round(FLOAT_HASH_PRECISION)` works correctly since Decimal preserves all digits.
- For critical high-precision columns, consider casting Oracle NUMBER to string in the extraction SQL before ConnectorX processes it

---

## Summary

| ID | Priority | Category | Issue | Status |
|----|----------|----------|-------|--------|
| E-1 | P0 | Hash Hygiene | Oracle empty string = NULL phantom updates | TODO |
| E-2 | P0 | SCD2 Integrity | INSERT-first vs filtered unique index conflict | TODO — verify on production immediately |
| E-3 | P0 | SCD2 Integrity | BCP `-b` flag breaks SCD2 atomicity | TODO |
| E-4 | P0 | Hash Hygiene | Trailing space divergence phantom hashes | TODO |
| E-5 | P0 | SCD2 Integrity | UPDATE FROM JOIN staging dedup (defense-in-depth) | TODO |
| E-6 | P1 | SCD2 Logic | Resurrection pattern ordering | TODO |
| E-7 | P1 | Reconciliation | Full-PK reconciliation for windowed delete drift | TODO |
| E-8 | P1 | Concurrency | RCSI transient inconsistency window documentation | TODO |
| E-9 | P1 | Performance | UPDATE TOP(500K) lock escalation + RCSI verification | TODO |
| E-10 | P1 | Operations | Transaction log sizing for large UPDATEs | TODO |
| E-11 | P1 | Schema Safety | Pre-extraction schema validation | TODO |
| E-12 | P2 | Monitoring | Phantom change rate alerting | TODO |
| E-13 | P2 | Monitoring | SCD2 version velocity tracking | TODO |
| E-14 | P2 | Monitoring | Active-to-total ratio monitoring | TODO |
| E-15 | P2 | Monitoring | Data freshness alerting | TODO |
| E-16 | P2 | Monitoring | Cross-platform type drift detection | TODO |
| E-17 | P2 | Reconciliation | Daily aggregate column reconciliation | TODO |
| E-18 | P3 | SCD2 Logic | Re-insert version chain (audit trail) | TODO |
| E-19 | P3 | Hash Hygiene | Column delimiter verification (already addressed) | DONE — verify + document |
| E-20 | P3 | Hash Safety | Categorical column hash safety check | TODO |
| E-21 | P3 | Extraction | Oracle NUMBER precision rounding order | TODO |

### Top priorities:

1. **E-2 (P0):** Verify filtered unique index interaction on production — this could be silently failing NOW
2. **E-3 (P0):** Remove BCP `-b` flag for Bronze loads — SCD2 atomicity is broken
3. **E-1 (P0):** Oracle empty string normalization — eliminates the #1 phantom update source
4. **E-4 (P0):** Trailing space RTRIM — eliminates phantom updates from CHAR padding
5. **E-6 (P1):** Resurrection pattern — prevents incorrect version closures
6. **E-7 (P1):** Full-PK reconciliation — catches windowed CDC delete drift

### Already addressed by prior work (excluded from this TODO):

- BCP tab-delimiter data corruption → DONE via `sanitize_strings()` (strips `\t \n \r \x00`)
- Concurrent pipeline instances → DONE via `sp_getapplock` in `table_lock.py`
- NULL PK filtering → DONE (Polars `join_nulls=False` default + explicit filter)
- Dtype alignment before joins → DONE via `align_pk_dtypes()` (P0-12)
- Count validation → DONE (P0-12)
- Post-SCD2 integrity checks → DONE (V-3: overlapping intervals, zero-active PKs, version gaps)
- MERGE avoidance → DONE (confirmed by research)
- INSERT-first ordering → DONE (P0-8, confirmed by research)
- Column separator `\x1F` → DONE (P0-6)
- polars-hash over built-in hashing → DONE (H-1/H-2/H-3)
- Unicode NFC normalization → DONE (V-2)
- Float rounding → DONE (V-1, extended by W-3)
- ±0.0 and NaN/Infinity → DONE (W-3)
- NULL sentinel `\x1FNULL\x1F` → DONE (W-2)
- Schema validation before concat → DONE (W-7)
- `sp_getapplock` RCSI analysis → DONE (W-8)
- `shrink_to_fit()` → DONE (W-12)
- BCP format files → DONE (W-13)
- Quarantine table → DONE (W-17)

### Validated architecture decisions (no action needed):

- SHA-256 truncated to 64-bit is safe for per-PK CDC comparison (birthday paradox doesn't apply)
- Three-way anti-join pattern is logically correct when invariants hold
- MERGE avoidance confirmed by Aaron Bertrand's bug catalogue and new February 2025 SQL Server bug
- Filtered unique index scales correctly to billions of rows (indexes only active subset)
- INSERT-first ordering confirmed as the correct crash-recovery pattern

*Source: "CDC + SCD2 Pipeline: Every Edge Case and Failure Mode" — comprehensive research covering Databricks, Snowflake, dbt, Debezium/Zalando production post-mortems, SQL Server internals (Paul White, Aaron Bertrand, Erland Sommarskog), and Polars GitHub issues.*
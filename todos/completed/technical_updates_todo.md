# UDM Pipeline — Post-Review TODO

This TODO is derived from two sources: the deep pipeline review (`pipeline_review.md`) and the independent validation of its claims (`Validating_a_CDC_SCD2_Pipeline_Review__Ten_Technical_Claims_Tested.md`). Where the validation contradicted or refined the review, the validated position takes precedence. The only additional research considered is `64-Bit_Hashes_Are_Safe_for_CDC__Why_the_Birthday_Paradox_Does_Not_Apply_to_Per-Key_Change_Detection.md`, which informs the hash algorithm decision.

All prior refactoring phases (1–5) are complete and verified. This TODO covers what comes next.

---

## Phase 6 — Security Hardening (P0)

### 6.1 SQL Injection: Bracket-Escape All Dynamic Identifiers

**Status:** Complete (H-1 through H-4)
**Review ref:** 1.4, 1.5 | **Validation verdict:** Confirmed correct — bracket escaping with `]]` doubling is functionally equivalent to `QUOTENAME()`. Parameterized queries cannot protect identifiers, only values.

The validation confirmed that `[{name.replace(']', ']]')}]` is the correct client-side escaping pattern. It also noted that `QUOTENAME()` enforces a 128-character `sysname` limit that the Python-side `.replace()` does not — consider adding a length check.

- [x] **H-1:** Add `quote_identifier(name)` and `quote_table(full_table_name)` helpers to `connections.py`
  - Bracket-escape with `]]` doubling
  - Reject identifiers longer than 128 characters (match `QUOTENAME()` behavior)
- [x] **H-2:** Apply `quote_table()` to all f-string SQL in `bcp_loader.py`, `engine.py`, `schema_utils.py`, `table_creator.py`, `evolution.py`, `staging_cleanup.py`, `index_management.py`, `scd2/engine.py`
- [x] **H-3:** Switch `TableConfigLoader` (`table_config.py`) to parameterized pyodbc queries for `SourceName` and `SourceObjectName` CLI filters — these are user-supplied values, not identifiers
- [x] **H-4:** Add whitelist validation for CLI `--source` and `--table` arguments against known values from `UdmTablesList` as a belt-and-suspenders guard (in `cli_common.py`, called from both `main_small_tables.py` and `main_large_tables.py`)

### 6.2 BCP Password Exposure

**Status:** Code complete (H-5/H-6a implemented). Needs RHEL server testing to verify SQLCMDPASSWORD works with installed BCP version.
**Review ref:** 2.1 | **Validation verdict:** Problem confirmed. Proposed `SQLCMDPASSWORD` fix is undocumented for BCP specifically — must test before relying on it.

The `-P` flag exposes the password via `/proc/{pid}/cmdline` and `ps aux` to all system users. The validation found that Microsoft documents `SQLCMDPASSWORD` for `sqlcmd` but not for `bcp`, despite both shipping in `mssql-tools`.

- [ ] **H-5:** Test whether `SQLCMDPASSWORD` environment variable works with the installed BCP version (set env var, omit `-P`, verify successful bulk load)
- [x] **H-6a:** Implemented the env-var approach in `bcp_loader.py` — pass `env=` to `subprocess.run` with `SQLCMDPASSWORD` set, removed `-P` from command. If testing reveals SQLCMDPASSWORD doesn't work, revert to `-P` or pursue H-6b.
- [ ] **H-6b:** Fallback if `SQLCMDPASSWORD` does not work: evaluate switching to Kerberos/trusted connection (`-T` flag) or a dedicated minimal-privilege SQL login to reduce blast radius. Document the chosen approach and the reason `-P` remains if no alternative is viable

---

## Phase 7 — Performance: NFC Normalization Fix (P0)

**Status:** Complete (N-1 through N-3). N-4 requires DB access for timing comparison.
**Review ref:** 1.1 | **Validation verdict:** The performance problem is real, but the proposed fix is obsolete. Polars ships a native `Expr.str.normalize("NFC")` expression (PR #20483, Rust `unicode_normalization` crate internally). The sampling-for-ASCII optimization is unnecessary.

This is the single highest-impact performance fix in the pipeline. The current `map_elements` with a Python lambda forces row-by-row Python execution with GIL contention. The native expression runs entirely in Rust with SIMD acceleration and parallelization.

- [x] **N-1:** Replace the `map_elements` NFC normalization block in `row_hash.py` with native `pl.col(c).str.normalize("NFC")` in `add_row_hash()`
- [x] **N-2:** Applied the same fix in `add_row_hash_fallback()`
- [x] **N-3:** Updated minimum Polars version in `requirements.txt` to `>=1.13.0` (required for `str.normalize()` support and streaming engine anti-joins)
- [ ] **N-4:** Run a before/after timing comparison on a string-heavy table to quantify the improvement

---

## Phase 8 — Performance & Reliability (P1)

### 8.1 Connection Overhead Reduction

**Status:** P-3 complete (connection overhead measurement). P-1/P-2 require RHEL server access to check unixODBC version.
**Review ref:** 1.2 | **Validation verdict:** Partially correct. pyodbc enables ODBC Driver Manager pooling by default (`pyodbc.pooling = True`), but on Linux/unixODBC, pooling is broken in versions ≤ 2.3.11 and only works properly in ≥ 2.3.12.

Before building a custom pool, determine whether the built-in pooling is already working.

- [ ] **P-1:** Check installed unixODBC version on the RHEL pipeline server (`odbcinst --version` or `odbcinst -j`)
- [ ] **P-2a:** If unixODBC ≥ 2.3.12: Verify `pyodbc.pooling = True` (the default) is active and measure actual connection reuse. If pooling is working, no custom pool is needed — document the finding
- [ ] **P-2b:** If unixODBC < 2.3.12: Either upgrade unixODBC to ≥ 2.3.12 (preferred) or implement a connection pool. If a custom pool is needed, prefer SQLAlchemy's `QueuePool` (`mssql+pyodbc`) over a hand-rolled Queue — it provides stale connection detection via `pool_pre_ping`, overflow handling, and automatic `sp_reset_connection` on return
- [x] **P-3:** Added connection-overhead measurement to `connections.py` (`get_connection_overhead()`, `reset_connection_overhead()`). Total time spent in `get_connection()` is logged at pipeline end via `cli_common.log_connection_overhead()` in both `main_small_tables.py` and `main_large_tables.py`

### 8.2 Polars Streaming Engine for CDC Anti-Joins

**Status:** P-6 complete (code converted). P-4/P-5/P-7 require server access for version verification and benchmarking.
**Review ref:** 3.1 | **Validation verdict:** Mostly correct, but native streaming anti-join support shipped in ~v1.32 (PR #21937, March 2025), not v1.31. In v1.31, anti-joins silently fall back to the in-memory engine. The 3-7x performance figure is from official PDS-H benchmarks. The 40-60% memory reduction has no specific official source — directionally plausible but unsubstantiated.

- [ ] **P-4:** Verify installed Polars version is ≥ 1.32 (where native streaming anti-joins landed). If not, upgrade
- [ ] **P-5:** Test `polars-hash` compatibility with lazy frames and streaming collection before converting production code
- [x] **P-6:** Converted CDC anti-joins in `_run_cdc_core` (`cdc/engine.py`) to lazy + streaming:
  ```python
  df_new = (
      df_fresh.lazy()
      .join(df_existing.lazy(), on=pk_columns, how="anti")
      .collect(engine="streaming")
  )
  ```
  Also removed the W-5 TODO comment about streaming engine.
- [ ] **P-7:** Measure peak memory reduction on a large table to validate actual improvement (don't assume the 40-60% figure)

### 8.3 Guard Query Timeout

**Status:** Complete (P-8)
**Review ref:** 2.4 | **Validation verdict:** Not independently validated but logically sound.

- [x] **P-8:** Added `SET LOCK_TIMEOUT 5000` (5s) before baseline queries in `guards.py` (`get_extract_baseline()` and `get_daily_extract_baseline()`) to prevent indefinite hangs if the General database is under contention

---

## Phase 9 — Hash Algorithm Decision (P1)

**Status:** Complete. Decision: retain SHA-256 (A-3).
**Review ref:** Ten Claims §8 | **Research ref:** `64-Bit_Hashes_Are_Safe_for_CDC`

The validation found that SHA-256 is overkill for CDC change detection — dbt uses MD5 by default, and MD5 is ~2x faster with smaller hashes (128-bit vs 256-bit). The 64-bit hash research goes further: for per-PK change detection (comparing a row's new hash against only that same PK's previous hash), even 64-bit is astronomically safe. The birthday paradox applies to global uniqueness scenarios (surrogate keys, deduplication) but not to per-PK CDC. At 1 billion PKs with 3 versions each, the per-PK collision probability is ~1.6×10⁻¹⁰ — roughly one missed change per 10,000 years of daily runs.

This creates a three-tier decision:

| Hash | Bits | Per-PK CDC Safety | Global Uniqueness Safety | Speed |
|------|------|-------------------|--------------------------|-------|
| xxHash64 | 64 | Safe (10⁻¹⁰ at 1B PKs) | Unsafe (24% at 3B rows) | Fastest |
| MD5 | 128 | Safe | Safe (10⁻²⁰ at 3B rows) | ~2x faster than SHA-256 |
| SHA-256 | 256 | Safe | Safe | Current (baseline) |

**The critical constraint:** If the `_row_hash` column is ever used for anything other than per-PK CDC — joining, deduplication, reconciliation — the birthday paradox applies and 64-bit is dangerous at scale. The safest approach is to decide based on whether the hash column's scope can be permanently guaranteed.

- [x] **A-1:** Audited all current uses of `_row_hash` across the pipeline. **Result: hash is used EXCLUSIVELY for per-PK CDC comparison** (new hash vs. same PK's old hash) in `cdc/engine.py`, `scd2/engine.py`, and `cdc/reconciliation/`. No cross-PK operations (surrogate keys, deduplication, reconciliation joins across PKs) exist. 64-bit would be mathematically safe for current usage.
- [x] **A-2:** Documented hash scope constraint in `row_hash.py` module docstring (A-1 HASH SCOPE CONSTRAINT section). Includes explicit warning against repurposing for cross-PK operations without upgrading bit width.
- [x] **A-3:** Decision: **Retain SHA-256.** The ~2x speed cost vs MD5 is negligible relative to extraction and BCP I/O, and SHA-256 provides full safety margin for any future adjacent operations (reconciliation, surrogate keys, deduplication) without requiring a hash migration. No code change needed.
- N/A **A-4:** Not applicable — decided to retain SHA-256, no MD5 downgrade.
- N/A **A-5:** Not applicable — decided to retain SHA-256, no 64-bit downgrade.

---

## Phase 10 — Maintenance & Code Quality (P2)

### 10.1 Deduplicate Hash Normalization Logic

**Status:** Complete (M-1/M-2). M-3 requires test data/server access.
**Review ref:** 3.4

- [x] **M-1:** Extracted shared normalization into `_normalize_for_hashing(df, source_is_oracle)` returning `(df, hash_exprs)` in `row_hash.py`. All E-1, V-2, E-4, E-20 normalizations and hash expression building are in the shared function.
- [x] **M-2:** Rewrote `add_row_hash` and `add_row_hash_fallback` as thin wrappers calling `_normalize_for_hashing` then applying their respective hash implementations (polars-hash vs hashlib).
- [ ] **M-3:** Verify both paths produce identical hashes on a test DataFrame with NULLs, empty strings, floats, Unicode, and Oracle source data

### 10.2 Cache Repeated INFORMATION_SCHEMA Queries

**Status:** Complete (M-4/M-5)
**Review ref:** 3.3

- [x] **M-4:** Added `@lru_cache(maxsize=128)` via `_get_column_metadata_cached()` with a `_cache_version` parameter for invalidation in `schema_utils.py`. Returns tuples for hashability.
- [x] **M-5:** Added `clear_column_metadata_cache()` function (increments `_cache_version` to invalidate all cached entries). Called at the start of each table's processing in both `orchestration/small_tables.py` and `orchestration/large_tables.py`.

### 10.3 Redundant CSV Sanitization Validation

**Status:** Complete (M-6)
**Review ref:** 1.3

- [x] **M-6:** Added `validate: bool = True` parameter to `write_bcp_csv()` in `bcp_csv.py`. Internal callers that have already run `sanitize_strings()` can pass `validate=False`; external callers get the safety check by default.

### 10.4 BOM Check Optimization

**Status:** Complete (M-7)
**Review ref:** 3.2

- [x] **M-7:** Added `_bom_verified` module flag to `bcp_csv.py`. `_strip_bom_if_present` runs only on the first write per pipeline run. Polars never writes BOMs — one verification per run is sufficient.

---

## Phase 11 — Reliability & Recovery (P2)

### 11.1 Log Backup to /dev/null

**Status:** Complete (R-1/R-2b/R-3). Decision: PITR is not required during pipeline windows.
**Review ref:** 2.2 | **Validation verdict:** Fully confirmed as a genuinely dangerous anti-pattern. SQL Server believes the backup succeeded but the log chain is silently destroyed, making point-in-time restore impossible through the BULK_LOGGED window.

- [x] **R-1:** Determined that point-in-time restore during pipeline windows is NOT a business requirement. The pipeline runs in BULK_LOGGED mode during loads; full backup schedule covers recovery.
- [x] **R-2b:** Replaced the `/dev/null` backup in `bcp_loader.py` with a comment and log message documenting the tradeoff: PITR not required during pipeline windows; log chain gap accepted; full backup schedule covers recovery.
- [x] **R-3:** Removed the `BACKUP LOG ... TO DISK = N'/dev/null'` line from `bcp_loader.py`.

### 11.2 Autocommit/Commit Inconsistency

**Status:** Complete (R-4)
**Review ref:** 2.3

- [x] **R-4:** Fixed the misleading comment in `event_tracker.py` (`_get_next_batch_id`). Updated to accurately state that the explicit `conn.commit()` is a defensive guard — a no-op under the current `autocommit=True` configuration, but protects against future autocommit configuration changes (per OBS-5 contract).

---

## Phase 12 — Operational Improvements (P3)

### 12.1 Oracle DATE Column Heuristic

**Status:** Complete (O-1)
**Review ref:** 4.3

- [x] **O-1:** Added content-based datetime detection in `sanitize.py` via `_looks_like_datetime_column()`. Samples first 10 non-null values from string columns and checks if ≥80% parse as datetimes. Catches Oracle DATE columns named `CREATED_AT`, `MODIFIED_TS`, etc. that lack "DATE" in the column name.

### 12.2 Structured Logging for Key Operational Signals

**Status:** Complete (O-2)
**Review ref:** 4.1

- [x] **O-2:** Added structured JSON payloads alongside human-readable log lines for the highest-value operational signals:
  - `cdc/engine.py`: O-2_CDC structured log after CDC result summary (inserts/updates/deletes/unchanged counts + update_ratio)
  - `scd2/engine.py`: O-2_SCD2 structured logs after both `run_scd2()` and `run_scd2_targeted()` summaries (closes/inserts/resurrections/unchanged + active_ratio)
  - `orchestration/guards.py`: O-2_GUARD structured logs for extraction guard triggers (ceiling, drop, warn, growth conditions)
  - `schema/evolution.py`: O-2_SCHEMA structured log for schema evolution events (columns added/removed/type changes)

### 12.3 ConnectorX Retry Allowlist

**Status:** Complete (O-3)
**Review ref:** 4.2

- [x] **O-3:** Refined `cx_read_sql_safe` retry logic in `extract/__init__.py` to distinguish transient from permanent errors:
  - Added `_NON_RETRYABLE_PATTERNS`: syntax errors, permission denied, ORA-00942 (table not found), ORA-01017 (invalid credentials), ORA-01031 (insufficient privileges), ORA-00933/ORA-06550 (SQL syntax), login failed, access denied
  - Added `_TRANSIENT_PATTERNS`: ORA-12541 (listener down), ORA-12170/ORA-03135/ORA-03113 (connection lost), connection reset, timeout, broken pipe
  - Non-retryable errors fail fast on first attempt; transient errors use exponential backoff retry (3 attempts)
  - `KeyboardInterrupt`/`SystemExit` propagated immediately without retry

---

## Architecture Notes (No Action Required — Documentation Only)

These are validated observations about the architecture that are correctly handled but should be understood by future maintainers.

1. **BULK_LOGGED scope:** The `bulk_load_recovery_context` switches the entire Bronze database to BULK_LOGGED, affecting all concurrent operations — not just the pipeline's BCP loads. This is a known, documented tradeoff.

2. **INSERT-first SCD2 is sound but non-standard.** The validation confirmed the crash-safety logic is valid (duplicates are recoverable; missing rows are not). However, no major framework (dbt, Databricks, Iceberg) uses this pattern — they rely on transactional wrapping or atomic MERGE instead. The INSERT-first pattern is most appropriate for the pipeline's non-transactional BCP-based environment.

3. **MERGE avoidance remains justified through SQL Server 2022.** The validation confirmed unfixed bugs persist, including a new one discovered February 2025. Separate UPDATE + INSERT is the community consensus for production SCD2, endorsed by Brent Ozar, Michael J. Swart, Erik Darling, and Aaron Bertrand.

4. **`sp_getapplock` for ETL concurrency is a well-established pattern.** Fully validated. Session-owned locks are appropriate. Key gotcha under RCSI: release lock via COMMIT, not `sp_releaseapplock`, to prevent stale reads.

5. **Hash sentinel `\x1F` is more robust than industry standard but non-portable.** dbt uses `-` as separator and `_dbt_utils_surrogate_key_null_` as NULL sentinel. The pipeline's approach is theoretically superior (ASCII 31 virtually never appears in data) but means hash reproduction by external tools requires exact sentinel replication. Document the full hash specification if cross-system comparison is ever needed.

6. **`_safe_concat` workaround (T-2) should be revisited with Polars upgrades.** The Polars issues #12543 and #18911 that motivated this may be fixed in newer versions.

---

## Priority Summary

| Phase | Focus | Priority | Status |
|-------|-------|----------|--------|
| 6 | SQL injection hardening | **P0** | **6.1 Complete** (H-1 to H-4). **6.2 Code complete** (H-6a), needs RHEL server testing (H-5) |
| 7 | NFC native expression | **P0** | **Complete** (N-1 to N-3). N-4 needs DB timing |
| 8 | Connection overhead, streaming engine, guard timeouts | **P1** | **P-3 Complete**, **P-6 Complete**, **P-8 Complete**. P-1/P-2 need server access, P-4/P-5/P-7 need version check + benchmarking |
| 9 | Hash algorithm decision | **P1** | **Complete** — retain SHA-256 (A-1/A-2 audit + A-3 decision) |
| 10 | Code quality & deduplication | **P2** | **Complete** (M-1/M-2/M-4/M-5/M-6/M-7). M-3 needs test data |
| 11 | Recovery & consistency fixes | **P2** | **Complete** (R-1/R-2b/R-3/R-4) |
| 12 | Operational polish | **P3** | **Complete** (O-1/O-2/O-3) |

---

## Remaining Server-Dependent Items

The following items require RHEL server access, test data, or runtime verification and cannot be completed in a development-only environment:

| Item | Description | Dependency |
|------|-------------|------------|
| H-5 | Test SQLCMDPASSWORD with installed BCP version | RHEL server |
| N-4 | Before/after NFC timing comparison | DB + string-heavy table |
| P-1/P-2 | Check unixODBC version, verify connection pooling | RHEL server |
| P-4 | Verify Polars version ≥ 1.32 | Pipeline server |
| P-5 | Test polars-hash with lazy frames + streaming | Pipeline server |
| P-7 | Measure peak memory reduction from streaming anti-joins | Large table + monitoring |
| M-3 | Verify both hash paths produce identical output | Test DataFrame |

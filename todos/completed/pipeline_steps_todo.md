# UDM Pipeline — TODO

Review date: 2026-02-24.

---

## Bugs / Issues

### 1. ~~SQL Injection Surface in `pipeline_steps.py`~~ — COMPLETE

**Files:** `orchestration/pipeline_steps.py`

Fixed: wrapped `bronze_table` in `quote_table()` in both `log_active_ratio` and `log_data_freshness`. Also migrated both functions from verbose `conn/try/finally` pattern to `cursor_for()` (Item 6).

- [x] Fix `log_active_ratio` to use `quote_table(bronze_table)`
- [x] Fix `log_data_freshness` to use `quote_table(bronze_table)`

---

### 2. ~~`reconcile_table_windowed` Does Not Reconcile Against Source~~ — COMPLETE

**File:** `cdc/reconciliation/core.py`

Fixed: implemented full windowed source extraction and comparison. Added `_extract_source_windowed()` which routes to Oracle/SQL Server windowed extractors. The function now:
1. Extracts source rows within the date window via ConnectorX windowed extractors
2. Reads Stage current rows for the same window
3. Compares PKs (anti-joins for source-only and stage-only rows)
4. Performs column-by-column comparison on matched rows
5. Persists results to ReconciliationLog (OBS-6) with `WINDOWED_RECONCILIATION` check type

- [x] Implement windowed source extraction and comparison in `reconcile_table_windowed()`

---

### 3. ~~Polars Version Floor Doesn't Match Runtime Requirements~~ — COMPLETE

**File:** `requirements.txt`

Fixed: raised minimum pin from `polars>=1.13.0` to `polars>=1.32.0`. Updated comment to reference both `str.normalize` and streaming anti-join support.

- [x] Raise minimum pin in `requirements.txt` from `polars>=1.13.0` to `polars>=1.32.0`
- [x] Update the N-3 comment to reference both `str.normalize` and streaming anti-join support

---

### 4. ~~Dead Feature Flags~~ — COMPLETE

**Files:** `orchestration/small_tables.py`, `orchestration/large_tables.py`

Both orchestrators defined `USE_POLARS_CDC = True` and `USE_POLARS_SCD2 = True`, but nothing branched on them. Removed from both files. Also updated CLAUDE.md to remove stale feature flag reference.

---

### 5. ~~Orphaned Staging Table Risk in `_expire_cdc_rows`~~ — COMPLETE

**File:** `cdc/engine.py`

Fixed: restructured `_expire_cdc_rows` so the staging table creation happens first, then ALL subsequent work (BCP load, index creation, UPDATE JOIN, rowcount validation) is wrapped in a `try/finally` block with `DROP TABLE IF EXISTS` in the `finally`. Also migrated to `cursor_for()` (Item 6).

- [x] Restructure to ensure staging table DROP is in a `finally` block

---

## Code Quality Improvements

### 6. ~~Migrate Manual Connection Handling to `cursor_for()`~~ — COMPLETE

**Files:** Multiple

Migrated the following functions from verbose `conn = get_connection(); try: ... finally: conn.close()` to `with cursor_for(db) as cur:`:

- `cdc/engine.py` — `_expire_cdc_rows` (3 connection blocks → 3 `cursor_for` calls + `finally` block)
- `data_load/bcp_loader.py` — `truncate_table`, `execute_sql`, `get_row_count`, `create_staging_index`
- `orchestration/pipeline_steps.py` — `log_active_ratio`, `log_data_freshness`
- `orchestration/guards.py` — `get_extract_baseline`, `get_daily_extract_baseline`
- `connections.py` — `verify_rcsi_enabled` (already used `cursor_for()`)

Not migrated (intentionally):
- `data_load/bcp_loader.py` — `bulk_load_recovery_context` (complex lifecycle: two separate connections for ALTER DATABASE before/after yield, doesn't fit the simple cursor pattern)

- [x] Migrate applicable functions to use `cursor_for()`

---

### 7. ~~polars-hash Upgrade Evaluation~~ — PARTIAL (test harness built)

**Priority:** P2 — Dependency Risk
**File:** `requirements.txt`, `data_load/row_hash.py`, `tests/test_hash_regression.py`

The pipeline pins `polars-hash==0.4.5` while v0.5.6 (January 2026) has been available for over a month. This is the single highest-risk dependency: a single-maintainer package with 84 GitHub stars that could break on a Polars upgrade.

Built `tests/test_hash_regression.py` — a self-contained test harness with a deterministic reference dataset covering all dtypes (strings, Unicode/NFC, integers, floats with IEEE 754 edge cases, dates, datetimes, booleans, NULLs). Usage:
1. `python3 tests/test_hash_regression.py --self-check` — verify polars-hash and hashlib produce identical output
2. `python3 tests/test_hash_regression.py --save-baseline` — save hashes with current version (0.4.5)
3. Upgrade polars-hash, then `python3 tests/test_hash_regression.py --compare` — compare against baseline

- [x] Build hash regression test harness
- [ ] Run comparison between polars-hash 0.4.5 and 0.5.6 (requires server access)
- [ ] Upgrade if hashes match, or plan rehash migration window if they don't

---

### 8. ~~Combine String Normalization Passes in `_normalize_for_hashing`~~ — COMPLETE

**Priority:** P3 — Performance
**File:** `data_load/row_hash.py`

Fixed: combined three separate `with_columns` calls (E-1 Oracle empty→NULL, N-1/V-2 NFC normalization, E-4 RTRIM) into a single pass. Each string column builds one chained expression: `pl.when(...).then(None).otherwise(expr).str.normalize("NFC").str.rstrip(" ")`. Eliminates two intermediate DataFrame materializations.

- [x] Combine the three `with_columns` passes into one

---

### 9. Test Suite

**Priority:** P2 — Long-term Reliability
**Status:** Deferred — to be created later

The pipeline has thorough documentation of what should be tested (hash regression, join behavior, CDC count validation, SCD2 crash recovery), but no test files exist. Priority areas when tests are built:

- Hash regression tests (polars-hash version stability, H-2)
- CDC correctness against known fixtures (insert/update/delete detection)
- NULL PK filtering behavior
- PK dtype alignment across Oracle/SQL Server type systems
- SCD2 INSERT-first crash recovery (duplicate current row dedup)
- Schema evolution (new columns, removed columns, type changes)
- Extraction guard threshold logic
- BCP CSV contract validation (separators, terminators, quoting)

---

### 10. ~~Content-Based Datetime Detection Is Fragile~~ — COMPLETE

**Priority:** P3 — Robustness
**File:** `data_load/sanitize.py` (`_looks_like_datetime_column`)

Fixed: hardened the content-based detection with three changes:
1. Sample size raised from 10 to 20 non-null values for better statistical significance.
2. Match threshold raised from 80% to 90% to reduce false positive risk.
3. Multiple datetime formats now checked in sequence: standard `%Y-%m-%d %H:%M:%S`, ISO-8601 with `T` separator, fractional seconds variants, and Oracle `NLS_DATE_FORMAT` variants (`%d-%b-%Y`, `%d-%b-%y`).

- [x] Evaluate if content-based detection has caused issues in practice
- [x] Added additional format patterns and raised the match threshold

---

### 11. ~~BCP Subprocess Inherits Full `os.environ`~~ — COMPLETE

**Priority:** P3 — Security Hardening
**File:** `data_load/bcp_loader.py`

Fixed: replaced `{**os.environ, "SQLCMDPASSWORD": ...}` with a minimal environment that only propagates `SQLCMDPASSWORD`, `PATH`, `HOME`, `LANG`, `LC_ALL`, `LC_CTYPE`, `TMPDIR`, and `TMP`. Other secrets (Oracle passwords, API keys, tokens) are no longer leaked to the BCP subprocess.

- [x] Construct a minimal environment with only PATH, LANG/locale vars, HOME, and SQLCMDPASSWORD

---

### 12. ~~Extraction Guard Baseline Limited to 5 Data Points~~ — COMPLETE

**Priority:** P3 — Stability
**File:** `orchestration/guards.py`

Fixed: changed all four `TOP 5` queries to `TOP 14` in both `get_extract_baseline` and `get_daily_extract_baseline` (day-of-week and any-day fallback queries). A median of 14 values is significantly more stable — two weeks of daily data points means one anomalous run has minimal impact on the baseline.

- [x] Increase `TOP 5` to `TOP 14` for more stable median baselines

---

## Completed

- [x] **Item 1** — SQL injection in `log_active_ratio` / `log_data_freshness` fixed with `quote_table()` + `cursor_for()`
- [x] **Item 2** — `reconcile_table_windowed` now extracts from source and does full PK + column comparison
- [x] **Item 3** — Polars version floor raised to `>=1.32.0`
- [x] **Item 4** — Dead feature flags removed from both orchestrators + CLAUDE.md
- [x] **Item 5** — Orphaned staging table risk fixed with `finally` block in `_expire_cdc_rows`
- [x] **Item 6** — Manual connections migrated to `cursor_for()` across 4 files (10 functions)
- [x] **Item 7** — Hash regression test harness built (`tests/test_hash_regression.py`); actual version comparison requires server access
- [x] **Item 8** — String normalization combined into single `with_columns` pass in `_normalize_for_hashing`
- [x] **Item 10** — Content-based datetime detection hardened: sample 20→90% threshold, 6 format patterns
- [x] **Item 11** — BCP subprocess now uses minimal environment (PATH, HOME, locale, SQLCMDPASSWORD only)
- [x] **Item 12** — Extraction guard baseline expanded from TOP 5 to TOP 14
- [x] **O-3** — ConnectorX retry allowlist verified in `extract/__init__.py`
- [x] **O-2_SCD2** — SCD2 structured logging bug fix (added `resurrections` field to `SCD2Result`)

---

## Priority Summary

| # | Item | Priority | Status |
|---|------|----------|--------|
| 1 | SQL injection in `log_active_ratio` / `log_data_freshness` | **P1** | **Complete** |
| 2 | `reconcile_table_windowed` source comparison | **P1** | **Complete** |
| 3 | Polars version floor for streaming engine | **P1** | **Complete** |
| 4 | Dead feature flags | **P2** | **Complete** |
| 5 | Orphaned staging table risk in `_expire_cdc_rows` | **P2** | **Complete** |
| 6 | Migrate manual connections to `cursor_for()` | **P2** | **Complete** |
| 7 | polars-hash upgrade evaluation (0.4.5 → 0.5.6) | **P2** | **Partial** (harness built, needs server run) |
| 9 | Test suite (deferred) | **P2** | Deferred |
| 8 | Combine string normalization passes | **P3** | **Complete** |
| 10 | Fragile content-based datetime detection | **P3** | **Complete** |
| 11 | BCP subprocess inherits full environment | **P3** | **Complete** |
| 12 | Extraction guard baseline too small (TOP 5) | **P3** | **Complete** |

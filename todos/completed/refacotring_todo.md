# UDM Pipeline — Refactoring TODO

## Phase 1 — Directory Restructure (Pure File Moves) ✅ COMPLETE

See `project_restructure.md` for full plan. Zero logic changes — only file moves + import updates.

*Verified 2026-02-23: All restructuring already in place. No `pipeline/` directory exists. All 7 packages have `__init__.py`. CLAUDE.md reflects current structure. Zero stale `from pipeline.*` imports.*

- [x] **R-1:** Create directories with `__init__.py`: `cdc/`, `scd2/`, `orchestration/`, `schema/`, `observability/`
- [x] **R-2:** Move `observability/` (2 files, fewest dependents) — update imports
- [x] **R-3:** Move `schema/` (4 files) — update imports
- [x] **R-4:** Move `cdc/` (2 files) — update imports
- [x] **R-5:** Move `scd2/` (1 file) — update imports
- [x] **R-6:** Move `orchestration/` (5 files, do last — it's the hub) — update imports
- [x] **R-7:** Update CLAUDE.md Structure section to reflect new paths
- [x] **R-8:** Test — `py_compile` passes on all modules; live DB test requires deployment
- [x] **R-9:** Delete empty `pipeline/` directory — no `pipeline/` directory exists

---

## Phase 2 — Orchestrator Deduplication (P0) ✅ COMPLETE

**Problem:** `small_tables.py` (705 lines) and `large_tables.py` (905 lines) copy-paste 3 functions verbatim and duplicate the CDC/SCD2 promotion block patterns.

*Verified 2026-02-23: `orchestration/pipeline_steps.py` (425 lines) created with shared functions. small_tables.py reduced from 706→501 lines, large_tables.py from 905→694 lines. All three files pass py_compile. No stale references to `_cleanup_csvs`, `_log_active_ratio`, `_log_data_freshness`.*

- [x] **D-1:** Extract `orchestration/pipeline_steps.py` with shared functions:
  - `log_active_ratio(table_config, scd2_event)` — duplicated in both orchestrators (~45 lines each)
  - `log_data_freshness(table_config)` — duplicated in both orchestrators (~55 lines each)
  - `cleanup_csvs(output_dir, table_config)` — large_tables unused `target_date` param dropped
- [x] **D-2:** Extract shared CDC promotion step into `pipeline_steps.py`:
  - `run_cdc_promotion(table_config, df, event_tracker, schema_result, output_dir)` — event tracking, update_ratio monitoring, metadata assembly, E-12 phantom change detection. Routes to `run_cdc()` or `run_cdc_windowed()` via `windowed` flag.
- [x] **D-3:** Extract shared SCD2 promotion step into `pipeline_steps.py`:
  - `run_scd2_promotion(table_config, cdc_result, event_tracker, output_dir)` — index disable/rebuild, row count before/after, active ratio, freshness check. Routes to `run_scd2()` or `run_scd2_targeted()` via `targeted` flag. Preserves P1-8 (narrow BULK_LOGGED scope) and P2-6 (conditional index management) for large tables.
- [x] **D-4:** Slim both orchestrators to call shared steps — small_tables 501 lines, large_tables 694 lines
- [x] **D-5:** py_compile passes on all 3 files, no circular imports, no stale references. Live DB test requires deployment.

**Lines saved:** ~200 duplicated lines eliminated.

---

## Phase 3 — CDC Core Unification (P0) ✅ COMPLETE

**Problem:** `run_cdc()` (lines 100–287) and `run_cdc_windowed()` (lines 816–1026) in `engine.py` share ~70% identical code.

*Verified 2026-02-23: `CDCContext` dataclass + `_run_cdc_core()` extracted. Both public functions are thin wrappers. engine.py reduced from 1027→899 lines. All four importing modules pass py_compile. No stale `read_stage_table(` or `read_stage_table_windowed(` calls outside wrappers.*

- [x] **C-1:** Create `CDCContext` dataclass to parameterize the differences (read_existing, track_deleted_pks, log_label, log_window)
- [x] **C-2:** Extract `_run_cdc_core(table_config, df_fresh, batch_id, output_dir, ctx)` with all shared logic (~180 lines)
- [x] **C-3:** Rewrite `run_cdc()` as thin wrapper constructing context + calling `_run_cdc_core()`
- [x] **C-4:** Rewrite `run_cdc_windowed()` as thin wrapper constructing context + calling `_run_cdc_core()`
- [x] **C-5:** Public API unchanged — verified no import changes needed in orchestrators or pipeline_steps.py
- [x] **C-6:** py_compile passes on engine.py, pipeline_steps.py, small_tables.py, large_tables.py. Live DB test requires deployment.

**Lines saved:** ~128 lines eliminated (1027→899).

---

## Phase 4 — Large File Splits (P1) ✅ COMPLETE

### 4a. Reconciliation Subpackage

**Problem:** `reconciliation.py` is 2,171 lines with 12 distinct strategies, 8 dataclasses, and shared persistence infra.

*Verified 2026-02-23: `cdc/reconciliation/` package created with 9 modules. Original `reconciliation.py` deleted. `__init__.py` re-exports all public names. All modules pass py_compile. Zero stale imports found.*

- [x] **S-1:** Create `cdc/reconciliation/` package directory
- [x] **S-2:** Extract `cdc/reconciliation/models.py` — all 10 result dataclasses (~180 lines):
  - `ReconciliationResult`, `CountReconciliationResult`, `ActivePKReconciliationResult`
  - `VersionVelocityResult`, `AggregateReconciliationResult`, `BronzeReconciliationResult`
  - `SCD2IntegrityResult`, `DistributionShiftResult`, `TransformationBoundaryResult`
  - `ReferentialIntegrityResult`
- [x] **S-3:** Extract `cdc/reconciliation/persistence.py` — `ensure_reconciliation_log_table()`, `_persist_reconciliation_result()` (~100 lines)
- [x] **S-4:** Extract `cdc/reconciliation/core.py` — `reconcile_table()`, `reconcile_table_windowed()`, `_extract_source()`, `_RECON_MAX_ROWS` (~270 lines)
- [x] **S-5:** Extract `cdc/reconciliation/counts.py` — `reconcile_counts()`, `reconcile_active_pks()`, helpers (~280 lines)
- [x] **S-6:** Extract `cdc/reconciliation/scd2_integrity.py` — `validate_scd2_integrity()`, `reconcile_bronze()` (~340 lines)
- [x] **S-7:** Extract `cdc/reconciliation/data_quality.py` — `reconcile_aggregates()`, `detect_distribution_shift()`, baseline helpers (~330 lines)
- [x] **S-8:** Extract `cdc/reconciliation/boundaries.py` — `reconcile_transformation_boundary()`, `check_referential_integrity()` (~290 lines)
- [x] **S-9:** Extract `cdc/reconciliation/velocity.py` — `check_version_velocity()` (~110 lines)
- [x] **S-10:** Create `cdc/reconciliation/__init__.py` re-exporting all public names for backward compatibility
- [x] **S-11:** Delete original `reconciliation.py` after verifying imports

### 4b. BCP CSV Concern Separation

**Problem:** `bcp_csv.py` (811 lines) mixes row hashing (~300 lines), DataFrame preparation (~250 lines), and CSV contract enforcement (~250 lines).

*Verified 2026-02-23: 3 new modules created. `bcp_csv.py` slimmed to ~190 lines with re-exports for backward compatibility. Imports updated in `cdc/engine.py`, `scd2/engine.py`, `extract/udm_connectorx_extractor.py`. All modules pass py_compile. Zero stale `from data_load.bcp_csv import sanitize_strings` or `cast_bit_columns` found.*

- [x] **B-1:** Extract `data_load/row_hash.py`:
  - `add_row_hash()`, `add_row_hash_fallback()`
  - Float normalization (V-1, W-3), NFC normalization (V-2)
  - Oracle empty-string normalization (E-1), RTRIM (E-4)
  - Categorical cast guard (E-20)
  - Constants: `FLOAT_HASH_PRECISION`, `_NULL_SENTINEL`, `_NAN_SENTINEL`, `_INF_SENTINEL`, `_NEG_INF_SENTINEL`
- [x] **B-2:** Extract `data_load/bcp_format.py`:
  - `generate_bcp_format_file()`, `_SQL_TO_BCP_TYPE` mapping
- [x] **B-3:** Extract `data_load/sanitize.py`:
  - `sanitize_strings()`, `cast_bit_columns()`, `reinterpret_uint64()`
  - `reorder_columns_for_bcp()`, `fix_oracle_date_columns()`
  - `ColumnOrderError`
- [x] **B-4:** Slim `bcp_csv.py` to:
  - `prepare_dataframe_for_bcp()` — orchestrates the pipeline calling into extracted modules
  - `write_bcp_csv()`, `_validate_sanitized()`, `_strip_bom_if_present()`
  - `validate_schema_before_concat()`, `SchemaValidationError`
  - `add_extracted_at()`
  - Re-exports from new modules for backward compatibility
- [x] **B-5:** Update imports in `cdc/engine.py`, `scd2/engine.py`, `extract/udm_connectorx_extractor.py`

---

## Phase 5 — Nice-to-Haves (P2–P3) ✅ COMPLETE

*Verified 2026-02-23: All Phase 5 items implemented. `extract/router.py` centralizes extraction routing (160 lines). `cli_common.py` centralizes CLI boilerplate (178 lines). `cursor_for()` added to `connections.py`. `orchestration/guards.py` unifies extraction guards (334 lines). small_tables.py reduced 502→275, large_tables.py 695→487, main_small_tables.py 299→173, main_large_tables.py 287→168. All files pass py_compile. Zero stale references found.*

### Extraction Router (P2)

- [x] **E-1:** Create `extract/router.py` with:
  - `extract_full(table_config, output_dir)` — routes Oracle/SQL Server × ConnectorX/oracledb
  - `extract_windowed(table_config, target_date, output_dir)` — routes windowed extraction
- [x] **E-2:** Replace `_extract()` in `small_tables.py` and `_extract_day()` in `large_tables.py` with router calls
- [x] **E-3:** Prevents extraction routing logic from drifting between orchestrators

### CLI Common Boilerplate (P2)

- [x] **L-1:** Create `cli_common.py` with:
  - Module-level: MALLOC_ARENA_MAX, POLARS_MAX_THREADS, sys.path
  - `setup_logging(batch_id)` — console + SQL Server log handler
  - `startup_checks()` — RCSI verification, staging cleanup
  - `warn_malloc_arena()` — W-4 external env check
  - `warn_workers(workers)` — M-2 worker count check
  - `check_rss_memory(source_name, table_name)` — B-8 RSS monitoring
  - `table_config_to_dict(tc, batch_id)` — TableConfig serialization
- [x] **L-2:** Slim both `main_*.py` files to argument parsing + worker orchestration

### Connection Context Managers (P3)

- [x] **X-1:** Add `cursor_for(database)` context manager to `connections.py`:
  ```python
  @contextmanager
  def cursor_for(database: str):
      conn = get_connection(database)
      try:
          cursor = conn.cursor()
          yield cursor
          cursor.close()
      finally:
          conn.close()
  ```
- [x] **X-2:** Gradually replace `conn = get_connection(); try: ... finally: conn.close()` pattern across codebase — applied to `verify_rcsi_enabled()` as first adoption

### Extraction Guard Unification (P3)

- [x] **G-1:** Create `orchestration/guards.py` with parameterized `check_extraction_guard()` + convenience wrappers `run_extraction_guard()` and `run_daily_extraction_guard()`
- [x] **G-2:** Refactor `_check_extraction_guard()` (small) and `_check_daily_extraction_guard()` (large) to use shared base — baseline retrieval strategies: `get_extract_baseline()` and `get_daily_extract_baseline()`

---

## Execution Notes

- Each phase is independently testable and deployable
- No phase depends on a later phase
- Phase 1 is purely structural (file moves) — safest to do first
- Phases 2–3 eliminate ~400 lines of duplication — highest code quality ROI
- Phase 4 is organizational splitting — no logic changes, just better file boundaries
- Phase 5 items can be done opportunistically as you touch those files
- Always test with `--table <single_table> --source DNA` before running full pipeline
- Always verify BCP CSV format compatibility after Phase 4b changes
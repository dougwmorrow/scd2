# UDM Pipeline — Refactoring Candidates

## Executive Summary

The directory reorganization from `project_restructure.md` is already in place — the codebase has a good top-level structure (`cdc/`, `scd2/`, `orchestration/`, `schema/`, `observability/`, `extract/`, `data_load/`). The next phase of refactoring is **within-module decomposition**: breaking apart oversized files, extracting duplicated code into shared utilities, and tightening the single-responsibility principle inside the existing directory boundaries.

This document identifies **8 refactoring candidates** ranked by impact. None require logic changes — all are structural moves that reduce duplication, improve testability, and make the codebase more navigable.

---

## Refactoring Priority Matrix

| # | Target | Current Lines | Problem | Risk | Effort |
|---|--------|--------------|---------|------|--------|
| R1 | `reconciliation.py` | 2,172 | God module — 8 result classes, 15+ functions across 6 unrelated reconciliation types | Low | Medium |
| R2 | `main_small_tables.py` + `main_large_tables.py` | 299 + 287 | ~85% identical code | Low | Low |
| R3 | `small_tables.py` + `large_tables.py` (orchestrators) | 706 + 905 | 3 functions copy-pasted verbatim | Low | Low |
| R4 | `bcp_csv.py` | 812 | Mixes hashing, sanitization, CSV writing, schema validation, format files | Low | Medium |
| R5 | `engine.py` (CDC) | 1,027 | `run_cdc()` and `run_cdc_windowed()` share ~80% logic | Medium | Medium |
| R6 | Extractor duplication | 498 + 287 | `_supplement_null_partition_rows()` and `_log_partition_skew()` duplicated across Oracle/SQL Server extractors | Low | Low |
| R7 | `column_sync.py` | 656 | Generic sync + Oracle PK discovery + SQL Server PK discovery + type drift detection in one file | Low | Low |
| R8 | `table_creator.py` + `evolution.py` | 430 + 542 | Quarantine logic in evolution.py is unrelated to schema evolution proper | Low | Low |

---

## R1 — Split `reconciliation.py` (2,172 lines → 4 modules)

### Problem

`reconciliation.py` is the largest file in the project and contains six distinct reconciliation types that share almost no internal logic. When debugging a count mismatch, you're scrolling past distribution shift detection and referential integrity checks. Each reconciliation type has its own result dataclass, its own query patterns, and its own concerns.

### Current Contents (grouped by concern)

**CDC Hash Reconciliation** (lines 1–276)
- `ReconciliationResult` — result dataclass
- `reconcile_table()` — full column-by-column Stage vs Source comparison
- `reconcile_table_windowed()` — same for large tables (date-scoped)
- `_extract_source()` — shared extraction helper

**Count & PK Reconciliation** (lines 277–629)
- `CountReconciliationResult` + `reconcile_counts()` — Source vs Stage vs Bronze row counts
- `_get_source_row_count()`, `_get_current_stage_count()`, `_get_active_bronze_count()` — count helpers
- `ActivePKReconciliationResult` + `reconcile_active_pks()` — Bronze duplicate/orphan detection

**Version & Aggregate Reconciliation** (lines 630–933)
- `VersionVelocityResult` + `check_version_velocity()` — SCD2 version churn detection
- `AggregateReconciliationResult` + `reconcile_aggregates()` — numeric aggregate comparison across layers

**Bronze & SCD2 Integrity** (lines 1,068–1,475)
- `BronzeReconciliationResult` + `reconcile_bronze()` — Bronze active-row validation
- `SCD2IntegrityResult` + `validate_scd2_integrity()` — SCD2 timeline gap/overlap detection

**Statistical & Cross-Layer** (lines 1,476–2,079)
- `DistributionShiftResult` + `detect_distribution_shift()` — z-score distribution monitoring
- `TransformationBoundaryResult` + `reconcile_transformation_boundary()` — layer boundary validation
- `ReferentialIntegrityResult` + `check_referential_integrity()` — FK validation across SCD2 tables

**Persistence** (lines 2,080–2,172)
- `ensure_reconciliation_log_table()` — DDL for ReconciliationLog
- `_persist_reconciliation_result()` — write results to General.ops.ReconciliationLog

### Proposed Split

```
cdc/
├── reconciliation/
│   ├── __init__.py               # Re-export all public functions for backward compat
│   ├── hash_reconciliation.py    # reconcile_table(), reconcile_table_windowed()
│   ├── count_reconciliation.py   # reconcile_counts(), reconcile_active_pks()
│   ├── bronze_reconciliation.py  # reconcile_bronze(), validate_scd2_integrity(),
│   │                             # check_version_velocity()
│   ├── cross_layer.py            # reconcile_aggregates(),
│   │                             # reconcile_transformation_boundary(),
│   │                             # detect_distribution_shift(),
│   │                             # check_referential_integrity()
│   └── persistence.py            # ensure_reconciliation_log_table(),
│                                 # _persist_reconciliation_result()
```

**Backward compatibility:** The `__init__.py` re-exports everything, so existing `from cdc.reconciliation import reconcile_table` still works. The CLAUDE.md usage example stays valid.

### Why This Grouping

- `hash_reconciliation.py` owns the CDC-specific concern: "did the hash-based CDC miss anything?"
- `count_reconciliation.py` owns the fast-check concern: "do row counts add up across layers?"
- `bronze_reconciliation.py` owns the Bronze/SCD2 structural concern: "is SCD2 data internally consistent?"
- `cross_layer.py` owns the statistical/boundary concern: "do layers agree with each other numerically?"
- `persistence.py` is shared infrastructure consumed by all the above

---

## R2 — Extract Shared CLI Infrastructure (main_*.py)

### Problem

`main_small_tables.py` and `main_large_tables.py` are ~85% identical. These functions are copy-pasted verbatim:

- `setup_logging()` — 18 lines, character-for-character identical
- `_table_config_to_dict()` — 36 lines, character-for-character identical
- `_check_rss_memory()` — 25 lines, character-for-character identical
- MALLOC_ARENA_MAX setup block — 6 lines identical
- W-4 warning block — 8 lines identical
- M-2 worker warning — 5 lines identical
- Parallel execution with ProcessPoolExecutor — 15 lines nearly identical

The only differences are: which orchestrator function is called, which `load_*_tables()` method is used, and `--list-tables` column formatting.

### Proposed Extraction

```
orchestration/
├── cli_common.py      # NEW — shared CLI infrastructure
│   ├── setup_environment()     # MALLOC_ARENA_MAX, POLARS_MAX_THREADS
│   ├── setup_logging()
│   ├── table_config_to_dict()  # Serialization for cross-process transfer
│   ├── table_config_from_dict() # Deserialization (currently inline in workers)
│   ├── check_rss_memory()
│   ├── run_pipeline()          # Generic parallel/sequential execution loop
│   └── pre_flight_checks()     # staging cleanup + RCSI verification
```

After this extraction, each `main_*.py` becomes ~40 lines: argument parsing, config loading, and a single call to `run_pipeline()` with a callback to the appropriate orchestrator. The `_process_table_worker()` functions differ only in which orchestrator they call — a single parameterized worker handles both.

### Additional Benefit

The `table_config_to_dict()` / reconstruction pattern is fragile — it manually lists every field and breaks silently when a new field is added to `TableConfig`. Centralizing it means one place to maintain, and you could replace the manual dict with `dataclasses.asdict()` + a proper `from_dict()` classmethod on `TableConfig`.

---

## R3 — Extract Shared Orchestrator Utilities

### Problem

`small_tables.py` and `large_tables.py` contain three functions that are copy-pasted verbatim or near-verbatim:

| Function | small_tables.py | large_tables.py | Difference |
|----------|----------------|-----------------|------------|
| `_log_active_ratio()` | lines 599–645 | lines 803–844 | Identical except log message wording |
| `_log_data_freshness()` | lines 648–705 | lines 847–904 | Identical except import style |
| `_cleanup_csvs()` | lines 582–596 | lines 786–800 | Large version accepts `target_date` param (unused in glob) |
| Feature flags | `USE_POLARS_CDC`, `USE_POLARS_SCD2` | Same | Identical dead flags |

### Proposed Extraction

```
orchestration/
├── shared.py           # NEW — shared orchestrator utilities
│   ├── log_active_ratio()
│   ├── log_data_freshness()
│   ├── cleanup_csvs()
│   └── USE_POLARS_CDC, USE_POLARS_SCD2  # (or remove if truly dead)
```

This is the lowest-effort, highest-confidence refactoring on the list — pure function extraction with no behavioral change.

---

## R4 — Decompose `bcp_csv.py` (812 lines → 3 modules)

### Problem

`bcp_csv.py` houses five distinct responsibilities:

1. **Row hashing** — `add_row_hash()`, `add_row_hash_fallback()`, float normalization, NULL/NaN sentinels, Categorical casting. This is ~250 lines of intricate logic with its own constants, edge cases, and the polars-hash plugin dependency.

2. **DataFrame preparation** — `sanitize_strings()`, `cast_bit_columns()`, `fix_oracle_date_columns()`, `reinterpret_uint64()`, `add_extracted_at()`, `prepare_dataframe_for_bcp()`. These transform a raw extraction DataFrame into BCP-ready format.

3. **Column ordering** — `reorder_columns_for_bcp()`. This is the P0-1 safeguard that enforces deterministic column order.

4. **CSV writing** — `write_bcp_csv()`, `_strip_bom_if_present()`, `_validate_sanitized()`. The actual BCP CSV contract enforcement.

5. **Schema validation** — `validate_schema_before_concat()`, `ColumnOrderError`, `SchemaValidationError`. These are consumed by `engine.py`, not by BCP loading.

When debugging a hashing issue, you're in the same file as BOM stripping. When a schema validation error fires, the traceback points to `bcp_csv.py`, which is misleading.

### Proposed Split

```
data_load/
├── row_hash.py         # NEW — add_row_hash(), add_row_hash_fallback(),
│                       #        float normalization, sentinels, constants
├── bcp_csv.py          # SLIMMED — sanitize, cast, reorder, write, format file,
│                       #           prepare_dataframe_for_bcp
├── df_validation.py    # NEW — validate_schema_before_concat(),
│                       #        ColumnOrderError, SchemaValidationError
├── bcp_loader.py       # (unchanged)
├── schema_utils.py     # (unchanged)
└── index_management.py # (unchanged)
```

**Why separate `row_hash.py`:** Hashing is the single most sensitive operation in the pipeline — it determines whether CDC detects changes correctly. Isolating it makes auditing straightforward ("is the hash stable across Polars versions?"), makes it testable in isolation, and clearly scopes the polars-hash dependency.

**Why separate `df_validation.py`:** `validate_schema_before_concat()` is consumed by `engine.py` (CDC), not by BCP loading. Its current location in `bcp_csv.py` is an artifact of history. Moving it to its own module clarifies that it's a general-purpose DataFrame safety check.

---

## R5 — DRY Up CDC Engine (`engine.py`)

### Problem

`run_cdc()` (lines 100–287) and `run_cdc_windowed()` (lines 816–1027) share ~80% of their logic:

| Step | `run_cdc()` | `run_cdc_windowed()` | Identical? |
|------|------------|---------------------|------------|
| PK validation (S-4) | ✓ | ✓ | Verbatim |
| NULL PK filter (P0-4) | ✓ | ✓ | Verbatim |
| Source PK dedup (S-1) | ✓ | ✓ | Verbatim |
| First-run guard | ✓ | ✓ | Verbatim |
| Stage dedup (L-1) | ✓ | ✓ | Verbatim |
| PK dtype alignment (P0-12) | ✓ | ✓ | Verbatim |
| Anti-join insert detection | ✓ | ✓ | Verbatim |
| Anti-join delete detection | ✓ | ✓ | Verbatim (windowed adds `deleted_pks`) |
| Hash comparison (P0-10) | ✓ | ✓ | Verbatim |
| Count validation (P0-12) | ✓ | ✓ | Verbatim |
| Change application (P0-9) | ✓ | ✓ | Verbatim |
| df_current assembly | ✓ | ✓ | Verbatim |
| **Read existing rows** | `read_stage_table()` | `read_stage_table_windowed()` | **Different** |
| **Deleted PKs capture** | N/A | `result.deleted_pks` | **Different** |

The only meaningful difference is how existing rows are loaded (full table vs windowed) and that the windowed path captures deleted PKs for targeted SCD2.

### Proposed Refactoring

Extract the shared comparison logic into a private `_compare_and_apply()` function:

```python
def _compare_and_apply(
    table_config: TableConfig,
    df_fresh: pl.DataFrame,
    df_existing: pl.DataFrame,
    pk_columns: list[str],
    batch_id: int,
    output_dir: str | Path,
    stage_table: str,
    context: str,          # "CDC" or "windowed CDC [start, end)"
    capture_deleted_pks: bool = False,
) -> CDCResult:
    """Core CDC comparison logic shared by run_cdc() and run_cdc_windowed()."""
    ...
```

Then `run_cdc()` and `run_cdc_windowed()` become thin wrappers that:
1. Validate inputs (PK existence, NULL filter, dedup)
2. Load existing rows (full vs windowed)
3. Call `_compare_and_apply()`
4. Handle mode-specific post-processing (deleted_pks capture)

This eliminates ~150 lines of verbatim duplication and ensures that any future CDC fix (like a new edge case guard) is applied to both paths automatically.

### Risk Mitigation

This is the only refactoring with medium risk, because CDC is the core data integrity engine. The safeguard: both `run_cdc()` and `run_cdc_windowed()` should call the exact same `_compare_and_apply()` — not diverge again. A good integration test is: run `--table ACCT --source DNA` before and after the refactor and confirm identical CDCResult counts.

---

## R6 — Extract Shared Extractor Utilities

### Problem

`connectorx_oracle_extractor.py` and `connectorx_sqlserver_extractor.py` both implement:

- `_supplement_null_partition_rows()` — handles NULL values in the `partition_on` column that ConnectorX's range partitioning misses. The logic is identical: build a `WHERE partition_col IS NULL` query, extract those rows, concat with the main DataFrame.
- `_log_partition_skew()` — calculates partition size standard deviation and logs a warning if skew is high. Identical algorithm.

### Proposed Extraction

```
extract/
├── partition_utils.py              # NEW — shared ConnectorX partition helpers
│   ├── supplement_null_partition_rows()
│   └── log_partition_skew()
├── connectorx_oracle_extractor.py  # (slimmed)
├── connectorx_sqlserver_extractor.py # (slimmed)
├── oracle_extractor.py             # (unchanged)
└── udm_connectorx_extractor.py     # (unchanged)
```

The functions are already well-isolated with clear interfaces — this is a straightforward move-and-import refactoring.

---

## R7 — Split `column_sync.py` by Concern

### Problem

`column_sync.py` (656 lines) interleaves three concerns:

1. **Generic column sync** — `sync_columns()`, `_columns_exist()`, `_insert_columns_from_info_schema()`, `_reload_columns_into_config()`. These work for any source.
2. **PK discovery (Oracle)** — `_discover_oracle_pks()` with Oracle-specific `ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS` queries and composite PK ordering.
3. **PK discovery (SQL Server)** — `_discover_sqlserver_pks()` with SQL Server-specific `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` / `KEY_COLUMN_USAGE` queries.
4. **Type drift detection** — `detect_source_type_drift()`, `_check_oracle_type_drift()`, `_check_sqlserver_type_drift()`. Each ~80 lines with source-specific INFORMATION_SCHEMA queries.

### Proposed Split

```
schema/
├── column_sync.py       # SLIMMED — sync_columns(), _columns_exist(),
│                        #   _insert_columns_from_info_schema(), _reload_columns_into_config()
├── pk_discovery.py      # NEW — _discover_pks(), _discover_oracle_pks(),
│                        #   _discover_sqlserver_pks(), _refresh_pk_flags(), _update_pk_flags()
├── type_drift.py        # NEW — detect_source_type_drift(), Oracle/SQL Server drift checks
```

This makes `column_sync.py` a clean ~200-line orchestrator that calls out to source-specific discovery, rather than a monolith that knows how to query both Oracle and SQL Server internals.

---

## R8 — Move Quarantine Logic Out of `evolution.py`

### Problem

`evolution.py` (542 lines) contains schema evolution proper (detect new/removed/changed columns, ALTER tables) and an unrelated quarantine subsystem (`ensure_quarantine_table()`, `quarantine_record()`, `quarantine_batch()`). Quarantine is a data quality mechanism that writes rejected rows to a separate table — it has nothing to do with DDL or schema drift detection.

### Proposed Move

```
schema/
├── evolution.py        # SLIMMED — evolve_schema(), _evolve_table(),
│                       #   _types_compatible(), _detect_potential_renames()
├── quarantine.py       # NEW — ensure_quarantine_table(), quarantine_record(),
│                       #   quarantine_batch()
```

This is a trivial extraction — the quarantine functions have no dependencies on evolution internals.

---

## Dependency Impact Summary

After all refactorings, the import graph remains acyclic and top-down:

```
main_*.py ──→ orchestration/cli_common.py
              orchestration/{small,large}_tables.py
                    │
                    ├──→ orchestration/shared.py        (active ratio, freshness, cleanup)
                    ├──→ extract/*                       (extractors)
                    ├──→ extract/partition_utils.py      (ConnectorX partition helpers)
                    ├──→ data_load/row_hash.py           (hashing)
                    ├──→ data_load/bcp_csv.py            (CSV writing)
                    ├──→ data_load/df_validation.py      (schema concat validation)
                    ├──→ cdc/engine.py                   (CDC comparison)
                    ├──→ cdc/reconciliation/*             (reconciliation suite)
                    ├──→ scd2/engine.py                  (SCD2 promotion)
                    ├──→ schema/evolution.py              (DDL evolution)
                    ├──→ schema/quarantine.py             (data quality quarantine)
                    ├──→ schema/column_sync.py            (column metadata)
                    ├──→ schema/pk_discovery.py           (PK detection per source)
                    ├──→ schema/type_drift.py             (source type monitoring)
                    └──→ observability/*                  (logging, metrics)
```

No circular dependencies. No cross-directory coupling introduced.

---

## Recommended Execution Order

Lowest risk, most immediate value first:

| Step | Refactoring | Effort | Why This Order |
|------|------------|--------|----------------|
| 1 | **R3** — Shared orchestrator utils | ~15 min | 3 functions, zero risk, immediate DRY win |
| 2 | **R2** — Shared CLI infra | ~30 min | Eliminates largest duplication surface |
| 3 | **R6** — Extractor partition utils | ~15 min | Isolated, no downstream impact |
| 4 | **R8** — Quarantine extraction | ~15 min | Trivial move, no shared state |
| 5 | **R4** — Decompose bcp_csv.py | ~45 min | Clear module boundaries, many importers to update |
| 6 | **R7** — Split column_sync.py | ~30 min | Source-specific code cleanly separable |
| 7 | **R1** — Split reconciliation.py | ~1 hour | Largest file, most modules to create |
| 8 | **R5** — DRY CDC engine | ~1 hour | Core data integrity path, needs careful testing |

Each step is independently deployable. Test after each step with:
```bash
python3 main_small_tables.py --table ACCT --source DNA
```

---

## What NOT to Refactor

Some apparent "issues" are deliberate and should stay as-is:

- **Two separate main entry points** (`main_small_tables.py`, `main_large_tables.py`). These are intentionally separate for operational clarity — cron jobs, monitoring, and failure alerting are per-pipeline-type. Merging them into one CLI with subcommands adds complexity without value.

- **`engine.py` naming** (both `cdc/engine.py` and `scd2/engine.py`). The directory provides the namespace disambiguation. Renaming to `cdc_engine.py` would be redundant.

- **`udm_connectorx_extractor.py`** staying in `extract/`. It reads from UDM target databases (Stage/Bronze), not source systems, so it's conceptually different from the other extractors. But it uses ConnectorX and DataFrame patterns identical to the other extractors, so it belongs in `extract/` by implementation affinity.

- **Feature flags** (`USE_POLARS_CDC`, `USE_POLARS_SCD2`). These appear to be legacy toggles that are always `True`. If confirmed dead, remove entirely rather than extracting — dead code extraction is still dead code.
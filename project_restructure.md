# UDM Pipeline — Project Structure

> **Status: COMPLETE** — All 5 refactoring phases finished 2026-02-23. The `pipeline/` directory no longer exists. Post-restructure hardening (25 items) completed 2026-02-24: SQL injection fixes, connection pooling, `cursor_for()` migration, memory management, security hardening, and test infrastructure. See `TODO.md` for the full completion log.

## Original Problem

The `pipeline/` directory was a catch-all for 13 files spanning at least 5 unrelated concerns: CDC logic, SCD2 logic, orchestration, schema management, observability, concurrency control, and config loading. When you needed to debug a CDC issue, you were sifting through the same directory that held log handlers and staging cleanup.

The old structure told you *where* code lived (`pipeline/`), but not *what* it did.

## Guiding Principles

1. **Directory name = mental model.** Each directory answers one question: "What is this responsible for?"
2. **Minimize cross-directory coupling.** Modules within a directory should depend on each other more than on siblings in other directories.
3. **Preserve the existing `extract/` and `data_load/` directories.** They're already well-scoped.
4. **Keep entry points at root.** `main_small_tables.py` and `main_large_tables.py` stay where they are.

## Current Structure

```
python_udm/
│
├── main_small_tables.py              # CLI entry point — small tables
├── main_large_tables.py              # CLI entry point — large tables
├── config.py                         # env vars, thresholds, paths (.env at /debi/.env)
├── connections.py                    # SQL Server target connections, cursor_for() with connection pool (Item-18), H-1 quoting
├── sources.py                        # source system registry (Oracle/SQL Server), register_source()
├── cli_common.py                     # shared CLI boilerplate (logging, startup checks, RSS monitoring, shutdown_connections)
├── requirements.txt
├── sql_tables_ddl.sql                # DDL for 6 General.ops observability tables
├── CLAUDE.md
├── TODO.md
│
├── extract/                          # ── Source Data Extraction ──
│   ├── __init__.py                   #   cx_read_sql_safe() — Rust panic recovery + retry (B-7)
│   ├── router.py                     #   extraction routing: Oracle/SQL Server × ConnectorX/oracledb (E-3)
│   ├── connectorx_oracle_extractor.py#   ConnectorX Oracle extraction → Polars → BCP CSV
│   ├── connectorx_sqlserver_extractor.py # ConnectorX SQL Server extraction → Polars → BCP CSV
│   ├── oracle_extractor.py           #   oracledb fallback (date-chunked with INDEX hints)
│   └── udm_connectorx_extractor.py   #   ConnectorX for internal UDM SQL Server reads
│
├── data_load/                        # ── BCP Loading Infrastructure ──
│   ├── bcp_csv.py                    #   prepare_dataframe_for_bcp(), write_bcp_csv(), schema validation
│   ├── row_hash.py                   #   polars-hash SHA-256 hashing + single-pass normalization (E-1, E-4, W-2, W-3)
│   ├── sanitize.py                   #   string sanitization, BIT cast, UInt64, Oracle dates (Item-20 INFORMATION_SCHEMA guard), column reorder
│   ├── bcp_format.py                 #   BCP XML format file generation (W-13)
│   ├── bcp_loader.py                 #   BCP subprocess wrapper (atomic/non-atomic, format files, minimal env Item-11)
│   ├── schema_utils.py               #   INFORMATION_SCHEMA queries, PK type lookup, dtype alignment
│   └── index_management.py           #   index disable/rebuild around BCP loads
│
├── cdc/                              # ── Change Data Capture ──
│   ├── engine.py                     #   CDCContext + _run_cdc_core unified engine (run_cdc, run_cdc_windowed)
│   └── reconciliation/               #   full reconciliation subpackage (9 modules)
│       ├── __init__.py               #     re-exports all 15 public names
│       ├── models.py                 #     10 result dataclasses
│       ├── persistence.py            #     ReconciliationLog table + writer (OBS-6)
│       ├── core.py                   #     reconcile_table, reconcile_table_windowed (C-7 size guard)
│       ├── counts.py                 #     reconcile_counts (W-11), reconcile_active_pks (E-7)
│       ├── scd2_integrity.py         #     reconcile_bronze (P2-13), validate_scd2_integrity (V-3), H-1 quoting (Item-13)
│       ├── data_quality.py           #     reconcile_aggregates (E-17), detect_distribution_shift (B-10)
│       ├── boundaries.py             #     reconcile_transformation_boundary (B-11), check_referential_integrity (B-12)
│       └── velocity.py               #     check_version_velocity (E-13)
│
├── scd2/                             # ── Slowly Changing Dimension Type 2 ──
│   └── engine.py                     #   run_scd2, run_scd2_targeted, 3-step INSERT-first pattern
│
├── orchestration/                    # ── Table Processing Flows ──
│   ├── small_tables.py               #   full pipeline for small tables (276 lines)
│   ├── large_tables.py               #   date-chunked pipeline, per-day checkpoints (487 lines)
│   ├── pipeline_steps.py             #   shared CDC/SCD2 promotion steps (D-1 through D-3)
│   ├── guards.py                     #   parameterized extraction guard + day-of-week aware baselines TOP 14 (G-1/G-2)
│   ├── table_config.py               #   TableConfig + TableConfigLoader from UdmTablesList
│   ├── table_lock.py                 #   sp_getapplock Session-owned locking + heartbeat
│   └── pipeline_state.py             #   checkpoint MERGE, gap detection, dates_to_process
│
├── schema/                           # ── DDL & Schema Management ──
│   ├── evolution.py                  #   schema drift: ADD new cols, WARN removed, ERROR type changes
│   ├── column_sync.py                #   auto-populate UdmTablesColumnsList + PK discovery
│   ├── table_creator.py              #   auto-create Stage/Bronze tables + index types
│   └── staging_cleanup.py            #   orphaned _staging_* table cleanup (P3-3)
│
├── observability/                    # ── Logging & Metrics ──
│   ├── event_tracker.py              #   PipelineEventTracker → General.ops.PipelineEventLog
│   └── log_handler.py                #   SqlServerLogHandler → General.ops.PipelineLog
│
├── migrations/                       # ── One-time Schema Migrations ──
│   └── b1_hash_varchar64.py          #   ALTER _row_hash/UdmHash from BIGINT to VARCHAR(64)
│
└── tests/                            # ── Test Infrastructure ──
    └── test_hash_regression.py       #   polars-hash upgrade regression harness (Item-7)
```

## Why This Breakdown

### `extract/` — Source Data Extraction

| File | Responsibility |
|------|---------------|
| `__init__.py` | `cx_read_sql_safe()` — wraps all ConnectorX calls with Rust panic recovery and exponential backoff retry (B-7) |
| `router.py` | Single routing authority for extraction: `extract_full()` and `extract_windowed()` route Oracle/SQL Server × ConnectorX/oracledb based on UdmTablesList columns (E-3) |
| `connectorx_oracle_extractor.py` | ConnectorX Oracle bulk/windowed extraction with sentinel date guard, NULL partition supplement, partition skew warnings |
| `connectorx_sqlserver_extractor.py` | ConnectorX SQL Server bulk/windowed extraction |
| `oracle_extractor.py` | oracledb fallback with INDEX hints, date-chunked extraction, empty day skip (P2-2) |
| `udm_connectorx_extractor.py` | Internal UDM reads: Stage/Bronze tables, PK-scoped Bronze reads, row counts, table_exists |

The extraction router (`router.py`) was added in Phase 5 to prevent extraction routing logic from drifting between orchestrators. Before this, both `small_tables.py` and `large_tables.py` duplicated the Oracle/SQL Server × ConnectorX/oracledb decision tree.

### `data_load/` — BCP Loading Infrastructure

| File | Responsibility |
|------|---------------|
| `bcp_csv.py` | `prepare_dataframe_for_bcp()` orchestrator, `write_bcp_csv()`, `validate_schema_before_concat()` (W-7) |
| `row_hash.py` | `add_row_hash()` (polars-hash SHA-256) + `add_row_hash_fallback()` (hashlib). Single-pass normalization (Item-8): Oracle empty→NULL (E-1) + NFC (V-2) + RTRIM (E-4) combined into one `with_columns`. Float (W-3), `\x1F` separators (E-19), Categorical→Utf8 (E-20) |
| `sanitize.py` | `sanitize_strings()` (B-6), `cast_bit_columns()`, `reinterpret_uint64()`, `fix_oracle_date_columns()` with INFORMATION_SCHEMA false-positive guard (B-13, Item-20), `reorder_columns_for_bcp()` with fixed dead comparison (P0-1, Item-21). Module-level `_DATETIME_FORMATS` constant (Item-25). Multi-format datetime detection with 90% threshold (Item-10) |
| `bcp_format.py` | `generate_bcp_format_file()` — XML .fmt files from INFORMATION_SCHEMA (W-13) |
| `bcp_loader.py` | BCP subprocess: `bcp_load(atomic=True/False)` (E-3), `truncate_table()`, `create_staging_index()`, `bulk_load_recovery_context()` with post-ALTER verification (Item-17). Minimal BCP subprocess environment to prevent secret leakage (Item-11) |
| `schema_utils.py` | `get_column_metadata()`, `get_target_column_order()`, `get_column_types()`, `align_pk_dtypes()` (P0-12) |
| `index_management.py` | `disable_indexes()`, `rebuild_indexes()` around BCP loads |

Phase 4b split `bcp_csv.py` (811 lines) into `row_hash.py`, `sanitize.py`, and `bcp_format.py`. `bcp_csv.py` retains re-exports for backward compatibility.

### `cdc/` — Change Data Capture

| File | Responsibility |
|------|---------------|
| `engine.py` | `CDCContext` + `_run_cdc_core()` unified engine — `run_cdc()` and `run_cdc_windowed()` are thin wrappers (~895 lines) |
| `reconciliation/` | 9-module subpackage: full column-by-column reconciliation, count checks, SCD2 integrity (H-1 quoting, `cursor_for()` migration — Items 13, 16), aggregate comparison, distribution shift, referential integrity, version velocity |

Phase 3 unified the two CDC code paths via `CDCContext` dataclass, eliminating ~128 lines of duplication. Phase 4a split the monolithic `reconciliation.py` (2,171 lines) into 9 focused modules with a re-exporting `__init__.py` for backward compatibility.

### `scd2/` — SCD2 Promotion

| File | Responsibility |
|------|---------------|
| `engine.py` | `run_scd2()` (full Bronze comparison) + `run_scd2_targeted()` (PK-scoped for large tables) — 3-step INSERT-first pattern (~1,128 lines) |

SCD2 is conceptually downstream of CDC and has its own directory because:
- It has its own comparison logic (active Bronze rows vs CDC output)
- It has its own crash safety invariants (INSERT-first with deferred activation, E-2)
- It manages its own staging tables for UPDATE JOINs
- Debugging SCD2 issues (duplicate active rows, missed closes) is completely separate from debugging CDC issues (wrong insert/update/delete classification)

### `orchestration/` — How Tables Get Processed

| File | Responsibility |
|------|---------------|
| `small_tables.py` | End-to-end flow for full-extract tables (276 lines) |
| `large_tables.py` | End-to-end flow for date-windowed tables with per-day checkpoints (487 lines) |
| `pipeline_steps.py` | Shared CDC/SCD2 promotion steps, CSV cleanup, active ratio logging, freshness checks (D-1 through D-3) |
| `guards.py` | Parameterized extraction guard with day-of-week aware baselines, TOP 14 rolling window (G-1/G-2, Item-12) |
| `table_config.py` | `TableConfig` + `TableConfigLoader` from `UdmTablesList` |
| `table_lock.py` | `sp_getapplock` Session-owned concurrency control + heartbeat |
| `pipeline_state.py` | Per-date checkpoints, gap detection, resume logic |

Phase 2 extracted `pipeline_steps.py` to eliminate ~200 lines duplicated between orchestrators. Phase 5 added `guards.py` to unify the extraction guard logic, and `router.py` (in `extract/`) to centralize extraction routing. Combined with `cli_common.py` at root, small_tables.py shrank from 705→276 lines and large_tables.py from 905→487 lines.

### `schema/` — DDL & Schema Management

| File | Responsibility |
|------|---------------|
| `evolution.py` | Detect new/removed/changed columns (P0-2), quarantine infrastructure (W-17) |
| `column_sync.py` | Auto-populate `UdmTablesColumnsList` + PK discovery, source type drift detection (E-16) |
| `table_creator.py` | Auto-create Stage/Bronze tables from DataFrame dtypes, 3 index types (SCD-1, V-9, W-10) |
| `staging_cleanup.py` | Drop orphaned `_staging_*` tables from crashes (P3-3) |

All four files deal with DDL and table structure management. They share the same concerns: what columns exist, what types are they, do tables need to be created or altered.

### `observability/` — Logging & Metrics

| File | Responsibility |
|------|---------------|
| `event_tracker.py` | `PipelineEventTracker` context manager → `PipelineEventLog` (dashboard layer) |
| `log_handler.py` | `SqlServerLogHandler` → `PipelineLog` (investigation layer) |

Pure infrastructure — they instrument the pipeline but don't participate in data processing.

### Root-Level Shared Modules

| File | Responsibility |
|------|---------------|
| `config.py` | Env vars, DB names, BCP thresholds, CSV constants (.env at /debi/.env) |
| `connections.py` | SQL Server target DB connections (Stage/Bronze/General), `cursor_for()` with per-database connection pool (Item-18), `close_connection_pool()`, `verify_rcsi_enabled()`, H-1 quoting helpers (`quote_identifier`, `quote_table`) |
| `sources.py` | `SourceSystem` registry (DNA=Oracle, CCM/EPICOR=SQL Server), `connectorx_uri()`, `oracledb_connect_params()`, `register_source()` (Item-24) |
| `cli_common.py` | Module-level `MALLOC_ARENA_MAX=2` + `POLARS_MAX_THREADS=1`, `setup_logging()`, `startup_checks()`, `shutdown_connections()` (Item-18), RSS monitoring, TableConfig serialization |

`cli_common.py` was added in Phase 5 to consolidate ~130 lines of duplicated CLI boilerplate from both `main_*.py` entry points.

## Dependency Flow

```
main_small_tables.py ──→ cli_common.py (setup, logging, startup checks)
main_large_tables.py ──→ cli_common.py
                              │
                              ├──→ orchestration/small_tables.py
                              └──→ orchestration/large_tables.py
                                        │
                                        ├──→ extract/router.py       (route to correct extractor)
                                        │       └──→ extract/*       (ConnectorX/oracledb)
                                        ├──→ orchestration/pipeline_steps.py  (shared CDC/SCD2 steps)
                                        │       ├──→ cdc/engine.py   (detect changes)
                                        │       └──→ scd2/engine.py  (promote to Bronze)
                                        ├──→ orchestration/guards.py (extraction guard)
                                        ├──→ data_load/*             (BCP CSV + load)
                                        ├──→ schema/*                (ensure tables, evolve schema)
                                        └──→ observability/*         (log everything)

cdc/engine.py ──→ data_load/sanitize.py     (sanitize + reorder)
               → data_load/schema_utils.py   (PK types for staging)
               → data_load/bcp_loader.py     (staging table loads)

scd2/engine.py ──→ data_load/bcp_loader.py   (INSERT via BCP)
                 → data_load/schema_utils.py  (PK types for staging)
                 → extract/udm_connectorx_extractor.py  (read Bronze for PKs)

schema/table_creator.py ──→ extract/udm_connectorx_extractor.py  (table_exists check)
```

No circular dependencies. The flow is strictly top-down: `main → cli_common → orchestration → {cdc, scd2, schema} → {extract, data_load}`. Config, connections, sources, and observability are shared horizontally.

## Refactoring History

The restructuring was completed across 5 phases. See `TODO.md` for the detailed completion log with verification notes.

| Phase | Scope | Lines Saved | Key Outcome |
|-------|-------|-------------|-------------|
| 1 — Directory Restructure | Pure file moves from `pipeline/` | 0 (structural only) | 7 packages created, `pipeline/` deleted, all imports updated |
| 2 — Orchestrator Deduplication | Extract shared pipeline steps | ~200 | `orchestration/pipeline_steps.py` created; orchestrators slimmed significantly |
| 3 — CDC Core Unification | Merge `run_cdc` + `run_cdc_windowed` | ~128 | `CDCContext` dataclass + `_run_cdc_core()` unified engine |
| 4a — Reconciliation Split | Break 2,171-line monolith | 0 (organizational) | 9-module `cdc/reconciliation/` subpackage with re-exporting `__init__.py` |
| 4b — BCP CSV Split | Break 811-line monolith | 0 (organizational) | `row_hash.py`, `sanitize.py`, `bcp_format.py` extracted; `bcp_csv.py` slimmed to ~190 lines |
| 5 — Nice-to-Haves | Extract router, CLI common, guards | ~430+ | `extract/router.py`, `cli_common.py`, `orchestration/guards.py`, `cursor_for()` |

**Total lines eliminated:** ~760 of duplicated code. Largest files reduced: `small_tables.py` 705→276, `large_tables.py` 905→487, `main_small_tables.py` 299→173, `main_large_tables.py` 287→168.

## Post-Restructure Hardening (25 Items)

Completed 2026-02-24. A code review after restructuring identified 25 items across security, correctness, performance, and housekeeping.

### P0 — Critical Security

| Item | File(s) | Change |
|------|---------|--------|
| 13 | `cdc/reconciliation/scd2_integrity.py` | SQL injection fix: replaced all `f"[{c}]"` with `quote_identifier(c)` and raw table names with `quote_table()` (H-1 discipline). Also migrated to `cursor_for()` (Item 16) |
| 14 | `orchestration/large_tables.py` | SQL injection fix in `_check_null_date_column()`: SQL Server path uses `quote_identifier()`, Oracle path uses double-quote escaping |

### P1 — Security & Correctness

| Item | File(s) | Change |
|------|---------|--------|
| 11 | `data_load/bcp_loader.py` | BCP subprocess uses minimal env (`PATH`, `HOME`, `LANG`, `SQLCMDPASSWORD`) instead of `{**os.environ}` to prevent secret leakage |
| 15 | `cdc/engine.py` | `purge_expired_cdc_rows()` migrated from manual `conn/try/finally` to `cursor_for()` per batch iteration |
| 16 | `cdc/reconciliation/scd2_integrity.py` | Migrated from manual connection pattern to `cursor_for(config.BRONZE_DB)` |
| 17 | `data_load/bcp_loader.py` | `bulk_load_recovery_context()` now verifies ALTER DATABASE took effect via `sys.databases` query; logs WARNING on mismatch |

### P2 — Performance & Infrastructure

| Item | File(s) | Change |
|------|---------|--------|
| 8 | `data_load/row_hash.py` | Combined three `with_columns` passes (E-1 Oracle empty→NULL, V-2 NFC, E-4 RTRIM) into a single pass |
| 12 | `orchestration/guards.py` | Extraction guard baselines use TOP 14 (was TOP 5) for day-of-week aware rolling median over 2 full weeks |
| 18 | `connections.py`, `cli_common.py`, `main_*.py` | Per-database connection pool in `cursor_for()` with `OperationalError` eviction. `close_connection_pool()` at shutdown via `cli_common.shutdown_connections()` |

### P3 — Correctness & Robustness

| Item | File(s) | Change |
|------|---------|--------|
| 7 | `tests/test_hash_regression.py` | New file — hash regression test harness for polars-hash upgrades. Reference dataset covers all dtypes. Modes: `--self-check`, `--save-baseline`, `--compare` |
| 10 | `data_load/sanitize.py` | `_looks_like_datetime_column()` improved: sample size 20 (was 10), threshold 90% (was 80%), 6 datetime formats including Oracle NLS variants |
| 19 | `orchestration/large_tables.py` | Added `gc.collect()` after `del df` at extraction→SCD2 transition to release over-allocated memory |
| 20 | `data_load/sanitize.py`, `data_load/bcp_csv.py` | `fix_oracle_date_columns(stage_table=...)` checks INFORMATION_SCHEMA for string-typed columns before content-based datetime detection — prevents false positives on string ID columns |
| 21 | `data_load/sanitize.py` | Fixed dead comparison in `reorder_columns_for_bcp()` — compares against DataFrame's original column order instead of re-selected order |

### P4 — Documentation & Housekeeping

| Item | File(s) | Change |
|------|---------|--------|
| 9 | — | Test suite: deferred per TODO |
| 22 | `CLAUDE.md` | Documented CSV cleanup concurrency constraint (safe because each table's pipeline is sequential within a worker) |
| 23 | `connections.py` | Documented thread-safety constraint on `_connection_time_ms`/`_connection_count` counters (per-process, not thread-safe; safe under multiprocessing model) |
| 24 | `sources.py` | Added docstring to `register_source()` explaining it's for programmatic registration, not called by production code |
| 25 | `data_load/sanitize.py` | Moved `_DATETIME_FORMATS` from function body to module-level constant to avoid per-call allocation |

## Import Reference

Current import paths (the `pipeline.*` namespace no longer exists):

```python
# CDC
from cdc.engine import run_cdc, run_cdc_windowed, CDCResult, purge_expired_cdc_rows
from cdc.reconciliation import reconcile_table, reconcile_counts, validate_scd2_integrity, ...

# SCD2
from scd2.engine import run_scd2, run_scd2_targeted, SCD2Result

# Orchestration
from orchestration.small_tables import process_small_table
from orchestration.large_tables import process_large_table
from orchestration.pipeline_steps import run_cdc_promotion, run_scd2_promotion, cleanup_csvs
from orchestration.guards import check_extraction_guard, run_extraction_guard
from orchestration.table_config import TableConfig, TableConfigLoader
from orchestration.table_lock import acquire_table_lock, release_table_lock, keep_lock_alive
from orchestration.pipeline_state import save_checkpoint, get_dates_to_process, detect_gaps

# Extraction
from extract.router import extract_full, extract_windowed
from extract import cx_read_sql_safe
from extract.udm_connectorx_extractor import read_stage_table, read_bronze_table, table_exists

# Data Load
from data_load.bcp_csv import prepare_dataframe_for_bcp, write_bcp_csv, validate_schema_before_concat
from data_load.row_hash import add_row_hash, add_row_hash_fallback
from data_load.sanitize import sanitize_strings, reorder_columns_for_bcp, cast_bit_columns
from data_load.bcp_loader import bcp_load, truncate_table, bulk_load_recovery_context, create_staging_index
from data_load.bcp_format import generate_bcp_format_file
from data_load.schema_utils import get_column_metadata, get_column_types, align_pk_dtypes

# Schema
from schema.evolution import evolve_schema, validate_source_schema, SchemaEvolutionResult
from schema.column_sync import sync_columns, detect_source_type_drift
from schema.table_creator import ensure_stage_table, ensure_bronze_table
from schema.staging_cleanup import cleanup_orphaned_staging_tables

# Observability
from observability.event_tracker import PipelineEventTracker
from observability.log_handler import SqlServerLogHandler

# Shared root modules
from cli_common import setup_logging, startup_checks, shutdown_connections, check_rss_memory, table_config_to_dict
from connections import cursor_for, close_connection_pool, get_connection, verify_rcsi_enabled
from connections import quote_identifier, quote_table
from sources import get_source, register_source, SourceType
import config
```

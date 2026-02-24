# UDM Pipeline — Refactored Project Structure

## The Problem

The `pipeline/` directory is a catch-all for 13 files spanning at least 5 unrelated concerns: CDC logic, SCD2 logic, orchestration, schema management, observability, concurrency control, and config loading. When you need to debug a CDC issue, you're sifting through the same directory that holds log handlers and staging cleanup. When a new developer asks "how does SCD2 work?", the answer is buried alongside table locking and column sync.

The current structure tells you *where* code lives (`pipeline/`), but not *what* it does.

## Guiding Principles

1. **Directory name = mental model.** Each directory answers one question: "What is this responsible for?"
2. **Minimize cross-directory coupling.** Modules within a directory should depend on each other more than on siblings in other directories.
3. **Preserve the existing `extract/` and `data_load/` directories.** They're already well-scoped.
4. **Keep entry points at root.** `main_small_tables.py` and `main_large_tables.py` stay where they are.

## Proposed Structure

```
udm_pipeline/
│
├── main_small_tables.py              # CLI entry point (unchanged)
├── main_large_tables.py              # CLI entry point (unchanged)
├── config.py                         # env vars, thresholds, paths (unchanged)
├── connections.py                    # SQL Server target connections (unchanged)
├── sources.py                        # source system registry (unchanged)
├── requirements.txt
├── CLAUDE.md
├── TODO.md
│
├── extract/                          # ── Source Data Extraction ──
│   ├── __init__.py                   # (EXISTING - no changes)
│   ├── connectorx_oracle_extractor.py
│   ├── connectorx_sqlserver_extractor.py
│   ├── oracle_extractor.py
│   └── udm_connectorx_extractor.py
│
├── data_load/                        # ── BCP Loading Infrastructure ──
│   ├── __init__.py                   # (EXISTING - no changes)
│   ├── bcp_loader.py
│   ├── bcp_csv.py
│   ├── index_management.py
│   └── schema_utils.py
│
├── cdc/                              # ── Change Data Capture ──
│   ├── __init__.py                   # (NEW directory)
│   ├── engine.py                     # ← pipeline/cdc_polars.py
│   └── reconciliation.py             # ← pipeline/reconciliation.py
│
├── scd2/                             # ── Slowly Changing Dimension Type 2 ──
│   ├── __init__.py                   # (NEW directory)
│   └── engine.py                     # ← pipeline/scd2_polars.py
│
├── orchestration/                    # ── Table Processing Flows ──
│   ├── __init__.py                   # (NEW directory)
│   ├── small_tables.py               # ← pipeline/small_table_orchestrator.py
│   ├── large_tables.py               # ← pipeline/large_table_orchestrator.py
│   ├── table_config.py               # ← pipeline/table_config.py
│   ├── table_lock.py                 # ← pipeline/table_lock.py
│   └── pipeline_state.py             # ← pipeline/pipeline_state.py
│
├── schema/                           # ── DDL & Schema Management ──
│   ├── __init__.py                   # (NEW directory)
│   ├── evolution.py                  # ← pipeline/schema_evolution.py
│   ├── column_sync.py                # ← pipeline/column_sync.py
│   ├── table_creator.py              # ← pipeline/table_creator.py
│   └── staging_cleanup.py            # ← pipeline/staging_cleanup.py
│
└── observability/                    # ── Logging & Metrics ──
    ├── __init__.py                   # (NEW directory)
    ├── event_tracker.py              # ← pipeline/pipeline_event_tracker.py
    └── log_handler.py                # ← pipeline/pipeline_log_handler.py
```

## Why This Breakdown

### `cdc/` — Change Data Capture

| File | Origin | Responsibility |
|------|--------|---------------|
| `engine.py` | `cdc_polars.py` | `run_cdc()` + `run_cdc_windowed()` — the hash-based three-way comparison (anti-joins for I/D, hash diff for U) |
| `reconciliation.py` | `reconciliation.py` | Weekly full column-by-column comparison to catch hash collisions |

These two files are tightly coupled: reconciliation exists specifically to validate CDC's hash-based approach. Isolating CDC makes it easy to reason about the comparison algorithm independently of orchestration. When someone asks "how do we detect changes?", this is the only directory they need.

### `scd2/` — SCD2 Promotion

| File | Origin | Responsibility |
|------|--------|---------------|
| `engine.py` | `scd2_polars.py` | `run_scd2()` + `run_scd2_targeted()` — the two-step INSERT-first / UPDATE-close pattern against Bronze |

SCD2 is conceptually downstream of CDC and deserves its own directory because:
- It has its own comparison logic (active Bronze rows vs CDC output)
- It has its own crash safety invariants (INSERT-first design, P0-8/P0-9)
- It manages its own staging tables for UPDATE JOINs
- Debugging SCD2 issues (duplicate active rows, missed closes) is completely separate from debugging CDC issues (wrong insert/update/delete classification)

### `orchestration/` — How Tables Get Processed

| File | Origin | Responsibility |
|------|--------|---------------|
| `small_tables.py` | `small_table_orchestrator.py` | End-to-end flow for full-extract tables |
| `large_tables.py` | `large_table_orchestrator.py` | End-to-end flow for date-windowed tables |
| `table_config.py` | `table_config.py` | `TableConfig` + `TableConfigLoader` from `UdmTablesList` |
| `table_lock.py` | `table_lock.py` | `sp_getapplock` concurrency control |
| `pipeline_state.py` | `pipeline_state.py` | Per-date checkpoints, gap detection, resume |

These belong together because they answer: "How does a table move through the pipeline?" The orchestrators are the primary consumers of table_config, table_lock, and pipeline_state. Keeping them together means you can trace the full execution flow without jumping across directories.

### `schema/` — DDL & Schema Management

| File | Origin | Responsibility |
|------|--------|---------------|
| `evolution.py` | `schema_evolution.py` | Detect new/removed/changed columns (P0-2) |
| `column_sync.py` | `column_sync.py` | Auto-populate `UdmTablesColumnsList` + PK discovery |
| `table_creator.py` | `table_creator.py` | Auto-create Stage/Bronze tables from DataFrame dtypes |
| `staging_cleanup.py` | `staging_cleanup.py` | Drop orphaned `_staging_*` tables |

All four files deal with DDL and table structure management. They share the same concerns: what columns exist, what types are they, do tables need to be created or altered. Grouping them makes schema-related debugging straightforward.

### `observability/` — Logging & Metrics

| File | Origin | Responsibility |
|------|--------|---------------|
| `event_tracker.py` | `pipeline_event_tracker.py` | `PipelineEventTracker` context manager → `PipelineEventLog` (dashboard layer) |
| `log_handler.py` | `pipeline_log_handler.py` | `SqlServerLogHandler` → `PipelineLog` (investigation layer) |

These are pure infrastructure — they instrument the pipeline but don't participate in data processing. Isolating them means you can modify logging behavior without touching any data logic.

## Migration Map (Old → New Imports)

This is the complete set of import path changes:

```python
# CDC
pipeline.cdc_polars        → cdc.engine
pipeline.reconciliation    → cdc.reconciliation

# SCD2
pipeline.scd2_polars       → scd2.engine

# Orchestration
pipeline.small_table_orchestrator → orchestration.small_tables
pipeline.large_table_orchestrator → orchestration.large_tables
pipeline.table_config             → orchestration.table_config
pipeline.table_lock               → orchestration.table_lock
pipeline.pipeline_state           → orchestration.pipeline_state

# Schema
pipeline.schema_evolution  → schema.evolution
pipeline.column_sync       → schema.column_sync
pipeline.table_creator     → schema.table_creator  (was pipeline.table_creator)
pipeline.staging_cleanup   → schema.staging_cleanup

# Observability
pipeline.pipeline_event_tracker → observability.event_tracker
pipeline.pipeline_log_handler   → observability.log_handler
```

## Dependency Flow (Simplified)

```
main_small_tables.py ──→ orchestration/small_tables.py
main_large_tables.py ──→ orchestration/large_tables.py
                              │
                              ├──→ extract/*           (get data from source)
                              ├──→ data_load/*         (BCP CSV + load)
                              ├──→ schema/*            (ensure tables, evolve schema)
                              ├──→ cdc/engine.py       (detect changes)
                              ├──→ scd2/engine.py      (promote to Bronze)
                              └──→ observability/*     (log everything)

cdc/engine.py ──→ data_load/schema_utils.py   (PK types for staging)
scd2/engine.py ──→ data_load/bcp_loader.py    (INSERT via BCP)
                 → data_load/schema_utils.py   (PK types for staging)
schema/table_creator.py ──→ extract/udm_connectorx_extractor.py  (table_exists check)
```

No circular dependencies. The flow is strictly top-down: `main → orchestration → {cdc, scd2, schema} → {extract, data_load}`. Config, connections, and observability are shared horizontally.

## Execution Plan

The refactor is purely structural — no logic changes, no new features. Every step is safe to do in isolation:

1. **Create directories** with `__init__.py` files
2. **Move files** one directory at a time (start with `observability/` — fewest dependents)
3. **Update imports** in each file that references the moved module
4. **Update CLAUDE.md** Structure section to reflect new paths
5. **Test** with `python3 main_small_tables.py --table ACCT --source DNA`
6. **Repeat** for next directory

Suggested order (least to most import disruption):
1. `observability/` (2 files, imported by orchestrators + main files only)
2. `schema/` (4 files, imported by orchestrators)
3. `cdc/` (2 files, imported by orchestrators)
4. `scd2/` (1 file, imported by orchestrators)
5. `orchestration/` (5 files, imported by main files — do last since it's the hub)

After step 5, the `pipeline/` directory is empty and can be deleted.
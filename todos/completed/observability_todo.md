# Observability Tracking Issues — Code Review Audit

*Context: Code review of `event_tracker.py`, `log_handler.py`, `pipeline_state.py`, `small_tables.py`, `large_tables.py`, and `reconciliation.py` against the documented schema in `CLAUDE.md` and `Pipeline_Architecture.md`. All issues relate to gaps between what the General.ops tables are designed to track and what the implementation actually captures.*

*All 8 items addressed (2026-02-23).*

---

## 1. Event Tracking Gaps

### OBS-1 (P2): BCP_LOAD event in small_tables.py tracks no actual BCP work — DONE

**Resolution:** Removed the hollow BCP_LOAD event from `small_tables.py`. BCP timing is inherently captured within CDC_PROMOTION (staging table loads) and SCD2_PROMOTION (Bronze INSERT/UPDATE loads). Large tables already had no standalone BCP_LOAD event, so both paths are now consistent.

---

### OBS-2 (P2): Large table per-day events have no date context in EventDetail — DONE

**Resolution:** Added `event.event_detail = str(target_date)` on all four event types in `_process_single_day`: EXTRACT, CDC_PROMOTION, SCD2_PROMOTION, and CSV_CLEANUP. Enables direct filtering: `WHERE EventDetail = '2025-02-15'`.

---

### OBS-3 (P2): Lock-skipped tables are invisible in PipelineEventLog — DONE

**Resolution:** Both `small_tables.py` and `large_tables.py` now write a `TABLE_TOTAL` event with `status='SKIPPED'` and `event_detail='Lock held by another run'` when lock acquisition fails. Modified `track()` in `event_tracker.py` to preserve SKIPPED status (previously would override to SUCCESS). Enables monitoring: `WHERE Status = 'SKIPPED'`.

---

## 2. Log Handler Reliability

### OBS-4 (P1): PipelineLog buffer loss on crash — DONE

**Resolution:** Three changes to `log_handler.py`:
1. Reduced `_buffer_size` from 50 to 10 — narrows crash-loss window from ≤49 to ≤9 entries
2. Added `record.levelno >= logging.WARNING` to flush condition — WARNING+ entries flush immediately
3. Replaced `except Exception: pass` with stderr output — flush failures are now visible in systemd journal

---

## 3. Missing Commit Safety

### OBS-5 (P1): No explicit commit in _write_event and _flush_buffer — DONE

**Resolution:** Added explicit `conn.commit()` after:
- `cursor.close()` in `event_tracker._write_event()` (event INSERT)
- `cursor.close()` in `event_tracker._get_next_batch_id()` (batch sequence INSERT)
- `cursor.close()` in `log_handler._flush_buffer()` (log executemany)

Pipeline observability now persists regardless of autocommit configuration.

---

## 4. Reconciliation Persistence

### OBS-6 (P2): Reconciliation results not persisted to any ops table — DONE

**Resolution:** Added to `reconciliation.py`:
- `ensure_reconciliation_log_table()` — idempotent DDL for `General.ops.ReconciliationLog` with columns: Id, TableName, SourceName, CheckType, RunDate, Status, SourceRows, TargetRows, MismatchedRows, SourceOnlyRows, TargetOnlyRows, Metadata (JSON), Errors (JSON)
- `_persist_reconciliation_result()` — generic writer called by each public reconciliation function
- Added persistence calls to: `reconcile_table()`, `reconcile_counts()`, `reconcile_active_pks()`, `reconcile_bronze()`, `reconcile_aggregates()`
- Uses `CheckType` discriminator: TABLE_RECONCILIATION, COUNT_RECONCILIATION, ACTIVE_PK_RECONCILIATION, BRONZE_RECONCILIATION, AGGREGATE_RECONCILIATION

---

## 5. Metadata Handling

### OBS-7 (P3): _log_active_ratio overwrites scd2_event.metadata instead of merging — DONE

**Resolution:** Both `small_tables.py` and `large_tables.py` `_log_active_ratio()` now use `json.loads()`/`json.dumps()` merge pattern instead of overwriting. Existing metadata (from CDC or future SCD2 extensions) is preserved.

---

### OBS-8 (P3): Small table extraction guard uses single previous run instead of median — ALREADY DONE

**Resolution:** B-9 (implemented in prior session) already backported the median approach to `_get_previous_extract_count()`. The function queries TOP 5 same-day-of-week extractions from the last 30 days (with any-day fallback) and returns the median. No additional changes needed.

---

## Summary

| ID | Priority | Category | Status |
|----|----------|----------|--------|
| OBS-1 | P2 | Event Tracking | DONE — hollow BCP_LOAD removed |
| OBS-2 | P2 | Event Tracking | DONE — `event_detail = str(target_date)` on all events |
| OBS-3 | P2 | Event Tracking | DONE — SKIPPED event for lock failures |
| OBS-4 | P1 | Log Reliability | DONE — buffer 50→10, flush on WARNING+, stderr on failure |
| OBS-5 | P1 | Commit Safety | DONE — explicit `conn.commit()` in event/log writes |
| OBS-6 | P2 | Reconciliation | DONE — `ops.ReconciliationLog` table + persistence |
| OBS-7 | P3 | Metadata | DONE — merge pattern replaces overwrite |
| OBS-8 | P3 | Extraction Guard | ALREADY DONE — B-9 median baseline |

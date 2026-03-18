# CSV Lifecycle & Memory Optimization Guide

## Problem Statement

The small tables pipeline holds all extraction CSVs on disk and the extraction DataFrame in memory simultaneously through the entire pipeline run (extract → CDC → SCD2 → cleanup). For tables approaching the 20 GB memory ceiling, this causes OOM events. Large tables already handle this well via per-day cleanup.

This document describes the current CSV lifecycle, identifies where memory and disk accumulate unnecessarily, and provides the exact code changes needed to fix it.

---

## Current CSV Lifecycle

### Small Tables (`orchestration/small_tables.py`)

```
extract_full()
  → CSV 1 written to disk: {source}_{table}.csv         ← NEVER READ AGAIN
  → DataFrame (df) held in memory
  → ensure_stage_table() / ensure_bronze_table()         ← DDL only, no data
  → evolve_schema() / sync_columns()
  → run_cdc_promotion()
     → Reads df (in memory) + reads existing Stage table (in memory)
     → Anti-join comparison peaks at ~3x DataFrame size
     → Writes CSV 2: {source}_{table}_cdc_inserts.csv    ← BCP loads, then dead
     → Writes CSV 3: {source}_{table}_expire_pks.csv     ← BCP loads, then dead
  → run_scd2_promotion()
     → Uses cdc_result.df_current (NOT the original df)
     → Reads Bronze active rows into memory
     → Writes CSV 4: {source}_{table}_scd2_inserts.csv   ← BCP loads, then dead
     → Writes CSV 5: {source}_{table}_scd2_close_pks.csv ← BCP loads, then dead
  → cleanup_csvs()                                       ← ALL 5 deleted here
  → df still in memory when process_small_table() returns
```

**Problems:**
1. The extraction CSV (CSV 1) is never read by anything — CDC and SCD2 write their own staging CSVs. It sits on disk (or tmpfs, counting against RAM) through the entire run.
2. All 5 CSVs accumulate on disk until `cleanup_csvs()` at the very end.
3. The original extraction DataFrame `df` stays in memory through SCD2, even though SCD2 only uses `cdc_result.df_current` (returned by CDC).
4. When tmpfs is enabled (`/dev/shm/udm_bcp/`), CSVs written there consume physical RAM alongside the in-memory DataFrames.

### Large Tables (`orchestration/large_tables.py`)

```
for each day:
  extract_windowed(day)
    → CSV for this day written
    → df_day in memory
    → run_cdc_promotion()
    → del df + gc.collect()          ← DataFrame freed before SCD2
    → run_scd2_promotion()
    → cleanup_csvs()                 ← CSVs deleted per day
```

**Large tables already handle this correctly.** Per-day `del df` + `gc.collect()` + `cleanup_csvs()` keeps memory bounded. No changes needed.

---

## Why the Extraction CSV Is Never Read

After `extract_full()` returns `(df, csv_path)`:

1. `csv_path` is assigned but **never passed to any BCP load function**.
2. `run_cdc_promotion()` receives `df` (the in-memory DataFrame), not `csv_path`.
3. The CDC engine (`cdc/engine.py`) writes its **own** staging CSVs for the changes it detects (inserts, updates, expire PKs) and BCP-loads those.
4. The SCD2 engine (`scd2/engine.py`) writes its **own** staging CSVs (new versions, close PKs) and BCP-loads those.
5. `cleanup_csvs()` deletes the extraction CSV along with everything else — but by then the damage is done (it sat on disk the whole time).

The extraction CSV exists because the extractors call `prepare_dataframe_for_bcp()` then `write_bcp_csv()` as a pair. The `prepare_dataframe_for_bcp()` call is essential (adds `_row_hash`, `_extracted_at`, sanitizes strings). The `write_bcp_csv()` call produces a CSV that nothing reads.

---

## Changes Required

### Overview

| # | Change | Files | Effect |
|---|--------|-------|--------|
| 1 | Delete extraction CSV immediately after extraction | `orchestration/small_tables.py`, `orchestration/file_tables.py`, `orchestration/drive_tables.py` | Frees disk/tmpfs before CDC starts |
| 2 | `del df` + `gc.collect()` after CDC promotion | `orchestration/small_tables.py`, `orchestration/file_tables.py`, `orchestration/drive_tables.py` | Frees the largest in-memory object before SCD2 |
| 3 | Delete CDC staging CSVs immediately after BCP load | `cdc/engine.py` | Frees 2-3 CSVs during CDC instead of at cleanup |
| 4 | Delete SCD2 staging CSVs immediately after BCP load | `scd2/engine.py` | Frees 2-3 CSVs during SCD2 instead of at cleanup |

`cleanup_csvs()` remains as a safety net at the end — catches anything missed on error paths.

Large tables (`orchestration/large_tables.py`) need **no changes**.

---

### Change 1: Delete Extraction CSV Immediately

The extraction CSV is never read after `extract_full()` returns. Delete it before CDC begins.

#### `orchestration/small_tables.py`

Current code (around line 96-100):
```python
# --- EXTRACT ---
with event_tracker.track("EXTRACT", table_config) as extract_event:
    df, csv_path = extract_full(table_config, output_dir)
    extract_event.rows_processed = len(df)
```

Change to:
```python
# --- EXTRACT ---
with event_tracker.track("EXTRACT", table_config) as extract_event:
    df, csv_path = extract_full(table_config, output_dir)
    extract_event.rows_processed = len(df)
    # Free extraction CSV immediately — CDC/SCD2 write their own
    # staging CSVs for BCP loads. This CSV is never read again.
    if csv_path is not None:
        csv_file = Path(csv_path)
        if csv_file.exists():
            csv_file.unlink()
```

`Path` is already imported at the top of the file.

#### `orchestration/file_tables.py`

Same change in the EXTRACT block (around line 103-105):
```python
with event_tracker.track("EXTRACT", file_config) as extract_event:
    df, csv_path = extract_file(file_config, output_dir)
    extract_event.rows_processed = len(df)
    if csv_path is not None:
        csv_file = Path(csv_path)
        if csv_file.exists():
            csv_file.unlink()
```

#### `orchestration/drive_tables.py`

Same change in the EXTRACT block (around line 108-111):
```python
with event_tracker.track("EXTRACT", drive_config) as extract_event:
    df, csv_path = scan_drive(drive_config, output_dir)
    extract_event.rows_processed = len(df)
    extract_event.event_detail = f"{drive_name}: {drive_config.mount_path}"
    if csv_path is not None:
        csv_file = Path(csv_path)
        if csv_file.exists():
            csv_file.unlink()
```

---

### Change 2: Release Extraction DataFrame After CDC

After `run_cdc_promotion()`, the original `df` is no longer needed. CDC returns `cdc_result.df_current` which is what SCD2 uses. Large tables already do `del df` + `gc.collect()` at `large_tables.py` line 370-376.

#### `orchestration/small_tables.py`

Current code (around line 196-204):
```python
# --- CDC PROMOTION ---
cdc_result = run_cdc_promotion(
    table_config, df, event_tracker, schema_result, output_dir,
)

# --- SCD2 PROMOTION ---
run_scd2_promotion(
    table_config, cdc_result, event_tracker, output_dir,
)
```

Change to:
```python
# --- CDC PROMOTION ---
cdc_result = run_cdc_promotion(
    table_config, df, event_tracker, schema_result, output_dir,
)

# Free extraction DataFrame — SCD2 uses cdc_result.df_current, not df.
# Mirrors large_tables.py line 370-376 which already does this per day.
del df
gc.collect()

# --- SCD2 PROMOTION ---
run_scd2_promotion(
    table_config, cdc_result, event_tracker, output_dir,
)
```

Add `import gc` to the imports at the top of the file.

**Important:** The `total_event.rows_processed = len(df)` line at the bottom of the `with` block (around line 211) must be moved to **before** the `del df`. Or capture the count earlier:

```python
extraction_row_count = len(df)

# --- CDC PROMOTION ---
cdc_result = run_cdc_promotion(
    table_config, df, event_tracker, schema_result, output_dir,
)

del df
gc.collect()

# --- SCD2 PROMOTION ---
run_scd2_promotion(
    table_config, cdc_result, event_tracker, output_dir,
)

# ... later ...
total_event.rows_processed = extraction_row_count
```

#### `orchestration/file_tables.py`

Same pattern. Current code (around line 178-186):
```python
cdc_result = run_cdc_promotion(
    file_config, df, event_tracker, schema_result, output_dir,
)

run_scd2_promotion(
    file_config, cdc_result, event_tracker, output_dir,
)
```

Change to:
```python
extraction_row_count = len(df)

cdc_result = run_cdc_promotion(
    file_config, df, event_tracker, schema_result, output_dir,
)

del df
gc.collect()

run_scd2_promotion(
    file_config, cdc_result, event_tracker, output_dir,
)
```

Add `import gc` to the imports. Update `total_event.rows_processed` to use `extraction_row_count`.

#### `orchestration/drive_tables.py`

Same pattern in the CDC/SCD2 section. Add `import gc`, capture row count before `del df`.

---

### Change 3: Delete CDC Staging CSVs After BCP Load

CDC writes staging CSVs that are loaded by BCP and never read again. Delete each CSV immediately after its BCP load completes.

#### `cdc/engine.py`

There are two locations where CSVs are written and loaded:

**Location 1: `_write_and_load_cdc()`**

Find the function `_write_and_load_cdc()`. After the BCP load call (`smart_load()` or `bcp_load()`), add CSV deletion:

```python
# After the BCP load completes:
write_bcp_csv(df_changes, csv_path)
smart_load(csv_path, staging_table, ...)
# Add this:
if csv_path.exists():
    csv_path.unlink()
```

**Location 2: `_expire_cdc_rows()`**

Find where the expire PKs CSV is written and loaded. After the BCP load:

```python
write_bcp_csv(df_expire_pks, csv_path)
smart_load(csv_path, staging_table, ...)
# Add this:
if csv_path.exists():
    csv_path.unlink()
```

Wrap each `unlink()` in a try/except to be safe — don't let a cleanup failure break the pipeline:

```python
try:
    if csv_path.exists():
        csv_path.unlink()
except OSError:
    logger.debug("Failed to delete staging CSV: %s", csv_path)
```

---

### Change 4: Delete SCD2 Staging CSVs After BCP Load

Same pattern as CDC — SCD2 writes staging CSVs that BCP loads and never reads again.

#### `scd2/engine.py`

There are two locations:

**Location 1: `_write_and_load_bronze()`**

After BCP loads the inserts CSV:

```python
write_bcp_csv(df_inserts, csv_path)
bcp_load(csv_path, bronze_table, ...)
# Add this:
try:
    if csv_path.exists():
        csv_path.unlink()
except OSError:
    logger.debug("Failed to delete staging CSV: %s", csv_path)
```

This function may be called multiple times per SCD2 run (once for new inserts, once for new versions, once for resurrections). Each call writes a separate CSV. Delete each after its BCP load.

**Location 2: `_execute_bronze_updates()`**

After BCP loads the close-PKs staging CSV:

```python
write_bcp_csv(df_close_pks, csv_path)
smart_load(csv_path, staging_table, ...)
# Add this:
try:
    if csv_path.exists():
        csv_path.unlink()
except OSError:
    logger.debug("Failed to delete staging CSV: %s", csv_path)
```

---

## Before/After Memory Timeline

### Before (Current)

```
Time ──────────────────────────────────────────────────────────►

Memory:  [===== df extraction =================================]
                                                                ^ never freed

Disk:    [=== CSV 1 (extraction) ==============================]
                  [=== CSV 2 (cdc inserts) ====================]
                    [=== CSV 3 (expire pks) ===================]
                              [=== CSV 4 (scd2 inserts) =======]
                                [=== CSV 5 (close pks) ========]
                                                               ^ all deleted at cleanup

Phase:   EXTRACT    CDC                  SCD2            CLEANUP
```

### After (With Changes)

```
Time ──────────────────────────────────────────────────────────►

Memory:  [===== df extraction =========]
                                       ^ del df + gc.collect()

Disk:    [== CSV 1 ==]
                  [= CSV 2 =]
                    [= CSV 3 =]
                              [= CSV 4 =]
                                [= CSV 5 =]
                                           ^ nothing left to clean

Phase:   EXTRACT    CDC                  SCD2            CLEANUP
              ^                ^    ^          ^    ^
              |                |    |          |    |
         CSV 1 deleted    CSV 2,3  deleted  CSV 4,5 deleted
         immediately      after BCP         after BCP
```

Peak disk usage drops from 5 concurrent CSVs to 1-2 at most. Peak memory drops by the full extraction DataFrame size before SCD2 begins.

---

## Files to Change Summary

| File | Changes |
|------|---------|
| `orchestration/small_tables.py` | (1) Delete `csv_path` after extraction. (2) Add `import gc`. (3) Capture `extraction_row_count = len(df)` before CDC. (4) `del df` + `gc.collect()` after CDC, before SCD2. (5) Use `extraction_row_count` for `total_event.rows_processed`. |
| `orchestration/file_tables.py` | Same 5 changes as small_tables.py. |
| `orchestration/drive_tables.py` | Same 5 changes as small_tables.py. |
| `cdc/engine.py` | In `_write_and_load_cdc()`: delete CSV after BCP load. In `_expire_cdc_rows()`: delete expire PKs CSV after BCP load. |
| `scd2/engine.py` | In `_write_and_load_bronze()`: delete CSV after BCP load. In `_execute_bronze_updates()`: delete close PKs CSV after BCP load. |

**No changes needed:**
- `orchestration/large_tables.py` — already does per-day `del df` + `gc.collect()` + `cleanup_csvs()`.
- `cleanup_csvs()` in `orchestration/pipeline_steps.py` — remains as a safety net, unchanged.
- Extractors — unchanged. They still write the CSV (needed for `prepare_dataframe_for_bcp()` side effects), the orchestrator just deletes it earlier.

---

## Testing

1. Run a single small table: `python3 main_small_tables.py --table ACCT --source DNA`
2. Verify Stage and Bronze tables are correctly populated (same as before).
3. Re-run the same table — verify CDC detects 0 changes (idempotent).
4. Monitor RSS memory during run — should drop after CDC promotion completes.
5. Check `config.CSV_OUTPUT_DIR` during run — should see CSVs appear and disappear quickly instead of accumulating.
6. Verify `cleanup_csvs()` at the end finds 0 files to clean (all already deleted).
7. Test error paths: kill the pipeline mid-CDC, verify `cleanup_csvs()` still cleans up any remaining CSVs on the next run via `staging_cleanup.py`.

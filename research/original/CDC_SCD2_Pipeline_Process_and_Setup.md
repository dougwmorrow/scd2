# CDC/SCD2 Pipeline — Current Process and Setup

This document compiles all Change Data Capture (CDC) and Slowly Changing Dimension Type 2 (SCD2) events, logic, and safeguards implemented in the production pipeline. It is intended as a review artifact to evaluate our setup against the findings from *Validating a Production CDC/SCD2 Pipeline Against 2024–2025 Best Practices*.

---

## 1. Architecture Overview

The pipeline follows a **medallion architecture** with two core layers:

- **UDM_Stage (CDC layer):** Captures row-level change detection by comparing fresh source extractions against previously stored data using hash-based CDC. Tracks operation type (Insert/Update/Delete), validity windows, and current-row flags.
- **UDM_Bronze (SCD2 layer):** Maintains full version history using Slowly Changing Dimension Type 2 semantics. Every change produces a new version row; previous versions are expired (closed) but never deleted. Bronze is append-only by design.

Source systems are Oracle (DNA) and SQL Server (CCM, EPICOR). The tech stack is Python 3.12, Polars DataFrames, ConnectorX for extraction, polars-hash for deterministic hashing, and BCP for bulk loading into SQL Server. The pipeline runs on Red Hat Linux (6 CPU, 64GB RAM).

### Two Processing Modes

| Mode | Trigger | CDC Variant | SCD2 Variant | Memory Strategy |
|------|---------|-------------|--------------|-----------------|
| **Small tables** | No `SourceAggregateColumnName` set | `run_cdc()` — full in-memory comparison | `run_scd2()` — reads all active Bronze rows | Entire table fits in memory |
| **Large tables** | `SourceAggregateColumnName` set | `run_cdc_windowed()` — scoped to extraction date window | `run_scd2_targeted()` — PK-targeted Bronze read via staging join | One day at a time, per-day checkpoint |

---

## 2. Data Flow — Step by Step

### 2.1 Extraction

1. **Source routing** selects the correct extractor based on source type and configuration (Oracle with INDEX hints → oracledb; Oracle without → ConnectorX; SQL Server → ConnectorX).
2. For large tables, extraction is **windowed to a single calendar day** using `SourceAggregateColumnName` (date column). Small tables extract all rows.
3. Every extracted row gets two columns added:
   - `_row_hash` — SHA-256 hash truncated to Int64, computed via polars-hash (deterministic across Python sessions and Polars versions).
   - `_extracted_at` — UTC timestamp of extraction.
4. BCP CSV files are written per the **BCP CSV Contract** (tab-delimited, no quoting, no header, LF row terminator, NULL as empty string).

### 2.2 Pre-CDC Guards and Schema Management

Before CDC runs, several gates must pass:

- **Table lock (P1-2):** `sp_getapplock` with `@LockOwner='Session'` prevents concurrent pipeline runs on the same table. Session-owned locks auto-release on connection drop (crash-safe).
- **Schema evolution (P0-2):** Compares DataFrame columns against existing Stage/Bronze tables every run. New columns are `ALTER TABLE ADD`ed. Removed columns are logged as WARNINGs (never dropped). Type changes raise `SchemaEvolutionError` and skip the table entirely.
- **Column sync:** Auto-populates `General.dbo.UdmTablesColumnsList` and discovers primary keys from source metadata. PKs drive all CDC/SCD2 comparison logic.
- **Empty extraction guard (P1-1):** If row count drops >90% vs the rolling median of the last 5 successful runs, CDC is blocked to prevent false mass-delete cascades. A 50% drop triggers a WARNING but proceeds.
- **Explosion guard (P1-11):** If row count spikes >5× vs previous median, CDC is blocked to prevent OOM or runaway loads.
- **First-run ceiling (S-2):** Small tables with no extraction history are capped at 50M rows to catch misconfigured large tables.
- **Memory guard (P2-12 / ST-1):** Estimates CDC peak memory (3× DataFrame size for anti-join) and blocks if it would exceed 20GB.
- **Large table per-day guard (P1-13):** Each day's extraction is checked against the daily extraction median. Spikes >5× are blocked.

### 2.3 CDC Promotion — Change Data Capture

CDC detects row-level changes by comparing the fresh extraction against the existing Stage table using a **three-way comparison pattern**:

#### 2.3.1 Hash Computation (`add_row_hash` in `bcp_csv.py`)

All source columns are concatenated and hashed to produce a single `_row_hash` (Int64) per row:

- **Algorithm:** SHA-256 via polars-hash plugin → first 16 hex characters → parsed to UInt64 → reinterpreted as signed Int64 for SQL Server BIGINT.
- **Column separator:** Unit Separator `\x1F` (P0-6) — avoids delimiter collision risk that `||` would have with real data.
- **NULL sentinel:** `\x1FNULL\x1F` (W-2) — uses Unit Separator wrapping instead of null bytes (`\x00`) which risk C-string truncation in FFI/logging layers. NULLs must hash differently from empty strings and from NULLs in different column positions.
- **Float normalization (V-1 / W-3):** Floats are rounded to 10 decimal places. IEEE 754 edge cases are handled: `±0.0` → `+0.0`, `NaN` → `\x1FNaN\x1F`, `Infinity` → `\x1FINF\x1F`, `-Infinity` → `\x1F-INF\x1F`. Prevents phantom CDC updates from cross-platform representation differences.
- **Unicode normalization (V-2):** All string columns are NFC-normalized before hashing. Prevents phantom updates from normalization form differences (e.g., composed vs decomposed accented characters).
- **Hash collision risk (C-1):** At 64-bit truncation with 3B rows, the expected collision rate is ~0.0005/day or ~1–2 silently lost updates/year across 10 large tables. Weekly reconciliation catches these within 7 days. Upgrading to 128-bit would reduce this to near-zero.

#### 2.3.2 Three-Way CDC Comparison

The CDC engine performs three anti-join/join operations using Polars:

1. **NULL PK filter (P0-4):** Before any comparison, rows with NULL primary key values are filtered out. Polars' default `join_nulls=False` means NULL PKs would appear in every anti-join output, causing duplicate inserts every run. Filtered count is tracked in `PipelineEventLog` metadata.

2. **INSERTS — Anti-join (fresh LEFT ANTI JOIN existing on PKs):** Rows in the fresh extraction whose primary keys do not exist in the existing Stage table are classified as inserts.

3. **DELETES — Reverse anti-join (existing LEFT ANTI JOIN fresh on PKs):** Rows in the existing Stage whose primary keys do not appear in the fresh extraction are classified as deletes. For large tables (windowed CDC), delete detection is **scoped to the extraction window only** (P1-4) — rows outside the window are never marked deleted.

4. **UPDATES — Inner join + hash comparison:** Rows matched by PK are compared on `_row_hash`. If the hash differs (or either hash is NULL — P0-10 NULL hash guard), the row is classified as an update. Identical hashes mean unchanged.

5. **Count validation (P0-12):** After classification, a count check verifies that `inserts + updates + unchanged = total fresh PKs`. A mismatch indicates a PK dtype alignment issue.

#### 2.3.3 PK Dtype Alignment (P0-12)

Before every join operation, `align_pk_dtypes()` casts PK columns in both DataFrames to matching Polars dtypes. Mismatched types (e.g., Int32 vs Int64, or Utf8 vs LargeUtf8) cause silent join failures where rows never match — producing phantom inserts and deletes.

#### 2.3.4 CDC Stage Table Columns

Each row in the Stage table carries these CDC metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| `_row_hash` | BIGINT | SHA-256 truncated to 64-bit Int64 |
| `_extracted_at` | DATETIME2 | UTC timestamp of extraction |
| `_cdc_operation` | NVARCHAR(1) | `I` (insert), `U` (update), `D` (delete) |
| `_cdc_valid_from` | DATETIME2 | When this version became current |
| `_cdc_valid_to` | DATETIME2 | When this version was superseded (NULL = current) |
| `_cdc_is_current` | BIT | 1 = active current version, 0 = historical |
| `_cdc_batch_id` | INT | Pipeline batch ID for traceability |

#### 2.3.5 Windowed CDC (Large Tables)

For large tables, CDC comparison is scoped to the extraction date window:

- Fresh extraction covers a single calendar day (via `WHERE date_column >= @target_date AND date_column < @next_date`).
- The existing Stage is read only for rows within the same date window.
- Delete detection only applies within the window — rows outside the window are untouched (P1-4).
- **Cross-midnight overlap (V-7):** Configurable `OVERLAP_MINUTES` can shift the extraction window backward by full days (≥1440 minutes) to capture transactions spanning midnight. `LookbackDays` provides the primary mechanism — a lookback of N days means each date is re-processed across N runs.
- **NULL date column check (L-4):** Before processing, the pipeline checks for rows with NULL in the date column, which are silently excluded from windowed extraction.

### 2.4 SCD2 Promotion — Slowly Changing Dimension Type 2

SCD2 takes the CDC "current" rows (`_cdc_is_current = 1`) and promotes them to Bronze with full version history.

#### 2.4.1 Bronze Table Structure

Each Bronze table has these SCD2 metadata columns in addition to all source columns:

| Column | Type | Description |
|--------|------|-------------|
| `_scd2_key` | BIGINT IDENTITY(1,1) | Auto-increment surrogate key (excluded from all INSERTs/BCP) |
| `UdmHash` | BIGINT | Copy of `_row_hash` from CDC, used for Bronze-level comparison |
| `UdmEffectiveDateTime` | DATETIME2 | When this version became effective |
| `UdmEndDateTime` | DATETIME2 NULL | When this version was expired (NULL = currently active) |
| `UdmActiveFlag` | BIT | 1 = active row, 0 = expired/historical |
| `UdmScd2Operation` | NVARCHAR(10) | `I` (initial insert), `U` (update/new version) |

#### 2.4.2 SCD2 Change Detection Algorithm

The SCD2 engine compares CDC current rows against active Bronze rows using the same three-way pattern:

1. **First run:** If Bronze table doesn't exist or has zero active rows, all CDC current rows are classified as new inserts.
2. **Bronze dedup (P1-16):** Before comparison, active Bronze rows are deduplicated per PK — keeping only the row with the latest `UdmEffectiveDateTime`. This handles crash recovery artifacts from the INSERT-first design.
3. **PK dtype alignment (P0-12):** Same as CDC — ensures join correctness.
4. **New inserts:** Anti-join of CDC keys against Bronze keys on PKs — rows in CDC not in Bronze.
5. **Closes (source deletes):** Anti-join of Bronze keys against CDC keys — rows in Bronze not in CDC.
6. **Changed rows (new versions):** Inner join on PKs, then compare `_row_hash` against `UdmHash`. Changed rows generate both a new version (INSERT) and a close of the old version (UPDATE). NULL hash guard (P0-10) treats NULL hashes as "changed."
7. **Unchanged rows:** Matched PKs where hashes are identical — no action taken.
8. **Count validation (P0-12):** Verifies `inserts + new_versions + unchanged = CDC unique PKs`.

#### 2.4.3 INSERT-First SCD2 Ordering (P0-8)

The pipeline deliberately performs **INSERT before UPDATE** as a crash-safety measure:

- **Step 1 — INSERT:** New rows (operation `I`) and new versions of changed rows (operation `U`) are appended to Bronze via BCP. Both get `UdmActiveFlag = 1`, `UdmEndDateTime = NULL`.
- **Step 2 — UPDATE:** Old versions of changed rows and deleted rows are closed by setting `UdmActiveFlag = 0` and `UdmEndDateTime = now`.

**Crash behavior:**
- Crash after INSERT but before UPDATE → duplicate active rows. **Recoverable** via next run's dedup (P1-16) and the Bronze unique filtered index (SCD-1).
- Crash after UPDATE but before INSERT (if reversed) → zero active rows. **Unrecoverable data loss** requiring manual intervention.

The INSERT-first approach is a deliberate tradeoff: temporary inconsistency (duplicate actives) over data loss (zero actives).

#### 2.4.4 UPDATE via Staging Table + UPDATE JOIN

The pipeline avoids SQL Server's MERGE statement entirely (confirmed unsafe — new bugs found February 2025). Instead, it uses a **staging table + UPDATE JOIN** pattern:

1. A temporary staging table (`_staging_scd2_{table}`) is created with PK columns typed from the actual target table (P0-3 — never hardcoded NVARCHAR(MAX)).
2. PKs to close are BCP-loaded into the staging table.
3. An index is created on the staging table for efficient JOIN (P2-5).
4. The UPDATE JOIN sets `UdmActiveFlag = 0` and `UdmEndDateTime = @now` on matching active Bronze rows.
5. **Batched UPDATE (SCD-3):** For close sets >500K rows, the UPDATE is batched using `UPDATE TOP(500000)` to prevent transaction log exhaustion and lock escalation. The `WHERE UdmActiveFlag = 1` filter provides natural convergence — already-processed rows aren't touched again.
6. **Row count validation (P2-14):** Actual vs expected UPDATE row count is verified. Fewer rows affected is expected during retries (idempotent). More rows affected is logged as an ERROR.
7. Staging table is dropped in a `finally` block. Orphaned staging tables from crashes are cleaned by `staging_cleanup.py` at pipeline start (P3-3).

#### 2.4.5 Windowed Delete Propagation (P0-11)

For large tables using windowed CDC, deleted PKs detected by `run_cdc_windowed()` are passed explicitly to `run_scd2_targeted()` via the `deleted_pks` parameter. Without this, windowed deletes would never propagate to Bronze because the PK-targeted read only loads Bronze rows matching PKs in `df_current` — which excludes deleted rows.

The close set is deduplicated (C-5) before the UPDATE to handle PKs that appear in multiple sources (closed + changed + deleted).

#### 2.4.6 Targeted SCD2 for Large Tables (P1-3)

Instead of reading all active Bronze rows (impossible at 3B+ row scale), `run_scd2_targeted()` reads only the Bronze rows whose PKs exist in the current extraction via a staging table + INNER JOIN. The comparison logic is identical to the full SCD2.

#### 2.4.7 Schema Validation Before Concatenation (W-7)

Before any `pl.concat()` operation in CDC or SCD2, `validate_schema_before_concat()` verifies all DataFrames have identical schemas. Polars' `diagonal_relaxed` and `vertical_relaxed` modes silently coerce columns to common supertypes (e.g., Int64 → Float64), which can corrupt precision-sensitive values like PKs or monetary amounts.

#### 2.4.8 Column Order Enforcement (P0-1)

`reorder_columns_for_bcp()` reads `INFORMATION_SCHEMA.COLUMNS` ordinal position before every BCP write to enforce deterministic column ordering. BCP CSV has no header — column mapping is positional. Without this, schema evolution (new columns) or inconsistent `SELECT` ordering could silently map data to wrong columns, causing silent corruption.

---

## 3. Bronze Indexes and Constraints

### 3.1 Unique Filtered Index (SCD-1)

```sql
CREATE UNIQUE NONCLUSTERED INDEX UX_{table}_active
ON Bronze(pk1, pk2, ...) WHERE UdmActiveFlag = 1
```

Prevents duplicate active rows per business key. Serves as both a constraint and an optimization for active-row queries. Created once, idempotently.

### 3.2 Point-in-Time Index (V-9)

```sql
CREATE NONCLUSTERED INDEX IX_{table}_pit
ON Bronze(pk1, pk2, ..., UdmEffectiveDateTime DESC)
```

Optimizes historical queries that look up a specific version as-of a point in time.

### 3.3 Ordered Clustered Columnstore (W-10, opt-in)

For Bronze tables with 100M+ rows on SQL Server 2022:

```sql
CREATE CLUSTERED COLUMNSTORE INDEX CCI_{table}
ORDER (BusinessKey, UdmEffectiveDateTime)
```

Enables segment elimination for point-in-time queries without sacrificing columnstore compression. This is an opt-in migration requiring a maintenance window.

---

## 4. Post-SCD2 Integrity Checks

### 4.1 Duplicate Active Row Check (V-4)

After every SCD2 promotion, `_check_duplicate_active_rows()` queries Bronze for PKs with more than one `UdmActiveFlag = 1` row. This is a **non-blocking diagnostic** — it logs a WARNING with the count and suggests the safe query pattern:

```sql
ROW_NUMBER() OVER (PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC) WHERE rn = 1
```

### 4.2 SCD2 Structural Integrity Validation (V-3)

`validate_scd2_integrity()` runs three SQL checks against Bronze:

1. **Overlapping intervals:** Self-join detecting records where `[EffectiveDateTime, EndDateTime)` ranges overlap for the same business key. Uses `ISNULL(EndDateTime, '9999-12-31')` for active rows.
2. **Zero active PKs:** Business keys where all versions are expired (`SUM(CASE WHEN UdmActiveFlag=1 THEN 1 ELSE 0 END) = 0`). Indicates data loss — either UPDATE-first crash residue or incorrect close logic.
3. **Version gaps:** Uses `LAG(UdmEndDateTime)` windowed over `PARTITION BY PK ORDER BY EffectiveDateTime` to detect non-contiguous version chains. A gap >1 second between consecutive versions is flagged.

Each check returns sample PKs for investigation.

---

## 5. Reconciliation Framework

### 5.1 Daily Count Reconciliation (W-11)

`reconcile_counts()` compares row counts across three layers: source (via ConnectorX COUNT), Stage current rows (`_cdc_is_current = 1`), and Bronze active rows (`UdmActiveFlag = 1`). Fast and cheap — designed for daily scheduling to catch gross data loss within 24 hours.

### 5.2 Weekly Full Column-by-Column Reconciliation (P3-4)

`reconcile_table()` does a **full column-by-column comparison** (not hash-based) between current Stage rows and a fresh source extraction. For each PK-matched pair, every source column value is compared directly. Detects:

- **Hash collisions** — rows that changed but where the hash happened to match (expected ~1–2/year at billion-row scale).
- **CDC logic bugs** — any other discrepancy between CDC's view and source truth.

This deliberately uses a **different method than integration** (column-by-column vs hash), so a logical error in hashing cannot hide from reconciliation.

### 5.3 Bronze Reconciliation (P2-13)

`reconcile_bronze()` compares CDC Stage current rows against Bronze active rows:

- Hash mismatches (Stage `_row_hash` ≠ Bronze `UdmHash` for matched PKs)
- Orphaned Bronze rows (active in Bronze but not current in Stage)
- Missing Bronze rows (current in Stage but not active in Bronze)
- Duplicate active PKs (crash recovery artifacts)

Triggers `validate_scd2_integrity()` (V-3) as part of the Bronze reconciliation.

### 5.4 Windowed Reconciliation (C-7)

`reconcile_table_windowed()` provides full column-by-column reconciliation for large tables that exceed the 50M-row in-memory limit.

---

## 6. Concurrency and Crash Safety

### 6.1 Table-Level Locking (P1-2)

`sp_getapplock` with `@LockOwner='Session'` and `@LockTimeout=0` (non-blocking skip). The lock connection uses ODBC resiliency settings (`ConnectRetryCount=3`, `ConnectRetryInterval=10`) to survive transient network failures (N-1). A heartbeat query runs every 10 processing days during large-table backfills (P1-14) to prevent idle connection timeout.

The RCSI race condition (W-8) does not apply to Session-owned locks with autocommit — there is no pending transaction creating a pre-commit window.

### 6.2 Per-Day Checkpointing (P1-5 / P1-6)

Large tables checkpoint every completed day to `General.ops.PipelineExtractionState`. Statuses include `EXTRACTED` (extraction done, CDC pending), `SUCCESS` (all steps done), and `FAILED_{step}` (identifies which step failed). On restart, the pipeline resumes from the last successful date and fills gaps.

### 6.3 Idempotency

- **CDC:** Re-extracting the same data produces identical hashes for unchanged rows. Anti-join operations are naturally idempotent.
- **SCD2 INSERT:** The Bronze unique filtered index (SCD-1) prevents duplicate active rows on retry. If the INSERT-first step succeeded and the UPDATE failed, the next run's Bronze dedup (P1-16) resolves the duplicates.
- **SCD2 UPDATE:** The `WHERE UdmActiveFlag = 1` filter makes the UPDATE JOIN idempotent — already-closed rows are not touched again (C-2).

### 6.4 Empty Extraction Guard (P1-1)

Prevents a false mass-delete cascade when extraction fails silently (e.g., Oracle listener down, permissions revoked). Uses rolling median of last 5 successful extractions to smooth out single-run anomalies (P3-7). Overridable with `--force` for intentional reloads.

---

## 7. BCP Data Loading Safeguards

### 7.1 BCP CSV Contract

All CSV output is governed by a single specification:

- Tab delimiter (`\t`), LF row terminator (`-r 0x0A`), no header, `quote_style='never'`
- NULL → empty string, BIT → Int8 (0/1, never True/False)
- UInt64 (from polars-hash) → `.reinterpret(signed=True)` → Int64
- String sanitization: `\t`, `\n`, `\r`, `\x00` stripped before write
- `batch_size=4096` on `write_csv()` to avoid memory spikes

### 7.2 Data Corruption Prevention (B-1)

`quote_style='never'` means embedded tabs split fields and embedded newlines split rows. `sanitize_strings()` must be called before every `write_bcp_csv()`. The `prepare_dataframe_for_bcp()` function calls it automatically; direct callers must sanitize first.

### 7.3 BCP Format Files (W-13)

`generate_bcp_format_file()` produces XML `.fmt` files from `INFORMATION_SCHEMA` metadata. Provides explicit column mapping as defense-in-depth alongside `reorder_columns_for_bcp()`.

### 7.4 BULK_LOGGED Recovery Model (P1-8)

BCP loads use `bulk_load_recovery_context()` which temporarily switches the target database to BULK_LOGGED recovery model during BCP operations, then switches back. This scope is narrowed to only wrap `run_scd2_targeted()` — UPDATE JOINs are fully logged regardless.

---

## 8. Observability and Diagnostics

### 8.1 Pipeline Event Log

`General.ops.PipelineEventLog` records one row per step per table per batch, tracking:

- Event types: `EXTRACT`, `BCP_LOAD`, `CDC_PROMOTION`, `SCD2_PROMOTION`, `CSV_CLEANUP`, `TABLE_TOTAL`
- Row-level metrics: `RowsProcessed`, `RowsInserted`, `RowsUpdated`, `RowsDeleted`, `RowsUnchanged`, `RowsBefore`, `RowsAfter`
- Performance: `DurationMs`, `RowsPerSecond`
- Metadata: NULL PK counts, custom diagnostic JSON

### 8.2 Pipeline Log

`General.ops.PipelineLog` captures Python logging output via `SqlServerLogHandler`, providing searchable diagnostic logs.

### 8.3 Pipeline Extraction State

`General.ops.PipelineExtractionState` tracks per-table, per-date processing status for large tables. Supports gap detection and resume-from-failure.

---

## 9. Schema Evolution (P0-2)

Runs every extraction cycle (not just on first load):

| Source Change | Action | Risk Level |
|---------------|--------|------------|
| **New column** | `ALTER TABLE ADD` on both Stage and Bronze | Safe — additive change |
| **Removed column** | `WARNING` logged, column preserved | Safe — data preservation |
| **Type change** | `SchemaEvolutionError` raised, table skipped | Manual resolution required |

Records failing schema contracts can be routed to `General.ops.Quarantine` (W-17).

---

## 10. Memory Management

- **MALLOC_ARENA_MAX=2 (W-4):** Limits glibc to 2 memory arenas, preventing the 10× memory bloat documented in Polars issue #23128.
- **shrink_to_fit (W-12):** Called after large DataFrame operations (>100K rows) in CDC engine, large table extraction, and hash computation.
- **Per-day processing:** Large tables process one day at a time to bound memory.
- **Extraction DataFrame release (P2-10):** After CDC captures results, the original extraction DataFrame is deleted before SCD2 to reduce peak memory.
- **Memory pressure checks (P3-8):** Before each day's processing, system memory and file descriptor counts are checked. GC is triggered above 85% usage.

---

## 11. Known Risks and Monitoring Points

| Risk | Severity | Mitigation | Monitoring |
|------|----------|------------|------------|
| Hash collision (64-bit truncation) | ~1–2 missed updates/year across 10 large tables | Weekly full column reconciliation (P3-4) | Reconciliation result logs |
| INSERT-first duplicate active rows | Temporary inconsistency until next run | Bronze dedup (P1-16), unique filtered index (SCD-1), V-4 check | V-4 WARNING in logs |
| Windowed CDC missed deletes | Rows outside window not detected as deleted | Periodic full reconciliation, LookbackDays overlap | Reconciliation orphan count |
| ConnectorX Oracle instability | Panic on 9999-12-31, memory leaks | oracledb fallback, TRUNC() on dates, evaluate pydbzengine | Extraction error logs |
| Cross-midnight transaction splitting | Split rows processed in wrong window | LookbackDays, OVERLAP_MINUTES (V-7), weekly reconciliation | Reconciliation mismatch trends |
| NULL date column exclusion (L-4) | Rows silently excluded from windowed extraction | Pre-processing NULL check and WARNING | L-4 WARNING in logs |
| Bronze growth (L-3) | Append-only = unbounded growth | Partition on UdmActiveFlag, archive historical rows | Table size monitoring |
| polars-hash version change (H-2) | Hash output change = full CDC wave | Pin version, test before upgrade, fallback path (V-11) | Hash regression test suite |

---

## 12. Pipeline Events — Complete Lifecycle

For reference, here is the complete ordered sequence of events and logic for both processing modes:

### Small Table Lifecycle

```
1.  acquire_table_lock(source, table)                     [P1-2]
2.  EXTRACT: source → ConnectorX/oracledb → DataFrame
3.    add_row_hash() → _row_hash (SHA-256 Int64)
4.    add _extracted_at (UTC)
5.  Size guard: warn if >10M rows                         [P3-1]
6.  Memory guard: estimate CDC peak memory                [P2-12/ST-1]
7.  ensure_stage_table() — auto-create if missing
8.  ensure_bronze_table() — auto-create if missing
9.  evolve_schema() — ADD/WARN/ERROR columns              [P0-2]
10. sync_columns() — populate UdmTablesColumnsList + PKs
11. ensure_bronze_unique_active_index()                   [SCD-1]
12. ensure_bronze_point_in_time_index()                   [V-9]
13. _check_extraction_guard() — row count guards          [P1-1/V-10/P1-11/S-2]
14. CDC PROMOTION:
    a. _filter_null_pks() — remove NULL PK rows           [P0-4]
    b. align_pk_dtypes()                                  [P0-12]
    c. Anti-join: fresh vs existing → INSERTS
    d. Anti-join: existing vs fresh → DELETES
    e. Inner join + hash compare → UPDATES + UNCHANGED
    f. Count validation                                   [P0-12]
    g. BCP load changes to Stage (reorder columns P0-1)
15. SCD2 PROMOTION:
    a. Read all active Bronze rows
    b. _dedup_bronze_active()                             [P1-16]
    c. align_pk_dtypes()                                  [P0-12]
    d. Anti-join: CDC current vs Bronze → NEW INSERTS
    e. Anti-join: Bronze vs CDC current → CLOSES
    f. Inner join + hash compare → NEW VERSIONS + UNCHANGED
    g. NULL hash guard                                    [P0-10]
    h. Count validation                                   [P0-12]
    i. Step 1: INSERT new rows + new versions via BCP     [P0-8]
    j. Step 2: UPDATE close old versions via staging JOIN  [P0-8]
    k. _check_duplicate_active_rows()                     [V-4]
16. CSV cleanup
17. release_table_lock()
```

### Large Table Lifecycle (per day)

```
1.  acquire_table_lock(source, table)                     [P1-2]
2.  _check_null_date_column()                             [L-4]
3.  get_dates_to_process() — gap detection + lookback     [P1-5]
4.  FOR EACH target_date:
    a. Lock heartbeat (every 10 days)                     [P1-14]
    b. Memory pressure check                              [P3-8]
    c. EXTRACT: windowed single-day extraction
       - Apply OVERLAP_MINUTES shift                      [V-7]
       - shrink_to_fit()                                  [W-12]
    d. Intermediate checkpoint: EXTRACTED                  [P3-11]
    e. _check_daily_extraction_guard()                    [P1-13]
    f. ensure_stage/bronze_table (first day only)
    g. evolve_schema()                                    [P0-2]
    h. sync_columns() (first day only)
    i. ensure_bronze_unique_active_index()                [SCD-1]
    j. WINDOWED CDC:
       - Same as small table CDC but scoped to date window
       - Delete detection scoped to window only           [P1-4]
    k. Release extraction DataFrame                       [P2-10]
    l. TARGETED SCD2:
       - PK-targeted Bronze read via staging JOIN         [P1-3]
       - Same SCD2 logic as small tables
       - Includes deleted_pks from windowed CDC           [P0-11]
       - Close set dedup                                  [C-5]
       - Batched UPDATE if >500K closes                   [SCD-3]
    m. Checkpoint: SUCCESS                                [P1-5]
    n. CSV cleanup
5.  release_table_lock()
```

---

## 13. Summary of Alignment with Best Practices

Based on the *Validating a Production CDC/SCD2 Pipeline Against 2024–2025 Best Practices* research, here is the alignment status:

| Best Practice | Status | Notes |
|---------------|--------|-------|
| Hash-based CDC for cross-system batch comparison | ✅ Implemented | Three-way anti-join pattern with SHA-256 |
| polars-hash for deterministic hashing | ✅ Implemented | Replaced non-deterministic native `hash_rows()` |
| 64-bit hash truncation safe for row-level CDC | ✅ Validated | Not used as surrogate key |
| NULL sentinel in hash input | ✅ Implemented | `\x1FNULL\x1F` (Unit Separator, not null bytes) |
| Float normalization + IEEE 754 edge cases | ✅ Implemented | ±0.0, NaN, Infinity handled |
| Unicode NFC normalization | ✅ Implemented | Applied to DataFrame and hash input |
| INSERT-first SCD2 ordering | ✅ Implemented | Crash-safe, with dedup-on-next-run recovery |
| Avoid MERGE statement | ✅ Implemented | Staging table + UPDATE JOIN exclusively |
| SCD2 integrity checks (4 core) | ✅ Implemented | Overlapping intervals, zero-active, version gaps + duplicate active |
| BIGINT IDENTITY surrogate keys | ✅ Implemented | `_scd2_key BIGINT IDENTITY(1,1)` |
| Filtered unique index WHERE ActiveFlag=1 | ✅ Implemented | SCD-1 created idempotently |
| Medallion architecture (Stage CDC → Bronze SCD2) | ✅ Implemented | Separate databases, separate concerns |
| Schema evolution (ADD/WARN/ERROR) | ✅ Implemented | Additive-first, never drop |
| Per-day checkpoint/resume | ✅ Implemented | PipelineExtractionState with gap detection |
| sp_getapplock concurrent run protection | ✅ Implemented | Session-owned, non-blocking skip |
| Weekly full reconciliation (not hash-based) | ✅ Implemented | Column-by-column comparison |
| Daily count reconciliation | ✅ Implemented | Source vs Stage vs Bronze counts |
| MALLOC_ARENA_MAX=2 | ✅ Documented | Environment variable for glibc arena control |
| BCP UTF-8 fix (mssql-tools18 v18.6.1.1) | ⚠️ Verify | Confirm mssql-tools18 version is 18.6.1.1+ |
| Polars streaming engine for large operations | ⚠️ Not yet adopted | `collect(engine="streaming")` available since v1.31.1 |
| ConnectorX Oracle contingency | ⚠️ Partial | oracledb fallback exists; pydbzengine evaluation pending |
| SQL Server 2022 temporal tables evaluation | ⚠️ Pending | W-16 TODO noted; not yet prototyped |
| XXH3-128 / BLAKE3 hash alternatives | ⚠️ Pending | W-18 TODO; current SHA-256 is functional |

---

*Document generated from pipeline codebase review. Use this as the basis for identifying gaps and prioritizing remaining work.*
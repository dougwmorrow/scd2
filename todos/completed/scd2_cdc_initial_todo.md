# CDC/SCD2 Pipeline Validation — Action Items

*Context: Derived from "CDC/SCD2 Pipeline Validation Against Production Best Practices" — a research-backed validation across seven areas: hash-based CDC, SCD2 design, medallion separation, windowed processing, edge cases, ConnectorX/BCP caveats, and CDC approach selection. Items already addressed by prior audits (P0-1 through T-3) are excluded.*

---

## 1. Hash Input Hygiene

### V-1 (P0): Float/decimal columns cause phantom CDC changes on every run

**Files:** `bcp_csv.py` (`add_row_hash`), `engine.py`

**Problem:** IEEE 754 floating-point representation means `0.1 + 0.2 ≠ 0.3`. When Oracle stores a FLOAT as `0.30000000000000004` and SQL Server normalizes it to `0.3`, the hash will differ on every comparison — even though the logical data hasn't changed. This generates spurious SCD2 records on every pipeline run for any row containing float columns, inflating Bronze tables with false history.

At 3M daily rows, even a small percentage of float-affected rows compounds into millions of phantom SCD2 records per month, degrading query performance and corrupting change history.

**Impact:** Silent data corruption. Phantom SCD2 records inflate Bronze tables. Change history becomes unreliable. Query performance degrades as false history accumulates.

**Fix:**
- **Option A (recommended):** Cast FLOAT/REAL columns to fixed-precision DECIMAL before hashing — add a normalization step in `add_row_hash()` that identifies Float32/Float64 columns and casts them to `pl.Decimal` with explicit precision/scale before string conversion
- **Option B:** Exclude FLOAT/REAL columns from the hash entirely and compare them separately with an epsilon tolerance (e.g., `abs(a - b) < 1e-9`)
- Add a configuration option in `UdmTablesColumnsList` to specify per-column hash exclusion or precision override
- Log a WARNING at pipeline startup listing any tables with FLOAT/REAL columns in the hash

---

### V-2 (P1): Unicode normalization differences cause phantom hash changes

**Files:** `bcp_csv.py` (`add_row_hash`)

**Problem:** The same visual character can have multiple Unicode representations. For example, `é` can be encoded as U+00E9 (single codepoint, NFC form) or as `e` + combining accent U+0301 (NFD form). Oracle and SQL Server may normalize Unicode differently, or source systems may emit inconsistent forms. When hashing, these byte-different but visually-identical strings produce different hashes, triggering phantom CDC updates.

This affects any table with accented characters, CJK text, or other Unicode content where normalization forms may differ between source and target systems.

**Impact:** Phantom CDC updates for rows with Unicode text. False SCD2 history records. More common in multilingual datasets.

**Fix:**
- Apply Unicode NFC normalization to all string columns before hashing in `add_row_hash()`
- Implementation: `import unicodedata` → apply `unicodedata.normalize('NFC', value)` to Utf8 columns before the `pl.concat_str()` step
- This can be done as a Polars expression: `.map_elements(lambda s: unicodedata.normalize('NFC', s) if s else s)` or via a custom Rust plugin for performance
- Log INFO at startup if any tables contain NVARCHAR/Unicode-typed columns

---

## 2. SCD2 Integrity

### V-3 (P1): No SCD2 structural integrity validation beyond unique active index

**Files:** `reconciliation.py`, `engine.py`

**Problem:** The existing unique filtered index (SCD-1) ensures at most one active row per PK, but this is only one of three required SCD2 integrity invariants. The pipeline does not currently validate:

1. **No overlapping `[EffectiveDateTime, EndDateTime)` intervals per natural key** — overlapping intervals mean two versions of a record are simultaneously "valid," making point-in-time queries return ambiguous results
2. **No gaps between consecutive versions** — the `EndDateTime` of the expired record should equal the `EffectiveDateTime` of its successor; gaps create time periods where no version of the record exists
3. **Exactly one `UdmActiveFlag = 1` per natural key** — the unique index enforces "at most one" but doesn't detect "zero active" (all versions expired with no current record)

These violations can accumulate silently over time, especially after crash recovery or retry scenarios.

**Impact:** Point-in-time queries return wrong or ambiguous results. "Zero active" rows make records effectively disappear from current-state views. Issues compound over time and are expensive to remediate retroactively.

**Fix:**
- Add a periodic SCD2 integrity check (can run alongside weekly reconciliation) with three SQL validations per Bronze table:

```sql
-- Check 1: Overlapping intervals
SELECT pk_cols, COUNT(*)
FROM Bronze a JOIN Bronze b
  ON a.pk = b.pk AND a._scd2_key <> b._scd2_key
  AND a.UdmEffectiveDateTime < b.UdmEndDateTime
  AND b.UdmEffectiveDateTime < a.UdmEndDateTime
GROUP BY pk_cols HAVING COUNT(*) > 0

-- Check 2: Zero active records
SELECT pk_cols FROM Bronze
GROUP BY pk_cols
HAVING SUM(CASE WHEN UdmActiveFlag = 1 THEN 1 ELSE 0 END) = 0

-- Check 3: Gaps between consecutive versions
SELECT pk_cols, expired.UdmEndDateTime, successor.UdmEffectiveDateTime
FROM Bronze expired
JOIN Bronze successor ON expired.pk = successor.pk
  AND expired.UdmEndDateTime < successor.UdmEffectiveDateTime
  -- (ordered by EffectiveDateTime, checking consecutive pairs)
```

- Log violations as ERROR with affected PK counts
- Consider auto-remediation for "zero active" cases (reactivate the most recent version)

---

### V-4 (P1): INSERT-first SCD2 exposes downstream consumers to duplicate active rows

**Files:** `engine.py`, documentation

**Problem:** The INSERT-first SCD2 approach is crash-safe by design — a crash between INSERT and UPDATE leaves duplicate active rows rather than zero active rows. However, between a crash event and the next successful pipeline run, any downstream query against Bronze that selects `WHERE UdmActiveFlag = 1` will return duplicate rows for affected PKs.

The dedup recovery mechanism (next pipeline run) is sound, but there is no documented guidance for downstream consumers to protect themselves during the intermediate inconsistency window.

**Impact:** Downstream reports, dashboards, or Silver/Gold transformations may produce incorrect aggregations or duplicate records if they run between a crash and recovery. Not data loss, but operational inconsistency.

**Fix:**
- Document in CLAUDE.md that all downstream queries against Bronze should use `ROW_NUMBER() PARTITION BY (pk_cols) ORDER BY UdmEffectiveDateTime DESC` patterns with a `WHERE rn = 1` filter, rather than relying solely on `WHERE UdmActiveFlag = 1`
- Create a helper view or documented query template for safe current-state access
- Add a post-run check that validates no duplicate active rows exist before signaling "pipeline complete" to downstream dependencies

---

## 3. BCP Loading

### V-5 (P0): BCP `-C 65001` codepage flag does not work on Linux

**Files:** `bcp_loader.py`

**Problem:** The pipeline currently passes `-C 65001` to BCP for UTF-8 encoding support. However, Microsoft's documentation and community reports confirm that the `-C 65001` codepage flag **does not work with BCP on Linux**. The flag is silently ignored or causes incorrect encoding interpretation, which can corrupt multi-byte UTF-8 characters (accented characters, CJK text, emoji) during bulk load.

The current `bcp_loader.py` line 70 has `-C 65001` in the BCP command.

**Impact:** Silent data corruption for any row containing multi-byte UTF-8 characters. Characters may be truncated, replaced with garbage bytes, or cause column shifts. The corruption is silent — BCP reports success.

**Fix:**
- **Option A (recommended for SQL Server 2019+):** Remove `-C 65001` and ensure target SQL Server columns use a UTF-8 collation (e.g., `Latin1_General_100_CI_AS_SC_UTF8`). BCP on Linux then handles UTF-8 natively without the codepage flag
- **Option B:** Switch to `-w` (Unicode/UTF-16) format instead of `-c` (character mode), which uses UTF-16LE encoding that BCP on Linux handles correctly. This requires corresponding changes to CSV generation in `bcp_csv.py`
- **Option C:** Keep `-c` mode but explicitly validate that no multi-byte UTF-8 characters are present in the data, logging a WARNING if they are
- Add a unit test that round-trips multi-byte characters (e.g., `café`, `日本語`, `€100`) through the BCP pipeline to verify encoding integrity

---

### V-6 (P2): BCP UTF-8 BOM prefix corrupts first field of first row

**Files:** `bcp_csv.py`, `bcp_loader.py`

**Problem:** If the CSV file written by Polars includes a UTF-8 BOM (Byte Order Mark: `0xEF, 0xBB, 0xBF`), BCP treats the BOM bytes as data rather than a file marker. The BOM is prepended to the first field of the first row, corrupting the value. For example, a PK column value of `12345` becomes `\xEF\xBB\xBF12345`, which fails PK matching in CDC and SCD2 operations.

Polars does not write BOMs by default, but this can vary by platform, version, or if intermediate file processing (editors, scripts) introduces one.

**Impact:** First row of first column silently corrupted. PK matching fails for that row, potentially causing phantom inserts and orphaned SCD2 records.

**Fix:**
- Add a BOM detection check in `write_bcp_csv()` — after writing the file, read the first 3 bytes and verify they are not `0xEF, 0xBB, 0xBF`
- If BOM detected, strip it before BCP load
- Defensive: add a comment documenting that Polars does not write BOMs by default but this guard protects against future regressions

---

## 4. Windowed Processing

### V-7 (P1): Cross-day transactions may split across processing windows

**Files:** `large_tables.py`, `engine.py`

**Problem:** The per-day windowed CDC approach processes one calendar day at a time. If a source system transaction spans midnight (starts before 00:00, commits after 00:00), the records from that transaction may split across two processing windows. Depending on the extraction timestamp column semantics:

- Records may appear partially in day N and partially in day N+1
- If the commit timestamp is used, all records land in day N+1 but may reference pre-midnight state
- If the insert/modification timestamp is used, records may reflect the start of the transaction rather than its committed state

This can create inconsistent CDC state where related rows (e.g., order header and line items) appear in different windows.

**Impact:** Inconsistent state within a processing window. Related rows may process in different batches, causing transient referential integrity violations. The weekly reconciliation catches this eventually, but daily consumers see inconsistent data.

**Fix:**
- Document the cross-day split risk in CLAUDE.md with guidance on which timestamp column to use for windowing (prefer transaction commit time over row modification time)
- Consider adding a configurable overlap window (e.g., re-process the last N minutes of the previous day) to catch split transactions
- For tables with known high cross-midnight transaction volume, the overlap approach or processing by transaction batch rather than calendar day is preferable
- The weekly reconciliation already provides a safety net — document this explicitly as the fallback

---

## 5. Performance & Memory Optimization

### V-8 (P2): Polars LazyFrame streaming not utilized for memory optimization

**Files:** `bcp_csv.py`, `engine.py`, `large_tables.py`

**Problem:** The pipeline processes DataFrames eagerly, requiring full materialization in memory. Polars supports lazy evaluation with `collect(streaming=True)` for out-of-core processing, which has demonstrated processing datasets 2× larger than available RAM. On the constrained 64GB system processing 3B+ row tables, eager evaluation forces conservative batch sizing and limits single-batch throughput.

**Impact:** No data corruption. Memory ceiling limits batch sizes and throughput. Larger batches could improve pipeline speed by reducing per-batch overhead.

**Fix:**
- Evaluate converting the heaviest operations to LazyFrame workflows:
  - `scan_csv()` → lazy transformations → `collect(streaming=True)` for reading BCP CSV files
  - Hash computation can be chained in lazy mode
  - CDC anti-join operations may benefit from lazy execution
- Benchmark eager vs. lazy/streaming for the pipeline's typical workload (500K–3M row batches)
- Intermediate results between pipeline stages can be written to Parquet for columnar access and compression
- Data type optimization (categoricals for low-cardinality strings, smallest sufficient integer types) can reduce memory 2–5×

---

### V-9 (P2): SCD2 Bronze tables lack partitioning and point-in-time indexes

**Files:** `table_creator.py`, `index_management.py`

**Problem:** At 3B+ rows with 3M daily additions, Bronze SCD2 tables grow rapidly. The current indexing includes the unique filtered index on `(pk_cols) WHERE UdmActiveFlag = 1` (SCD-1), but lacks:

1. **Point-in-time lookup index:** `(pk_cols, UdmEffectiveDateTime DESC)` — needed for historical queries like "what was the value of this record on date X"
2. **Partitioning by ActiveFlag or date:** Databricks recommends partitioning SCD2 tables by `is_current` so current-state queries never scan historical records

Without the point-in-time index, historical lookups require full scans of potentially billions of rows. Without partitioning, even current-state queries pay the cost of scanning past expired records.

**Impact:** No data corruption. Query performance degrades as tables grow. Historical lookups become impractical at scale.

**Fix:**
- Add a non-clustered index on `(pk_cols, UdmEffectiveDateTime DESC)` including commonly queried columns for point-in-time coverage
- Evaluate partitioned views or SQL Server table partitioning on `UdmActiveFlag` or `UdmEffectiveDateTime` year for tables exceeding a configurable row threshold
- Consider archiving expired SCD2 records (e.g., `UdmEndDateTime < DATEADD(YEAR, -2, GETDATE())`) to a separate archive table for very large tables
- Add index creation to `table_creator.py` alongside the existing unique filtered index

---

## 6. Operational Improvements

### V-10 (P2): Enhanced empty extraction guard could prevent false blocks

**Files:** `large_tables.py`, `small_tables.py`

**Problem:** The current >90% row count drop threshold for blocking extraction is a single-tier system. This can cause either false blocks (legitimate large purges at end-of-quarter) or missed warnings (gradual 40% daily declines that individually fall under the threshold but indicate a systematic problem).

**Impact:** No data corruption. Operational friction from false blocks or missed degradation signals.

**Fix:**
- Implement a two-tier system: WARN at 50% drop, BLOCK at 90% drop
- Add trend-based comparison against a rolling 7-day or 30-day average row count rather than just the previous run
- Add a control table or configuration option for expected large deletes (end-of-quarter purges, annual archival) that temporarily raises the threshold for specific tables
- Log the trend comparison results at INFO level for operational visibility

---

### V-11 (P2): polars-hash single-maintainer dependency risk

**Files:** `requirements.txt`, `bcp_csv.py`

**Problem:** polars-hash has ~14K weekly PyPI downloads, 84 GitHub stars, and appears to be maintained by a single developer. The pipeline has a hard dependency on this plugin for SHA-256 hashing. If maintenance lapses (maintainer becomes unavailable, breaking change in a Polars update, dependency abandoned), the pipeline's core CDC mechanism stops working.

The version is already pinned (H-2: `polars-hash==0.4.5`), which protects against breaking changes but not against the plugin becoming incompatible with future Polars versions.

**Impact:** No immediate risk. Future Polars upgrades could break compatibility with no maintainer to fix. Pipeline becomes blocked on an abandoned dependency.

**Fix:**
- Implement a fallback hashing path using Python's `hashlib` (standard library, zero dependency risk):
  - `add_row_hash_fallback()` that concatenates columns to a string and applies `hashlib.sha256()`
  - Performance will be slower (Python vs. Rust) but functional
- Add a CI/CD test that validates polars-hash produces the same output as the hashlib fallback for a reference dataset
- Monitor the polars-hash GitHub repository for maintenance activity
- Document the fallback procedure in CLAUDE.md

---

### V-12 (P3): Column rename detection appears as simultaneous drop + add

**Files:** `column_sync.py`, `evolution.py`

**Problem:** The current schema drift detection correctly identifies column additions, removals, and type changes. However, a column rename in the source system appears as a simultaneous drop of the old column and addition of a new column. This triggers the pipeline to:
- Drop the old column's data from the hash (all rows appear "changed")
- Add the new column with NULLs in historical Bronze records
- Generate a full CDC update wave for every row in the table

No major CDC tool handles column renames automatically. This is a known limitation across the industry.

**Impact:** One-time massive CDC update wave after a column rename. All historical Bronze rows show a false change. Not recurring, but operationally disruptive for large tables.

**Fix:**
- Add a heuristic detection: when a column is simultaneously dropped and added in the same sync cycle, check if the new column has similar characteristics (same data type, similar name via Levenshtein distance < 3)
- If a potential rename is detected, log a WARNING and prompt for manual confirmation before processing
- Document in CLAUDE.md that column renames should be coordinated with the pipeline team to avoid full-table CDC reprocessing
- Accept as low priority — this is a rare event with a one-time (not recurring) impact

---

### V-13 (P3): NULL PK records should be logged and quarantined

**Files:** `engine.py`

**Problem:** The pipeline correctly filters NULL PK records before CDC comparison (as validated by industry standard — AWS DMS, SQL Server native CDC, PostgreSQL WAL all require PKs). However, the filtered records are silently discarded. At scale, a growing count of NULL PK records could indicate a source system data quality problem that should be investigated.

**Impact:** No data corruption (correctly filtered). Potential missed data quality signals from source systems.

**Fix:**
- Log a WARNING with the count of filtered NULL PK records per table per run
- If count exceeds a configurable threshold (e.g., >1% of extracted rows), escalate to ERROR
- Optionally write filtered NULL PK records to a quarantine table for investigation
- Track NULL PK counts in PipelineEventLog for trend analysis

---

## Summary

| ID | Priority | Category | Issue | Status |
|----|----------|----------|-------|--------|
| V-1 | P0 | Hash Hygiene | Float/decimal phantom CDC changes | DONE — `add_row_hash()` rounds Float32/Float64 to `FLOAT_HASH_PRECISION=10` before cast to Utf8 |
| V-5 | P0 | BCP Loading | `-C 65001` codepage broken on Linux | DONE — removed from `bcp_loader.py`; BCP handles UTF-8 natively in `-c` mode on Linux |
| V-2 | P1 | Hash Hygiene | Unicode normalization phantom changes | DONE — `add_row_hash()` applies `unicodedata.normalize("NFC")` to Utf8/String columns |
| V-3 | P1 | SCD2 Integrity | No overlap/gap/zero-active validation | DONE — `validate_scd2_integrity()` in `reconciliation.py`; hooked into `reconcile_bronze()` |
| V-4 | P1 | SCD2 Integrity | Downstream duplicate active row exposure | DONE — `_check_duplicate_active_rows()` in `scd2/engine.py`; ROW_NUMBER pattern documented in CLAUDE.md |
| V-7 | P1 | Windowed CDC | Cross-day transaction splitting | DONE — `OVERLAP_MINUTES` env var in `config.py`; `_extract_day()` shifts window; documented in CLAUDE.md |
| V-6 | P2 | BCP Loading | UTF-8 BOM corrupts first field | DONE — `_strip_bom_if_present()` in `bcp_csv.py`; checks/strips BOM after every CSV write |
| V-8 | P2 | Performance | Polars LazyFrame streaming not utilized | DEFERRED — requires benchmarking on production workloads; per-day processing already bounds memory |
| V-9 | P2 | Performance | Missing point-in-time indexes | DONE — `ensure_bronze_point_in_time_index()` in `table_creator.py`; creates `(pk_cols, UdmEffectiveDateTime DESC)` |
| V-10 | P2 | Operational | Single-tier empty extraction guard | DONE — WARN at 50% drop added to both `small_tables.py` and `large_tables.py`; existing 90% BLOCK unchanged |
| V-11 | P2 | Operational | polars-hash single-maintainer risk | DONE — `add_row_hash_fallback()` in `bcp_csv.py` using hashlib.sha256; fallback procedure documented in CLAUDE.md |
| V-12 | P3 | Operational | Column rename detection heuristic | DONE — `_detect_potential_renames()` in `evolution.py` with Levenshtein distance <= 3 + same type heuristic |
| V-13 | P3 | Operational | NULL PK logging and quarantine | DONE — percentage-based escalation (>1% = ERROR) in `_filter_null_pks()`; quarantine table deferred as future enhancement |

### Top priorities:

1. **V-1 (P0):** Float normalization — phantom CDC changes silently inflating Bronze on every run
2. **V-5 (P0):** BCP codepage — silent multi-byte UTF-8 corruption on Linux
3. **V-3 (P1):** SCD2 integrity — overlapping intervals and zero-active records undetected
4. **V-2 (P1):** Unicode normalization — phantom changes from encoding form differences
5. **V-7 (P1):** Cross-day splits — transactions spanning midnight create inconsistent windows
6. **V-4 (P1):** Downstream safety — document ROW_NUMBER pattern for crash recovery window

### Already addressed by prior audits (excluded from this TODO):

- Hash bit-width / polars-hash SHA-256 (H-1, H-2, H-3) — `\x1f` separator, `\x00NULL\x00` sentinel, version pinned
- NULL PK filtering before comparison (implemented in engine.py)
- INSERT-first SCD2 crash safety with unique filtered index (SCD-1)
- BIGINT IDENTITY overflow protection (SCD-2)
- ConnectorX Oracle 9999-12-31 sentinel date (E-4)
- BCP `quote_style='never'` data corruption (B-1) — sanitize_strings() implemented
- Memory/glibc arena fragmentation (M-1, M-2, M-3)
- sp_getapplock concurrent run protection (N-1)
- BCP row terminator — correctly using `0x0A` (LF) for Linux

*Source: "CDC/SCD2 Pipeline Validation Against Production Best Practices" — seven-area research validation*
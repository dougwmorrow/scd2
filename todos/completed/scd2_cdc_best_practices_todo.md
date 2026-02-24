# CDC/SCD2 Pipeline — Action Items (2024–2025 Best Practices)

*Context: Derived from "Validating a Production CDC/SCD2 Pipeline Against 2024–2025 Best Practices" — a research-backed revalidation covering hash-based CDC safety, SCD2 integrity, BCP Linux fixes, ConnectorX contingency, Polars maturation, medallion resilience, and ecosystem developments. Items already completed from the prior TODO (V-1 through V-13) and prior edge case audits (P0-1 through P3-4) are excluded. References to prior completed work are noted where new findings extend them.*

---

## 1. Critical Infrastructure

### W-1 (P0): Upgrade BCP to mssql-tools18 v18.6.1.1 for native UTF-8 codepage support

**Files:** `config.py`, `bcp_loader.py`, system packages

**Problem:** Microsoft released mssql-tools18 v18.6.1.1 in December 2025, which adds `-C 65001` codepage support on Linux/macOS for the first time. Prior to this version, the flag was silently ignored or errored. The prior TODO (V-5) removed `-C 65001` as a workaround, relying on BCP's native `-c` mode. With the fix now available, upgrading and re-enabling `-C 65001` provides explicit, reliable UTF-8 encoding control rather than relying on implicit behavior.

This is the single most impactful infrastructure change identified by the research.

**Impact:** Explicit UTF-8 encoding control eliminates a class of silent multi-byte character corruption risks that the current implicit approach cannot fully guarantee against.

**Fix:**
- Upgrade the mssql-tools18 package on the Red Hat server to v18.6.1.1+
- Re-add `-C 65001` to the BCP command in `bcp_loader.py`
- Validate with a round-trip test: write multi-byte characters (`café`, `日本語`, `€100`, emoji) through BCP and verify correct storage/retrieval
- Update `BCP_PATH` in `config.py` if the binary path changes
- Update CLAUDE.md to document the version requirement

---

### W-2 (P0): NULL sentinel `\x00NULL\x00` risks C-string truncation — migrate to `\x1FNULL\x1F`

**Files:** `bcp_csv.py` (`add_row_hash`, `add_row_hash_fallback`)

**Problem:** The current NULL sentinel `\x00NULL\x00` uses null bytes (`\x00`), which some processing layers interpret as C-style string terminators. If any intermediate step (logging, debugging tools, FFI boundary, serialization layer) truncates at `\x00`, the hash input is silently shortened — producing incorrect hashes without any error. The research recommends `\x1FNULL\x1F` (Unit Separator wrapping) as a safer alternative that is equally unlikely in real data but avoids null-byte truncation.

The pipeline already uses `\x1F` (Unit Separator) as the column separator (P0-6), so this change maintains consistency in the sentinel character strategy.

**Impact:** Silent hash corruption if any layer truncates at `\x00`. The risk is low in the current Python/Polars/Rust stack but becomes significant during debugging (log inspection), future tool changes, or if hash inputs are ever serialized through C-based libraries.

**Fix:**
- In `add_row_hash()` and `add_row_hash_fallback()`, replace `.fill_null("\x00NULL\x00")` with `.fill_null("\x1fNULL\x1f")`
- **CRITICAL:** This changes hash output for every row containing NULLs. Requires a coordinated migration:
  1. Stage tables: truncate and rebuild (Stage is ephemeral, no history concern)
  2. Bronze `_row_hash` column: the next CDC run will see every NULL-containing row as "changed" and generate a one-time SCD2 update wave
  3. Schedule the change during a maintenance window and communicate the expected one-time spike in CDC updates
- Add a comment in `bcp_csv.py` documenting why `\x1F` is used over `\x00`
- Update CLAUDE.md to document the sentinel convention

---

### W-3 (P0): Float normalization missing ±0.0 and NaN/Infinity edge cases

**Files:** `bcp_csv.py` (`add_row_hash`, `add_row_hash_fallback`)

**Problem:** The prior V-1 fix rounds Float32/Float64 columns to `FLOAT_HASH_PRECISION=10` decimal places before hashing. This addresses cross-system representation differences but misses two additional IEEE 754 edge cases identified in the new research:

1. **±0.0 normalization:** IEEE 754 defines `+0.0` and `-0.0` as equal (`+0.0 == -0.0` is true), but they have different bit patterns. When cast to string, they may produce `"0.0"` vs `"-0.0"`, yielding different hashes for logically equal values.
2. **NaN and Infinity sentinels:** `NaN != NaN` in IEEE 754, and different NaN representations (signaling vs quiet) exist. `Infinity` and `-Infinity` may render differently across systems. Without consistent sentinels, these values produce unpredictable hash inputs.

**Impact:** Phantom CDC updates for rows containing ±0.0, NaN, or Infinity values. These are rare in typical business data but can occur in scientific, financial, or computed columns.

**Fix:**
- After the `.round(FLOAT_HASH_PRECISION)` step, add normalization expressions:
  ```python
  # Normalize -0.0 to +0.0
  pl.when(pl.col(c) == 0.0).then(pl.lit(0.0)).otherwise(pl.col(c))
  # Map NaN to sentinel before cast to Utf8
  pl.when(pl.col(c).is_nan()).then(pl.lit("\x1fNaN\x1f"))
  # Map Infinity to sentinel
  pl.when(pl.col(c).is_infinite()).then(
      pl.when(pl.col(c) > 0).then(pl.lit("\x1fINF\x1f")).otherwise(pl.lit("\x1f-INF\x1f"))
  )
  ```
- Apply the same normalization in `add_row_hash_fallback()`
- Log a DEBUG message listing any columns where these edge cases were encountered

---

## 2. Production Reliability

### W-4 (P1): Enforce MALLOC_ARENA_MAX=2 in pipeline startup

**Files:** `config.py`, `main_small_tables.py`, `main_large_tables.py`, deployment scripts

**Problem:** A Polars GitHub issue (#23128, June 2025) documented 10× memory bloat — 300GB of actual data consuming 2.7TB RSS — caused by glibc malloc creating up to `8 × num_cores` memory arenas for multithreaded applications. Polars' Rust operations create/destroy allocations across arenas that are never returned to the OS. On the pipeline's 6-CPU, 64GB system, this means up to 48 arenas competing for memory.

The prior audit (M-1) identified this risk, but the research confirms it as a documented, serious issue with a specific environment variable mitigation. The variable must be set *before* the Python process starts — it cannot be set from within Python.

**Impact:** Memory bloat leading to OOM kills, swap thrashing, or degraded performance. Particularly dangerous during multi-table parallel processing.

**Fix:**
- Set `MALLOC_ARENA_MAX=2` in the pipeline's systemd unit file, shell wrapper script, or `.bashrc` for the pipeline user
- Add a startup check in `main_small_tables.py` and `main_large_tables.py`:
  ```python
  arena_max = os.environ.get("MALLOC_ARENA_MAX")
  if arena_max is None or int(arena_max) > 4:
      logger.warning("MALLOC_ARENA_MAX not set or >4 (current: %s). "
                      "Recommended: MALLOC_ARENA_MAX=2 to prevent glibc arena fragmentation.", arena_max)
  ```
- Document in CLAUDE.md as a required deployment configuration
- Consider adding `df.shrink_to_fit()` calls after large operations (see W-12)

---

### W-5 (P1): Adopt Polars streaming engine for memory-constrained operations

**Files:** `bcp_csv.py`, `engine.py`, `large_tables.py`

**Problem:** The prior V-8 item was DEFERRED pending benchmarks. Since then, Polars has shipped a **completely redesigned streaming engine** (v1.31.1+, January 2025) based on morsel-driven parallelism. It delivers 3–7× faster performance than the in-memory engine on benchmarks and supports anti-joins, group-bys, and all standard operations natively in streaming mode. Polars has demonstrated processing 300M rows on systems with as little as 512MB RAM using streaming mode.

On a 64GB system processing billion-row datasets, this is transformative. The old `collect(streaming=True)` API has been superseded by `collect(engine="streaming")`.

**Impact:** No data corruption. Enables processing significantly larger batches within the 64GB memory constraint. Reduces OOM risk and may improve throughput through better cache utilization.

**Fix:**
- Identify the three highest-memory operations in the pipeline (likely: large table CDC anti-joins, SCD2 comparison, full reconciliation)
- Convert these to LazyFrame workflows: `scan_csv()` → lazy transforms → `collect(engine="streaming")`
- Benchmark against current eager execution on production-scale data
- Note: the old `collect(streaming=True)` API is deprecated in favor of `collect(engine="streaming")`
- Start with large table per-day processing (`large_tables.py`) where memory pressure is highest
- Test anti-join behavior in streaming mode to verify correct NULL handling (`join_nulls=False` default)

---

### W-6 (P1): ConnectorX Oracle contingency — evaluate pydbzengine and oracledb+PyArrow

**Files:** `connectorx_oracle_extractor.py`, `oracle_extractor.py`, `requirements.txt`

**Problem:** ConnectorX Oracle support remains unreliable. The critical Issue #644 (June 2024) panics on `9999-12-31` dates — a sentinel value the pipeline uses. The dlt documentation explicitly states ConnectorX is "not recommended for Oracle." While the pipeline has a workaround (TRUNC in SQL, oracledb fallback), the new research identifies two better alternatives:

1. **pydbzengine** (released February 2025): Python interface to Debezium embedded engine via JPype. Supports Oracle CDC through log-based capture — structurally superior to hash-based comparison for change detection. Integrates with dlt and Apache Iceberg.
2. **oracledb with PyArrow backend**: Python's native Oracle driver with columnar output path, avoiding the ConnectorX FFI boundary entirely.

Additionally, ConnectorX has a **known memory leak** in loop-based extraction patterns. Memory is not released even after `del df` and `gc.collect()` due to the Rust-Python FFI boundary.

**Impact:** ConnectorX Oracle path is a production reliability risk. Memory leak compounds during multi-table pipeline runs.

**Fix:**
- **Phase 1 (immediate):** For multi-table Oracle extraction, evaluate process-level isolation — run each table's ConnectorX extraction in a subprocess to prevent memory accumulation
- **Phase 2 (evaluate):** Prototype `oracledb` with PyArrow backend as a drop-in replacement for ConnectorX Oracle extraction. Benchmark against the current oracledb fallback path
- **Phase 3 (evaluate):** Prototype `pydbzengine` for the most critical Oracle tables. This would shift those tables from hash-based to log-based CDC — a significant architectural improvement
- Document the evaluation results and chosen path in CLAUDE.md

---

### W-7 (P1): Add schema validation assertions before Polars concat operations

**Files:** `engine.py`, `large_tables.py`, anywhere `pl.concat()` is used

**Problem:** Polars' `vertical_relaxed` and `diagonal_relaxed` concat modes silently coerce columns to common supertypes — for example, Int64 → Float64, losing precision for large integers (>2^53). The research confirms this is a documented risk with implicit casting. If source schema drift causes one DataFrame to have a column as Int64 and another as Float64, concat silently promotes to Float64, potentially corrupting large integer values (like IDs or monetary amounts in cents).

Issue #18911 remains open: lazy diagonal concat with list-type columns in non-first LazyFrames throws `SchemaError`.

**Impact:** Silent precision loss for large integers. Can corrupt primary keys, monetary values, or other precision-sensitive data during multi-batch or multi-source concatenation.

**Fix:**
- Add `assert df.schema == expected_schema` before every `pl.concat()` call in the pipeline
- Create a helper function:
  ```python
  def validate_schema_before_concat(dfs: list[pl.DataFrame], context: str) -> None:
      """Validate all DataFrames have identical schemas before concatenation."""
      if not dfs:
          return
      reference = dfs[0].schema
      for i, df in enumerate(dfs[1:], 1):
          if df.schema != reference:
              mismatches = {col: (reference.get(col), df.schema.get(col))
                           for col in set(reference) | set(df.schema)
                           if reference.get(col) != df.schema.get(col)}
              raise SchemaValidationError(f"Schema mismatch in {context} at index {i}: {mismatches}")
  ```
- Prefer explicit schema alignment (cast and align manually) before concat rather than relying on `diagonal_relaxed`
- Avoid list-type columns in lazy diagonal concat (issue #18911)

---

### W-8 (P1): Verify sp_getapplock RCSI race condition is not present

**Files:** `table_lock.py`

**Problem:** Under Read Committed Snapshot Isolation (RCSI), calling `sp_releaseapplock` explicitly before COMMIT creates a race condition where another process can acquire the lock and read pre-transaction state. The research recommends letting the lock release happen implicitly at COMMIT time when using transaction-scoped locks.

**Impact:** If the pipeline uses RCSI and explicitly releases app locks before commit, a concurrent run could read intermediate/inconsistent data during the window between lock release and commit.

**Fix:**
- Review `table_lock.py` to verify:
  1. Locks use `@LockOwner = 'Transaction'` scope (not `'Session'`)
  2. `sp_releaseapplock` is NOT called explicitly before COMMIT — the lock should release automatically when the transaction commits/rolls back
  3. `@LockTimeout = 0` is used for non-blocking "skip if already running" semantics
- If Session-owned locks are used (as noted in CLAUDE.md), this specific RCSI risk doesn't apply but should be documented as a known consideration for any future migration to transaction-scoped locks
- Check the database's isolation level and document in CLAUDE.md

---

## 3. Performance & Optimization

### W-9 (P2): Evaluate polars-hash upgrade from 0.4.5 to 0.5.6

**Files:** `requirements.txt`, `bcp_csv.py`

**Problem:** polars-hash has had 20+ releases since the pipeline pinned v0.4.5. Version 0.5.6 was released January 2026. While the pin was correct to prevent silent hash changes (H-2), staying on an old version means missing bug fixes, performance improvements, and compatibility updates. The plugin remains healthy — active maintenance with regular releases.

**Impact:** No immediate risk. Future Polars upgrades may become incompatible with polars-hash 0.4.5, blocking Polars version updates.

**Fix:**
- Create a test harness that computes hashes for a reference dataset (covering all data types: strings, integers, floats, NULLs, dates) using both the current polars-hash 0.4.5 and the candidate 0.5.6
- **If hashes match:** Upgrade the pin safely
- **If hashes differ:** This confirms the H-2 risk. Upgrading would require a one-time full rehash of all Stage tables. Schedule during a maintenance window if the upgrade provides meaningful benefits
- Document the test results and decision in CLAUDE.md

---

### W-10 (P2): SQL Server 2022 ordered clustered columnstore indexes for billion-row SCD2

**Files:** `table_creator.py`, `index_management.py`

**Problem:** SQL Server 2022 introduced ordered clustered columnstore indexes (`CREATE CLUSTERED COLUMNSTORE INDEX ... ORDER (BusinessKey, EffectiveDateTime)`). For billion-row SCD2 tables, this enables segment elimination for point-in-time queries without sacrificing columnstore compression benefits. The prior V-9 added B-tree point-in-time indexes, but columnstore provides both analytical scan performance and compression at this scale.

**Impact:** No data corruption. Significant query performance improvement for both analytical scans and point-in-time lookups on the largest Bronze tables.

**Fix:**
- Verify the SQL Server instances are running SQL Server 2022
- For Bronze tables exceeding a configurable row threshold (e.g., 100M+ rows), evaluate a hybrid indexing strategy:
  - Clustered columnstore with ORDER on `(BusinessKey, EffectiveDateTime)` for analytical workloads
  - Retain the existing filtered nonclustered B-tree index `WHERE UdmActiveFlag=1` for active-row point lookups
- Benchmark query performance for the pipeline's most common access patterns before and after
- Consider as a table-by-table migration for the largest Bronze tables only

---

### W-11 (P2): Split reconciliation cadence — daily count vs weekly full-column

**Files:** `reconciliation.py`, `config.py`

**Problem:** The current weekly reconciliation performs a full column-by-column comparison, which is thorough but expensive. The research recommends separating **count reconciliation** (fast, cheap, run daily) from **full-column hash reconciliation** (thorough, expensive, run weekly). Count reconciliation catches gross data loss, extraction failures, or bulk delete scenarios within 24 hours rather than waiting up to 7 days.

Additionally, tracking reconciliation divergence trends in a metadata table enables detection of systemic pipeline problems — increasing divergence over time indicates a growing issue.

**Impact:** Faster detection of gross data quality issues. No change to data integrity guarantees.

**Fix:**
- Extract the row count comparison from `reconciliation.py` into a lightweight `reconcile_counts()` function
- Schedule `reconcile_counts()` daily (can run as part of normal pipeline completion)
- Keep full column-by-column reconciliation weekly for high-value tables, monthly for stable dimensions
- Add a `ReconciliationHistory` table or extend `PipelineEventLog` to track reconciliation divergence trends per table
- Alert if divergence trend is increasing over a configurable window (e.g., 3 consecutive runs with growing count mismatch)

---

### W-12 (P2): Add `shrink_to_fit()` calls after large DataFrame operations

**Files:** `engine.py`, `large_tables.py`, `bcp_csv.py`

**Problem:** Polars DataFrames may retain over-allocated memory buffers after filter, join, or select operations. Calling `df.shrink_to_fit()` releases this excess memory back to the allocator. On a memory-constrained 64GB system processing multi-million row DataFrames, this is meaningful — particularly when combined with W-4 (MALLOC_ARENA_MAX=2) which reduces the number of arenas holding onto freed memory.

**Impact:** Reduced memory pressure. No functional change.

**Fix:**
- Add `df = df.shrink_to_fit()` or `df.shrink_to_fit(in_place=True)` after:
  - CDC anti-join operations in `engine.py` (after the result is computed but before further processing)
  - Large table per-day extraction in `large_tables.py` (after filtering to the day's window)
  - After hash computation in `bcp_csv.py` (the intermediate concat column can be large)
- Only apply to operations processing >100K rows to avoid overhead on small DataFrames
- Combine with data type downcasting where possible (Int32 instead of Int64, Categorical for low-cardinality strings)

---

### W-13 (P2): BCP format files for version-controlled column mappings

**Files:** `bcp_loader.py`, `table_creator.py`

**Problem:** The pipeline currently relies on positional column mapping (P0-1: `reorder_columns_for_bcp()` enforces correct order by reading INFORMATION_SCHEMA before every write). BCP format files (XML `.fmt`) provide explicit, version-controlled column mappings that are independent of INFORMATION_SCHEMA queries. This eliminates the per-write metadata query and provides an auditable artifact of the expected column layout.

**Impact:** No data corruption risk (P0-1 already prevents column mismatch). Operational improvement: eliminates per-write INFORMATION_SCHEMA queries and provides auditable column mapping artifacts.

**Fix:**
- Generate `.fmt` files as part of table creation in `table_creator.py`
- Store `.fmt` files alongside CSV output in the CSV_OUTPUT_DIR
- Modify `bcp_loader.py` to use `-f format_file.fmt` instead of relying solely on positional ordering
- Regenerate `.fmt` files on schema evolution (column add/remove/type change)
- Keep `reorder_columns_for_bcp()` as a validation step even with format files — defense in depth

---

## 4. Operational Improvements

### W-14 (P3): Evaluate pydbzengine for log-based Oracle CDC

**Files:** New module or integration path

**Problem:** pydbzengine (released February 2025) is the most significant new development for Python-based CDC. It provides a Python interface to the Debezium embedded engine via JPype, supporting all Debezium connectors including Oracle. For tables where the pipeline currently relies on hash-based CDC with ConnectorX Oracle extraction (the weakest link), pydbzengine could provide reliable log-based CDC — capturing every change including intermediate states between pipeline runs, and detecting hard deletes natively.

**Impact:** No immediate risk from deferral. Represents a potential architectural improvement for Oracle source reliability and CDC fidelity.

**Fix:**
- Prototype pydbzengine with a single low-volume Oracle table
- Evaluate: Oracle LogMiner connector reliability, JPype overhead, memory footprint, error handling
- Compare CDC accuracy against the existing hash-based approach (does it catch changes the hash-based path misses?)
- If viable, create an alternative extraction path that can coexist with the current approach (per-table configuration in UdmTablesList)
- Document evaluation findings

---

### W-15 (P3): Monitor ADBC/Columnar ecosystem for Oracle driver availability

**Files:** Documentation only (for now)

**Problem:** Columnar (launched October 2025, backed by Databricks, dbt Labs, Microsoft, Snowflake) released ADBC drivers for SQL Server and 9 other databases, delivering near-COPY performance via end-to-end Arrow RecordBatch passing. However, no ADBC driver exists for Oracle yet. DuckDB's ODBC scanner extension provides an alternative Oracle path through ODBC.

When an Oracle ADBC driver becomes available, it would be the ideal extraction path — columnar, zero-copy, and standardized.

**Impact:** No action needed now. Future opportunity for performance improvement.

**Fix:**
- Track the Columnar and ADBC ecosystem for Oracle driver announcements
- When available, evaluate as a replacement for both ConnectorX and oracledb Oracle extraction paths
- In the meantime, DuckDB's ODBC scanner is a potential fallback if ConnectorX Oracle becomes untenable

---

### W-16 (P3): Evaluate SQL Server 2022 temporal tables for stable dimension tables

**Files:** Documentation / evaluation only

**Problem:** SQL Server 2022 temporal tables provide automatic SCD2-like versioning with native `FOR SYSTEM_TIME AS OF` query syntax. For dimension tables where column-level change tracking granularity isn't needed, temporal tables would eliminate custom SCD2 code entirely — the database engine handles versioning automatically.

Limitations: any column update creates a new version (no column-level selectivity), and the history table must be in the same database.

**Impact:** Code simplification for applicable tables. No change to tables requiring fine-grained change tracking.

**Fix:**
- Identify candidate dimension tables in UdmTablesList that are: low-to-moderate change volume, don't require column-level change tracking, and are in SQL Server 2022 databases
- Prototype conversion of one such table to temporal and compare behavior with the current SCD2 implementation
- Document findings and decision criteria for which tables are suitable

---

### W-17 (P3): Schema contract quarantine table for failed records

**Files:** `evolution.py`, `engine.py`

**Problem:** The research recommends routing records that fail schema contracts to a quarantine table with rejection reasons, following the pattern hardened by the Debezium and Confluent ecosystems through schema registries. Currently, schema evolution errors skip the entire table. A quarantine approach would allow the pipeline to continue processing valid records while isolating problematic ones for investigation.

**Impact:** Operational improvement. Enables partial processing rather than all-or-nothing table skipping on schema issues.

**Fix:**
- Create a `UDM_Stage.ops.Quarantine` table with columns: TableName, RecordData (NVARCHAR(MAX) JSON), RejectionReason, QuarantinedAt, BatchId
- On type change errors in `evolution.py`, quarantine the affected records instead of skipping the entire table
- On data quality failures (NULL PKs beyond threshold, data type cast failures), route to quarantine
- Add a monitoring query for quarantine table growth in the operational dashboard
- Low priority — current all-or-nothing approach is safe, just less granular

---

### W-18 (P3): Evaluate XXH3-128 or BLAKE3 as faster hash alternatives

**Files:** `bcp_csv.py` (evaluation only)

**Problem:** For performance-sensitive pipelines, the research identifies two hash alternatives that offer significant speed improvements over SHA-256 while maintaining adequate collision resistance:
- **XXH3-128**: ~10× faster than SHA-256 with 128-bit output (collision probability ~10⁻²¹ at 3B rows)
- **BLAKE3**: ~5× faster than SHA-256 with cryptographic strength

The current SHA-256 → 64-bit truncation is validated as safe for row-level CDC (per-row false-negative probability of 1 in 2^64), but if hashing ever becomes a bottleneck, these alternatives provide headroom.

**Impact:** No immediate need. Performance optimization opportunity if hash computation becomes a throughput bottleneck.

**Fix:**
- Benchmark SHA-256 hash computation time as a percentage of total pipeline duration for the largest tables
- If hashing accounts for >10% of pipeline runtime, prototype XXH3-128 via a Polars plugin or Rust extension
- If moving to 128-bit, use two BIGINT columns or a BINARY(16) column in Stage/Bronze tables
- This is purely a performance evaluation — current SHA-256 truncation is functionally correct

---

## Summary

| ID | Priority | Category | Issue | Status |
|----|----------|----------|-------|--------|
| W-1 | P0 | Infrastructure | Upgrade BCP to mssql-tools18 v18.6.1.1 for UTF-8 codepage | DEFERRED — requires server package upgrade; internal comment added to `bcp_loader.py` with upgrade steps |
| W-2 | P0 | Hash Hygiene | NULL sentinel \x00 → \x1F migration | DONE — `_NULL_SENTINEL = "\x1fNULL\x1f"` in `bcp_csv.py`; both `add_row_hash()` and `add_row_hash_fallback()` updated |
| W-3 | P0 | Hash Hygiene | Float ±0.0 and NaN/Infinity normalization | DONE — ±0.0 normalized to +0.0, NaN/Infinity mapped to `\x1F`-wrapped sentinels in both hash functions |
| W-4 | P1 | Memory | Enforce MALLOC_ARENA_MAX=2 in startup | DONE — startup warning in both `main_small_tables.py` and `main_large_tables.py` if not set externally; documented in CLAUDE.md |
| W-5 | P1 | Performance | Adopt Polars streaming engine (`engine="streaming"`) | DEFERRED — requires Polars upgrade; internal comment added to `cdc/engine.py` with evaluation steps |
| W-6 | P1 | Extraction | ConnectorX Oracle contingency (pydbzengine, oracledb+PyArrow) | DEFERRED — requires pydbzengine evaluation; internal comment added to `connectorx_oracle_extractor.py` |
| W-7 | P1 | Data Integrity | Schema validation assertions before Polars concat | DONE — `validate_schema_before_concat()` in `bcp_csv.py`; wired into all `pl.concat()` sites in CDC, SCD2, and extractors |
| W-8 | P1 | Concurrency | Verify sp_getapplock RCSI race condition not present | DONE — Session-owned locks are immune; analysis documented in `table_lock.py` and CLAUDE.md |
| W-9 | P2 | Dependencies | Evaluate polars-hash upgrade 0.4.5 → 0.5.6 | DEFERRED — internal comment added to `bcp_csv.py` with evaluation steps |
| W-10 | P2 | Performance | SQL Server 2022 ordered clustered columnstore indexes | DONE — `ensure_bronze_columnstore_index()` in `table_creator.py`; opt-in for 100M+ row tables, requires SQL Server 2022 |
| W-11 | P2 | Operational | Split reconciliation: daily count vs weekly full-column | DONE — `reconcile_counts()` in `reconciliation.py`; lightweight COUNT(*) comparison for daily scheduling |
| W-12 | P2 | Memory | Add shrink_to_fit() after large DataFrame operations | DONE — `shrink_to_fit(in_place=True)` in `cdc/engine.py`, `large_tables.py`, `bcp_csv.py` (>100K rows) |
| W-13 | P2 | Operational | BCP format files for version-controlled column mappings | DONE — `generate_bcp_format_file()` in `bcp_csv.py`; `bcp_load()` accepts optional `format_file` param |
| W-14 | P3 | Extraction | Evaluate pydbzengine for log-based Oracle CDC | DEFERRED — internal comment added to `connectorx_oracle_extractor.py` with evaluation checklist |
| W-15 | P3 | Ecosystem | Monitor ADBC/Columnar for Oracle driver | DEFERRED — internal comment added to `connectorx_oracle_extractor.py` |
| W-16 | P3 | Architecture | Evaluate SQL Server 2022 temporal tables for dimensions | DEFERRED — internal comment added to `scd2/engine.py` with evaluation criteria |
| W-17 | P3 | Operational | Schema contract quarantine table | DONE — `ensure_quarantine_table()`, `quarantine_record()`, `quarantine_batch()` in `evolution.py`; hooked into type change errors |
| W-18 | P3 | Performance | Evaluate XXH3-128 or BLAKE3 hash alternatives | DEFERRED — internal comment added to `bcp_csv.py` with benchmarking criteria |

### Top priorities:

1. **W-1 (P0):** BCP upgrade — enables proper UTF-8 codepage control on Linux for the first time
2. **W-2 (P0):** NULL sentinel migration — eliminates C-string truncation risk in hash inputs
3. **W-3 (P0):** Float edge cases — ±0.0 and NaN/Infinity produce phantom CDC changes
4. **W-4 (P1):** MALLOC_ARENA_MAX — prevents documented 10× memory bloat from glibc arenas
5. **W-5 (P1):** Polars streaming engine — transformative for 64GB system processing billion-row datasets
6. **W-6 (P1):** ConnectorX Oracle contingency — address the weakest extraction path

### Already addressed by prior TODOs and audits (excluded):

- V-1: Float rounding to 10 decimal places (DONE — W-3 extends with ±0.0/NaN/Infinity)
- V-2: Unicode NFC normalization (DONE)
- V-3: SCD2 integrity validation — overlaps, zero-active, gaps (DONE)
- V-4: Downstream duplicate active row protection (DONE)
- V-5: BCP -C 65001 removal workaround (DONE — W-1 replaces with proper fix)
- V-6: BCP UTF-8 BOM stripping (DONE)
- V-7: Cross-day overlap window (DONE)
- V-8: Polars LazyFrame streaming (DEFERRED — W-5 supersedes with new streaming engine)
- V-9: Point-in-time indexes (DONE — W-10 extends with columnstore)
- V-10: Two-tier empty extraction guard (DONE)
- V-11: polars-hash hashlib fallback (DONE)
- V-12: Column rename detection heuristic (DONE)
- V-13: NULL PK logging and quarantine (DONE)
- H-1/H-2/H-3: Hash bit-width, \x1f separator, polars-hash pinning (DONE)
- P0-1 through P3-4: All 14 edge cases from prior audit (DONE)
- M-1/M-2/M-3: Memory/glibc arena documentation (DONE — W-4 enforces)
- MERGE avoidance — confirmed still correct (new February 2025 bug validates the decision)
- INSERT-first SCD2 — confirmed as correct pattern
- BIGINT IDENTITY — confirmed adequate for billion-row scale
- Weekly reconciliation cadence — confirmed appropriate (W-11 refines)

*Source: "Validating a Production CDC/SCD2 Pipeline Against 2024–2025 Best Practices" — section-by-section revalidation*
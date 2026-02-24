# Polars — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## Memory Management on Constrained Systems

The pipeline runs on Red Hat Linux with 6 CPU and 64 GB RAM — memory management is critical.

### glibc Arena Fragmentation (M-1 / W-4)

The single most impactful reliability risk for the pipeline. Polars uses Rust's Rayon thread pool internally, and Rust allocations go through glibc's malloc. On a 6-core system, glibc creates up to 48 memory arenas (8 × cores). When Polars threads allocate and free large DataFrames across iterations, small long-lived allocations pin entire arenas, preventing memory return to the OS.

Real-world evidence: Polars GitHub issue #23128 (June 2025) — 10 GB actual data consuming 2,766 GB RSS (9:1 waste ratio). Issue #21732 (December 2024) — OOM from fragmentation in Polars ≥1.7.0. Critically, `gc.collect()` or `malloc_trim(0)` from Python has **no effect** on Polars/Rust allocations because they use a different heap.

**Resolution:** `MALLOC_ARENA_MAX=2` set before any imports in both main scripts. Reduces arena count from 48 to 2, with <1% CPU impact (validated by Presto team on issue #8993). Combined with `POLARS_MAX_THREADS=1` per worker to prevent thread oversubscription.

### Worker Count and Memory Budget (M-2)

With OS overhead at 2–4 GB, effective per-worker budget is ~10 GB. A 3M × 200 column DataFrame of mixed types occupies 4–8 GB. Anti-join operations spike to 2–4× DataFrame size.

**Resolution:** Default workers=4 (not 6); warning when >4 on 64 GB systems. Per-worker memory budget: 15–20 GB.

### shrink_to_fit (W-12)

`shrink_to_fit(in_place=True)` releases over-allocated memory buffers back to the allocator after large DataFrame operations (>100K rows). Called in CDC engine, large table extraction, and hash computation. Combines with MALLOC_ARENA_MAX=2 for best results.

### RSS Monitoring (B-8)

`_check_rss_memory()` monitors RSS between table iterations in sequential mode. WARNING at 85% of `config.MAX_RSS_GB` (default 48), ERROR at limit. psutil is optional — silently skipped if not installed.

### Small Table Memory Guard (ST-1)

`_check_small_table_memory()` estimates CDC peak memory (3× DataFrame size for anti-join) and blocks if it would exceed 20 GB ceiling. Tables exceeding 8–10M rows should be reconfigured as large tables.

---

## Anti-Join NULL Behavior

Since Polars ≥0.20 (December 2023), `join_nulls=False` is the default — NULL values never produce matches in joins (SQL-compliant). For CDC anti-joins, rows with NULL join keys always appear in output. The pipeline's `_filter_null_pks()` handles this explicitly.

**Polars issue #19624:** Join cardinality validation (`validate="1:m"`) incorrectly fails when NULL values exist in join keys even with `join_nulls=False`. The pipeline does not use join validation, and `_filter_null_pks()` prevents exposure.

---

## DataFrame Concatenation Safety

### diagonal_relaxed Crash Bug (T-2)

Polars issue #12543: Python crashes (full interpreter termination) when DataFrames have large column count differences. Issue #18911: List column dtype errors in lazy mode.

**Resolution:** `_safe_concat()` pre-aligns schemas before vertical concat, replacing all `diagonal_relaxed` uses. Missing columns are added as explicit null columns before concatenation.

### Implicit Casting in Relaxed Concat (W-7)

`vertical_relaxed` and `diagonal_relaxed` silently coerce columns to common supertypes (e.g., Int64 → Float64, losing precision for large integers).

**Resolution:** `validate_schema_before_concat()` called before every `pl.concat()`. In extractors, schema mismatches are logged as warnings (non-blocking). In CDC/SCD2, mismatches raise `SchemaValidationError`.

---

## Categorical Column Hashing (E-20)

polars-hash hashes Categorical columns' physical integer encoding, not the logical string value (Polars Issue #21533). The same string could hash differently depending on construction order.

**Resolution:** `add_row_hash()` and `add_row_hash_fallback()` detect Categorical columns and cast to Utf8 before hashing.

---

## Streaming Engine

Polars' streaming engine (`collect(engine="streaming")`) available since v1.31.1 enables out-of-core processing. Polars has demonstrated processing 31 GB CSV (2× available RAM) using `scan_csv` + streaming collect. Not yet adopted in the pipeline but available for future use.

**Data type optimization** (Categoricals for low-cardinality strings, smallest sufficient numeric types) can reduce memory 2–5×. Between pipeline stages, writing intermediate results to Parquet provides excellent compression and columnar access.

---

## polars-hash Dependency

polars-hash is required — Polars' built-in `.hash()` explicitly does not guarantee stable results across versions (GitHub #3966, #7758). Version 0.5.6 released January 2026, with 20+ releases since October 2023. Provides stable cryptographic hashing via `chash` namespace.

**Risk:** Single-maintainer dependency with relatively small community (84 GitHub stars). Mitigations: version pinned, hash determinism validation, fallback path to Python hashlib.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| M-1 | Memory | glibc arena fragmentation causes OOM | MALLOC_ARENA_MAX=2 + POLARS_MAX_THREADS=1 |
| M-2 | Memory | 6 workers exceed 64 GB budget | Default workers=4; warning when >4 |
| M-3 | Resources | File descriptor leak across iterations | psutil FD monitoring in _check_memory_pressure() |
| B-8 | Memory | RSS monitoring between iterations | _check_rss_memory() with configurable threshold |
| W-12 | Memory | Over-allocated buffers not released | shrink_to_fit(in_place=True) after large operations |
| ST-1 | Memory | Small tables OOM at 8-10M rows | Memory guard at 20 GB ceiling |
| T-2 | Concat | diagonal_relaxed crash bugs | _safe_concat() pre-aligns schemas |
| W-7 | Concat | Silent type coercion in relaxed concat | validate_schema_before_concat() before every concat |
| E-20 | Hashing | Categorical physical vs logical encoding | Cast to Utf8 before hashing |
| B-5 | Joins | NULL key join validation bug (#19624) | No validate param used; _filter_null_pks() prevents exposure |

---

*Sources: Polars GitHub issues #23128, #21732, #19624, #12543, #18911, #3966, #7758, #21533, #8993 (Presto team); polars-hash GitHub; Polars documentation on streaming engine.*
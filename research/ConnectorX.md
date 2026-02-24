# ConnectorX — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## Overview

ConnectorX is 3–13× faster than Pandas with 3× less memory via zero-copy Rust architecture. It is the primary extraction engine for both Oracle and SQL Server sources. However, it has several critical caveats at billion-row scale that required explicit mitigation.

---

## Critical: Torn Reads on Active Tables (E-1)

**Problem:** When `partition_num > 1`, ConnectorX opens N independent connections, each executing its own range query. On Oracle, each connection gets its own statement-level SCN. On SQL Server under default READ COMMITTED, each sees committed data at slightly different times. If the source table is being actively written to during extraction:
- Rows can be **duplicated** (partition key updated between snapshots)
- Rows can be **missed** (row moves between ranges between snapshots)

This manifests as phantom CDC changes that compound if phantom deletes trigger SCD2 closures.

**Resolution:** `partition_num` forced to 1 in all 4 extractor functions. Single connection ensures a consistent snapshot. For Oracle, Flashback Query (`AS OF SCN`) could pin all partitions to the same SCN if parallelism is needed in the future.

---

## Critical: NULL Partition Column Rows Silently Dropped (E-2)

**Problem:** ConnectorX's generated WHERE clauses use `>` and `<` operators which never match NULL. Rows with NULL partition columns vanish from the result set — no error, no log, no indication. Permanent silent data loss.

**Resolution:** `_supplement_null_partition_rows()` adds NULL partition rows via a supplementary `WHERE partition_col IS NULL` query and concatenates results. Implemented in both Oracle and SQL Server extractors.

---

## Oracle DATE Sentinel Crashes (E-4)

**Problem:** ConnectorX GitHub issue #644 — reading `DATE '9999-12-31'` from Oracle causes a Rust panic: `PanicException: out of range DateTime`. This terminates the Python process with no recovery. Sentinel dates like `9999-12-31` are extremely common in SCD2 source tables and ERP systems.

**Resolution:** `_build_safe_select_with_uri()` wraps DATE columns in `CASE WHEN date_col >= DATE '9999-01-01' THEN NULL ELSE date_col END` to NULL-ify sentinels before ConnectorX processes them.

---

## Rust Panic Recovery (B-7)

**Problem:** ConnectorX is implemented in Rust and can panic (not just throw Python exceptions) on various edge cases. Rust panics terminate the Python process if uncaught.

**Resolution:** `cx_read_sql_safe()` in `extract/__init__.py` wraps all ConnectorX calls with:
- `BaseException` catch (not just `Exception`) for Rust panic recovery
- Exponential backoff retry (3 attempts, 2s base delay)
- Non-retryable errors (syntax, permissions) fail fast
- All extractors route through this wrapper

---

## Connection/Session Management (E-5)

**Problem:** ConnectorX opens N+1 connections per `read_sql` call (one for metadata plus one per partition). With 6 partitions across 365 backfill iterations, that's 2,555 Oracle sessions if connections leak. Oracle's default `SESSIONS` parameter is often 100–300.

**Resolution:** Mitigated by E-1 (partition_num=1 → 2 connections instead of 7+). File descriptor monitoring via `_check_memory_pressure()` with psutil (warn at 500, error at 800).

---

## Partition Skew (E-3)

**Problem:** ConnectorX partitions by computing `MIN(col), MAX(col)` and splitting evenly. If 90% of values cluster in one range, one thread processes most rows while others handle trivial amounts — no parallelism benefit, just connection overhead.

**Resolution:** Mitigated by E-1 (partition_num=1 makes skew moot). If partitioned extraction is re-enabled in the future, add skew detection via percentile queries.

---

## Oracle-Specific ConnectorX Issues

- **Oracle support is unreliable:** dlt documentation notes ConnectorX is "not recommended for Oracle" — slower than PyArrow backend in thick mode
- **Oracle NUMBER** is always treated as decimal, which can cause precision issues
- **Oracle timezone handling (P3-2):** Windowed extraction uses `TRUNC()` in WHERE clauses to prevent timezone boundary drift. Both ConnectorX and oracledb paths use TRUNC().

**Contingency:** oracledb fallback path exists for Oracle extraction. pydbzengine (released February 2025) provides Pythonic interface to Debezium embedded engine for log-based CDC as a future alternative.

---

## SQL Server ConnectorX

SQL Server support is well-established with significant performance gains. Partitioning only works on non-nullable numerical columns. The SQL Server extractor uses ConnectorX windowed extraction with no Oracle-specific workarounds needed.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| E-1 | Torn reads | Partitioned reads inconsistent on active tables | partition_num=1 in all extractors |
| E-2 | NULL partition | Rows with NULL partition col silently dropped | Supplementary NULL query + concat |
| E-3 | Skew | Partition skew degrades parallelism | Mitigated by E-1 |
| E-4 | DATE panic | 9999-12-31 sentinel crashes ConnectorX | CASE WHEN to NULL-ify sentinels |
| E-5 | Sessions | Connection leak exhausts Oracle sessions | partition_num=1 reduces to 2 connections |
| B-7 | Rust panic | Panics terminate Python process | cx_read_sql_safe() with BaseException catch + retry |
| P3-2 | Timezone | Windowed extraction timezone drift | TRUNC() in WHERE clauses |

---

*Sources: ConnectorX GitHub issues #644 and documentation, dlt documentation, Polars GitHub, Oracle documentation, pydbzengine release notes (February 2025).*
# CDC (Change Data Capture) — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## Why Hash-Based CDC

Hash-based CDC is the correct approach for this pipeline's constraints: no WAL/transaction log access on Oracle, cross-system comparison (Oracle + SQL Server sources), batch periodicity, and no streaming infrastructure. Log-based CDC via Debezium is the gold standard when transaction log access exists, but requires Kafka + ZooKeeper + connectors + monitoring — reports indicate 4–6 full-time engineers to maintain at scale.

Hash-based CDC occupies a distinct and legitimate niche. Databricks' `AUTO CDC FROM SNAPSHOT` explicitly supports this pattern. The Data Vault 2.0 hashdiff pattern is used in production by dbtvault/automate-dv across thousands of deployments. The industry is moving toward hybrid approaches — log-based for real-time, hash/snapshot for reconciliation and non-log-accessible sources.

**Hash-based CDC is the right choice when:**
- Source log access is restricted (Oracle XStream requires licensing; LogMiner deprecated Continuous Mine Mode in 19c)
- Heterogeneous sources require a unified approach
- No streaming infrastructure exists
- Batch-tolerant workloads (daily analytics loading)
- Schema evolution — hash-based naturally adapts; log-based often breaks on DDL changes

SQL Server's native CDC cannot extract from Oracle, wasn't designed for 3B+ row tables, and adds significant overhead through Agent jobs and shadow change tables.

---

## Three-Way Anti-Join Pattern

The core CDC comparison uses three operations in Polars:

1. **INSERTS** — `fresh LEFT ANTI JOIN existing ON PKs`: rows in fresh extraction not in existing Stage
2. **DELETES** — `existing LEFT ANTI JOIN fresh ON PKs`: rows in existing Stage not in fresh extraction
3. **UPDATES** — `INNER JOIN ON PKs` + hash comparison: matched rows with different `_row_hash`

This is the canonical hash-based CDC approach documented across SQLServerCentral, DataForge, Estuary, and Jeffrey Aven's "CDC at Scale using Spark." It is logically correct when three invariants hold: PKs are truly unique in both source and target, PK dtypes match exactly, and no NULLs exist in PK columns.

**Count validation** after classification verifies `inserts + updates + unchanged = total fresh PKs`. A mismatch indicates a PK dtype alignment issue or join logic error. This mathematical invariant check is uncommon but excellent practice.

### Critical: NULL PK Filtering (P0-4)

Since Polars ≥0.20, `join_nulls=False` is the default — NULL PK values never produce matches in joins (SQL-compliant). Rows with NULL PKs silently appear in every anti-join output, causing duplicate inserts every run. The pipeline filters NULL PKs before all CDC comparison via `_filter_null_pks()`. With composite PKs, a NULL in any column causes the entire row to fail matching. Filtered count is tracked in PipelineEventLog metadata.

### Critical: PK Dtype Alignment (P0-12)

Polars does not perform implicit type casting on join keys — Int32 joined against Int64 will error or produce unexpected results. `align_pk_dtypes()` casts PK columns in both DataFrames to matching Polars dtypes before every join. Cross-platform type differences (Oracle NUMBER as Decimal or float64; SQL Server int or bigint) require explicit casting.

### Windowed CDC for Large Tables

For tables with 3B+ rows, CDC is scoped to single calendar days via `SourceAggregateColumnName`:

- Fresh extraction covers one day; existing Stage is read only within the same window
- Delete detection is scoped to the extraction window only (P1-4) — rows outside the window are never marked deleted. Pair with periodic full reconciliation for real deletes.
- Cross-midnight overlap (V-7): `LookbackDays` provides rolling re-extraction as the primary mechanism. `OVERLAP_MINUTES` extends windows backward. Weekly reconciliation catches remaining discrepancies.
- CDC is idempotent — overlapping extraction windows produce no phantom changes because unchanged rows hash identically.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| P0-4 | NULL PKs | NULL PKs cause duplicate inserts every run | `_filter_null_pks()` before all joins; count tracked |
| P0-12 | Dtype | PK dtype mismatch causes silent join failures | `align_pk_dtypes()` before every join |
| P1-4 | Windowed | Deletes outside window invisible | Scoped delete detection + weekly reconciliation |
| V-7 | Overlap | Cross-midnight transactions split across windows | LookbackDays + OVERLAP_MINUTES + weekly reconciliation |
| H-4 | Phantom | No-op source updates create phantom extraction work | Hash comparison correctly filters; accept as-is |
| E-12 | Monitoring | No alerting for systematic hash mismatches | Update ratio tracked; >50% with >1000 updates triggers WARNING |
| P0-10 | NULL hash | NULL hash values bypass comparison | NULL hash guard treats NULL hash as changed |

---

## Monitoring for CDC Correctness

- **Phantom change rate (E-12):** Track CDC update ratio over time. Spike from 0.1% to 50% indicates systematic hash mismatch (encoding change, schema drift, normalization bug), not real changes.
- **Empty extraction guard (P1-1):** Blocks CDC if row count drops >90% vs rolling median. 50% drop triggers WARNING.
- **Explosion guard (P1-11):** Blocks CDC if row count spikes >5× vs previous median.
- **Count validation (P0-12):** inserts + updates + unchanged = total fresh PKs — verified after every run.
- **Data freshness (E-15):** Warns if max `UdmEffectiveDateTime` in Bronze is >48 hours stale.

---

*Sources: Jeffrey Aven "CDC at Scale using Spark," dbt snapshot documentation, Data Vault 2.0 hashdiff pattern, Databricks AUTO CDC, Debezium/Zalando post-mortems, SQLServerCentral, Polars GitHub issues #19624 and #3966.*
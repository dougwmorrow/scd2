# Reconciliation — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## Why Count Validation Alone Is Insufficient

Count validation (`inserts + updates + unchanged = total`) is a necessary sanity check but cannot detect five categories of errors:
1. Hash collisions misclassifying individual rows
2. Compensating errors that net to zero
3. Phantom changes from cross-platform encoding differences
4. Missed deletes outside the extraction window
5. Row-level data corruption from BCP delimiter issues

A production-grade reconciliation strategy layers multiple validation levels using different detection methods.

---

## Multi-Tier Reconciliation Strategy

The pipeline's nine-element reconciliation strategy is more thorough than what most production pipelines implement, including Uber's UDQ platform.

### Tier 1 — Every Run
- **Count validation (P0-12):** `inserts + updates + unchanged = total fresh PKs` — mathematical invariant
- **SCD2 integrity checks:** Overlapping intervals, zero-active PKs, duplicate actives, version gaps
- **@@ROWCOUNT validation** after UPDATE JOIN
- **Post-SCD2 duplicate active check (V-4):** `_check_duplicate_active_rows()`
- **CDC update ratio tracking (E-12):** >50% with >1000 updates triggers WARNING for systematic hash mismatch

### Tier 2 — Daily
- **Count reconciliation (W-11):** `reconcile_counts()` compares source vs Stage vs Bronze row counts. Catches gross data loss within 24 hours.
- **Aggregate reconciliation (E-17):** `reconcile_aggregates()` compares SUM/COUNT/MIN/MAX of numeric columns between source and Bronze active rows. Catches value-level corruption that count validation misses.
- **Data freshness (E-15):** `_log_data_freshness()` checks max `UdmEffectiveDateTime` in Bronze. Warns if >48 hours stale. Two-tier alerting: WARNING at 36 hours (1.5× expected refresh), ERROR at 48 hours (2× — two missed cycles).

### Tier 3 — Weekly
- **Full column-by-column reconciliation (P3-4):** `cdc/reconciliation.py` does full comparison bypassing the hash entirely. This is the critical safety net — uses a different method than integration to catch hash collisions and CDC logic bugs.
- **Full-PK reconciliation:** Extract all source PKs, compare against all active target PKs. Catches any windowed-CDC drift including missed deletes.
- **Version velocity (E-13):** `check_version_velocity()` queries avg/max versions per PK, flags PKs with >10 versions. High velocity indicates genuine high-change-rate data or phantom version creation.
- **Distribution shift (B-10):** `detect_distribution_shift()` compares numeric column means against stored baseline using z-score analysis. Alerts when shift exceeds 2σ.
- **Active-to-total ratio (E-14):** Tracked in SCD2_PROMOTION event metadata. Ratio <1% triggers WARNING for possible mass incorrect closures. Gradual decline is expected as history accumulates.

### Tier 4 — Cross-Layer
- **Transformation boundary checks (B-11):** `reconcile_transformation_boundary()` validates row counts, key existence, and NULL rate comparisons between adjacent medallion layers (Bronze→Silver→Gold). Implements circuit-breaker pattern — callers can block downstream processing on failure.
- **Referential integrity (B-12):** `check_referential_integrity()` validates FK relationships across SCD2 tables. Supports both non-temporal (active flag) and temporal (BETWEEN effective/end dates) lookup modes. Detects orphaned FKs and ambiguous FKs.

---

## Extraction Guards

### Empty Extraction Guard (P1-1)
Blocks CDC if row count drops >90% vs rolling median of last 5 successful runs. 50% drop triggers WARNING but proceeds. Day-of-week aware queries (last 30 days, same weekday) reduce false positives on weekends/holidays.

### Explosion Guard (P1-11)
Blocks CDC if row count spikes >5× vs previous median. Prevents OOM or runaway loads.

### First-Run Ceiling (S-2)
Small tables with no extraction history capped at 50M rows to catch misconfigured large tables.

### Daily Extraction Guard (P1-13)
Each day's extraction checked against daily extraction median. Spikes >5× are blocked.

### Schema Migration Awareness (B-3)
Schema evolution (column adds) changes all row hashes on the next run. `evolve_schema()` returns `SchemaEvolutionResult` so orchestrators can suppress E-12 false warnings during schema migration runs. PipelineEventLog metadata includes `"schema_migration": true`.

---

## Key Design Principles

**Use a different method for reconciliation than for integration.** If hash-based CDC has a logical error, hash-based reconciliation won't catch it. The weekly column-by-column comparison bypasses the hash entirely.

**Separate count reconciliation (cheap, daily) from full-column reconciliation (expensive, weekly).** For high-value or compliance-critical tables, increase to daily. For stable dimension tables, monthly suffices.

**Track reconciliation divergence trends** in metadata tables — increasing divergence indicates a pipeline problem requiring investigation.

---

## Comparison to Industry Standards

| Framework | Coverage | Pipeline Comparison |
|-----------|----------|-------------------|
| Uber UDQ | Freshness, completeness, duplicates, outliers, distribution shifts | Pipeline covers all except distribution shifts were added via B-10 |
| dbt tests | unique, not_null, accepted_values, relationships | Pipeline exceeds with temporal integrity checks |
| DQOps | Circuit breaker at layer boundaries | Pipeline implements via B-11 |
| Acceldata | Distribution monitoring | Pipeline implements via B-10 |
| Kimball Group | SCD2 integrity (overlapping intervals, zero-active, duplicates) | Pipeline implements all four core checks |

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| P3-4 | Weekly | Hash collisions undetectable by hash-based checks | Full column-by-column reconciliation |
| W-11 | Daily | Gross data loss detection | reconcile_counts() source vs Stage vs Bronze |
| E-12 | Per-run | Systematic hash mismatch alerting | Update ratio tracked; >50% triggers WARNING |
| E-13 | Weekly | Phantom version creation detection | check_version_velocity() flags >10 versions/PK |
| E-14 | Per-run | Mass incorrect closures | Active-to-total ratio tracked |
| E-15 | Per-run | Silent pipeline failures | Data freshness check warns >48 hours |
| E-17 | Daily | Value-level corruption | reconcile_aggregates() SUM/COUNT/MIN/MAX |
| B-9 | Alerting | Static alerting thresholds | Dynamic baselines, day-of-week aware |
| B-10 | Weekly | Subtle data drift | detect_distribution_shift() z-score |
| B-11 | Cross-layer | Transformation boundary errors | reconcile_transformation_boundary() circuit breaker |
| B-12 | Cross-table | FK relationship survival | check_referential_integrity() temporal + non-temporal |

---

*Sources: Uber UDQ platform documentation, DQOps documentation, Acceldata documentation, Kimball Group SCD2 validation, dbt testing framework, production post-mortems from Netflix, Databricks, and Zalando.*
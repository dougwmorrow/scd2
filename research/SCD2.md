# SCD2 (Slowly Changing Dimension Type 2) — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## INSERT-First SCD2 Pattern

The pipeline uses an unconventional but defensibly crash-safe INSERT-first ordering:

1. **INSERT** new versions with `UdmActiveFlag=0` (for updates/resurrections)
2. **UPDATE** to close old active versions (`UdmActiveFlag=0`, set `UdmEndDateTime`)
3. **UPDATE** to activate new versions (`UdmActiveFlag=1`) via `_activate_new_versions()`

New inserts (operation='I') use `UdmActiveFlag=1` directly since no prior active row exists.

### Why INSERT-First

The dominant industry pattern is UPDATE-first (expire old records, then INSERT new). dbt snapshots, Databricks, Informatica, Oracle DI, and SSIS all follow this. However:

- **Crash after INSERT-first**: leaves duplicate active rows — detectable, recoverable via deduplication
- **Crash after UPDATE-first**: leaves zero active rows — data loss requiring reconstruction

At billion-row scale with batched operations, wrapping everything in a single transaction is impractical due to transaction log pressure. INSERT-first + dedup-on-next-run is a pragmatic tradeoff.

### Filtered Unique Index Interaction (E-2)

The filtered unique index on `(PKs) WHERE UdmActiveFlag=1` rejects INSERT of a new active row if the old active row isn't closed. The 3-step process resolves this: INSERT with `UdmActiveFlag=0`, close old versions, then activate new versions. This prevents constraint violations while maintaining crash safety.

### Transient Windows

**Zero-active-row window (B-14):** Between closing old versions and activating new versions, queries filtering `WHERE UdmActiveFlag=1` see zero active rows for affected PKs. Under RCSI, in-flight readers see consistent pre-operation data, but new readers during the window see the gap. Documented in `scd2/engine.py` module docstring.

**Duplicate-active window (E-8):** Under RCSI, INSERT and UPDATE are separate statements each with their own snapshot. Between INSERT commit and UPDATE commit, concurrent readers may see two active versions (transient, typically milliseconds).

**Defensive query pattern for both windows:**
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY pk_cols ORDER BY UdmEffectiveDateTime DESC
  ) AS rn
  FROM Bronze.table
) t WHERE rn = 1
```

---

## MERGE Avoidance — Strongly Validated

The decision to avoid SQL Server's MERGE is overwhelmingly supported by the expert community:

- **Aaron Bertrand** maintains a definitive catalog of MERGE bugs: silent wrong results with indexed views, filtered index violations, deadlocks, race conditions requiring HOLDLOCK hints
- **Paul White** validated a new MERGE bug in **February 2025**: assertion errors when updating through a view, reproduced on SQL Server 2008 through 2022 CU17
- **Hugo Kornelis** (September 2023): most bugs fixed in SQL Server 2022, but DELETE action on indexed views and temporal table targets remain broken
- **Microsoft's own documentation**: replacing MERGE with UPDATE + INSERT gives ~60% better performance

The staging table + UPDATE JOIN pattern is the production standard. Expert consensus (Brent Ozar, Aaron Bertrand, Michael J. Swart, Erik Darling) remains unanimous: use separate INSERT and UPDATE statements.

---

## SCD2 Integrity Checks — Four Core Validations

Run after every SCD2 promotion:

1. **Overlapping intervals:** Self-join detecting records where date ranges overlap for the same business key
2. **Zero-active records:** Business keys with no row where `UdmActiveFlag=1`
3. **Multiple active records (V-4):** Business keys with >1 active row (detects INSERT-first crash residue). `_check_duplicate_active_rows()` runs post-promotion.
4. **Version gaps:** Using `LEAD(BeginDate)` windowed against `EndDate` to detect non-contiguous version chains

These match the industry standard validation suite documented by Kimball Group, dbt, and multiple data quality frameworks.

---

## Crash Recovery

**Orphaned Flag=0 rows (B-4):** If pipeline crashes after INSERT but before activation, rows with `UdmActiveFlag=0, UdmEndDateTime IS NULL, UdmScd2Operation IN ('U','R')` are left orphaned. `_cleanup_orphaned_inactive_rows()` runs before each SCD2 comparison to delete these. Deletes in batches of `config.SCD2_UPDATE_BATCH_SIZE` to avoid lock escalation.

**Dedup recovery (P1-16):** `_dedup_bronze_active()` handles duplicate active rows from crash windows. Sort by EffectiveDateTime DESC, unique on PKs — tiebreaker logic must be deterministic.

**Resurrected PKs (E-18):** Previously deleted PKs reappearing in source get `UdmScd2Operation='R'` for audit trail. Version chain: active ('I') → closed → deleted ('D') → closed → reactivated ('R').

---

## UPDATE Batch Size — Lock Escalation (B-2)

SQL Server escalates from row locks to a table-level exclusive lock at approximately 5,000 locks. `UPDATE TOP(500000)` exceeded this by ~100×, guaranteeing lock escalation that overrides RCSI and blocks all concurrent readers.

**Resolution:** `config.SCD2_UPDATE_BATCH_SIZE` reduced to 4,000 rows. For 3M daily closes: ~750 iterations of ~4,000 rows each, each completing in milliseconds with no lock escalation.

This revises the E-9 assessment which concluded RCSI made the batch size a non-issue. Table-level exclusive locks override RCSI regardless.

---

## Bronze Table Design

- **Surrogate key:** `_scd2_key BIGINT IDENTITY(1,1)` — BIGINT range (~9.2 quintillion) would take ~292,000 years to exhaust at 1M inserts/second. Keep `IDENTITY_CACHE = ON`.
- **Filtered unique index:** `CREATE UNIQUE INDEX ... ON (pk_cols) WHERE UdmActiveFlag=1` — enforces single active row per business key, serves double duty as constraint and query index.
- **Point-in-time index (V-9):** `(business_key, EffectiveDateTime DESC)` for temporal lookups.
- **Append-only:** Bronze is never truncated. SCD2 is append-only by design.
- **Columnstore (W-10):** Optional ordered clustered columnstore for 100M+ row tables (SQL Server 2022). Requires nonclustered PK.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| E-2 | Index | INSERT-first conflicts with filtered unique index | 3-step: INSERT Flag=0, close old, activate new |
| E-3 | Atomicity | Partial BCP loads break SCD2 | `bcp_load(atomic=True)` for Bronze (single transaction) |
| E-5 | Dedup | Duplicate staging keys cause nondeterministic UPDATE | `.unique(subset=pk_columns)` before staging load |
| E-8 | RCSI | Transient duplicate active rows between INSERT/UPDATE | ROW_NUMBER defensive query pattern |
| E-9 | RCSI | RCSI verification at startup | `verify_rcsi_enabled()` in connections.py |
| E-10 | Log space | Large UPDATEs exhaust transaction log | `_check_log_space()` warns if <1.5× estimated need |
| B-2 | Locks | 500K batch triggers table-level exclusive locks | Reduced to 4,000 via config.SCD2_UPDATE_BATCH_SIZE |
| B-4 | Crash | Orphaned Flag=0 rows from crashes | `_cleanup_orphaned_inactive_rows()` at SCD2 start |
| B-14 | Window | Zero-active-row window during SCD2 update | Documented; ROW_NUMBER defensive query |
| SCD-1 | Retry | INSERT-first retry creates duplicate active rows | Filtered unique index prevents duplicates |
| SCD-2 | Overflow | INT IDENTITY overflow at 3B+ rows | Already BIGINT IDENTITY(1,1) |

---

*Sources: Kimball Group, dbt snapshots, Databricks SCD2 documentation, Aaron Bertrand MERGE bug catalog, Paul White (February 2025 MERGE bug), Hugo Kornelis (September 2023 reassessment), Brent Ozar lock escalation guidance, Michael J. Swart, Microsoft performance documentation.*
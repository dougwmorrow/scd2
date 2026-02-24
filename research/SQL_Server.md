# SQL Server — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## MERGE Statement — Unsafe for Production ETL

The pipeline's use of staging table + UPDATE JOIN instead of MERGE is strongly endorsed by the SQL Server expert community. This is a validated architecture decision.

**Aaron Bertrand** maintains a definitive catalog of MERGE bugs: silent wrong results with indexed views (acknowledged by Microsoft as critical unfixed), filtered index violations, deadlocks, and race conditions requiring HOLDLOCK hints. His conclusion: "I would not feel comfortable recommending anyone to use MERGE."

**Paul White** validated a new MERGE bug in **February 2025**: assertion errors when updating through a view (outer join scenario), reproduced on SQL Server 2008 through 2022 CU17 inclusive. New bugs continue to surface.

**Hugo Kornelis** (September 2023, most charitable analysis): most bugs fixed in 2022, but DELETE action on indexed views and temporal table targets remain broken. Concludes: "do not use MERGE with a DELETE action."

**Microsoft's own documentation** states replacing MERGE with UPDATE + INSERT gives ~60% better performance.

**Expert consensus (Brent Ozar, Aaron Bertrand, Michael J. Swart, Erik Darling):** unanimous — use separate INSERT and UPDATE statements.

---

## Lock Escalation (B-2)

SQL Server escalates from row locks to a table-level exclusive lock when a single statement acquires approximately 5,000 locks. A table-level exclusive lock **overrides RCSI**, making RCSI's non-blocking reader benefits irrelevant during the update.

### Impact on SCD2 Updates
`UPDATE TOP(500000)` exceeded the threshold by ~100×, guaranteeing lock escalation every batch. For a 3M-row daily close set with 500K batches, this meant 6 batches of prolonged blocking.

### Resolution
`config.SCD2_UPDATE_BATCH_SIZE` reduced to 4,000 rows. For 3M daily rows: ~750 iterations of ~4,000 rows each, each completing in milliseconds with no lock escalation. `WHILE` loop pattern with explicit `COMMIT` between batches releases locks and allows transaction log reuse.

### Alternatives
- `ALTER TABLE SET (LOCK_ESCALATION = DISABLE)` — risks memory pressure from millions of individual row locks at billion-row scale. Not recommended.
- Table partitioning with `LOCK_ESCALATION = AUTO` — enables partition-level escalation. Better long-term solution for billion-row tables but requires more planning.

---

## Read Committed Snapshot Isolation (RCSI)

RCSI provides non-blocking reads under normal circumstances — readers don't block writers and vice versa. However:

- **Table-level exclusive locks override RCSI** — see Lock Escalation above
- **INSERT and UPDATE are separate statements** each with their own snapshot under RCSI (E-8). Between commits, concurrent readers may see transient inconsistency.
- **sp_releaseapplock before COMMIT** creates a race condition under RCSI — another process reads pre-transaction state (W-8). Session-scoped locks avoid this.

`verify_rcsi_enabled()` runs at pipeline startup. Non-blocking — logs WARNING and continues if RCSI is disabled.

---

## sp_getapplock — Concurrent Run Protection

`sp_getapplock` with `@LockOwner='Session'` prevents concurrent pipeline runs on the same table. Session-owned locks auto-release on connection drop (crash-safe). Uses `@LockTimeout=0` for non-blocking "skip if already running" semantics.

**Key design decision (W-8):** Session-owned locks with `autocommit=True`. The RCSI race condition (sp_releaseapplock before COMMIT) does NOT apply to Session-scoped locks. If lock ownership is ever changed to Transaction-scoped, remove the explicit `sp_releaseapplock` call.

**Network resilience (N-1):** `_get_resilient_lock_connection()` adds `ConnectRetryCount=3, ConnectRetryInterval=10` to handle firewall timeout scenarios where locks are silently lost.

---

## Transaction Log Management

### BULK_LOGGED Recovery
`_bulk_load_recovery_context()` sets BULK_LOGGED during load windows, restored to FULL with a log backup after. Minimally logged operations reduce log volume for BCP inserts.

### Log Space Monitoring (E-10)
`_check_log_space()` runs before large UPDATE operations (>1M rows). UPDATEs are always fully logged (~400 bytes × 2 per row for before/after images). A 5M-row UPDATE may require 5–20 GB of log space. Warns if available space is <1.5× estimated need. Ensure frequent log backups (15–30 min) during pipeline runs.

---

## Indexing for Billion-Row SCD2 Tables

### Filtered Unique Index
`CREATE UNIQUE INDEX ... ON (pk_cols) WHERE UdmActiveFlag=1` — enforces single active row per business key. Serves double duty as constraint and active-row query index. Known limitations: cannot back FK constraints, parameterized queries may not match filter predicate (use literal values or `OPTION (RECOMPILE)`), incompatible with partition switching.

### Point-in-Time Index (V-9)
`(business_key, EffectiveDateTime DESC)` for temporal lookups.

### Ordered Clustered Columnstore (W-10)
SQL Server 2022: `CREATE CLUSTERED COLUMNSTORE INDEX ... ORDER (BusinessKey, EffectiveDateTime)` enables segment elimination for point-in-time queries without sacrificing compression benefits. Opt-in migration for 100M+ row tables. Requires dropping clustered PK and recreating as nonclustered.

### Index Management During Loads
`index_management.py` disables and rebuilds indexes around BCP loads. Prevents index maintenance overhead during bulk operations and avoids fragmentation.

---

## Staging Table Patterns

### Typed Staging Tables (P0-3)
Staging tables use actual PK types from target via `get_column_types()` from `schema_utils.py`. Never hardcode NVARCHAR(MAX) for PK columns — type mismatches in UPDATE JOIN cause silent comparison failures.

### Orphaned Staging Cleanup (P3-3)
`schema/staging_cleanup.py` drops orphaned `_staging_*` tables from crashes. Should run at pipeline start.

---

## SQL Server 2022 Features

### Temporal Tables (W-16)
Provide automatic SCD2-like versioning with native `FOR SYSTEM_TIME AS OF` query syntax. Limitations: any update creates a version (no column-level control), history table must be in same database. Evaluation pending.

### Ordered Columnstore (W-10)
Meaningful improvement for billion-row SCD2 tables. See Indexing section above.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| B-2 | Locks | 500K batch triggers table-level exclusive locks | Reduced to 4,000 rows |
| E-8 | RCSI | Transient inconsistency between INSERT/UPDATE | Documented; ROW_NUMBER defensive pattern |
| E-9 | RCSI | RCSI verification at startup | verify_rcsi_enabled() non-blocking check |
| W-8 | Locks | Session vs Transaction scope race condition | Session-scoped with autocommit=True |
| N-1 | Network | Lock connection lost on firewall timeout | Resilient connection with retry |
| E-10 | Log space | Large UPDATEs exhaust transaction log | _check_log_space() warns at <1.5× |
| P0-3 | Types | Staging table PK type mismatches | get_column_types() from schema_utils.py |
| P3-3 | Cleanup | Orphaned staging tables from crashes | staging_cleanup.py at pipeline start |
| SCD-4 | Rowcount | pyodbc rowcount -1 after executemany | Uses execute() (safe); documented |

---

*Sources: Aaron Bertrand MERGE bug catalog, Paul White (February 2025 bug), Hugo Kornelis (September 2023 reassessment), Brent Ozar lock escalation guidance, Microsoft BCP and MERGE documentation, Michael J. Swart, Erik Darling, SQLServerCentral, MSSQLTips.*
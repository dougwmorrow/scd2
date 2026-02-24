# B-Series Verification Gaps — Action Items

*Context: Found during codebase verification of all 14 B-series TODO items (2026-02-23). 12 of 14 items fully verified as DONE. Two items had gaps requiring follow-up.*

---

## B-2 (P1): Stale 500K references in connections.py — DONE

**Resolution (2026-02-23):**
- Updated `verify_rcsi_enabled()` docstring to reference `config.SCD2_UPDATE_BATCH_SIZE` (default 4,000) and explain the ~5,000-lock escalation threshold
- Updated warning message to dynamically format the batch size via `config.SCD2_UPDATE_BATCH_SIZE` (`%d` format) so it stays accurate if the value changes
- **Secondary concern verified:** `scd2/engine.py` line 600 has `_SCD3_BATCH_SIZE = config.SCD2_UPDATE_BATCH_SIZE` — the actual UPDATE SQL consumes the config value, not a hardcoded 500,000

---

## B-4 (P1): `_cleanup_orphaned_inactive_rows()` — VERIFIED (exists in codebase)

**Resolution (2026-02-23):** The function exists in `scd2/engine.py` at lines 748–839. The original grep returned zero results because the search was run on an incomplete set of uploaded project files (the full `scd2/engine.py` was not included in that upload).

**Verification details:**
- `_cleanup_orphaned_inactive_rows(bronze_table, table_config)` defined at `scd2/engine.py:748`
- Called in `run_scd2()` at line 175 (before `read_bronze_table()`)
- Called in `run_scd2_targeted()` at line 946 (before `read_bronze_for_pks()`)
- Queries for orphaned rows: `UdmActiveFlag=0 AND UdmEndDateTime IS NULL AND UdmScd2Operation IN ('U','R')`
- Deletes in batches of `config.SCD2_UPDATE_BATCH_SIZE` to avoid lock escalation
- Logs WARNING when orphans found, wrapped in try/except (non-fatal)
- Matches all expected behaviors documented in CLAUDE.md

---

## Summary

| Item | Priority | Status | Resolution |
|------|----------|--------|------------|
| B-2 | P1 | DONE | Docstring and warning updated to reference `config.SCD2_UPDATE_BATCH_SIZE`; SCD2 SQL verified |
| B-4 | P1 | VERIFIED | Function exists at `scd2/engine.py:748–839`, called at both SCD2 entry points |

All 14 B-series items are now fully verified.

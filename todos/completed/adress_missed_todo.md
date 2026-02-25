# UDM Pipeline — Remaining Items from Post-Review Audit

Audit date: 2026-02-24. These items were marked complete in the original TODO but were audited for presence in the codebase.

---

## 1. O-3 — ConnectorX Retry Allowlist (P3, Operational Reliability)

**Status:** VERIFIED PRESENT — code exists in `extract/__init__.py:109-172`.

The audit did not find this code because `extract/__init__.py` is a subpackage module that may not have been included in the flat file search. All three components are implemented:

- [x] **O-3a:** `_NON_RETRYABLE_PATTERNS` (lines 112-128): ORA-00942, ORA-01017, ORA-01031, ORA-00933, ORA-06550, plus SQL Server syntax/permission/login patterns.
- [x] **O-3b:** `_TRANSIENT_PATTERNS` (lines 133-146): ORA-12541, ORA-12170, ORA-03135, ORA-03113, ORA-12537, connection reset, timeout, broken pipe.
- [x] **O-3c:** `_is_non_retryable_error()` (lines 149-172) wired into `cx_read_sql_safe()` at line 80. Transient patterns checked first, non-retryable fail fast, `KeyboardInterrupt`/`SystemExit` propagated immediately, unknown errors retry conservatively.

---

## 2. O-2_SCD2 — Structured JSON Logging for SCD2 Operations (P3, Observability)

**Status:** VERIFIED PRESENT with bug fix applied. Code exists at `scd2/engine.py:270-280` (run_scd2) and `scd2/engine.py:1142-1152` (run_scd2_targeted).

The audit did not find this code because `scd2/engine.py` is a subpackage module. Both `O-2_SCD2` structured JSON log lines were present but referenced `result.resurrections` — a field that did not exist on `SCD2Result`. This would have raised `AttributeError` at runtime.

**Fix applied:**
- [x] **O-2d/O-2e:** Added `resurrections: int = 0` field to `SCD2Result` dataclass.
- [x] Set `result.resurrections = len(df_resurrected)` in both `run_scd2()` and `run_scd2_targeted()` when resurrection PKs are detected.

---

## 3. N-3/P-4 — Polars Minimum Version Pin (P3, Preventive)

**Status:** Server-dependent — no action until P-4 verification confirms Polars ≥1.32 is installed.

- [ ] **P-4b:** Once P-4 server verification confirms Polars ≥1.32 is installed, raise the minimum pin in `requirements.txt` from `polars>=1.13.0` to `polars>=1.32.0` to prevent accidental deployment on older versions. Update the N-3 comment to reference both `str.normalize` and streaming anti-join support.

---

## Priority Summary

| Item | Focus | Priority | Status |
|------|-------|----------|--------|
| O-3 | ConnectorX retry allowlist | **P3** | **Complete** — verified in `extract/__init__.py` |
| O-2_SCD2 | SCD2 structured logging | **P3** | **Complete** — bug fix applied (added `resurrections` field to `SCD2Result`) |
| P-4b | Polars version pin | **P3** | Blocked on server verification |

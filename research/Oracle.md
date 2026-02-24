# Oracle — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## Oracle Empty String = NULL Equivalence (E-1)

The single most likely source of persistent phantom changes in a cross-platform pipeline. Oracle treats `''` as `NULL` in VARCHAR2 columns; SQL Server does not. Without normalization, every row with an empty-string field hashes differently every run, producing phantom updates indefinitely and unbounded Bronze growth.

**Resolution:** `add_row_hash(source_is_oracle=True)` normalizes empty strings to NULL before hashing. Normalization direction is empty → NULL because Oracle semantically treats them as identical and the NULL sentinel already handles NULL distinctly. Applied to the DataFrame itself (not just hash input) so BCP CSV output is also normalized. First deployment triggers a one-time CDC update wave for affected rows.

---

## Six Oracle→SQL Server Type Conversion Pitfalls (B-13)

### 1. Oracle DATE Always Includes Time
Oracle DATE stores hours, minutes, seconds — it is not a date-only type. Mapping to SQL Server `date` silently truncates time components. **Resolution:** `fix_oracle_date_columns()` upcasts `pl.Date` to `pl.Datetime` to prevent truncation. Correct target mapping: `datetime2(0)`.

### 2. Oracle NUMBER Without Precision
Oracle NUMBER without specified precision can hold any numeric value up to 38 digits. ConnectorX or the extraction layer maps to DECIMAL(38,8) by default, which silently truncates values with more than 8 decimal places. **Resolution:** Documented and monitored. For critical columns with >15 digits, cast to VARCHAR2 in extraction SQL.

### 3. Oracle FLOAT(126) Precision Loss
Oracle FLOAT(126) provides ~38 decimal digits; SQL Server FLOAT(53) provides only ~15. Significant precision loss for scientific/financial data. **Resolution:** Documented. ConnectorX converts Oracle NUMBER to Python float64 (~15 significant digits) — precision is already lost before pipeline processing (E-21).

### 4. Oracle RAW(16) GUID Byte Ordering
Oracle stores bytes in big-endian; SQL Server UNIQUEIDENTIFIER uses mixed-endian. Direct byte copy produces different GUID string representations, breaking joins. **Resolution:** Documented. If GUIDs are used as join keys, byte reordering is required during extraction.

### 5. Oracle BLOB/CLOB Size Limits
Oracle supports 4GB; SQL Server VARBINARY(MAX)/VARCHAR(MAX) supports 2GB. Data exceeding 2GB is silently truncated. **Resolution:** Documented.

### 6. Character Length Semantics
Oracle VARCHAR2(100 BYTE) in AL32UTF8 may hold only 33 multi-byte characters, while SQL Server VARCHAR(100) holds 100 bytes. **Resolution:** Documented. Ensure SQL Server column sizes accommodate worst-case character expansion.

---

## Oracle Timezone and Date Handling

### TRUNC() for Windowed Extraction (P3-2)
Oracle DATE stores no timezone. Windowed extraction uses `TRUNC()` in WHERE clauses to prevent timezone boundary drift. Both oracledb and ConnectorX paths use TRUNC().

### DST Fall-Back Ambiguity (T-1)
During DST fall-back, 1:30 AM local time could be UTC-4 or UTC-5. Oracle DATE stores no timezone information, creating irrecoverable 1-hour ambiguity. **Resolution:** Pipeline uses `datetime.now(timezone.utc)` for all CDC timestamps — no source-derived times.

### DATE Sentinel Values (E-4)
`9999-12-31` is common in SCD2 source tables. ConnectorX Rust panics on this value (GitHub #644). **Resolution:** Extraction SQL wraps DATE columns in CASE WHEN to NULL-ify sentinels >= 9999-01-01.

---

## Oracle Extraction Routing

| Condition | Extractor | Notes |
|-----------|-----------|-------|
| Oracle + SourceIndexHint populated | oracledb | Per-day date chunks, INDEX hints, TRUNC() boundaries |
| Oracle + SourceIndexHint NULL | ConnectorX | FULL scan hint, TRUNC() boundaries |
| SQL Server | ConnectorX | Standard windowed extraction |

### oracledb Path
- Thick mode via Oracle Instant Client 19c
- Pre-queries for distinct dates to skip empty days (P2-2)
- Uses TRUNC() in WHERE clauses (P3-2)
- Fallback for ConnectorX Oracle issues

### Oracle Views
Oracle views do NOT expose primary keys. `schema/column_sync.py` attempts unique index discovery via `ALL_INDEXES` with `UNIQUENESS = 'UNIQUE'`, but IsPrimaryKey in UdmTablesColumnsList may still need manual setup for views without unique indexes.

---

## Other Oracle Behaviors

### No-Op Updates (H-4)
Oracle no-op updates (`UPDATE SET col=col`) still generate redo/undo and change `ORA_ROWSCN`. The hash comparison correctly filters these (hash unchanged), but extraction still pulls the rows. Accepted as-is.

### Metadata-Only DEFAULT Changes (T-3)
Oracle's metadata-only DEFAULT values (11g+) are transparent to `SELECT *`. When the default definition changes, old rows retain the original default while new rows get the new one. The pipeline correctly detects these as real data differences. Documented — expect a one-time CDC update surge after source DEFAULT changes.

### Bind Variable Peeking (O-2)
ConnectorX uses literals (no bind peeking issue). oracledb self-corrects via Adaptive Cursor Sharing. Documented.

### TRUNC(date_col) Performance Penalty (O-1)
`TRUNC(date_col)` in WHERE clauses prevents index usage, causing 50–60× performance penalty on large tables. **Resolution:** All WHERE clauses use range predicates (`date_col >= :start AND date_col < :end`) instead of `TRUNC(col)`.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| E-1 | Empty string | Oracle '' = NULL causes phantom updates | Empty → NULL normalization before hashing |
| E-4 | DATE panic | 9999-12-31 crashes ConnectorX | CASE WHEN to NULL-ify sentinels |
| E-21 | Precision | NUMBER → float64 loses precision >15 digits | Documented; cast to VARCHAR2 for critical columns |
| B-13 | Type mapping | Six type conversion pitfalls | Documented; DATE→Datetime upcast implemented |
| P3-2 | Timezone | Windowed extraction drift | TRUNC() in WHERE clauses |
| T-1 | DST | Fall-back 1-hour ambiguity | Pipeline uses UTC timestamps |
| T-3 | DEFAULT | Metadata-only DEFAULT changes | Correctly detected; documented |
| O-1 | Performance | TRUNC() prevents index usage | Range predicates instead |
| O-2 | Plans | Bind variable peeking inconsistency | ConnectorX uses literals; oracledb ACS |

---

*Sources: Oracle documentation (DATE semantics, NUMBER precision, VARCHAR2 BYTE vs CHAR), ConnectorX GitHub issue #644, Oracle Instant Client 19c documentation, Adaptive Cursor Sharing documentation.*
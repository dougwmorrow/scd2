# BCP (Bulk Copy Program) — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## BCP CSV Contract

All CSV writers MUST produce files matching this exact specification. Any deviation causes BCP load failures or data corruption.

```
Delimiter:          tab (\t)
Row terminator:     LF only (-r 0x0A)
Header:             none
Quoting:            quote_style='never'
NULL representation: empty string
Datetime format:    '%Y-%m-%d %H:%M:%S.%3f'
BIT columns:        Int8 (0/1) — never True/False
Hash columns:       Full SHA-256 hex string, VARCHAR(64)
UInt64 (non-hash):  .reinterpret(signed=True) -> Int64
String sanitization: replace \t \n \r \x00 with empty string BEFORE write_csv
Batch size:         write_csv(batch_size=4096) to avoid memory spikes
```

---

## Critical: Data Corruption from Embedded Control Characters

### Tab/Newline Corruption (B-1 edge case audit)

**Problem:** With `quote_style='never'`, Polars performs no escaping. Source data containing tabs (`\t`) splits a field into two columns; embedded newlines (`\n`, `\r\n`) split a row into two rows. BCP parses corrupted files without error — wrong data loads into wrong columns. Embedded newlines are extremely common in production data (addresses, comments, email bodies).

**Resolution:** `sanitize_strings()` pre-strips `\t`, `\n`, `\r`, `\x00` from all string columns before CSV write. `_validate_sanitized()` in `write_bcp_csv()` raises on unsanitized data as defense-in-depth.

### Extended Unicode Line-Break Characters (B-6)

**Problem:** Beyond standard `\t`, `\n`, `\r`, `\x00`, several extended Unicode characters also break BCP CSV boundaries: `\x0B` (vertical tab), `\x0C` (form feed), `\x85` (next line), `\u2028` (line separator), `\u2029` (paragraph separator). More likely in internationalized data, CMS content, or legacy system migrations.

**Resolution:** `sanitize_strings()` extended to strip all six extended characters in addition to standard control characters.

---

## BCP on Linux — Known Pitfalls

### Line Endings
BCP's default row terminator is `\r\n` (CRLF), but Linux generates `\n` (LF). This causes "Unexpected EOF" errors. **Resolution:** Explicit `-r 0x0A` flag for LF-only terminators.

### UTF-8 Encoding
The `-C 65001` codepage flag does not work on BCP for Linux. **Resolution:** Use mssql-tools18 v18.6.1.1+ which fixed this in December 2025. Alternatively, use `-w` (UTF-16) format or UTF-8 collation on SQL Server columns (2019+).

### BOM Corruption
UTF-8 files with BOM prefix (0xEF, 0xBB, 0xBF) store the BOM as data, corrupting the first field. **Resolution:** Always use UTF-8 without BOM.

---

## Column Ordering (P0-1)

BCP CSV has no header — column mapping is POSITIONAL. Source column order can vary between extractions. `reorder_columns_for_bcp()` enforces deterministic order by reading `INFORMATION_SCHEMA` ordinal position before every BCP write. Never use `SELECT *` in extraction queries. Never bypass column reordering.

---

## Format Files (W-13)

`generate_bcp_format_file()` in `bcp_csv.py` produces XML .fmt files from `INFORMATION_SCHEMA` metadata. `bcp_load()` accepts an optional `format_file` parameter for explicit column mapping. Uses character mode with tab delimiters and LF terminators per the BCP CSV Contract. `reorder_columns_for_bcp()` remains as defense-in-depth validation even when format files are used.

---

## Atomicity and Batch Size

### Bronze SCD2 Loads (E-3)
`bcp_load(atomic=True)` is required for Bronze — the entire INSERT is a single transaction. Without atomicity, a BCP failure partway through creates inconsistent Bronze state (some PKs with two active versions, others with none). Stage and ephemeral staging table loads use `atomic=False` for performance since they are ephemeral.

### Transaction Log Impact (B-2 edge case audit)
For tables with a clustered index, BCP is fully logged regardless of TABLOCK or recovery model. At 3M rows × ~200 bytes/row with logging overhead, expect 1.2–3 GB of transaction log per load. `_bulk_load_recovery_context()` sets BULK_LOGGED recovery during load window, restored to FULL with a log backup after.

### TABLOCK (B-3 edge case audit)
TABLOCK is not used in the pipeline's BCP command (no `-h` flag). Concurrent access is already allowed by default. TABLOCK acquires a bulk update lock that blocks all concurrent reads and writes — avoided by design.

---

## BIT Column Handling

SQL Server BIT columns require 0/1 values. Polars boolean True/False corrupts BCP imports. **Resolution:** Cast to Int8 before CSV write. `_cdc_is_current` and `UdmActiveFlag` are explicitly cast.

---

## UInt64 Handling

BCP and SQL Server cannot handle unsigned 64-bit integers. Non-hash UInt64 columns must be `.reinterpret(signed=True)` before writing. Values >2^63 would wrap to negative but this is a known trade-off for non-hash sources.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| P0-1 | Column order | Positional mapping requires deterministic order | reorder_columns_for_bcp() reads INFORMATION_SCHEMA |
| B-1 (audit) | Corruption | Embedded tabs/newlines corrupt CSV | sanitize_strings() + _validate_sanitized() |
| B-6 | Corruption | Extended Unicode line-breaks | sanitize_strings() strips \x0B, \x0C, \x85, \u2028, \u2029 |
| E-3 | Atomicity | Partial BCP loads break SCD2 | atomic=True for Bronze; atomic=False for Stage |
| W-13 | Mapping | Explicit column mapping via format files | generate_bcp_format_file() from INFORMATION_SCHEMA |
| B-2 (audit) | Logging | Full logging on non-empty clustered tables | bulk_load_recovery_context() provides BULK_LOGGED |
| B-3 (audit) | Locking | TABLOCK blocks concurrent access | TABLOCK not used; concurrent access allowed |

---

*Sources: Microsoft BCP documentation, Polars write_csv documentation (quote_style='never' warning), mssql-tools18 release notes (December 2025 UTF-8 fix), SQL Server transaction logging documentation.*
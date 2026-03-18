# File Extraction Pipeline Guide

The file extraction pipeline consumes Excel, CSV, text, and JSON files from network drives and processes them through the same Stage CDC + Bronze SCD2 medallion architecture as database sources.

## Quick Start

```bash
# List all configured file tables
python3 main_file_extract.py --list-tables

# Process all active file tables with 4 workers
python3 main_file_extract.py --workers 4

# Process a single file table
python3 main_file_extract.py --table RATES --source VENDOR_A

# Process all tables from a specific source
python3 main_file_extract.py --workers 4 --source VENDOR_A

# Force re-processing (skip extraction guard)
python3 main_file_extract.py --table RATES --source VENDOR_A --force
```

## Setup

### 1. Create the FileExtract Metadata Table

Run the DDL migration on your General database:

```bash
sqlcmd -S your_server -d General -i migrations/create_file_extract_table.sql
```

### 2. Install the fastexcel Dependency

```bash
pip install fastexcel>=0.9.0
```

Or install from the updated requirements.txt:

```bash
pip install -r requirements.txt
```

### 3. Register File Sources in FileExtract

Insert a row for each file (or each sheet of a multi-tab Excel file) you want to consume. See examples below.

## Supported File Types

| FileType | Description | Engine |
|----------|-------------|--------|
| `xlsx` | Excel 2007+ workbook | Polars + calamine (Rust-native) |
| `xls` | Legacy Excel workbook | Polars + calamine |
| `csv` | Comma/delimiter-separated | Polars read_csv |
| `txt` | Text files (delimited) | Polars read_csv (configurable separator) |
| `json` | JSON array of objects | Polars read_json |
| `ndjson` | Newline-delimited JSON | Polars read_ndjson |

## FileExtract Table Reference

| Column | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| SourceName | NVARCHAR(50) | Yes | — | Logical source name (e.g., `VENDOR_A`, `HR_EXPORTS`). Becomes the schema in UDM_Stage/UDM_Bronze. |
| TableName | NVARCHAR(128) | Yes | — | Logical table name (e.g., `RATES`, `EMPLOYEES`). Becomes the table name in UDM. |
| BasePath | NVARCHAR(500) | Yes | — | Directory where the file lives. Network drives should be mounted paths (e.g., `/mnt/f_drive/exports`). |
| FilePattern | NVARCHAR(255) | Yes | — | Glob pattern to match the file (e.g., `RATES_*.xlsx`, `daily_export.csv`). |
| FileType | NVARCHAR(10) | Yes | — | One of: `xlsx`, `xls`, `csv`, `txt`, `json`, `ndjson`. |
| SheetName | NVARCHAR(128) | No | First sheet | Excel sheet name to read. Required for multi-tab files. |
| HeaderRow | INT | No | 0 | 0-indexed row number where column headers are located. Set to 3 if headers are on the 4th row. |
| SkipRows | INT | No | 0 | Rows to skip AFTER the header (e.g., a units row or blank separator). |
| Delimiter | NVARCHAR(5) | No | `,` for csv | Field separator. Use `\t` for tab, `|` for pipe, `;` for semicolon. |
| Encoding | NVARCHAR(20) | No | `utf-8` | File encoding (e.g., `utf-8`, `latin-1`, `cp1252`). |
| ColumnMapping | NVARCHAR(MAX) | No | NULL | JSON object mapping file column names to target names: `{"File Col": "TargetCol"}`. |
| ColumnsToExtract | NVARCHAR(MAX) | No | NULL (all) | JSON array of columns to keep: `["col1", "col2"]`. Applied after ColumnMapping. |
| StageTableName | NVARCHAR(128) | No | TableName | Override Stage table name (without `_cdc` suffix). |
| BronzeTableName | NVARCHAR(128) | No | TableName | Override Bronze table name (without `_scd2_python` suffix). |
| PrimaryKeyColumns | NVARCHAR(500) | **Yes** | — | Comma-separated list of primary key columns. **Required** — files have no source database to discover PKs from. |
| ChangeMode | NVARCHAR(20) | No | `full_replace` | `full_replace` (standard CDC) or `append_only` (future). |
| ExpectedFrequency | NVARCHAR(20) | No | NULL | `daily`, `weekly`, `monthly`, `biannual`, `annual`. Informational. |
| ExpectedMinRows | INT | No | 1 | Minimum expected rows. Extraction fails if file has fewer. |
| ExpectedColumns | NVARCHAR(MAX) | No | NULL | JSON array of columns that must be present: `["col1", "col2"]`. |
| IsActive | BIT | No | 1 | Set to 0 to disable without deleting the row. |
| StageLoadTool | NVARCHAR(20) | No | `Python` | Must be `Python` for the pipeline to pick it up. |

## Table Naming

Files follow the same naming convention as database sources:

| Layer | Name |
|-------|------|
| Stage | `UDM_Stage.{SourceName}.{TableName}_cdc` |
| Bronze | `UDM_Bronze.{SourceName}.{TableName}_scd2_python` |

Example for `SourceName='VENDOR_A'`, `TableName='RATES'`:
- Stage: `UDM_Stage.VENDOR_A.RATES_cdc`
- Bronze: `UDM_Bronze.VENDOR_A.RATES_scd2_python`

## Examples

### Simple CSV File

A daily rates export at `/mnt/f_drive/vendor_a/rates_20260317.csv`:

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     PrimaryKeyColumns, ExpectedFrequency, ExpectedMinRows)
VALUES
    ('VENDOR_A', 'RATES', '/mnt/f_drive/vendor_a', 'rates_*.csv', 'csv',
     'RateCode,EffectiveDate', 'daily', 10);
```

### Pipe-Delimited Text File

A legacy system exports pipe-delimited `.txt` files:

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     Delimiter, PrimaryKeyColumns)
VALUES
    ('LEGACY_SYS', 'TRANSACTIONS', '/mnt/d_drive/legacy', 'txn_export_*.txt', 'txt',
     '|', 'TransactionId');
```

### Tab-Delimited Text File

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     Delimiter, Encoding, PrimaryKeyColumns)
VALUES
    ('MAINFRAME', 'ACCOUNTS', '/mnt/h_drive/mainframe', 'ACCT_*.txt', 'txt',
     '\t', 'cp1252', 'AccountNumber');
```

### Excel File — Header Not on First Row

A vendor sends an Excel file where row 0 is a title, row 1 is a date stamp, row 2 is blank, and row 3 has the actual column headers:

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     HeaderRow, PrimaryKeyColumns, ExpectedColumns)
VALUES
    ('VENDOR_B', 'INVENTORY', '/mnt/p_drive/vendor_b', 'inventory_*.xlsx', 'xlsx',
     3, 'ItemCode', '["ItemCode","Description","Quantity","UnitPrice"]');
```

`HeaderRow=3` tells the pipeline the 4th row (0-indexed) contains headers. Rows 0-2 are skipped automatically.

### Excel File — Header + Extra Row to Skip

Same as above, but row 4 contains units (`"—", "—", "ea", "USD"`) that should not be data:

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     HeaderRow, SkipRows, PrimaryKeyColumns)
VALUES
    ('VENDOR_B', 'INVENTORY', '/mnt/p_drive/vendor_b', 'inventory_*.xlsx', 'xlsx',
     3, 1, 'ItemCode');
```

`HeaderRow=3` + `SkipRows=1` means: headers on row 3, skip 1 row after headers (row 4 = units), data starts at row 5.

### Multi-Tab Excel File

A single Excel file `financials_2026Q1.xlsx` has three tabs: `Revenue`, `Expenses`, `Headcount`. Each tab becomes its own table:

```sql
-- Tab 1: Revenue
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     SheetName, PrimaryKeyColumns)
VALUES
    ('FINANCE', 'REVENUE', '/mnt/f_drive/finance', 'financials_*.xlsx', 'xlsx',
     'Revenue', 'CostCenter,Period');

-- Tab 2: Expenses
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     SheetName, PrimaryKeyColumns)
VALUES
    ('FINANCE', 'EXPENSES', '/mnt/f_drive/finance', 'financials_*.xlsx', 'xlsx',
     'Expenses', 'CostCenter,Period,ExpenseCategory');

-- Tab 3: Headcount
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     SheetName, PrimaryKeyColumns)
VALUES
    ('FINANCE', 'HEADCOUNT', '/mnt/f_drive/finance', 'financials_*.xlsx', 'xlsx',
     'Headcount', 'Department,Period');
```

All three rows point to the same file pattern but specify different `SheetName` values. Each tab gets its own Stage/Bronze table pair with independent CDC/SCD2 tracking.

### Excel with Column Mapping

File has columns like `"Item #"`, `"Qty on Hand"`, `"$ Unit Price"` that need cleaner names:

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     SheetName, ColumnMapping, PrimaryKeyColumns)
VALUES
    ('VENDOR_C', 'PRODUCTS', '/mnt/f_drive/vendor_c', 'products_*.xlsx', 'xlsx',
     'Sheet1',
     '{"Item #": "ItemCode", "Qty on Hand": "Quantity", "$ Unit Price": "UnitPrice"}',
     'ItemCode');
```

The `ColumnMapping` JSON renames columns from the file to clean target names. The `PrimaryKeyColumns` value (`ItemCode`) uses the mapped (target) name.

### JSON Array File

An API dumps a JSON file with an array of objects:

```json
[
    {"employee_id": "E001", "name": "Alice", "department": "Engineering"},
    {"employee_id": "E002", "name": "Bob", "department": "Sales"}
]
```

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     PrimaryKeyColumns)
VALUES
    ('HR_API', 'EMPLOYEES', '/mnt/d_drive/hr_exports', 'employees_*.json', 'json',
     'employee_id');
```

### Newline-Delimited JSON (NDJSON)

One JSON object per line — common for log exports and streaming data:

```
{"event_id": "evt_001", "timestamp": "2026-03-17T10:00:00Z", "type": "login"}
{"event_id": "evt_002", "timestamp": "2026-03-17T10:01:00Z", "type": "purchase"}
```

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     PrimaryKeyColumns)
VALUES
    ('EVENT_STREAM', 'USER_EVENTS', '/mnt/d_drive/events', 'events_*.ndjson', 'ndjson',
     'event_id');
```

### Selecting Specific Columns

A file has 50 columns but you only need 5:

```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     ColumnsToExtract, PrimaryKeyColumns)
VALUES
    ('VENDOR_A', 'RATES_SLIM', '/mnt/f_drive/vendor_a', 'rates_*.csv', 'csv',
     '["RateCode", "EffectiveDate", "Rate", "Currency", "Region"]',
     'RateCode,EffectiveDate');
```

`ColumnsToExtract` is applied after `ColumnMapping`, so use the mapped names.

## Data Flow

Each file table follows the same pipeline as database-sourced small tables:

```
File (Excel/CSV/TXT/JSON)
  → Polars read (calamine for Excel, native for CSV/JSON)
  → Apply column mapping (rename)
  → Select columns (if ColumnsToExtract configured)
  → Validate (min rows, expected columns, PK columns present)
  → add _row_hash (SHA-256 via polars-hash) + _extracted_at
  → Write BCP CSV (tab-delimited, per BCP CSV Contract)
  → Ensure Stage/Bronze tables exist (auto-create from DataFrame dtypes)
  → Schema evolution: detect new/removed/changed columns
  → Column sync: populate UdmTablesColumnsList with PKs from FileExtract
  → Empty extraction guard: skip if row count dropped >90% vs previous run
  → CDC promotion (hash comparison → inserts/updates/deletes)
  → SCD2 promotion (2-step INSERT + UPDATE to Bronze)
  → CSV cleanup
```

## How CDC Works with Files

The pipeline uses **hash-based CDC** — it computes a SHA-256 hash of every row and compares it against the existing Stage table. This means:

- **First run**: All rows are INSERTs into Stage and Bronze.
- **Subsequent runs (no changes)**: All row hashes match — 0 inserts, 0 updates, 0 deletes. The pipeline is idempotent.
- **Subsequent runs (with changes)**: New rows are INSERTs, modified rows are UPDATEs (detected by hash mismatch), rows missing from the file are DELETEs (soft delete).

This works identically to database sources — the downstream CDC and SCD2 engines don't know or care that the data came from a file.

## File Resolution

The pipeline uses glob matching to find the file:

1. Looks in `BasePath` for files matching `FilePattern`
2. If multiple files match, picks the **most recently modified** one
3. Logs which file was selected

This means you can have a directory with daily files (`rates_20260315.csv`, `rates_20260316.csv`, `rates_20260317.csv`) and the pipeline always picks the latest one without configuration changes.

## Observability

File pipeline events appear in `General.ops.PipelineEventLog` with the same event types as database sources:

- `EXTRACT` — time to read the file and write BCP CSV
- `CDC_PROMOTION` — hash comparison and change detection
- `SCD2_PROMOTION` — Bronze version management
- `CSV_CLEANUP` — temp file deletion
- `TABLE_TOTAL` — end-to-end wall time

Query example:
```sql
SELECT TableName, EventType, DurationMs, RowsProcessed, RowsInserted, RowsUpdated
FROM General.ops.PipelineEventLog
WHERE BatchId = 1042 AND SourceName = 'VENDOR_A'
ORDER BY StartedAt;
```

## Gotchas

- **PrimaryKeyColumns is mandatory.** Files have no database metadata to discover PKs from. The pipeline will not run CDC/SCD2 without PKs.
- **ColumnMapping uses file column names as keys.** If a file column is `"Item #"`, the mapping key must be exactly `"Item #"`. The mapped (target) names are what appear in Stage/Bronze.
- **HeaderRow is 0-indexed.** If your header is on the 4th physical row, set `HeaderRow=3`.
- **SkipRows skips rows AFTER the header.** Not from the top of the file. Use `HeaderRow` to skip junk before the header, `SkipRows` to skip junk after it.
- **Multi-tab Excel requires one FileExtract row per tab.** Each row must have a distinct `SheetName`. All rows can share the same `BasePath` and `FilePattern`.
- **JSON/NDJSON ignore HeaderRow and SkipRows.** These formats are self-describing.
- **File glob picks the most recent file.** If you need to process a specific historical file, temporarily change `FilePattern` to match exactly.
- **The extraction guard compares row counts across runs.** If a new file has >90% fewer rows than the last run, the pipeline blocks CDC to prevent accidental data wipes. Use `--force` to override.
- **Network drive availability.** If a mounted drive is temporarily unavailable, `BasePath` won't resolve and the table is skipped with a `FileNotFoundError`. Check mount status before investigating pipeline failures.

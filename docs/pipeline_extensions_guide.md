# Pipeline Extensions: File Extract & Network Drive Scanner

Two new pipelines extend the existing Oracle/SQL Server ETL to consume file-based sources and scan network drives for metadata. Both pipelines feed into the same medallion architecture (UDM_Stage CDC → UDM_Bronze SCD2) and reuse all existing infrastructure — CDC engine, SCD2 engine, BCP loading, event tracking, schema evolution, and column sync.

---

## Table of Contents

- [Overview](#overview)
- [What Was Built](#what-was-built)
- [Setup](#setup)
- [Pipeline 1: File Extract](#pipeline-1-file-extract)
- [Pipeline 2: Network Drive Scanner](#pipeline-2-network-drive-scanner)
- [Files Created](#files-created)
- [Files Modified](#files-modified)
- [Architecture: How It All Fits Together](#architecture-how-it-all-fits-together)
- [Shared Infrastructure Reused](#shared-infrastructure-reused)
- [Troubleshooting](#troubleshooting)

---

## Overview

| Pipeline | Purpose | Entry Point | Config Table |
|----------|---------|-------------|--------------|
| **File Extract** | Read Excel, CSV, text, and JSON files from network drives into Stage/Bronze | `main_file_extract.py` | `General.dbo.FileExtract` |
| **Network Drive Scanner** | Collect file metadata (size, owner, timestamps, permissions) across 7 mounted drives for cleanup dashboards | `main_drive_scanner.py` | `General.dbo.NetworkDriveConfig` |

Both pipelines are independent of each other and independent of the existing database pipelines (`main_small_tables.py`, `main_large_tables.py`). They can run on their own schedules.

---

## What Was Built

### New Files (10)

| File | Pipeline | Purpose |
|------|----------|---------|
| `main_file_extract.py` | File Extract | CLI entry point — `--workers`, `--table`, `--source`, `--list-tables`, `--force` |
| `orchestration/file_config.py` | File Extract | `FileConfig` dataclass + `FileConfigLoader` from `General.dbo.FileExtract` |
| `orchestration/file_tables.py` | File Extract | `process_file_table()` orchestrator — mirrors `process_small_table()` |
| `extract/file_extractor.py` | File Extract | `extract_file()` — reads Excel/CSV/text/JSON via Polars, writes BCP CSV |
| `migrations/create_file_extract_table.sql` | File Extract | DDL for `General.dbo.FileExtract` metadata table |
| `main_drive_scanner.py` | Drive Scanner | CLI entry point — `--workers`, `--drive`, `--list-drives`, `--force` |
| `orchestration/drive_config.py` | Drive Scanner | `DriveConfig` dataclass + `DriveConfigLoader` from `General.dbo.NetworkDriveConfig` |
| `orchestration/drive_tables.py` | Drive Scanner | `process_drive_table()` orchestrator — mirrors `process_file_table()` |
| `extract/drive_scanner.py` | Drive Scanner | `scan_drive()` — `os.walk()` + `os.stat()` metadata collector, never opens files |
| `migrations/create_network_drive_config_table.sql` | Drive Scanner | DDL for `General.dbo.NetworkDriveConfig` metadata table |

### Modified Files (4)

| File | Change |
|------|--------|
| `sources.py` | Added `SourceType.FILE` to the enum |
| `schema/column_sync.py` | Added `file_pk_columns` parameter to `sync_columns()` — skips source DB PK discovery for file/drive sources |
| `config.py` | Added `DRIVE_SCANNER_BATCH_SIZE` env var (default 50,000) |
| `requirements.txt` | Added `fastexcel>=0.9.0` for Rust-native Excel reading |

### Documentation (2)

| File | Content |
|------|---------|
| `docs/file_extraction_guide.md` | Detailed File Extract usage with SQL INSERT examples for every file type |
| `docs/pipeline_extensions_guide.md` | This file — covers both pipelines |

---

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

This adds `fastexcel>=0.9.0` (calamine Python binding for Polars Excel reading).

### 2. Create Metadata Tables

Run the DDL migrations on your General database:

```bash
# File Extract metadata table
sqlcmd -S $SQL_SERVER_HOST -d General -i migrations/create_file_extract_table.sql

# Network Drive Scanner metadata table
sqlcmd -S $SQL_SERVER_HOST -d General -i migrations/create_network_drive_config_table.sql
```

Both scripts are idempotent (`IF NOT EXISTS`).

### 3. Register Sources

Insert configuration rows into the appropriate metadata table. See the pipeline-specific sections below for examples.

---

## Pipeline 1: File Extract

### What It Does

Reads data from Excel, CSV, text, and JSON files on mounted network drives and loads it through the standard CDC + SCD2 pipeline. Files are consumed as full extracts — the entire file is read each run, and hash-based CDC detects what changed.

### Supported File Types

| FileType | Engine | Notes |
|----------|--------|-------|
| `xlsx` | Polars + calamine (Rust) | Multi-tab via `SheetName` |
| `xls` | Polars + calamine | Legacy Excel |
| `csv` | Polars `read_csv` | Configurable delimiter, encoding |
| `txt` | Polars `read_csv` | Same as CSV — set `Delimiter` for pipe, tab, etc. |
| `json` | Polars `read_json` | Array of objects or column-oriented |
| `ndjson` | Polars `read_ndjson` | One JSON object per line |

### CLI Usage

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

### Registering a File Source

Insert a row into `General.dbo.FileExtract` for each file (or each Excel sheet) you want to consume.

**Simple CSV:**
```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     PrimaryKeyColumns, ExpectedFrequency)
VALUES
    ('VENDOR_A', 'RATES', '/mnt/f_drive/vendor_a', 'rates_*.csv', 'csv',
     'RateCode,EffectiveDate', 'daily');
```

**Pipe-delimited text file:**
```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     Delimiter, PrimaryKeyColumns)
VALUES
    ('LEGACY', 'TRANSACTIONS', '/mnt/d_drive/legacy', 'txn_*.txt', 'txt',
     '|', 'TransactionId');
```

**Excel with header on row 3 and a units row to skip:**
```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     HeaderRow, SkipRows, PrimaryKeyColumns)
VALUES
    ('VENDOR_B', 'INVENTORY', '/mnt/p_drive/vendor_b', 'inventory_*.xlsx', 'xlsx',
     3, 1, 'ItemCode');
```

`HeaderRow=3` means headers are on the 4th row (0-indexed). `SkipRows=1` skips 1 row after the header (a units row) before data starts.

**Multi-tab Excel (one row per sheet):**
```sql
-- Revenue tab
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     SheetName, PrimaryKeyColumns)
VALUES
    ('FINANCE', 'REVENUE', '/mnt/f_drive/finance', 'financials_*.xlsx', 'xlsx',
     'Revenue', 'CostCenter,Period');

-- Expenses tab (same file, different sheet)
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     SheetName, PrimaryKeyColumns)
VALUES
    ('FINANCE', 'EXPENSES', '/mnt/f_drive/finance', 'financials_*.xlsx', 'xlsx',
     'Expenses', 'CostCenter,Period,ExpenseCategory');
```

**Excel with column renaming:**
```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType,
     ColumnMapping, PrimaryKeyColumns)
VALUES
    ('VENDOR_C', 'PRODUCTS', '/mnt/f_drive/vendor_c', 'products_*.xlsx', 'xlsx',
     '{"Item #": "ItemCode", "Qty on Hand": "Quantity", "$ Unit Price": "UnitPrice"}',
     'ItemCode');
```

**JSON array file:**
```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType, PrimaryKeyColumns)
VALUES
    ('HR_API', 'EMPLOYEES', '/mnt/d_drive/hr', 'employees_*.json', 'json', 'employee_id');
```

**Newline-delimited JSON:**
```sql
INSERT INTO General.dbo.FileExtract
    (SourceName, TableName, BasePath, FilePattern, FileType, PrimaryKeyColumns)
VALUES
    ('EVENTS', 'USER_EVENTS', '/mnt/d_drive/events', 'events_*.ndjson', 'ndjson', 'event_id');
```

### FileExtract Column Reference

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `SourceName` | Yes | — | Logical source name → becomes the UDM schema |
| `TableName` | Yes | — | Logical table name → becomes the UDM table |
| `BasePath` | Yes | — | Directory where the file lives (mounted path) |
| `FilePattern` | Yes | — | Glob pattern to find the file |
| `FileType` | Yes | — | `xlsx`, `xls`, `csv`, `txt`, `json`, `ndjson` |
| `SheetName` | No | First sheet | Excel sheet name for multi-tab files |
| `HeaderRow` | No | `0` | 0-indexed row with column headers |
| `SkipRows` | No | `0` | Rows to skip AFTER header (units row, etc.) |
| `Delimiter` | No | `,` for csv | Field separator (`\|`, `\t`, `;`, etc.) |
| `Encoding` | No | `utf-8` | File encoding (`latin-1`, `cp1252`, etc.) |
| `ColumnMapping` | No | NULL | JSON: `{"FileCol": "TargetCol"}` |
| `ColumnsToExtract` | No | NULL (all) | JSON: `["col1", "col2"]` — applied after mapping |
| `StageTableName` | No | TableName | Override Stage table name |
| `BronzeTableName` | No | TableName | Override Bronze table name |
| `PrimaryKeyColumns` | **Yes** | — | Comma-separated PKs (**mandatory** — files have no database PKs) |
| `ChangeMode` | No | `full_replace` | `full_replace` or `append_only` |
| `ExpectedFrequency` | No | NULL | `daily`, `weekly`, `monthly` (informational) |
| `ExpectedMinRows` | No | `1` | Fail extraction if file has fewer rows |
| `ExpectedColumns` | No | NULL | JSON: `["col1", "col2"]` — must be present or fail |
| `IsActive` | No | `1` | Set to 0 to disable without deleting |
| `StageLoadTool` | No | `Python` | Must be `Python` to be picked up |

### Table Naming

| Layer | Name |
|-------|------|
| Stage | `UDM_Stage.{SourceName}.{TableName}_cdc` |
| Bronze | `UDM_Bronze.{SourceName}.{TableName}_scd2_python` |

Example: `SourceName='VENDOR_A'`, `TableName='RATES'` →
- Stage: `UDM_Stage.VENDOR_A.RATES_cdc`
- Bronze: `UDM_Bronze.VENDOR_A.RATES_scd2_python`

### File Resolution

When multiple files match the glob pattern, the pipeline picks the **most recently modified** file. This means daily files like `rates_20260315.csv`, `rates_20260316.csv`, `rates_20260317.csv` work without changing the configuration.

---

## Pipeline 2: Network Drive Scanner

### What It Does

Walks 7 mounted network drives and collects file-level metadata (size, owner, timestamps, permissions, extension, depth) for every file on every drive. **Never opens or modifies any file** — only uses `os.stat()` and `os.walk()`. The metadata is loaded through CDC + SCD2 so changes are tracked over time:

- **New file appears** → CDC INSERT → SCD2 new row
- **File metadata changes** (size, mtime, permissions, owner) → CDC UPDATE → SCD2 new version
- **File deleted** → CDC DELETE → SCD2 close row

This data powers dashboards for drive cleanup, storage analysis, and governance.

### Metadata Columns Captured (20)

| Column | Type | Description |
|--------|------|-------------|
| `drive_name` | NVARCHAR(128) | Logical drive name from config |
| `full_file_path` | NVARCHAR(2048) | Relative path from mount root (**PK**) |
| `file_name` | NVARCHAR(512) | File name with extension |
| `file_extension` | NVARCHAR(64) | Extension lowercased (`.xlsx`, `.pdf`, etc.) |
| `parent_directory` | NVARCHAR(2048) | Immediate parent directory path |
| `directory_depth` | INT | Depth from mount root (for hierarchy analysis) |
| `file_size_bytes` | BIGINT | File size in bytes |
| `created_time` | DATETIME2 | Birth time (Linux: metadata change time) |
| `modified_time` | DATETIME2 | Last content modification |
| `accessed_time` | DATETIME2 | Last access time (may be unreliable with noatime) |
| `owner_uid` | INT | Numeric UID |
| `owner_name` | NVARCHAR(256) | Resolved username (empty on CIFS mounts) |
| `group_gid` | INT | Numeric GID |
| `group_name` | NVARCHAR(256) | Resolved group name |
| `permissions_octal` | NVARCHAR(10) | Octal permissions (`0o755`) |
| `is_symlink` | BIT | 1 if symbolic link |
| `is_hidden` | BIT | 1 if filename starts with `.` |
| `inode` | BIGINT | Inode number (detects renames — same inode, different path) |
| `hard_link_count` | INT | Number of hard links |
| `mount_path` | NVARCHAR(1024) | Linux mount point from config |

**Primary Key:** `drive_name` + `full_file_path`

### CLI Usage

```bash
# List all configured drives
python3 main_drive_scanner.py --list-drives

# Scan all 7 drives in parallel (one worker per drive)
python3 main_drive_scanner.py --workers 7

# Scan a single drive
python3 main_drive_scanner.py --drive FINANCE_SHARE

# Force scan (skip extraction guard — use if mount was temporarily offline)
python3 main_drive_scanner.py --drive FINANCE_SHARE --force
```

### Registering Network Drives

Insert one row per mounted drive into `General.dbo.NetworkDriveConfig`:

```sql
-- F: drive — Finance
INSERT INTO General.dbo.NetworkDriveConfig
    (DriveName, MountPath, UNCPath, TableName, ExcludePatterns)
VALUES
    ('F_DRIVE', '/mnt/f_drive', '\\fileserver\finance$', 'F_DRIVE_FILES',
     '["*.tmp", "~$*", "Thumbs.db", "desktop.ini", ".DS_Store"]');

-- D: drive — Data
INSERT INTO General.dbo.NetworkDriveConfig
    (DriveName, MountPath, UNCPath, TableName, ExcludePatterns)
VALUES
    ('D_DRIVE', '/mnt/d_drive', '\\fileserver\data$', 'D_DRIVE_FILES',
     '["*.tmp", "~$*", "Thumbs.db"]');

-- H: drive — HR
INSERT INTO General.dbo.NetworkDriveConfig
    (DriveName, MountPath, UNCPath, TableName, ExcludePatterns)
VALUES
    ('H_DRIVE', '/mnt/h_drive', '\\fileserver\hr$', 'H_DRIVE_FILES',
     '["*.tmp", "~$*", "Thumbs.db"]');

-- P: drive — Projects
INSERT INTO General.dbo.NetworkDriveConfig
    (DriveName, MountPath, UNCPath, TableName, ExcludePatterns, MaxDepth)
VALUES
    ('P_DRIVE', '/mnt/p_drive', '\\fileserver\projects$', 'P_DRIVE_FILES',
     '["*.tmp", "~$*", "Thumbs.db", "node_modules"]', 20);
```

All drives default to `SourceName='NETWORK_DRIVES'`, which means their UDM tables are:
- Stage: `UDM_Stage.NETWORK_DRIVES.{TableName}_cdc`
- Bronze: `UDM_Bronze.NETWORK_DRIVES.{TableName}_scd2_python`

### NetworkDriveConfig Column Reference

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `DriveName` | Yes | — | Logical drive name (unique identifier) |
| `MountPath` | Yes | — | Linux mount point (`/mnt/f_drive`) |
| `UNCPath` | No | NULL | Windows UNC for reference (`\\server\share$`) |
| `SourceName` | No | `NETWORK_DRIVES` | UDM schema name — shared across all drives |
| `TableName` | Yes | — | UDM table name — one per drive |
| `StageTableName` | No | TableName | Override Stage table name |
| `BronzeTableName` | No | TableName | Override Bronze table name |
| `ExcludePatterns` | No | NULL | JSON: `["*.tmp", "~$*", "Thumbs.db"]` |
| `IncludePatterns` | No | NULL (all files) | JSON: `["*.pdf", "*.xlsx"]` — only scan these |
| `MaxDepth` | No | NULL (unlimited) | Max directory recursion depth |
| `FollowSymlinks` | No | `0` | Set to 1 to follow symbolic links (risk of loops) |
| `ScanEnabled` | No | `1` | Set to 0 to temporarily stop scanning this drive |
| `IsActive` | No | `1` | Set to 0 to disable |
| `PrimaryKeyColumns` | No | `drive_name,full_file_path` | PK columns for CDC/SCD2 |

### Dashboard Query Examples

**Largest files across all drives:**
```sql
SELECT TOP 100
    drive_name, full_file_path, file_name, file_extension,
    file_size_bytes / 1048576.0 AS size_mb,
    modified_time, owner_name
FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python
WHERE UdmActiveFlag = 1
ORDER BY file_size_bytes DESC;
```

**Files not modified in over 2 years (cleanup candidates):**
```sql
SELECT drive_name, full_file_path, file_name,
    file_size_bytes / 1048576.0 AS size_mb,
    modified_time, owner_name
FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python
WHERE UdmActiveFlag = 1
  AND modified_time < DATEADD(YEAR, -2, GETUTCDATE())
ORDER BY file_size_bytes DESC;
```

**Storage by file extension:**
```sql
SELECT file_extension,
    COUNT(*) AS file_count,
    SUM(file_size_bytes) / 1073741824.0 AS total_gb
FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python
WHERE UdmActiveFlag = 1
GROUP BY file_extension
ORDER BY total_gb DESC;
```

**Storage by top-level directory:**
```sql
SELECT
    LEFT(parent_directory, CHARINDEX('\', parent_directory + '\', 2) - 1) AS top_folder,
    COUNT(*) AS file_count,
    SUM(file_size_bytes) / 1073741824.0 AS total_gb
FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python
WHERE UdmActiveFlag = 1
GROUP BY LEFT(parent_directory, CHARINDEX('\', parent_directory + '\', 2) - 1)
ORDER BY total_gb DESC;
```

**Files deleted since last scan (via SCD2 history):**
```sql
SELECT drive_name, full_file_path, file_name, file_size_bytes,
    UdmEffectiveDateTime AS deleted_at
FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python
WHERE UdmActiveFlag = 0
  AND UdmScd2Operation = 'D'
  AND UdmEndDateTime > DATEADD(DAY, -7, GETUTCDATE())
ORDER BY UdmEndDateTime DESC;
```

**Duplicate files (same name + size, different paths):**
```sql
WITH dupes AS (
    SELECT file_name, file_size_bytes, COUNT(*) AS cnt
    FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python
    WHERE UdmActiveFlag = 1
    GROUP BY file_name, file_size_bytes
    HAVING COUNT(*) > 1
)
SELECT f.drive_name, f.full_file_path, f.file_name,
    f.file_size_bytes / 1048576.0 AS size_mb
FROM UDM_Bronze.NETWORK_DRIVES.F_DRIVE_FILES_scd2_python f
JOIN dupes d ON f.file_name = d.file_name AND f.file_size_bytes = d.file_size_bytes
WHERE f.UdmActiveFlag = 1
ORDER BY f.file_name, f.full_file_path;
```

---

## Files Created

### `main_file_extract.py`

CLI entry point for the File Extract pipeline. Follows the exact same pattern as `main_small_tables.py`: argparse, ProcessPoolExecutor for parallel execution, config serialization via `_file_config_to_dict()` for cross-process pickling, startup checks, shutdown, and exit codes.

### `main_drive_scanner.py`

CLI entry point for the Drive Scanner pipeline. Same pattern as above but defaults to `--workers 7` (one per drive). Uses `--drive` instead of `--table`/`--source` for filtering.

### `orchestration/file_config.py`

`FileConfig` dataclass that duck-types the `TableConfig` interface. Provides all 26 properties that shared pipeline functions access (`source_object_name`, `stage_full_table_name`, `pk_columns`, `is_oracle`, etc.) so CDC, SCD2, schema evolution, and column sync work without modification.

`FileConfigLoader` reads from `General.dbo.FileExtract` using parameterized pyodbc queries (H-3), parses JSON columns (`ColumnMapping`, `ColumnsToExtract`, `ExpectedColumns`), resolves schema casing, and attaches column metadata from `UdmTablesColumnsList`.

### `orchestration/drive_config.py`

Same duck-typing pattern as `FileConfig` but for drive scans. `DriveConfig` defaults PK to `["drive_name", "full_file_path"]`. `DriveConfigLoader` reads from `General.dbo.NetworkDriveConfig`.

### `extract/file_extractor.py`

`extract_file(file_config, output_dir)` → `(DataFrame, Path)`

Resolves files via glob pattern (picks most recent), validates accessibility, reads via the appropriate Polars reader, applies column mapping and selection, validates data, then produces a BCP CSV via `prepare_dataframe_for_bcp()` + `write_bcp_csv()`.

Key reader details:
- **Excel:** Uses calamine engine with `read_options` for header positioning. `HeaderRow` maps to calamine's `skip_rows` + `header_row=0`. `SkipRows` is applied by DataFrame slicing after read (calamine doesn't support post-header skipping natively).
- **CSV/text:** Maps `HeaderRow` → `skip_rows`, `SkipRows` → `skip_rows_after_header`.
- **JSON/NDJSON:** Direct Polars readers. `HeaderRow`/`SkipRows` are ignored.

### `extract/drive_scanner.py`

`scan_drive(drive_config, output_dir)` → `(DataFrame, Path)`

Walks a mount point via `os.walk()`, calls `os.stat()` / `os.lstat()` per file, and collects 20 metadata columns into a Polars DataFrame. **Never opens any file.**

Key design:
- **Memory-bounded:** Accumulates metadata in batches of 50,000 rows (configurable via `DRIVE_SCANNER_BATCH_SIZE`), converting each batch to a Polars DataFrame chunk. Final `pl.concat()` produces the result.
- **Error handling:** `PermissionError` → skip + log at DEBUG. `FileNotFoundError` (race condition) → skip silently. `OSError` (broken symlink, network glitch) → skip + log WARNING. Walk errors → `onerror` callback.
- **Directory pruning:** Always skips `$RECYCLE.BIN`, `System Volume Information`, `.snapshot`. User-configured `ExcludePatterns` prune additional directories and files via `fnmatch`.
- **Owner resolution:** `pwd.getpwuid()` / `grp.getgrgid()` with a per-process cache dict. Falls back to empty string on CIFS mounts where UIDs map to generic accounts.
- **Symlinks:** `os.lstat()` detects symlinks. `os.stat()` follows them for metadata. `FollowSymlinks=0` (default) prevents `os.walk()` from descending into symlinked directories.

### `orchestration/file_tables.py`

`process_file_table(file_config, event_tracker, output_dir, force)` → `bool`

Mirrors `process_small_table()` step for step: table lock → extract → memory guard → ensure tables → schema evolution → column sync → extraction guard → CDC → SCD2 → cleanup → release lock. Skips E-11 source schema validation (files don't have a prior source schema).

### `orchestration/drive_tables.py`

`process_drive_table(drive_config, event_tracker, output_dir, force)` → `bool`

Same structure as `process_file_table()`. The extraction guard is especially critical here — if a mount drops, the scan returns 0 files, and without the guard CDC would soft-delete every file record.

### `migrations/create_file_extract_table.sql`

Idempotent DDL (`IF NOT EXISTS`) for `General.dbo.FileExtract` — 24 columns with a unique constraint on `(SourceName, TableName)`.

### `migrations/create_network_drive_config_table.sql`

Idempotent DDL (`IF NOT EXISTS`) for `General.dbo.NetworkDriveConfig` — 17 columns with a unique constraint on `(DriveName)`.

---

## Files Modified

### `sources.py`

Added `FILE = "FILE"` to the `SourceType` enum. File and drive sources don't register a `SourceSystem` entry in `_SOURCES` since there's no database connection. The `is_oracle` / `is_sql_server` properties on `FileConfig` and `DriveConfig` return `False` directly.

### `schema/column_sync.py`

Added `file_pk_columns: list[str] | None = None` parameter to `sync_columns()`. When provided, the function skips `_discover_pks()` (files and drives have no source database to query) and uses the explicit PK list directly in `_update_pk_flags()`. All existing callers are unaffected — the parameter defaults to `None`.

### `config.py`

Added one new environment variable:

```python
DRIVE_SCANNER_BATCH_SIZE = int(os.getenv("DRIVE_SCANNER_BATCH_SIZE", "50000"))
```

Controls how many file metadata rows are accumulated in memory before converting to a Polars DataFrame chunk during drive scanning. 50,000 rows ≈ 5 MB per batch.

### `requirements.txt`

Added `fastexcel>=0.9.0` — the calamine Python binding that Polars uses for Rust-native Excel reading. Handles both `.xlsx` and `.xls` formats.

---

## Architecture: How It All Fits Together

Both new pipelines follow the same duck-typing pattern established by `TableConfig`:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLI Entry Points                             │
│  main_small_tables.py   main_file_extract.py   main_drive_scanner.py│
└──────────┬──────────────────────┬─────────────────────┬─────────────┘
           │                      │                     │
           ▼                      ▼                     ▼
┌──────────────────┐  ┌───────────────────┐  ┌──────────────────────┐
│   TableConfig    │  │    FileConfig     │  │     DriveConfig      │
│ (UdmTablesList)  │  │  (FileExtract)    │  │ (NetworkDriveConfig) │
└────────┬─────────┘  └────────┬──────────┘  └──────────┬───────────┘
         │ same interface      │ same interface          │ same interface
         ▼                     ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Shared Pipeline Functions                         │
│  ensure_stage_table()    run_cdc_promotion()    sync_columns()      │
│  ensure_bronze_table()   run_scd2_promotion()   evolve_schema()     │
│  acquire_table_lock()    cleanup_csvs()         extraction guard    │
│  PipelineEventTracker    SqlServerLogHandler     BCP loading        │
└─────────────────────────────────────────────────────────────────────┘
         │                     │                         │
         ▼                     ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│              UDM_Stage (CDC)  →  UDM_Bronze (SCD2)                  │
└─────────────────────────────────────────────────────────────────────┘
```

The key insight is that `FileConfig` and `DriveConfig` expose the same property interface as `TableConfig` (26 properties including `source_object_name`, `stage_full_table_name`, `pk_columns`, `is_oracle`, etc.). This means every shared function — CDC engine, SCD2 engine, BCP loading, schema evolution, table creation, index management, event tracking — works without any modifications.

---

## Shared Infrastructure Reused

These existing modules are called directly by the new pipelines without any changes:

| Module | Functions Used |
|--------|---------------|
| `data_load/bcp_csv.py` | `prepare_dataframe_for_bcp()`, `write_bcp_csv()` |
| `data_load/row_hash.py` | `add_row_hash()` (called by prepare_dataframe_for_bcp) |
| `cdc/engine.py` | `run_cdc()` |
| `scd2/engine.py` | `run_scd2()` |
| `orchestration/pipeline_steps.py` | `run_cdc_promotion()`, `run_scd2_promotion()`, `cleanup_csvs()` |
| `orchestration/guards.py` | `run_extraction_guard()` |
| `orchestration/table_lock.py` | `acquire_table_lock()`, `release_table_lock()` |
| `schema/table_creator.py` | `ensure_stage_table()`, `ensure_bronze_table()`, index functions |
| `schema/evolution.py` | `evolve_schema()` |
| `schema/column_sync.py` | `sync_columns()` (with new `file_pk_columns` param) |
| `observability/event_tracker.py` | `PipelineEventTracker` |
| `observability/log_handler.py` | `SqlServerLogHandler` |
| `connections.py` | `cursor_for()`, `resolve_schema_name()` |
| `cli_common.py` | `setup_logging()`, `startup_checks()`, `shutdown_connections()` |

---

## Troubleshooting

### File Extract

| Problem | Cause | Fix |
|---------|-------|-----|
| `FileNotFoundError` on extraction | No file matches `FilePattern` in `BasePath` | Check that the mount is accessible and the pattern is correct |
| `ValueError: missing PK columns` | PK columns listed in `PrimaryKeyColumns` not found in file | Check column names — if using `ColumnMapping`, PKs use the **mapped** (target) names |
| All rows show as CDC updates on every run | Column names or order changed between file versions | Check `ColumnMapping` and `ExpectedColumns` — stabilize the column names |
| `Extraction row count dropped >90%` | New file has far fewer rows than previous | Use `--force` if intentional, or investigate the source file |
| Empty DataFrame from Excel | `HeaderRow` is wrong — Polars is reading junk rows as data | Set `HeaderRow` to the 0-indexed row number containing actual column headers |

### Drive Scanner

| Problem | Cause | Fix |
|---------|-------|-----|
| `FileNotFoundError: Mount path does not exist` | Network drive not mounted | Check mount: `ls /mnt/f_drive`. Remount if needed. |
| `Extraction row count dropped >90%` | Mount was temporarily offline during scan | Remount the drive and re-run with `--force` |
| Empty scan (0 files) | Mount exists but is empty or permissions block `os.walk()` | Check `ls -la /mnt/f_drive` and verify read/execute permissions |
| Very slow scan | Network latency or millions of files | Each `os.stat()` is a network round-trip on CIFS. Normal for large drives. Progress logs every 50K files. |
| `owner_name` is always empty | CIFS mount maps all UIDs to a single user | Expected on Windows shares. Use `owner_uid` for grouping instead. |
| Scan picks up temp files (`~$*.xlsx`) | Office lock files not excluded | Add `"~$*"` to `ExcludePatterns` in `NetworkDriveConfig` |
| Memory usage too high | Drive has tens of millions of files | Reduce `DRIVE_SCANNER_BATCH_SIZE` or split the drive into multiple configs by subdirectory |

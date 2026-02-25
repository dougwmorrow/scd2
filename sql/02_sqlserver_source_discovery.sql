/*
=============================================================================
  02_sqlserver_source_discovery.sql
  
  SQL Server Source PK, Index, and Key Discovery (CCM / EPICOR)
  
  Purpose:
    Discover primary keys, unique constraints, all indexes, and identify
    PK candidates for SQL Server source tables and views. Used to
    populate/validate UdmTablesColumnsList.IsPrimaryKey and identify
    index optimization opportunities.
  
  Run against: CCM or EPICOR source SQL Server database
  
  Usage: 
    Replace @SourceSchema with the target schema (e.g., 'dbo')
    Run each section independently or as a batch.
  
  Sections:
    1. Tables — PK constraints, all indexes
    2. Views  — column analysis, PK candidates
    3. Date column index coverage (large table extraction)
    4. Objects without any uniqueness guarantee
    5. Index usage statistics (identify unused indexes)
=============================================================================
*/

DECLARE @SourceSchema NVARCHAR(128) = 'dbo';  -- Change per source


-- ==========================================================================
-- SECTION 1: TABLE PRIMARY KEY CONSTRAINTS
-- Maps directly to UdmTablesColumnsList.IsPrimaryKey.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 1: TABLE PRIMARY KEY CONSTRAINTS'
PRINT '==================================================================='

SELECT 
    t.TABLE_NAME,
    'PK_CONSTRAINT' AS KEY_TYPE,
    kcu.CONSTRAINT_NAME,
    STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) AS PK_COLUMNS,
    COUNT(kcu.COLUMN_NAME) AS COLUMN_COUNT
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
JOIN INFORMATION_SCHEMA.TABLES t
    ON tc.TABLE_NAME = t.TABLE_NAME
    AND tc.TABLE_SCHEMA = t.TABLE_SCHEMA
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_SCHEMA = @SourceSchema
  AND t.TABLE_TYPE = 'BASE TABLE'
GROUP BY t.TABLE_NAME, kcu.CONSTRAINT_NAME
ORDER BY t.TABLE_NAME;


-- ==========================================================================
-- SECTION 2: UNIQUE CONSTRAINTS (NON-PK)
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 2: UNIQUE CONSTRAINTS (NON-PK)'
PRINT '==================================================================='

SELECT 
    tc.TABLE_NAME,
    'UNIQUE_CONSTRAINT' AS KEY_TYPE,
    tc.CONSTRAINT_NAME,
    STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) AS UNIQUE_COLUMNS,
    COUNT(kcu.COLUMN_NAME) AS COLUMN_COUNT
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
  AND tc.TABLE_SCHEMA = @SourceSchema
GROUP BY tc.TABLE_NAME, tc.CONSTRAINT_NAME
ORDER BY tc.TABLE_NAME;


-- ==========================================================================
-- SECTION 3: ALL INDEXES (COMPREHENSIVE)
-- Every index on every table in the schema with full detail.
-- Includes: clustered/nonclustered, unique, filtered, included columns.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 3: ALL INDEXES (COMPREHENSIVE)'
PRINT '==================================================================='

SELECT 
    OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,           -- CLUSTERED, NONCLUSTERED, HEAP, etc.
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    i.is_unique_constraint AS IsUniqueConstraint,
    i.is_disabled AS IsDisabled,
    i.has_filter AS HasFilter,
    i.filter_definition AS FilterDefinition,
    -- Key columns
    STRING_AGG(
        CASE WHEN ic.is_included_column = 0 
             THEN COL_NAME(ic.object_id, ic.column_id) 
        END, ', '
    ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyColumns,
    -- Included columns
    STRING_AGG(
        CASE WHEN ic.is_included_column = 1 
             THEN COL_NAME(ic.object_id, ic.column_id) 
        END, ', '
    ) WITHIN GROUP (ORDER BY ic.index_column_id) AS IncludedColumns,
    -- Stats
    p.rows AS ApproxRowCount,
    SUM(a.total_pages) * 8 / 1024.0 AS IndexSizeMB,
    s.last_user_seek,
    s.last_user_scan,
    s.last_user_lookup,
    s.user_seeks + s.user_scans + s.user_lookups AS TotalReads,
    s.user_updates AS TotalWrites
FROM sys.indexes i
JOIN sys.index_columns ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
JOIN sys.objects o
    ON i.object_id = o.object_id
LEFT JOIN sys.partitions p
    ON i.object_id = p.object_id
    AND i.index_id = p.index_id
LEFT JOIN sys.allocation_units a
    ON p.partition_id = a.container_id
LEFT JOIN sys.dm_db_index_usage_stats s
    ON i.object_id = s.object_id
    AND i.index_id = s.index_id
    AND s.database_id = DB_ID()
WHERE OBJECT_SCHEMA_NAME(i.object_id) = @SourceSchema
  AND o.type IN ('U', 'V')              -- Tables and indexed views
  AND i.type > 0                         -- Exclude heaps (type=0)
GROUP BY 
    i.object_id, i.name, i.type_desc, i.is_unique, i.is_primary_key,
    i.is_unique_constraint, i.is_disabled, i.has_filter, i.filter_definition,
    p.rows, s.last_user_seek, s.last_user_scan, s.last_user_lookup,
    s.user_seeks, s.user_scans, s.user_lookups, s.user_updates
ORDER BY OBJECT_NAME(i.object_id), i.name;


-- ==========================================================================
-- SECTION 4: OBJECT TYPE IDENTIFICATION (TABLE vs VIEW)
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 4: OBJECT TYPES (TABLE vs VIEW)'
PRINT '==================================================================='

SELECT 
    o.name AS ObjectName,
    o.type_desc AS ObjectType,
    o.create_date,
    o.modify_date,
    p.rows AS ApproxRowCount,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM sys.indexes i 
            WHERE i.object_id = o.object_id AND i.is_primary_key = 1
        ) THEN 'YES'
        ELSE 'NO'
    END AS HasPrimaryKey,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM sys.indexes i 
            WHERE i.object_id = o.object_id AND i.is_unique = 1
        ) THEN 'YES'
        ELSE 'NO'
    END AS HasUniqueIndex,
    (SELECT COUNT(*) FROM sys.indexes i 
     WHERE i.object_id = o.object_id AND i.type > 0) AS IndexCount
FROM sys.objects o
LEFT JOIN sys.partitions p
    ON o.object_id = p.object_id
    AND p.index_id IN (0, 1)
WHERE o.schema_id = SCHEMA_ID(@SourceSchema)
  AND o.type IN ('U', 'V')  -- User tables and views
ORDER BY o.type_desc, o.name;


-- ==========================================================================
-- SECTION 5: VIEW COLUMN ANALYSIS — PK CANDIDATES
-- For views, analyze columns to identify likely PK candidates.
-- Checks: nullability, naming patterns, data types.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 5: VIEW COLUMNS — PK CANDIDATE ANALYSIS'
PRINT '==================================================================='

SELECT 
    v.name AS ViewName,
    c.name AS ColumnName,
    c.column_id AS Ordinal,
    TYPE_NAME(c.user_type_id) AS DataType,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    c.is_computed,
    CASE
        WHEN c.is_nullable = 0 AND c.is_identity = 1
        THEN 'HIGH — identity column, not nullable'
        WHEN c.is_nullable = 0 AND (
            c.name LIKE '%[_]ID' OR c.name LIKE '%[_]Id'
            OR c.name LIKE '%[_]KEY' OR c.name LIKE '%[_]Key'
            OR c.name LIKE '%[_]CODE' OR c.name LIKE '%[_]Code'
            OR c.name LIKE '%[_]NUM' OR c.name LIKE '%[_]Num'
            OR c.name LIKE '%[_]NO' OR c.name LIKE '%[_]No'
            OR c.name LIKE '%[_]NBR' OR c.name LIKE '%[_]Nbr'
            OR c.name LIKE 'ID[_]%' OR c.name = 'ID' OR c.name = 'Id'
            OR c.name LIKE '%[_]SEQ' OR c.name LIKE '%[_]Seq'
            OR c.name LIKE '%[_]NUMBER' OR c.name LIKE '%[_]Number'
            OR c.name LIKE '%ID' OR c.name LIKE '%Key'
        ) THEN 'HIGH — naming pattern + not nullable'
        WHEN c.is_nullable = 0 AND TYPE_NAME(c.user_type_id) IN ('int', 'bigint', 'smallint', 'uniqueidentifier')
        THEN 'MEDIUM — numeric/guid, not nullable'
        WHEN c.is_nullable = 0
        THEN 'MEDIUM — not nullable'
        ELSE 'LOW — nullable'
    END AS PK_CANDIDATE_SCORE,
    CASE
        WHEN c.is_nullable = 0 AND (
            c.name LIKE '%[_]ID' OR c.name LIKE '%[_]Id'
            OR c.name LIKE '%[_]KEY' OR c.name LIKE '%[_]Key'
            OR c.name = 'ID' OR c.name = 'Id'
            OR c.is_identity = 1
        ) THEN '** LIKELY PK **'
        ELSE ''
    END AS RECOMMENDATION
FROM sys.views v
JOIN sys.columns c
    ON v.object_id = c.object_id
WHERE v.schema_id = SCHEMA_ID(@SourceSchema)
ORDER BY v.name, c.column_id;


-- ==========================================================================
-- SECTION 6: VIEW DEFINITIONS (SOURCE SQL)
-- Retrieves view SQL for manual analysis of base tables and join keys.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 6: VIEW DEFINITIONS (SOURCE SQL)'
PRINT '==================================================================='

SELECT 
    v.name AS ViewName,
    m.definition AS ViewSQL
FROM sys.views v
JOIN sys.sql_modules m
    ON v.object_id = m.object_id
WHERE v.schema_id = SCHEMA_ID(@SourceSchema)
ORDER BY v.name;


-- ==========================================================================
-- SECTION 7: INDEXED VIEWS (MATERIALIZED)
-- SQL Server indexed views have a unique clustered index — these already
-- have a guaranteed PK equivalent.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 7: INDEXED VIEWS'
PRINT '==================================================================='

SELECT 
    v.name AS ViewName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique,
    STRING_AGG(
        COL_NAME(ic.object_id, ic.column_id), ', '
    ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyColumns,
    p.rows AS ApproxRowCount
FROM sys.views v
JOIN sys.indexes i
    ON v.object_id = i.object_id
JOIN sys.index_columns ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
    AND ic.is_included_column = 0
LEFT JOIN sys.partitions p
    ON v.object_id = p.object_id
    AND i.index_id = p.index_id
WHERE v.schema_id = SCHEMA_ID(@SourceSchema)
GROUP BY v.name, i.name, i.type_desc, i.is_unique, p.rows
ORDER BY v.name, i.name;


-- ==========================================================================
-- SECTION 8: DATE COLUMN INDEX COVERAGE
-- For large table windowed extraction, identifies date/datetime columns
-- and whether they have indexes suitable for range predicates.
-- Maps to UdmTablesList.SourceAggregateColumnName.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 8: DATE COLUMN INDEX COVERAGE (LARGE TABLE EXTRACTION)'
PRINT '==================================================================='

SELECT 
    OBJECT_NAME(c.object_id) AS TableName,
    c.name AS DateColumn,
    TYPE_NAME(c.user_type_id) AS DataType,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique,
    ic.key_ordinal AS ColumnPositionInIndex,
    CASE
        WHEN i.type_desc = 'NONCLUSTERED' AND ic.key_ordinal = 1
        THEN '** IDEAL — leading column of nonclustered index **'
        WHEN i.type_desc = 'CLUSTERED' AND ic.key_ordinal = 1
        THEN '** GOOD — leading column of clustered index **'
        WHEN ic.key_ordinal > 1
        THEN 'USABLE — not leading column'
        WHEN i.name IS NULL
        THEN 'NO INDEX — consider adding for windowed extraction'
        ELSE 'REVIEW'
    END AS ASSESSMENT
FROM sys.columns c
JOIN sys.objects o
    ON c.object_id = o.object_id
LEFT JOIN sys.index_columns ic
    ON c.object_id = ic.object_id
    AND c.column_id = ic.column_id
    AND ic.is_included_column = 0
LEFT JOIN sys.indexes i
    ON ic.object_id = i.object_id
    AND ic.index_id = i.index_id
WHERE o.schema_id = SCHEMA_ID(@SourceSchema)
  AND o.type IN ('U', 'V')
  AND TYPE_NAME(c.user_type_id) IN (
      'date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset'
  )
ORDER BY OBJECT_NAME(c.object_id), c.name, i.name;


-- ==========================================================================
-- SECTION 9: TABLES/VIEWS WITH NO UNIQUENESS GUARANTEE
-- Highest risk for CDC — need manual IsPrimaryKey in UdmTablesColumnsList.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 9: OBJECTS WITHOUT ANY UNIQUENESS GUARANTEE'
PRINT '==================================================================='

SELECT 
    o.name AS ObjectName,
    o.type_desc AS ObjectType,
    'NO PK, NO UNIQUE CONSTRAINT, NO UNIQUE INDEX' AS Status,
    'Manual IsPrimaryKey required in UdmTablesColumnsList' AS ActionRequired
FROM sys.objects o
WHERE o.schema_id = SCHEMA_ID(@SourceSchema)
  AND o.type IN ('U', 'V')
  -- No PK
  AND NOT EXISTS (
      SELECT 1 FROM sys.indexes i
      WHERE i.object_id = o.object_id AND i.is_primary_key = 1
  )
  -- No unique index or constraint
  AND NOT EXISTS (
      SELECT 1 FROM sys.indexes i
      WHERE i.object_id = o.object_id AND i.is_unique = 1
  )
ORDER BY o.type_desc, o.name;


-- ==========================================================================
-- SECTION 10: INDEX USAGE STATISTICS
-- Identifies unused indexes (high writes, zero reads) and heavily-used
-- indexes. Useful for understanding which indexes are actually effective.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 10: INDEX USAGE STATISTICS (SINCE LAST RESTART)'
PRINT '==================================================================='

SELECT 
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique,
    i.is_primary_key,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_seeks + s.user_scans + s.user_lookups AS TotalReads,
    s.user_updates AS TotalWrites,
    CASE 
        WHEN s.user_seeks + s.user_scans + s.user_lookups = 0 
             AND s.user_updates > 0
        THEN '** UNUSED — consider removing **'
        WHEN s.user_seeks > s.user_scans * 10
        THEN 'EFFICIENT — mostly seeks'
        WHEN s.user_scans > s.user_seeks * 10
        THEN 'SCAN-HEAVY — may need optimization'
        ELSE 'NORMAL'
    END AS UsagePattern,
    s.last_user_seek,
    s.last_user_scan,
    s.last_user_update
FROM sys.dm_db_index_usage_stats s
JOIN sys.indexes i
    ON s.object_id = i.object_id
    AND s.index_id = i.index_id
JOIN sys.objects o
    ON i.object_id = o.object_id
WHERE s.database_id = DB_ID()
  AND o.schema_id = SCHEMA_ID(@SourceSchema)
  AND o.type IN ('U', 'V')
  AND i.type > 0
ORDER BY TotalReads DESC, OBJECT_NAME(s.object_id), i.name;


-- ==========================================================================
-- SECTION 11: VIEW NOT-NULL COLUMNS (PROFILE CANDIDATES)
-- Generates profiling queries to test uniqueness of candidate PK columns.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 11: VIEW NOT-NULL COLUMNS — PROFILING QUERIES'
PRINT '==================================================================='

SELECT 
    v.name AS ViewName,
    c.name AS ColumnName,
    TYPE_NAME(c.user_type_id) AS DataType,
    'SELECT COUNT(*) AS TotalRows, COUNT(DISTINCT [' + c.name + ']) AS DistinctValues FROM ' 
        + @SourceSchema + '.' + v.name AS ProfilingQuery
FROM sys.views v
JOIN sys.columns c
    ON v.object_id = c.object_id
WHERE v.schema_id = SCHEMA_ID(@SourceSchema)
  AND c.is_nullable = 0
ORDER BY v.name, c.column_id;


-- ==========================================================================
-- SECTION 12: HEAP TABLES (NO CLUSTERED INDEX)
-- Tables without a clustered index are heaps — they're slower for range
-- scans and don't benefit from index seek patterns.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 12: HEAP TABLES (NO CLUSTERED INDEX)'
PRINT '==================================================================='

SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    p.rows AS ApproxRowCount,
    'HEAP — no clustered index. Consider adding clustered index.' AS Assessment
FROM sys.indexes i
JOIN sys.partitions p
    ON i.object_id = p.object_id
    AND i.index_id = p.index_id
JOIN sys.objects o
    ON i.object_id = o.object_id
WHERE o.schema_id = SCHEMA_ID(@SourceSchema)
  AND o.type = 'U'
  AND i.type = 0   -- Heap
ORDER BY p.rows DESC;
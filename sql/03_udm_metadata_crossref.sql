/*
=============================================================================
  03_udm_metadata_crossref.sql
  
  UDM Pipeline Metadata Cross-Reference & Gap Analysis
  
  Purpose:
    Cross-reference the pipeline's metadata tables (General.dbo.UdmTablesList
    and General.dbo.UdmTablesColumnsList) against actual Stage/Bronze indexes
    and identify configuration gaps:
    
    - Tables with SourceIndexHint but no matching Oracle index (stale hints)
    - Tables with IsPrimaryKey=0 for all columns (CDC/SCD2 will fail)
    - Tables with indexes tracked in UdmTablesColumnsList vs actual target indexes
    - Large tables missing SourceAggregateColumnName (date col for windowed extraction)
    - Stage/Bronze tables missing PKs that the source has
  
  Run against: General database (SQL Server target)
=============================================================================
*/

USE [General];
GO


-- ==========================================================================
-- SECTION 1: PIPELINE TABLE INVENTORY
-- Overview of all tables in UdmTablesList with their configuration status.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 1: PIPELINE TABLE INVENTORY'
PRINT '==================================================================='

SELECT 
    utl.SourceName,
    utl.SourceObjectName,
    utl.SourceSchemaName,
    utl.SourceServer,
    utl.SourceDatabase,
    utl.StageTableName,
    utl.BronzeTableName,
    utl.SourceAggregateColumnName,
    utl.SourceAggregateColumnType,
    utl.SourceIndexHint,
    utl.PartitionOn,
    utl.StageLoadTool,
    utl.LookbackDays,
    -- Classification
    CASE 
        WHEN utl.SourceAggregateColumnName IS NOT NULL THEN 'LARGE'
        ELSE 'SMALL'
    END AS TableSizeClass,
    CASE
        WHEN utl.SourceIndexHint IS NOT NULL THEN 'oracledb (INDEX hint)'
        WHEN utl.PartitionOn IS NOT NULL THEN 'ConnectorX (partitioned)'
        ELSE 'ConnectorX (bulk)'
    END AS ExtractionPath
FROM dbo.UdmTablesList utl
WHERE utl.StageLoadTool = 'Python'
ORDER BY utl.SourceName, utl.SourceObjectName;


-- ==========================================================================
-- SECTION 2: PK CONFIGURATION STATUS
-- Tables where NO columns have IsPrimaryKey=1 — CDC/SCD2 will fail.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 2: TABLES WITHOUT ANY PK CONFIGURED'
PRINT '==================================================================='

SELECT 
    utl.SourceName,
    utl.SourceObjectName,
    utl.SourceSchemaName,
    CASE 
        WHEN utl.SourceAggregateColumnName IS NOT NULL THEN 'LARGE'
        ELSE 'SMALL'
    END AS TableSizeClass,
    COALESCE(pk_count.PKColumns, 0) AS ConfiguredPKCount,
    COALESCE(total_count.TotalColumns, 0) AS TotalColumnCount,
    '** CRITICAL — CDC/SCD2 requires at least one PK column **' AS ActionRequired
FROM dbo.UdmTablesList utl
LEFT JOIN (
    SELECT SourceName, TableName, COUNT(*) AS PKColumns
    FROM dbo.UdmTablesColumnsList
    WHERE IsPrimaryKey = 1 AND Layer = 'Stage'
    GROUP BY SourceName, TableName
) pk_count 
    ON utl.SourceName = pk_count.SourceName 
    AND COALESCE(utl.StageTableName, utl.SourceObjectName) = pk_count.TableName
LEFT JOIN (
    SELECT SourceName, TableName, COUNT(*) AS TotalColumns
    FROM dbo.UdmTablesColumnsList
    WHERE Layer = 'Stage'
    GROUP BY SourceName, TableName
) total_count 
    ON utl.SourceName = total_count.SourceName 
    AND COALESCE(utl.StageTableName, utl.SourceObjectName) = total_count.TableName
WHERE utl.StageLoadTool = 'Python'
  AND COALESCE(pk_count.PKColumns, 0) = 0
ORDER BY utl.SourceName, utl.SourceObjectName;


-- ==========================================================================
-- SECTION 3: PK COLUMN DETAIL
-- All columns marked as IsPrimaryKey=1 across all pipeline tables.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 3: CONFIGURED PRIMARY KEY COLUMNS'
PRINT '==================================================================='

SELECT 
    ucl.SourceName,
    ucl.TableName,
    ucl.ColumnName,
    ucl.OrdinalPosition,
    ucl.Layer,
    ucl.IsIndex,
    ucl.IndexName,
    ucl.IndexType
FROM dbo.UdmTablesColumnsList ucl
WHERE ucl.IsPrimaryKey = 1
ORDER BY ucl.SourceName, ucl.TableName, ucl.Layer, ucl.OrdinalPosition;


-- ==========================================================================
-- SECTION 4: INDEX CONFIGURATION IN UdmTablesColumnsList
-- All columns marked as IsIndex=1 — these drive index_management.py
-- disable/rebuild around BCP loads.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 4: CONFIGURED INDEXES IN UdmTablesColumnsList'
PRINT '==================================================================='

SELECT 
    ucl.SourceName,
    ucl.TableName,
    ucl.ColumnName,
    ucl.IndexName,
    ucl.IndexType,
    ucl.Layer,
    ucl.IsPrimaryKey
FROM dbo.UdmTablesColumnsList ucl
WHERE ucl.IsIndex = 1
ORDER BY ucl.SourceName, ucl.TableName, ucl.IndexName;


-- ==========================================================================
-- SECTION 5: SOURCE INDEX HINT ANALYSIS
-- Tables with SourceIndexHint — these use the oracledb extraction path.
-- Cross-reference: do the hints look valid? Are they being used?
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 5: SOURCE INDEX HINT CONFIGURATION'
PRINT '==================================================================='

SELECT 
    utl.SourceName,
    utl.SourceObjectName,
    utl.SourceSchemaName,
    utl.SourceIndexHint,
    utl.SourceAggregateColumnName AS DateColumn,
    utl.SourceAggregateColumnType AS DateColumnType,
    CASE 
        WHEN utl.SourceAggregateColumnName IS NOT NULL THEN 'LARGE — windowed extraction'
        ELSE 'SMALL — full-scan extraction'
    END AS ExtractionMode,
    utl.PartitionOn,
    -- Validation flag
    CASE
        WHEN utl.SourceIndexHint IS NOT NULL AND utl.SourceAggregateColumnName IS NULL
        THEN 'INDEX hint on full-scan (P1-12 mode) — verify index exists'
        WHEN utl.SourceIndexHint IS NOT NULL AND utl.SourceAggregateColumnName IS NOT NULL
        THEN 'INDEX hint on date-windowed extraction — verify index covers date column'
        ELSE 'No hint configured'
    END AS HintAssessment,
    -- Oracle-specific: the hint goes into /*+ INDEX(table hint) */ 
    'Validate in Oracle: SELECT INDEX_NAME FROM ALL_INDEXES WHERE TABLE_OWNER = ''' 
        + UPPER(utl.SourceSchemaName) + ''' AND TABLE_NAME = ''' 
        + UPPER(utl.SourceObjectName) + ''' AND INDEX_NAME = ''' 
        + COALESCE(utl.SourceIndexHint, 'N/A') + '''' AS ValidationQuery
FROM dbo.UdmTablesList utl
WHERE utl.StageLoadTool = 'Python'
  AND utl.SourceIndexHint IS NOT NULL
ORDER BY utl.SourceName, utl.SourceObjectName;


-- ==========================================================================
-- SECTION 6: ACTUAL INDEXES ON UDM STAGE TABLES
-- What indexes actually exist on the Stage CDC tables in UDM_Stage?
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 6: ACTUAL INDEXES ON UDM_STAGE CDC TABLES'
PRINT '==================================================================='

SELECT 
    OBJECT_SCHEMA_NAME(i.object_id, DB_ID('UDM_Stage')) AS SchemaName,
    OBJECT_NAME(i.object_id, DB_ID('UDM_Stage')) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique,
    i.is_primary_key,
    i.is_disabled,
    STRING_AGG(
        COL_NAME(ic.object_id, ic.column_id), ', '
    ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyColumns
FROM UDM_Stage.sys.indexes i
JOIN UDM_Stage.sys.index_columns ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
    AND ic.is_included_column = 0
WHERE i.type > 0
GROUP BY i.object_id, i.name, i.type_desc, i.is_unique, i.is_primary_key, i.is_disabled
ORDER BY OBJECT_SCHEMA_NAME(i.object_id, DB_ID('UDM_Stage')), 
         OBJECT_NAME(i.object_id, DB_ID('UDM_Stage')), 
         i.name;


-- ==========================================================================
-- SECTION 7: ACTUAL INDEXES ON UDM BRONZE TABLES
-- What indexes actually exist on the Bronze SCD2 tables in UDM_Bronze?
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 7: ACTUAL INDEXES ON UDM_BRONZE SCD2 TABLES'
PRINT '==================================================================='

SELECT 
    OBJECT_SCHEMA_NAME(i.object_id, DB_ID('UDM_Bronze')) AS SchemaName,
    OBJECT_NAME(i.object_id, DB_ID('UDM_Bronze')) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique,
    i.is_primary_key,
    i.is_disabled,
    i.has_filter,
    i.filter_definition,
    STRING_AGG(
        COL_NAME(ic.object_id, ic.column_id), ', '
    ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyColumns
FROM UDM_Bronze.sys.indexes i
JOIN UDM_Bronze.sys.index_columns ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
    AND ic.is_included_column = 0
WHERE i.type > 0
GROUP BY i.object_id, i.name, i.type_desc, i.is_unique, i.is_primary_key, 
         i.is_disabled, i.has_filter, i.filter_definition
ORDER BY OBJECT_SCHEMA_NAME(i.object_id, DB_ID('UDM_Bronze')), 
         OBJECT_NAME(i.object_id, DB_ID('UDM_Bronze')), 
         i.name;


-- ==========================================================================
-- SECTION 8: STAGE/BRONZE INDEX GAP ANALYSIS
-- Compare UdmTablesColumnsList index config vs actual indexes on target.
-- Finds: configured but missing, present but not tracked.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 8: STAGE INDEX GAP — CONFIGURED vs ACTUAL'
PRINT '==================================================================='

-- Configured in metadata but may not exist on target
SELECT 
    ucl.SourceName,
    ucl.TableName,
    ucl.IndexName AS ConfiguredIndexName,
    ucl.IndexType AS ConfiguredIndexType,
    ucl.ColumnName,
    CASE 
        WHEN si.name IS NOT NULL THEN 'EXISTS'
        ELSE '** MISSING — configured but not on target **'
    END AS ActualStatus
FROM dbo.UdmTablesColumnsList ucl
LEFT JOIN UDM_Stage.sys.indexes si
    ON si.name = ucl.IndexName
    AND OBJECT_NAME(si.object_id, DB_ID('UDM_Stage')) = ucl.TableName + '_cdc'
WHERE ucl.IsIndex = 1
  AND ucl.Layer = 'Stage'
ORDER BY ucl.SourceName, ucl.TableName, ucl.IndexName;


-- ==========================================================================
-- SECTION 9: LARGE TABLES WITHOUT INDEX HINTS
-- Oracle large tables that use ConnectorX instead of oracledb.
-- These might benefit from INDEX hints for windowed extraction.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 9: LARGE ORACLE TABLES WITHOUT INDEX HINTS'
PRINT '==================================================================='

SELECT 
    utl.SourceName,
    utl.SourceObjectName,
    utl.SourceSchemaName,
    utl.SourceAggregateColumnName AS DateColumn,
    utl.PartitionOn,
    CASE 
        WHEN utl.PartitionOn IS NOT NULL
        THEN 'Using ConnectorX partitioned — INDEX hint may still help if partition_on != date column'
        ELSE '** Consider adding SourceIndexHint for oracledb path — better date windowing **'
    END AS Recommendation
FROM dbo.UdmTablesList utl
WHERE utl.StageLoadTool = 'Python'
  AND utl.SourceAggregateColumnName IS NOT NULL  -- large table
  AND utl.SourceIndexHint IS NULL                -- no hint
  AND utl.SourceName = 'DNA'                     -- Oracle only
ORDER BY utl.SourceObjectName;


-- ==========================================================================
-- SECTION 10: TABLES WITHOUT COLUMN METADATA
-- Tables in UdmTablesList that have NO rows in UdmTablesColumnsList.
-- These haven't been synced yet (column_sync.py hasn't run).
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 10: TABLES WITHOUT COLUMN METADATA'
PRINT '==================================================================='

SELECT 
    utl.SourceName,
    utl.SourceObjectName,
    utl.SourceSchemaName,
    '** No column metadata — column_sync.py has not run for this table **' AS Status
FROM dbo.UdmTablesList utl
WHERE utl.StageLoadTool = 'Python'
  AND NOT EXISTS (
      SELECT 1 FROM dbo.UdmTablesColumnsList ucl
      WHERE ucl.SourceName = utl.SourceName
        AND ucl.TableName = COALESCE(utl.StageTableName, utl.SourceObjectName)
  )
ORDER BY utl.SourceName, utl.SourceObjectName;


-- ==========================================================================
-- SECTION 11: SUMMARY DASHBOARD
-- One-row-per-table summary of PK/index/hint status.
-- ==========================================================================

PRINT '==================================================================='
PRINT 'SECTION 11: CONFIGURATION SUMMARY DASHBOARD'
PRINT '==================================================================='

SELECT 
    utl.SourceName,
    utl.SourceObjectName,
    CASE 
        WHEN utl.SourceAggregateColumnName IS NOT NULL THEN 'LARGE'
        ELSE 'SMALL'
    END AS SizeClass,
    CASE
        WHEN utl.SourceIndexHint IS NOT NULL THEN 'oracledb'
        WHEN utl.PartitionOn IS NOT NULL THEN 'CX-partitioned'
        ELSE 'CX-bulk'
    END AS ExtractionPath,
    -- PK status
    COALESCE(pk_count.PKColumns, 0) AS PKColumnCount,
    CASE 
        WHEN COALESCE(pk_count.PKColumns, 0) = 0 THEN 'CRITICAL'
        ELSE 'OK'
    END AS PKStatus,
    -- Index hint status  
    utl.SourceIndexHint,
    CASE
        WHEN utl.SourceName != 'DNA' THEN 'N/A (SQL Server)'
        WHEN utl.SourceIndexHint IS NOT NULL THEN 'CONFIGURED'
        WHEN utl.SourceAggregateColumnName IS NOT NULL THEN 'MISSING (large table)'
        ELSE 'OPTIONAL (small table)'
    END AS HintStatus,
    -- Target index status
    COALESCE(idx_count.IndexCount, 0) AS TargetIndexCount,
    -- Column metadata status
    COALESCE(col_count.TotalColumns, 0) AS ColumnMetadataCount,
    CASE 
        WHEN COALESCE(col_count.TotalColumns, 0) = 0 THEN 'NOT SYNCED'
        ELSE 'SYNCED'
    END AS MetadataStatus
FROM dbo.UdmTablesList utl
LEFT JOIN (
    SELECT SourceName, TableName, COUNT(*) AS PKColumns
    FROM dbo.UdmTablesColumnsList
    WHERE IsPrimaryKey = 1 AND Layer = 'Stage'
    GROUP BY SourceName, TableName
) pk_count 
    ON utl.SourceName = pk_count.SourceName 
    AND COALESCE(utl.StageTableName, utl.SourceObjectName) = pk_count.TableName
LEFT JOIN (
    SELECT SourceName, TableName, COUNT(*) AS TotalColumns
    FROM dbo.UdmTablesColumnsList
    WHERE Layer = 'Stage'
    GROUP BY SourceName, TableName
) col_count 
    ON utl.SourceName = col_count.SourceName 
    AND COALESCE(utl.StageTableName, utl.SourceObjectName) = col_count.TableName
LEFT JOIN (
    SELECT 
        OBJECT_SCHEMA_NAME(i.object_id, DB_ID('UDM_Stage')) AS SchemaName,
        OBJECT_NAME(i.object_id, DB_ID('UDM_Stage')) AS TableName,
        COUNT(DISTINCT i.index_id) AS IndexCount
    FROM UDM_Stage.sys.indexes i
    WHERE i.type > 0
    GROUP BY i.object_id
) idx_count 
    ON utl.SourceName = idx_count.SchemaName
    AND COALESCE(utl.StageTableName, utl.SourceObjectName) + '_cdc' = idx_count.TableName
WHERE utl.StageLoadTool = 'Python'
ORDER BY 
    CASE WHEN COALESCE(pk_count.PKColumns, 0) = 0 THEN 0 ELSE 1 END,  -- Critical first
    utl.SourceName, 
    utl.SourceObjectName;
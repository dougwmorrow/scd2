/*
=============================================================================
  05_oracle_index_hint_validation.sql
  
  Oracle INDEX Hint Validation & Optimization
  
  Purpose:
    Specifically validates that SourceIndexHint values in UdmTablesList
    reference real Oracle indexes, and that those indexes are appropriate
    for the extraction pattern (date-windowed or full-scan).
  
  Context from oracle_extractor.py:
    - Uses: SELECT /*+ INDEX(table hint) */ * FROM schema.table WHERE date >= :s AND date < :e
    - O-1: Range predicates on date column — requires standard B-tree index (NOT function-based)
    - Bind variable peeking (O-2): oracledb uses bind vars, so index selectivity matters
  
  Run against: Oracle DNA source
  Variable: Set :source_schema to the Oracle schema name (e.g., 'DNA')
  
  Designed to be run alongside 01_oracle_source_discovery.sql results
  for comprehensive analysis.
=============================================================================
*/


-- ==========================================================================
-- VALIDATION 1: INDEX EXISTENCE CHECK
-- Does the SourceIndexHint value match an actual Oracle index?
-- Non-existent hints are silently ignored by Oracle optimizer, meaning
-- the query runs as a full table scan — potentially catastrophic for
-- large tables.
-- ==========================================================================

PROMPT ===================================================================
PROMPT VALIDATION 1: INDEX HINT EXISTENCE CHECK
PROMPT ===================================================================
PROMPT Run this query for each SourceIndexHint value from UdmTablesList.
PROMPT Replace :hint_name and :table_name with actual values.
PROMPT ===================================================================

-- Template: check a single hint
SELECT 
    ai.TABLE_NAME,
    ai.INDEX_NAME,
    ai.INDEX_TYPE,
    ai.UNIQUENESS,
    ai.STATUS,
    ai.VISIBILITY,
    CASE 
        WHEN ai.STATUS = 'UNUSABLE' THEN '** CRITICAL: Index is UNUSABLE — hint will be ignored **'
        WHEN ai.VISIBILITY = 'INVISIBLE' THEN '** WARNING: Index is INVISIBLE — hint MAY still work **'
        WHEN ai.INDEX_TYPE LIKE 'FUNCTION%' THEN '** WARNING: Function-based — range predicates may not use it **'
        ELSE 'OK'
    END AS VALIDATION_STATUS,
    LISTAGG(aic.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY aic.COLUMN_POSITION) AS INDEX_COLUMNS
FROM ALL_INDEXES ai
JOIN ALL_IND_COLUMNS aic
    ON ai.INDEX_NAME = aic.INDEX_NAME
    AND ai.TABLE_OWNER = aic.INDEX_OWNER
WHERE ai.TABLE_OWNER = :source_schema
  AND ai.INDEX_NAME = :hint_name        -- Replace with actual SourceIndexHint
  AND ai.TABLE_NAME = :table_name       -- Replace with actual SourceObjectName
GROUP BY ai.TABLE_NAME, ai.INDEX_NAME, ai.INDEX_TYPE, ai.UNIQUENESS, 
         ai.STATUS, ai.VISIBILITY;


-- ==========================================================================
-- VALIDATION 2: BATCH INDEX HINT CHECK (ALL PIPELINE TABLES)
-- Run this to validate ALL index hints at once.
-- Returns all indexes for all tables that are in the pipeline.
-- Cross-reference results with UdmTablesList.SourceIndexHint values.
-- ==========================================================================

PROMPT ===================================================================
PROMPT VALIDATION 2: ALL INDEXES ON PIPELINE TABLES
PROMPT Match INDEX_NAME against SourceIndexHint values from UdmTablesList.
PROMPT ===================================================================

-- Replace the IN list with actual table names from UdmTablesList
-- where SourceIndexHint IS NOT NULL
SELECT 
    ai.TABLE_NAME,
    ai.INDEX_NAME,
    ai.INDEX_TYPE,
    ai.UNIQUENESS,
    ai.STATUS,
    ai.VISIBILITY,
    ai.NUM_ROWS,
    ai.LAST_ANALYZED,
    LISTAGG(aic.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY aic.COLUMN_POSITION) AS INDEX_COLUMNS
FROM ALL_INDEXES ai
JOIN ALL_IND_COLUMNS aic
    ON ai.INDEX_NAME = aic.INDEX_NAME
    AND ai.TABLE_OWNER = aic.INDEX_OWNER
WHERE ai.TABLE_OWNER = :source_schema
GROUP BY 
    ai.TABLE_NAME, ai.INDEX_NAME, ai.INDEX_TYPE, ai.UNIQUENESS,
    ai.STATUS, ai.VISIBILITY, ai.NUM_ROWS, ai.LAST_ANALYZED
ORDER BY ai.TABLE_NAME, ai.INDEX_NAME;


-- ==========================================================================
-- VALIDATION 3: DATE COLUMN INDEX ALIGNMENT
-- For large tables with SourceAggregateColumnName, verify the INDEX hint
-- actually covers the date column used in WHERE predicates.
-- A hint pointing to an index on a different column won't help.
-- ==========================================================================

PROMPT ===================================================================
PROMPT VALIDATION 3: DATE COLUMN COVERAGE BY INDEX HINT
PROMPT For each large table, does the hinted index cover the date column?
PROMPT ===================================================================

-- Template: Check if a specific index contains the date column
-- Replace :hint_name, :table_name, :date_column
SELECT 
    ai.TABLE_NAME,
    ai.INDEX_NAME,
    aic.COLUMN_NAME,
    aic.COLUMN_POSITION,
    CASE 
        WHEN aic.COLUMN_NAME = :date_column AND aic.COLUMN_POSITION = 1
        THEN '** OPTIMAL: Date column is the LEADING column **'
        WHEN aic.COLUMN_NAME = :date_column AND aic.COLUMN_POSITION > 1
        THEN 'SUBOPTIMAL: Date column is at position ' || aic.COLUMN_POSITION || ' — range scan less efficient'
        ELSE 'Other index column'
    END AS ASSESSMENT
FROM ALL_INDEXES ai
JOIN ALL_IND_COLUMNS aic
    ON ai.INDEX_NAME = aic.INDEX_NAME
    AND ai.TABLE_OWNER = aic.INDEX_OWNER
WHERE ai.TABLE_OWNER = :source_schema
  AND ai.INDEX_NAME = :hint_name
  AND ai.TABLE_NAME = :table_name
ORDER BY aic.COLUMN_POSITION;


-- ==========================================================================
-- VALIDATION 4: ALTERNATIVE INDEX CANDIDATES
-- For each large table, find all indexes that include the date column.
-- If the current hint is suboptimal, these are better alternatives.
-- ==========================================================================

PROMPT ===================================================================
PROMPT VALIDATION 4: ALTERNATIVE INDEX CANDIDATES FOR DATE COLUMNS
PROMPT ===================================================================

-- Replace :table_name and :date_column
SELECT 
    ai.TABLE_NAME,
    ai.INDEX_NAME,
    ai.INDEX_TYPE,
    ai.UNIQUENESS,
    aic.COLUMN_POSITION AS DATE_COL_POSITION,
    ai.STATUS,
    ai.VISIBILITY,
    CASE
        WHEN ai.INDEX_TYPE = 'NORMAL' AND aic.COLUMN_POSITION = 1
        THEN '** BEST — standard B-tree, date is leading column **'
        WHEN ai.INDEX_TYPE = 'NORMAL' AND aic.COLUMN_POSITION > 1
        THEN 'GOOD — standard B-tree, date at position ' || aic.COLUMN_POSITION
        WHEN ai.INDEX_TYPE LIKE 'FUNCTION%'
        THEN 'AVOID — function-based, range predicates bypass this'
        WHEN ai.INDEX_TYPE = 'BITMAP'
        THEN 'AVOID — bitmap indexes are slow for range scans'
        ELSE 'REVIEW'
    END AS SUITABILITY_FOR_RANGE_SCAN,
    (SELECT LISTAGG(aic2.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY aic2.COLUMN_POSITION)
     FROM ALL_IND_COLUMNS aic2
     WHERE aic2.INDEX_NAME = ai.INDEX_NAME AND aic2.INDEX_OWNER = ai.TABLE_OWNER
    ) AS ALL_INDEX_COLUMNS
FROM ALL_INDEXES ai
JOIN ALL_IND_COLUMNS aic
    ON ai.INDEX_NAME = aic.INDEX_NAME
    AND ai.TABLE_OWNER = aic.INDEX_OWNER
WHERE ai.TABLE_OWNER = :source_schema
  AND ai.TABLE_NAME = :table_name
  AND aic.COLUMN_NAME = :date_column
ORDER BY 
    -- Best candidates first
    CASE 
        WHEN ai.INDEX_TYPE = 'NORMAL' AND aic.COLUMN_POSITION = 1 THEN 0
        WHEN ai.INDEX_TYPE = 'NORMAL' THEN aic.COLUMN_POSITION
        ELSE 100
    END;


-- ==========================================================================
-- VALIDATION 5: STALE INDEX STATISTICS
-- Indexes with outdated statistics can cause the optimizer to choose 
-- suboptimal plans even with INDEX hints. Flag indexes not analyzed
-- in the last 30 days.
-- ==========================================================================

PROMPT ===================================================================
PROMPT VALIDATION 5: STALE INDEX STATISTICS
PROMPT ===================================================================

SELECT 
    ai.TABLE_NAME,
    ai.INDEX_NAME,
    ai.INDEX_TYPE,
    ai.NUM_ROWS,
    ai.DISTINCT_KEYS,
    ai.LAST_ANALYZED,
    TRUNC(SYSDATE - ai.LAST_ANALYZED) AS DAYS_SINCE_ANALYZED,
    CASE
        WHEN ai.LAST_ANALYZED IS NULL THEN '** NEVER ANALYZED **'
        WHEN SYSDATE - ai.LAST_ANALYZED > 90 THEN '** STALE (>90 days) **'
        WHEN SYSDATE - ai.LAST_ANALYZED > 30 THEN 'AGING (>30 days)'
        ELSE 'CURRENT'
    END AS STATS_FRESHNESS
FROM ALL_INDEXES ai
WHERE ai.TABLE_OWNER = :source_schema
  AND ai.STATUS != 'UNUSABLE'
ORDER BY 
    CASE 
        WHEN ai.LAST_ANALYZED IS NULL THEN 0
        ELSE 1
    END,
    ai.LAST_ANALYZED NULLS FIRST;


-- ==========================================================================
-- VALIDATION 6: TABLE STATISTICS FRESHNESS
-- Table statistics affect ALL index-based query plans.
-- ==========================================================================

PROMPT ===================================================================
PROMPT VALIDATION 6: TABLE STATISTICS FRESHNESS
PROMPT ===================================================================

SELECT 
    TABLE_NAME,
    NUM_ROWS,
    BLOCKS,
    AVG_ROW_LEN,
    LAST_ANALYZED,
    TRUNC(SYSDATE - LAST_ANALYZED) AS DAYS_SINCE_ANALYZED,
    CASE
        WHEN LAST_ANALYZED IS NULL THEN '** NEVER ANALYZED — run DBMS_STATS **'
        WHEN SYSDATE - LAST_ANALYZED > 30 THEN '** STALE — consider regathering **'
        ELSE 'CURRENT'
    END AS STATS_FRESHNESS,
    STALE_STATS    -- YES/NO from DBA_TAB_STATISTICS if available
FROM ALL_TAB_STATISTICS
WHERE OWNER = :source_schema
  AND OBJECT_TYPE IN ('TABLE', 'FIXED TABLE')
ORDER BY LAST_ANALYZED NULLS FIRST;
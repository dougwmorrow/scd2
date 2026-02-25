/*
=============================================================================
  01_oracle_source_discovery.sql
  
  Oracle Source PK, Index, and INDEX Hint Discovery
  
  Purpose:
    Discover primary keys, unique constraints, all indexes, and validate
    SourceIndexHint values for Oracle (DNA) source tables and views.
    Identifies opportunities for INDEX hint optimization and missing PK
    configuration in the UDM pipeline.
  
  Run against: Oracle DNA source (via SQL*Plus, DBeaver, or oracledb)
  
  Sections:
    1. Tables — PK constraints, unique constraints, all indexes
    2. Views  — underlying column analysis, uniqueness candidates
    3. INDEX hint validation — cross-reference with actual indexes
    4. Date column index coverage — for large table windowed extraction
    5. Composite analysis — tables/views without any useful key
=============================================================================
*/


-- ==========================================================================
-- SECTION 1: TABLE PRIMARY KEYS AND CONSTRAINTS
-- All PK constraints on tables in the source schema.
-- Maps directly to UdmTablesColumnsList.IsPrimaryKey.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 1: TABLE PRIMARY KEY CONSTRAINTS
PROMPT ===================================================================

SELECT 
    ac.TABLE_NAME,
    'PK_CONSTRAINT' AS KEY_TYPE,
    ac.CONSTRAINT_NAME,
    LISTAGG(acc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY acc.POSITION) AS PK_COLUMNS,
    COUNT(acc.COLUMN_NAME) AS COLUMN_COUNT
FROM ALL_CONSTRAINTS ac
JOIN ALL_CONS_COLUMNS acc
    ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
    AND ac.OWNER = acc.OWNER
WHERE ac.OWNER = :source_schema   -- e.g. 'DNA' or the Oracle schema
  AND ac.CONSTRAINT_TYPE = 'P'
GROUP BY ac.TABLE_NAME, ac.CONSTRAINT_NAME
ORDER BY ac.TABLE_NAME;


-- ==========================================================================
-- SECTION 2: ALL UNIQUE CONSTRAINTS (NON-PK)
-- Unique constraints can serve as CDC comparison keys when PK is absent.
-- column_sync.py falls back to unique indexes; this finds unique constraints.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 2: UNIQUE CONSTRAINTS (NON-PK)
PROMPT ===================================================================

SELECT 
    ac.TABLE_NAME,
    'UNIQUE_CONSTRAINT' AS KEY_TYPE,
    ac.CONSTRAINT_NAME,
    LISTAGG(acc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY acc.POSITION) AS UNIQUE_COLUMNS,
    COUNT(acc.COLUMN_NAME) AS COLUMN_COUNT
FROM ALL_CONSTRAINTS ac
JOIN ALL_CONS_COLUMNS acc
    ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
    AND ac.OWNER = acc.OWNER
WHERE ac.OWNER = :source_schema
  AND ac.CONSTRAINT_TYPE = 'U'
GROUP BY ac.TABLE_NAME, ac.CONSTRAINT_NAME
ORDER BY ac.TABLE_NAME;


-- ==========================================================================
-- SECTION 3: ALL INDEXES (COMPREHENSIVE)
-- Every index on every table/view in the schema.
-- Identifies: unique vs non-unique, B-tree vs bitmap vs function-based.
-- Cross-reference with UdmTablesList.SourceIndexHint to validate hints.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 3: ALL INDEXES (COMPREHENSIVE)
PROMPT ===================================================================

SELECT 
    ai.TABLE_NAME,
    ai.INDEX_NAME,
    ai.INDEX_TYPE,                        -- NORMAL, BITMAP, FUNCTION-BASED NORMAL, etc.
    ai.UNIQUENESS,                        -- UNIQUE or NONUNIQUE
    ai.STATUS,                            -- VALID, UNUSABLE, N/A (partitioned)
    ai.PARTITIONED,                       -- YES/NO
    ai.VISIBILITY,                        -- VISIBLE/INVISIBLE
    LISTAGG(aic.COLUMN_NAME, ', ') 
        WITHIN GROUP (ORDER BY aic.COLUMN_POSITION) AS INDEX_COLUMNS,
    COUNT(aic.COLUMN_NAME) AS COLUMN_COUNT,
    ai.NUM_ROWS,
    ai.DISTINCT_KEYS,
    ai.LEAF_BLOCKS,
    ai.BLEVEL AS TREE_HEIGHT,
    ai.LAST_ANALYZED
FROM ALL_INDEXES ai
JOIN ALL_IND_COLUMNS aic
    ON ai.INDEX_NAME = aic.INDEX_NAME
    AND ai.TABLE_OWNER = aic.INDEX_OWNER
WHERE ai.TABLE_OWNER = :source_schema
GROUP BY 
    ai.TABLE_NAME, ai.INDEX_NAME, ai.INDEX_TYPE, ai.UNIQUENESS,
    ai.STATUS, ai.PARTITIONED, ai.VISIBILITY, ai.NUM_ROWS,
    ai.DISTINCT_KEYS, ai.LEAF_BLOCKS, ai.BLEVEL, ai.LAST_ANALYZED
ORDER BY ai.TABLE_NAME, ai.INDEX_NAME;


-- ==========================================================================
-- SECTION 4: FUNCTION-BASED INDEXES
-- Function-based indexes (e.g., TRUNC(date_col)) are relevant because
-- oracle_extractor.py uses range predicates (O-1) instead of TRUNC()
-- in WHERE clauses. If a table ONLY has a function-based index on the
-- date column, range predicates won't use it — this is a flag.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 4: FUNCTION-BASED INDEX EXPRESSIONS
PROMPT ===================================================================

SELECT 
    aie.TABLE_NAME,
    aie.INDEX_NAME,
    aie.COLUMN_EXPRESSION,
    aie.COLUMN_POSITION,
    ai.UNIQUENESS,
    ai.STATUS
FROM ALL_IND_EXPRESSIONS aie
JOIN ALL_INDEXES ai
    ON aie.INDEX_NAME = ai.INDEX_NAME
    AND aie.INDEX_OWNER = ai.TABLE_OWNER
WHERE aie.INDEX_OWNER = :source_schema
ORDER BY aie.TABLE_NAME, aie.INDEX_NAME, aie.COLUMN_POSITION;


-- ==========================================================================
-- SECTION 5: VIEWS — OBJECT TYPE IDENTIFICATION
-- Separate tables from views so we know which objects need manual PK
-- assignment. Views won't have PK constraints or indexes.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 5: OBJECT TYPES (TABLE vs VIEW vs MATERIALIZED VIEW)
PROMPT ===================================================================

SELECT 
    OBJECT_NAME,
    OBJECT_TYPE,    -- TABLE, VIEW, MATERIALIZED VIEW
    STATUS,         -- VALID / INVALID
    CREATED,
    LAST_DDL_TIME
FROM ALL_OBJECTS
WHERE OWNER = :source_schema
  AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
ORDER BY OBJECT_TYPE, OBJECT_NAME;


-- ==========================================================================
-- SECTION 6: VIEW COLUMN ANALYSIS — PK CANDIDATES
-- For views, analyze the underlying columns to suggest PK candidates.
-- Looks for: NOT NULL constraints, naming patterns (ID, KEY, CODE, NUM).
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 6: VIEW COLUMNS — PK CANDIDATE ANALYSIS
PROMPT ===================================================================

SELECT 
    v.VIEW_NAME,
    atc.COLUMN_NAME,
    atc.COLUMN_ID AS ORDINAL,
    atc.DATA_TYPE,
    atc.DATA_LENGTH,
    atc.DATA_PRECISION,
    atc.DATA_SCALE,
    atc.NULLABLE,                         -- 'N' = NOT NULL (good PK candidate)
    CASE
        WHEN atc.NULLABLE = 'N' AND (
            UPPER(atc.COLUMN_NAME) LIKE '%_ID'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_KEY'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_CODE'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_NUM'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_NO'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_NBR'
            OR UPPER(atc.COLUMN_NAME) LIKE 'ID_%'
            OR UPPER(atc.COLUMN_NAME) = 'ID'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_SEQ'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_NUMBER'
        ) THEN 'HIGH'
        WHEN atc.NULLABLE = 'N' AND atc.DATA_TYPE IN ('NUMBER', 'VARCHAR2', 'CHAR')
        THEN 'MEDIUM'
        WHEN atc.NULLABLE = 'Y' THEN 'LOW'
        ELSE 'LOW'
    END AS PK_CANDIDATE_SCORE,
    CASE
        WHEN atc.NULLABLE = 'N' AND (
            UPPER(atc.COLUMN_NAME) LIKE '%_ID'
            OR UPPER(atc.COLUMN_NAME) LIKE '%_KEY'
            OR UPPER(atc.COLUMN_NAME) = 'ID'
        ) THEN '** LIKELY PK **'
        ELSE ''
    END AS RECOMMENDATION
FROM ALL_VIEWS v
JOIN ALL_TAB_COLUMNS atc
    ON v.VIEW_NAME = atc.TABLE_NAME
    AND v.OWNER = atc.OWNER
WHERE v.OWNER = :source_schema
ORDER BY v.VIEW_NAME, atc.COLUMN_ID;


-- ==========================================================================
-- SECTION 7: VIEW SOURCE TEXT (for manual analysis)
-- Retrieves the view definition SQL so you can identify the base tables
-- and understand join logic to infer correct PKs.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 7: VIEW DEFINITIONS (SOURCE SQL)
PROMPT ===================================================================

SELECT 
    VIEW_NAME,
    TEXT_LENGTH,
    TEXT AS VIEW_SQL
FROM ALL_VIEWS
WHERE OWNER = :source_schema
ORDER BY VIEW_NAME;


-- ==========================================================================
-- SECTION 8: MATERIALIZED VIEW INDEXES
-- Materialized views CAN have indexes and PKs — discover them separately.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 8: MATERIALIZED VIEW METADATA
PROMPT ===================================================================

SELECT 
    mv.MVIEW_NAME,
    mv.CONTAINER_NAME,           -- base table name
    mv.REFRESH_MODE,             -- DEMAND, ON COMMIT, FORCE, etc.
    mv.REFRESH_METHOD,           -- COMPLETE, FAST, FORCE
    mv.FAST_REFRESHABLE,
    mv.LAST_REFRESH_DATE,
    mv.STALENESS                 -- FRESH, STALE, NEEDS_COMPILE, UNKNOWN
FROM ALL_MVIEWS mv
WHERE mv.OWNER = :source_schema
ORDER BY mv.MVIEW_NAME;


-- ==========================================================================
-- SECTION 9: INDEX HINT CANDIDATES FOR LARGE TABLE DATE-WINDOWED EXTRACTION
-- Identifies indexes on date/timestamp columns that could be used as
-- SourceIndexHint in UdmTablesList for windowed extraction.
-- The oracle_extractor.py uses range predicates (>=, <) so standard
-- B-tree indexes on date columns are ideal — NOT function-based.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 9: DATE COLUMN INDEX COVERAGE (INDEX HINT CANDIDATES)
PROMPT ===================================================================

SELECT 
    atc.TABLE_NAME,
    atc.COLUMN_NAME AS DATE_COLUMN,
    atc.DATA_TYPE,
    ai.INDEX_NAME,
    ai.INDEX_TYPE,
    ai.UNIQUENESS,
    aic.COLUMN_POSITION,
    CASE
        WHEN ai.INDEX_TYPE = 'NORMAL' AND aic.COLUMN_POSITION = 1
        THEN '** IDEAL INDEX HINT CANDIDATE **'
        WHEN ai.INDEX_TYPE = 'NORMAL' AND aic.COLUMN_POSITION > 1
        THEN 'USABLE (date is not leading column)'
        WHEN ai.INDEX_TYPE LIKE 'FUNCTION%'
        THEN 'FUNCTION-BASED — range predicates may not use this'
        WHEN ai.INDEX_TYPE = 'BITMAP'
        THEN 'BITMAP — not ideal for range scans'
        ELSE 'REVIEW'
    END AS HINT_ASSESSMENT
FROM ALL_TAB_COLUMNS atc
LEFT JOIN ALL_IND_COLUMNS aic
    ON atc.TABLE_NAME = aic.TABLE_NAME
    AND atc.COLUMN_NAME = aic.COLUMN_NAME
    AND atc.OWNER = aic.INDEX_OWNER
LEFT JOIN ALL_INDEXES ai
    ON aic.INDEX_NAME = ai.INDEX_NAME
    AND aic.INDEX_OWNER = ai.TABLE_OWNER
WHERE atc.OWNER = :source_schema
  AND atc.DATA_TYPE IN ('DATE', 'TIMESTAMP(6)', 'TIMESTAMP(3)', 'TIMESTAMP(9)',
                         'TIMESTAMP(6) WITH TIME ZONE', 'TIMESTAMP(6) WITH LOCAL TIME ZONE')
ORDER BY atc.TABLE_NAME, atc.COLUMN_NAME, ai.INDEX_NAME;


-- ==========================================================================
-- SECTION 10: TABLES/VIEWS WITH NO PK, NO UNIQUE CONSTRAINT, NO UNIQUE INDEX
-- These are the highest-risk objects for the CDC pipeline — they need
-- manual IsPrimaryKey assignment in UdmTablesColumnsList.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 10: OBJECTS WITHOUT ANY UNIQUENESS GUARANTEE
PROMPT ===================================================================

SELECT 
    obj.OBJECT_NAME,
    obj.OBJECT_TYPE,
    'NO PK, NO UNIQUE CONSTRAINT, NO UNIQUE INDEX' AS STATUS,
    'Manual IsPrimaryKey required in UdmTablesColumnsList' AS ACTION_REQUIRED
FROM ALL_OBJECTS obj
WHERE obj.OWNER = :source_schema
  AND obj.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
  -- No PK constraint
  AND NOT EXISTS (
      SELECT 1 FROM ALL_CONSTRAINTS ac
      WHERE ac.OWNER = obj.OWNER
        AND ac.TABLE_NAME = obj.OBJECT_NAME
        AND ac.CONSTRAINT_TYPE = 'P'
  )
  -- No unique constraint
  AND NOT EXISTS (
      SELECT 1 FROM ALL_CONSTRAINTS ac
      WHERE ac.OWNER = obj.OWNER
        AND ac.TABLE_NAME = obj.OBJECT_NAME
        AND ac.CONSTRAINT_TYPE = 'U'
  )
  -- No unique index
  AND NOT EXISTS (
      SELECT 1 FROM ALL_INDEXES ai
      WHERE ai.TABLE_OWNER = obj.OWNER
        AND ai.TABLE_NAME = obj.OBJECT_NAME
        AND ai.UNIQUENESS = 'UNIQUE'
  )
ORDER BY obj.OBJECT_TYPE, obj.OBJECT_NAME;


-- ==========================================================================
-- SECTION 11: NOT NULL COLUMNS ON VIEWS (UNIQUENESS PROFILING CANDIDATES)
-- For views that need manual PK assignment, these NOT NULL columns are
-- the first candidates to profile with COUNT(DISTINCT ...) queries.
-- ==========================================================================

PROMPT ===================================================================
PROMPT SECTION 11: VIEW NOT-NULL COLUMNS (PROFILE THESE FOR UNIQUENESS)
PROMPT ===================================================================

SELECT 
    atc.TABLE_NAME AS VIEW_NAME,
    atc.COLUMN_NAME,
    atc.DATA_TYPE,
    atc.NULLABLE,
    'Run: SELECT COUNT(*), COUNT(DISTINCT ' || atc.COLUMN_NAME || ') FROM ' 
        || :source_schema || '.' || atc.TABLE_NAME AS PROFILING_QUERY
FROM ALL_TAB_COLUMNS atc
JOIN ALL_VIEWS v
    ON atc.TABLE_NAME = v.VIEW_NAME
    AND atc.OWNER = v.OWNER
WHERE atc.OWNER = :source_schema
  AND atc.NULLABLE = 'N'
ORDER BY atc.TABLE_NAME, atc.COLUMN_ID;
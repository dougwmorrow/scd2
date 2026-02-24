/*
=============================================================================
  General.ops — Pipeline Observability & Tracking Tables
  
  DDL for all SQL Server tables that track pipeline run information.
  Run against the [General] database. Idempotent — safe to re-run.
  
  Tables:
    1. ops.PipelineBatchSequence    — Run ID generator (IDENTITY)
    2. ops.PipelineEventLog         — Dashboard layer (1 row per step per table)
    3. ops.PipelineLog              — Investigation layer (Python logging sink)
    4. ops.PipelineExtractionState  — Large table checkpoint/resume tracking
    5. ops.Quarantine               — Schema contract failure isolation (W-17)
    6. ops.ReconciliationLog        — Reconciliation result persistence (OBS-6)
=============================================================================
*/

USE [General];
GO

-- Ensure the ops schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ops')
    EXEC('CREATE SCHEMA [ops]');
GO


-- ==========================================================================
-- 1. PipelineBatchSequence
--    Identity table that generates a unique BatchId per pipeline run.
--    All events and logs within a single run share the same BatchId.
--
--    Python usage:
--      INSERT INTO ops.PipelineBatchSequence DEFAULT VALUES;
--      SELECT SCOPE_IDENTITY();
-- ==========================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'PipelineBatchSequence'
)
BEGIN
    CREATE TABLE ops.PipelineBatchSequence (
        BatchId     BIGINT IDENTITY(1,1)    PRIMARY KEY,
        CreatedAt   DATETIME2 NOT NULL       DEFAULT GETUTCDATE()
    );

    PRINT 'Created ops.PipelineBatchSequence';
END
GO


-- ==========================================================================
-- 2. PipelineEventLog
--    Dashboard layer — one row per step per table per batch.
--    Primary use: bottleneck analysis, throughput trending, alerting.
--
--    EventType values:
--      EXTRACT         — Source extraction into Polars DataFrame + BCP CSV write
--      BCP_LOAD        — BCP subprocess load into staging/CDC table
--      CDC_PROMOTION   — Hash-based insert/update/delete detection
--      SCD2_PROMOTION  — Bronze versioning (UPDATE close + INSERT new)
--      CSV_CLEANUP     — Temp BCP CSV file deletion
--      TABLE_TOTAL     — End-to-end wall time for entire table pipeline
--
--    Status values:
--      SUCCESS  — Step completed normally
--      FAILED   — Step threw an exception (see ErrorMessage)
--      SKIPPED  — Table skipped (e.g., lock held by another run) [OBS-3]
-- ==========================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'PipelineEventLog'
)
BEGIN
    CREATE TABLE ops.PipelineEventLog (
        EventId         BIGINT IDENTITY(1,1)    PRIMARY KEY,
        BatchId         BIGINT                  NOT NULL,
        TableName       NVARCHAR(255)           NOT NULL,
        SourceName      NVARCHAR(255)           NOT NULL,
        EventType       NVARCHAR(50)            NOT NULL,
        EventDetail     NVARCHAR(500)           NULL,       -- Free-text (e.g., target_date for large tables)
        StartedAt       DATETIME2               NOT NULL,
        CompletedAt     DATETIME2               NULL,
        DurationMs      INT                     NOT NULL    DEFAULT 0,
        Status          NVARCHAR(20)            NOT NULL    DEFAULT 'SUCCESS',
        ErrorMessage    NVARCHAR(4000)          NULL,
        RowsProcessed   INT                     NOT NULL    DEFAULT 0,
        RowsInserted    INT                     NOT NULL    DEFAULT 0,
        RowsUpdated     INT                     NOT NULL    DEFAULT 0,
        RowsDeleted     INT                     NOT NULL    DEFAULT 0,
        RowsUnchanged   INT                     NOT NULL    DEFAULT 0,
        RowsBefore      INT                     NOT NULL    DEFAULT 0,
        RowsAfter       INT                     NOT NULL    DEFAULT 0,
        TableCreated    BIT                     NOT NULL    DEFAULT 0,
        Metadata        NVARCHAR(MAX)           NULL,       -- JSON: update_ratio, null_pk_rows, schema_migration, active_ratio, etc.
        RowsPerSecond   DECIMAL(18,2)           NULL,

        -- FK to batch sequence
        CONSTRAINT FK_PipelineEventLog_Batch
            FOREIGN KEY (BatchId)
            REFERENCES ops.PipelineBatchSequence (BatchId)
    );

    -- Primary query patterns:
    -- 1. "Show me all events for batch X"
    CREATE NONCLUSTERED INDEX IX_PipelineEventLog_BatchId
        ON ops.PipelineEventLog (BatchId)
        INCLUDE (TableName, SourceName, EventType, DurationMs, Status);

    -- 2. "Show me the 10 slowest tables this week" / "Is ACCT degrading over time?"
    CREATE NONCLUSTERED INDEX IX_PipelineEventLog_Table_Event
        ON ops.PipelineEventLog (TableName, SourceName, EventType, CompletedAt DESC)
        INCLUDE (DurationMs, RowsProcessed, Status);

    -- 3. "Did any step fail recently?"
    CREATE NONCLUSTERED INDEX IX_PipelineEventLog_Status
        ON ops.PipelineEventLog (Status, CompletedAt DESC)
        INCLUDE (BatchId, TableName, EventType, ErrorMessage)
        WHERE Status != 'SUCCESS';

    PRINT 'Created ops.PipelineEventLog with indexes';
END
GO


-- ==========================================================================
-- 3. PipelineLog
--    Investigation layer — Python logging.Handler sink.
--    Many rows per step. Detailed narrative of what happened inside each step.
--    Join to PipelineEventLog on BatchId + TableName + SourceName.
--
--    Retention policy (implement via SQL Agent job):
--      DEBUG/INFO:           30 days
--      WARNING:              90 days
--      ERROR/CRITICAL:       Indefinite
-- ==========================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'PipelineLog'
)
BEGIN
    CREATE TABLE ops.PipelineLog (
        LogId           BIGINT IDENTITY(1,1)    PRIMARY KEY,
        BatchId         BIGINT                  NOT NULL,
        TableName       NVARCHAR(255)           NULL,       -- NULL for pipeline-wide entries
        SourceName      NVARCHAR(255)           NULL,       -- NULL for pipeline-wide entries
        LogLevel        NVARCHAR(20)            NOT NULL,   -- DEBUG, INFO, WARNING, ERROR, CRITICAL
        Module          NVARCHAR(255)           NULL,       -- Python module (e.g., extract.connectorx_oracle_extractor)
        FunctionName    NVARCHAR(255)           NULL,       -- Python function (e.g., extract_with_partition)
        Message         NVARCHAR(4000)          NOT NULL,
        ErrorType       NVARCHAR(255)           NULL,       -- Exception class name (e.g., ConnectionError)
        StackTrace      NVARCHAR(4000)          NULL,       -- Full traceback for ERROR/CRITICAL
        Metadata        NVARCHAR(MAX)           NULL,       -- JSON: query text, memory usage, chunk range, etc.
        CreatedAt       DATETIME2               NOT NULL,

        -- FK to batch sequence
        CONSTRAINT FK_PipelineLog_Batch
            FOREIGN KEY (BatchId)
            REFERENCES ops.PipelineBatchSequence (BatchId)
    );

    -- Primary query pattern: "Show me all logs for this batch + table"
    CREATE NONCLUSTERED INDEX IX_PipelineLog_Batch_Table
        ON ops.PipelineLog (BatchId, TableName, CreatedAt)
        INCLUDE (LogLevel, Module, Message);

    -- "Show me all errors in the last 7 days"
    CREATE NONCLUSTERED INDEX IX_PipelineLog_Level_Created
        ON ops.PipelineLog (LogLevel, CreatedAt DESC)
        INCLUDE (BatchId, TableName, Module, Message)
        WHERE LogLevel IN ('WARNING', 'ERROR', 'CRITICAL');

    PRINT 'Created ops.PipelineLog with indexes';
END
GO


-- ==========================================================================
-- 4. PipelineExtractionState
--    Per-table, per-date checkpoint for large table processing.
--    Enables resume-from-failure and gap detection.
--
--    Status values:
--      EXTRACTED       — Extraction succeeded, CDC/SCD2 pending (P3-11)
--      SUCCESS         — All steps completed for this date
--      FAILED          — Any step failed (may include step name: FAILED_CDC, FAILED_SCD2, etc.)
--      FAILED_SCHEMA_EVOLUTION — Schema drift with type changes
--      FAILED_CDC_OR_SCD2      — CDC or SCD2 processing error
-- ==========================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'PipelineExtractionState'
)
BEGIN
    CREATE TABLE ops.PipelineExtractionState (
        ExtractionStateId   BIGINT IDENTITY(1,1)    PRIMARY KEY,
        TableName           NVARCHAR(255)           NOT NULL,
        SourceName          NVARCHAR(255)           NOT NULL,
        DateValue           DATE                    NOT NULL,
        Status              NVARCHAR(50)            NOT NULL,   -- SUCCESS, FAILED, EXTRACTED, FAILED_*
        BatchId             BIGINT                  NULL,
        ProcessedAt         DATETIME2               NOT NULL    DEFAULT GETUTCDATE(),

        -- Natural key for MERGE upsert
        CONSTRAINT UQ_PipelineExtractionState_Key
            UNIQUE (TableName, SourceName, DateValue)
    );

    -- "What's the last successful date for this table?"
    CREATE NONCLUSTERED INDEX IX_PipelineExtractionState_LastSuccess
        ON ops.PipelineExtractionState (TableName, SourceName, Status, DateValue DESC)
        WHERE Status = 'SUCCESS';

    -- "Show me all failed dates in a range"
    CREATE NONCLUSTERED INDEX IX_PipelineExtractionState_Failures
        ON ops.PipelineExtractionState (TableName, SourceName, DateValue)
        INCLUDE (Status, BatchId, ProcessedAt)
        WHERE Status LIKE 'FAILED%';

    PRINT 'Created ops.PipelineExtractionState with indexes';
END
GO


-- ==========================================================================
-- 5. Quarantine (W-17)
--    Stores records that fail schema contracts.
--    Allows pipeline to continue processing valid records while isolating
--    problematic ones for investigation.
-- ==========================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'Quarantine'
)
BEGIN
    CREATE TABLE ops.Quarantine (
        QuarantineId        BIGINT IDENTITY(1,1)    PRIMARY KEY,
        TableName           NVARCHAR(255)           NOT NULL,
        SourceName          NVARCHAR(255)           NOT NULL,
        RejectionReason     NVARCHAR(MAX)           NOT NULL,
        RecordData          NVARCHAR(MAX)           NULL,       -- JSON representation of the failed record
        BatchId             BIGINT                  NULL,
        QuarantinedAt       DATETIME2               NOT NULL    DEFAULT GETUTCDATE()
    );

    CREATE NONCLUSTERED INDEX IX_Quarantine_Table_Date
        ON ops.Quarantine (TableName, SourceName, QuarantinedAt DESC)
        INCLUDE (RejectionReason, BatchId);

    PRINT 'Created ops.Quarantine with indexes';
END
GO


-- ==========================================================================
-- 6. ReconciliationLog (OBS-6)
--    Persists reconciliation results for trending and automated alerting.
--    Populated by reconcile_table(), reconcile_bronze(),
--    check_boundary_integrity(), and check_referential_integrity().
--
--    CheckType values:
--      STAGE_VS_SOURCE     — reconcile_table(): Stage CDC vs fresh source extract
--      BRONZE_VS_STAGE     — reconcile_bronze(): Bronze active vs Stage current
--      BOUNDARY_INTEGRITY  — check_boundary_integrity(): cross-layer count/key/null checks
--      REFERENTIAL_INTEGRITY — check_referential_integrity(): FK relationship checks
-- ==========================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'ReconciliationLog'
)
BEGIN
    CREATE TABLE ops.ReconciliationLog (
        ReconciliationId    BIGINT IDENTITY(1,1)    PRIMARY KEY,
        TableName           NVARCHAR(255)           NOT NULL,
        SourceName          NVARCHAR(255)           NOT NULL,
        CheckType           NVARCHAR(50)            NOT NULL,   -- STAGE_VS_SOURCE, BRONZE_VS_STAGE, BOUNDARY_INTEGRITY, REFERENTIAL_INTEGRITY
        IsClean             BIT                     NOT NULL    DEFAULT 1,
        -- Row counts
        SourceRows          INT                     NULL,
        TargetRows          INT                     NULL,
        MatchedRows         INT                     NULL,
        MismatchedRows      INT                     NULL,
        SourceOnlyRows      INT                     NULL,
        TargetOnlyRows      INT                     NULL,
        -- Referential integrity specifics
        OrphanedKeys        INT                     NULL,
        AmbiguousKeys       INT                     NULL,
        -- Boundary integrity specifics
        CountDelta          INT                     NULL,
        NullRateAnomalies   INT                     NULL,
        -- Detail
        MismatchedColumns   NVARCHAR(MAX)           NULL,       -- JSON: {"column_name": mismatch_count, ...}
        Errors              NVARCHAR(MAX)           NULL,       -- JSON array of error messages
        BatchId             BIGINT                  NULL,
        RunAt               DATETIME2               NOT NULL    DEFAULT GETUTCDATE()
    );

    -- "Is ACCT reconciliation getting worse over time?"
    CREATE NONCLUSTERED INDEX IX_ReconciliationLog_Table_Date
        ON ops.ReconciliationLog (TableName, SourceName, CheckType, RunAt DESC)
        INCLUDE (IsClean, MismatchedRows, SourceOnlyRows, TargetOnlyRows);

    -- "Show me all failed reconciliations this week"
    CREATE NONCLUSTERED INDEX IX_ReconciliationLog_Failures
        ON ops.ReconciliationLog (IsClean, RunAt DESC)
        INCLUDE (TableName, SourceName, CheckType, MismatchedRows)
        WHERE IsClean = 0;

    PRINT 'Created ops.ReconciliationLog with indexes';
END
GO


-- ==========================================================================
-- Log Retention Cleanup Job
-- 
-- Recommended: Run daily via SQL Agent.
-- Retention policy:
--   PipelineLog DEBUG/INFO:    30 days
--   PipelineLog WARNING:       90 days
--   PipelineLog ERROR/CRITICAL: Indefinite (never deleted)
--   PipelineEventLog:          365 days (keep 1 year for trending)
--   PipelineExtractionState:   Indefinite (small table, needed for gap detection)
--   Quarantine:                90 days
--   ReconciliationLog:         365 days
-- ==========================================================================

-- Uncomment and schedule via SQL Agent:
/*
DELETE FROM ops.PipelineLog
WHERE LogLevel IN ('DEBUG', 'INFO')
  AND CreatedAt < DATEADD(DAY, -30, GETUTCDATE());

DELETE FROM ops.PipelineLog
WHERE LogLevel = 'WARNING'
  AND CreatedAt < DATEADD(DAY, -90, GETUTCDATE());

DELETE FROM ops.PipelineEventLog
WHERE StartedAt < DATEADD(DAY, -365, GETUTCDATE());

DELETE FROM ops.Quarantine
WHERE QuarantinedAt < DATEADD(DAY, -90, GETUTCDATE());

DELETE FROM ops.ReconciliationLog
WHERE RunAt < DATEADD(DAY, -365, GETUTCDATE());
*/
GO


-- ==========================================================================
-- Verification: List all ops tables and their row counts
-- ==========================================================================

SELECT
    t.TABLE_SCHEMA + '.' + t.TABLE_NAME AS TableName,
    p.rows AS ApproxRowCount
FROM INFORMATION_SCHEMA.TABLES t
    INNER JOIN sys.partitions p
        ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
        AND p.index_id IN (0, 1)
WHERE t.TABLE_SCHEMA = 'ops'
ORDER BY t.TABLE_NAME;
GO
-- DDL: General.dbo.FileExtract
-- Metadata table for file-based extraction pipeline (Excel/CSV sources).
-- Mirrors UdmTablesList for database sources but with file-specific fields.

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'FileExtract'
)
BEGIN
    CREATE TABLE General.dbo.FileExtract (
        FileExtractId       INT IDENTITY(1,1) PRIMARY KEY,
        SourceName          NVARCHAR(50)   NOT NULL,
        TableName           NVARCHAR(128)  NOT NULL,
        BasePath            NVARCHAR(500)  NOT NULL,
        FilePattern         NVARCHAR(255)  NOT NULL,
        FileType            NVARCHAR(10)   NOT NULL,        -- 'xlsx', 'xls', 'csv', 'txt', 'json', 'ndjson'
        SheetName           NVARCHAR(128)  NULL,
        HeaderRow           INT            NOT NULL DEFAULT 0,
        SkipRows            INT            NOT NULL DEFAULT 0,
        Delimiter           NVARCHAR(5)    NULL,
        Encoding            NVARCHAR(20)   NULL DEFAULT 'utf-8',
        ColumnMapping       NVARCHAR(MAX)  NULL,             -- JSON: {"FileCol": "TargetCol"}
        ColumnsToExtract    NVARCHAR(MAX)  NULL,             -- JSON: ["col1", "col2"]
        StageTableName      NVARCHAR(128)  NULL,
        BronzeTableName     NVARCHAR(128)  NULL,
        PrimaryKeyColumns   NVARCHAR(500)  NOT NULL,         -- Comma-separated (REQUIRED)
        ChangeMode          NVARCHAR(20)   NOT NULL DEFAULT 'full_replace',
        ExpectedFrequency   NVARCHAR(20)   NULL,             -- 'daily','weekly','monthly','biannual','annual'
        ExpectedMinRows     INT            NULL DEFAULT 1,
        ExpectedColumns     NVARCHAR(MAX)  NULL,             -- JSON: ["col1", "col2"]
        IsActive            BIT            NOT NULL DEFAULT 1,
        StageLoadTool       NVARCHAR(20)   NOT NULL DEFAULT 'Python',
        CreatedAt           DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
        UpdatedAt           DATETIME2      NULL,
        CONSTRAINT UQ_FileExtract_Source_Table UNIQUE (SourceName, TableName)
    );

    PRINT 'Created General.dbo.FileExtract table';
END
ELSE
BEGIN
    PRINT 'General.dbo.FileExtract already exists — skipping';
END
GO

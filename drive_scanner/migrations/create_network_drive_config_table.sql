-- DDL: General.dbo.NetworkDriveConfig
-- Metadata table for network drive metadata scanner pipeline.
-- One row per mounted network drive to scan for file metadata.

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'NetworkDriveConfig'
)
BEGIN
    CREATE TABLE General.dbo.NetworkDriveConfig (
        NetworkDriveConfigId INT IDENTITY(1,1) PRIMARY KEY,
        DriveName           NVARCHAR(128)  NOT NULL,          -- Logical name: "FINANCE_SHARE"
        MountPath           NVARCHAR(1024) NOT NULL,          -- Linux mount point: "/mnt/finance"
        UNCPath             NVARCHAR(1024) NULL,              -- Windows UNC for reference: "\\server\finance$"
        SourceName          NVARCHAR(128)  NOT NULL DEFAULT 'NETWORK_DRIVES',
        TableName           NVARCHAR(128)  NOT NULL,          -- Logical table name: "FINANCE_FILES"
        StageTableName      NVARCHAR(128)  NULL,
        BronzeTableName     NVARCHAR(128)  NULL,
        ExcludePatterns     NVARCHAR(MAX)  NULL,              -- JSON: ["*.tmp", "~$*", "Thumbs.db"]
        IncludePatterns     NVARCHAR(MAX)  NULL,              -- JSON: ["*.pdf", "*.xlsx"] (NULL = all files)
        MaxDepth            INT            NULL,              -- Max recursion depth (NULL = unlimited)
        FollowSymlinks      BIT            NOT NULL DEFAULT 0,
        ScanEnabled         BIT            NOT NULL DEFAULT 1,
        IsActive            BIT            NOT NULL DEFAULT 1,
        StageLoadTool       NVARCHAR(50)   NOT NULL DEFAULT 'Python',
        PrimaryKeyColumns   NVARCHAR(512)  NOT NULL DEFAULT 'drive_name,full_file_path',
        CreatedAt           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        UpdatedAt           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_NetworkDriveConfig_DriveName UNIQUE (DriveName)
    );

    PRINT 'Created General.dbo.NetworkDriveConfig table';
END
ELSE
BEGIN
    PRINT 'General.dbo.NetworkDriveConfig already exists — skipping';
END
GO

-- Phase 3: durable audit foundation (deploy manually to AdventureWorks).
-- Deliberately does NOT update ingestion.watermark_state.
-- Do not remove WATERMARK_CONFIRMATION_BLOCKED until destination reconciliation
-- and an atomic, optimistic-locking commit procedure have been implemented/tested.
SET XACT_ABORT ON;
IF DB_NAME() <> N'AdventureWorks'
    THROW 51200, 'Audit objects must be deployed to AdventureWorks.', 1;
GO
IF OBJECT_ID(N'ingestion.bronze_run_audit', N'U') IS NULL
BEGIN
    CREATE TABLE ingestion.bronze_run_audit
    (
        audit_id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
        config_id int NOT NULL,
        parent_run_id nvarchar(100) NOT NULL,
        child_run_id nvarchar(100) NOT NULL,
        bronze_path nvarchar(1000) NOT NULL,
        load_type varchar(11) NOT NULL,
        expected_rows bigint NOT NULL,
        copied_rows bigint NULL,
        reconciled_rows bigint NULL,
        watermark_lower datetime2(7) NULL,
        watermark_lower_key_json nvarchar(1000) NULL,
        watermark_upper datetime2(7) NULL,
        watermark_upper_key_json nvarchar(1000) NULL,
        expected_row_version varbinary(8) NULL,
        status varchar(20) NOT NULL CONSTRAINT DF_bronze_audit_status DEFAULT ('PLANNED'),
        error_message nvarchar(2000) NULL,
        created_at_utc datetime2(7) NOT NULL CONSTRAINT DF_bronze_audit_created DEFAULT SYSUTCDATETIME(),
        updated_at_utc datetime2(7) NOT NULL CONSTRAINT DF_bronze_audit_updated DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_bronze_audit_child UNIQUE(child_run_id),
        CONSTRAINT CK_bronze_audit_mode CHECK(load_type IN ('FULL','INCREMENTAL')),
        CONSTRAINT CK_bronze_audit_status CHECK(status IN ('PLANNED','COPIED','RECONCILED','COMMITTED','FAILED')),
        CONSTRAINT CK_bronze_audit_counts CHECK(expected_rows >= 0 AND
            (copied_rows IS NULL OR copied_rows >= 0) AND
            (reconciled_rows IS NULL OR reconciled_rows >= 0)),
        CONSTRAINT FK_bronze_audit_config FOREIGN KEY(config_id)
            REFERENCES ingestion.table_config(config_id)
    );
    CREATE INDEX IX_bronze_audit_config_created
        ON ingestion.bronze_run_audit(config_id, created_at_utc DESC);
END;
GO
-- Security boundary: the ingestion reader should not be granted direct
-- UPDATE permission on watermark_state. Future commit uses an EXECUTE-only
-- stored procedure after independent destination validation.

-- Additive upgrade: never rewrite or fabricate evidence for existing runs.
IF COL_LENGTH('ingestion.bronze_run_audit','primary_key_columns') IS NULL
    ALTER TABLE ingestion.bronze_run_audit ADD primary_key_columns nvarchar(400) NULL;
IF COL_LENGTH('ingestion.bronze_run_audit','watermark_column') IS NULL
    ALTER TABLE ingestion.bronze_run_audit ADD watermark_column sysname NULL;
IF COL_LENGTH('ingestion.bronze_run_audit','manifest_json') IS NULL
    ALTER TABLE ingestion.bronze_run_audit ADD manifest_json nvarchar(max) NULL,
        manifest_sha256 varchar(64) NULL, attested_by sysname NULL,
        reconciled_at_utc datetime2(7) NULL, committed_at_utc datetime2(7) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('ingestion.bronze_run_audit')
               AND name='UQ_bronze_audit_parent_config')
    CREATE UNIQUE INDEX UQ_bronze_audit_parent_config
        ON ingestion.bronze_run_audit(parent_run_id,config_id);
IF OBJECT_ID('ingestion.bronze_parent_audit','U') IS NULL
    CREATE TABLE ingestion.bronze_parent_audit
    (parent_run_id nvarchar(100) NOT NULL PRIMARY KEY,
     committed_children int NOT NULL CHECK(committed_children=11),
     manifest_sha256 varchar(64) NOT NULL,
     committed_at_utc datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME());
GO

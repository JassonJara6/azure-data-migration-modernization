SET NOCOUNT ON;
SET XACT_ABORT ON;
USE [AdventureWorks];

IF SCHEMA_ID(N'ingestion') IS NULL
    EXEC(N'CREATE SCHEMA ingestion AUTHORIZATION dbo;');

IF OBJECT_ID(N'ingestion.table_config', N'U') IS NULL
BEGIN
    CREATE TABLE ingestion.table_config
    (
        config_id             int IDENTITY(1,1) NOT NULL,
        source_schema         sysname NOT NULL,
        source_table          sysname NOT NULL,
        load_type             varchar(11) NOT NULL,
        primary_key_columns   nvarchar(400) NOT NULL,
        watermark_column      sysname NULL,
        bronze_relative_path  nvarchar(400) NOT NULL,
        is_active             bit NOT NULL CONSTRAINT DF_table_config_is_active DEFAULT (1),
        created_at_utc        datetime2(7) NOT NULL CONSTRAINT DF_table_config_created_at DEFAULT (SYSUTCDATETIME()),
        updated_at_utc        datetime2(7) NOT NULL CONSTRAINT DF_table_config_updated_at DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_table_config PRIMARY KEY (config_id),
        CONSTRAINT UQ_table_config_source UNIQUE (source_schema, source_table),
        CONSTRAINT UQ_table_config_bronze_path UNIQUE (bronze_relative_path),
        CONSTRAINT CK_table_config_load_type CHECK (load_type IN ('FULL', 'INCREMENTAL')),
        CONSTRAINT CK_table_config_primary_key_json CHECK (ISJSON(primary_key_columns) = 1),
        CONSTRAINT CK_table_config_watermark CHECK
        (
            (load_type = 'INCREMENTAL' AND watermark_column IS NOT NULL)
            OR (load_type = 'FULL' AND watermark_column IS NULL)
        )
    );
END;

IF OBJECT_ID(N'ingestion.watermark_state', N'U') IS NULL
BEGIN
    CREATE TABLE ingestion.watermark_state
    (
        config_id                int NOT NULL,
        watermark_value          datetime2(7) NOT NULL,
        watermark_key_json       nvarchar(1000) NOT NULL,
        last_successful_run_id   nvarchar(100) NULL,
        last_successful_at_utc   datetime2(7) NULL,
        last_successful_row_count bigint NULL,
        row_version              rowversion NOT NULL,
        CONSTRAINT PK_watermark_state PRIMARY KEY (config_id),
        CONSTRAINT FK_watermark_state_config FOREIGN KEY (config_id)
            REFERENCES ingestion.table_config(config_id),
        CONSTRAINT CK_watermark_state_key_json CHECK (ISJSON(watermark_key_json) = 1),
        CONSTRAINT CK_watermark_state_row_count CHECK
            (last_successful_row_count IS NULL OR last_successful_row_count >= 0)
    );
END;

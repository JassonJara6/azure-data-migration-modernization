-- Run BEFORE deploying audit/commit scripts. Read-only schema compatibility check.
-- Expected database: AdventureWorks.
SET NOCOUNT ON;
IF DB_NAME()<>N'AdventureWorks'
    THROW 51300, 'Select AdventureWorks database before validation.', 1;
IF OBJECT_ID(N'ingestion.table_config',N'U') IS NULL
    THROW 51301, 'Missing ingestion.table_config.', 1;
IF OBJECT_ID(N'ingestion.watermark_state',N'U') IS NULL
    THROW 51302, 'Missing ingestion.watermark_state.', 1;

DECLARE @required table (table_name sysname, column_name sysname, type_name sysname);
INSERT @required VALUES
('table_config','config_id','int'),
('table_config','load_type','varchar'),
('table_config','primary_key_columns','nvarchar'),
('table_config','is_active','bit'),
('watermark_state','config_id','int'),
('watermark_state','watermark_value','datetime2'),
('watermark_state','watermark_key_json','nvarchar'),
('watermark_state','row_version','timestamp'),
('watermark_state','last_successful_run_id','nvarchar'),
('watermark_state','last_successful_at_utc','datetime2'),
('watermark_state','last_successful_row_count','bigint');
IF EXISTS (
    SELECT 1 FROM @required AS r
    LEFT JOIN sys.columns AS c ON c.object_id=OBJECT_ID(N'ingestion.'+r.table_name)
       AND c.name=r.column_name
    LEFT JOIN sys.types AS t ON t.user_type_id=c.user_type_id
    WHERE c.column_id IS NULL
       OR (r.type_name='timestamp' AND c.system_type_id<>189)
       OR (r.type_name<>'timestamp' AND t.name<>r.type_name)
)
BEGIN
    SELECT r.table_name,r.column_name,r.type_name AS expected_type,
           t.name AS actual_type,c.system_type_id
    FROM @required AS r
    LEFT JOIN sys.columns AS c ON c.object_id=OBJECT_ID(N'ingestion.'+r.table_name)
       AND c.name=r.column_name
    LEFT JOIN sys.types AS t ON t.user_type_id=c.user_type_id
    WHERE c.column_id IS NULL
       OR (r.type_name='timestamp' AND c.system_type_id<>189)
       OR (r.type_name<>'timestamp' AND t.name<>r.type_name);
    THROW 51303, 'Metadata schema differs from expected contract.', 1;
END;
IF (SELECT COUNT_BIG(*) FROM ingestion.table_config WHERE is_active=1)<>11
    THROW 51304, 'Expected exactly eleven active source configurations.', 1;
IF EXISTS (SELECT 1 FROM ingestion.table_config
           WHERE is_active=1 AND load_type='INCREMENTAL'
           AND NOT EXISTS (SELECT 1 FROM ingestion.watermark_state AS s
                           WHERE s.config_id=ingestion.table_config.config_id))
    THROW 51305, 'Incremental table missing watermark state.', 1;
SELECT 'PASS' AS result, 'Metadata schema compatible with audit and commit scripts' AS details;

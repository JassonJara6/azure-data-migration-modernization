-- Register immutable bounded plan and successful Copy counters, not reconciliation.
-- This procedure does not advance watermarks or attest to destination rows.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE ingestion.register_bronze_copy
 @config_id int,
 @parent_run_id nvarchar(100),
 @child_run_id nvarchar(100),
 @bronze_path nvarchar(1000),
 @load_type varchar(11),
 @expected_rows bigint,
 @copied_rows bigint,
 @watermark_lower datetime2(7)=NULL,
 @watermark_lower_key_json nvarchar(1000)=NULL,
 @watermark_upper datetime2(7)=NULL,
 @watermark_upper_key_json nvarchar(1000)=NULL,
 @expected_row_version varbinary(8)=NULL
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 IF DB_NAME()<>N'AdventureWorks'
    THROW 51400, 'Incorrect database.', 1;
 IF NULLIF(LTRIM(RTRIM(@child_run_id)),N'') IS NULL
    OR NULLIF(LTRIM(RTRIM(@parent_run_id)),N'') IS NULL
    OR @expected_rows IS NULL OR @expected_rows<0
    OR @copied_rows IS NULL OR @copied_rows<>@expected_rows
    THROW 51401, 'Missing run identity or inconsistent Copy counters.', 1;
 IF @bronze_path IS NULL
    OR RIGHT(@bronze_path,LEN(N'/run_id='+@child_run_id))<>N'/run_id='+@child_run_id
    OR @bronze_path LIKE N'%..%'
    THROW 51402, 'Bronze destination path does not match child run.', 1;
 DECLARE @source_path nvarchar(400), @actual_mode varchar(11);
 SELECT @source_path=bronze_relative_path,@actual_mode=load_type
 FROM ingestion.table_config
 WHERE config_id=@config_id AND is_active=1;
 IF @source_path IS NULL OR @actual_mode<>@load_type
    OR LEFT(@bronze_path,LEN(@source_path)+1)<>@source_path+N'/'
    OR CHARINDEX(N'/load_type='+LOWER(@load_type)+N'/',@bronze_path)=0
    THROW 51403, 'Source config or Bronze path differs from approved plan.', 1;
 IF @load_type='INCREMENTAL'
 BEGIN
    IF @watermark_lower IS NULL OR @watermark_upper IS NULL
       OR ISJSON(@watermark_lower_key_json)<>1
       OR ISJSON(@watermark_upper_key_json)<>1
       OR @expected_row_version IS NULL
       THROW 51404, 'Incremental plan cursor evidence missing.', 1;
    IF NOT EXISTS(SELECT 1 FROM ingestion.watermark_state
       WHERE config_id=@config_id AND watermark_value=@watermark_lower
         AND row_version=@expected_row_version)
       THROW 51405, 'Planned watermark is already stale.', 1;
 END
 ELSE IF @load_type='FULL'
 BEGIN
    IF @watermark_lower IS NOT NULL OR @watermark_upper IS NOT NULL
       OR @expected_row_version IS NOT NULL
       THROW 51406, 'FULL load cannot carry incremental watermark.', 1;
 END
 ELSE THROW 51407, 'Unsupported load mode.', 1;
 BEGIN TRY
   BEGIN TRANSACTION;
   IF EXISTS(SELECT 1 FROM ingestion.bronze_run_audit WITH(UPDLOCK,HOLDLOCK)
             WHERE child_run_id=@child_run_id)
       THROW 51408, 'Duplicate child run: immutable audit cannot be overwritten.', 1;
   INSERT ingestion.bronze_run_audit
    (config_id,parent_run_id,child_run_id,bronze_path,load_type,
     expected_rows,copied_rows,watermark_lower,watermark_lower_key_json,
     watermark_upper,watermark_upper_key_json,expected_row_version,status)
   VALUES
    (@config_id,@parent_run_id,@child_run_id,@bronze_path,@load_type,
     @expected_rows,@copied_rows,@watermark_lower,@watermark_lower_key_json,
     @watermark_upper,@watermark_upper_key_json,@expected_row_version,'COPIED');
   COMMIT;
 END TRY
 BEGIN CATCH
   IF XACT_STATE()<>0 ROLLBACK;
   THROW;
 END CATCH
END;
GO
-- Deliberately no grants. Only trusted pipeline identity may execute.

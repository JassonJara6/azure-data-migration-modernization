-- EXECUTE-only ADF receipt; no reconciliation or watermark writes.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE ingestion.register_bronze_copy
 @config_id int, @parent_run_id nvarchar(100), @child_run_id nvarchar(100),
 @bronze_path nvarchar(1000), @load_type varchar(11),
 @expected_rows bigint, @copied_rows bigint,
 @watermark_lower datetime2(7)=NULL, @watermark_lower_key_json nvarchar(1000)=NULL,
 @watermark_upper datetime2(7)=NULL, @watermark_upper_key_json nvarchar(1000)=NULL,
 @expected_row_version varchar(18)=NULL
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 IF DB_NAME()<>'AdventureWorks' THROW 51400,'Incorrect database.',1;
 IF TRY_CONVERT(uniqueidentifier,@child_run_id) IS NULL OR LEN(@child_run_id)<>36
    OR TRY_CONVERT(uniqueidentifier,@parent_run_id) IS NULL OR LEN(@parent_run_id)<>36
    OR @expected_rows IS NULL OR @expected_rows<0 OR @copied_rows IS NULL
    OR @expected_rows<>@copied_rows THROW 51401,'Invalid identity or Copy counters.',1;
 DECLARE @version varbinary(8)=TRY_CONVERT(varbinary(8),@expected_row_version,1);
 IF @expected_row_version IS NOT NULL AND (LEN(@expected_row_version)<>18 OR @version IS NULL)
    THROW 51404,'Invalid rowversion hex.',1;
 BEGIN TRY
  BEGIN TRANSACTION;
  IF EXISTS(SELECT 1 FROM ingestion.bronze_run_audit WITH(UPDLOCK,HOLDLOCK) WHERE child_run_id=@child_run_id)
  BEGIN
   IF EXISTS(SELECT @config_id,@parent_run_id,@bronze_path,@load_type,@expected_rows,@copied_rows,
                    @watermark_lower,@watermark_lower_key_json,@watermark_upper,@watermark_upper_key_json,@version
             EXCEPT SELECT config_id,parent_run_id,bronze_path,load_type,expected_rows,copied_rows,
                    watermark_lower,watermark_lower_key_json,watermark_upper,watermark_upper_key_json,expected_row_version
                    FROM ingestion.bronze_run_audit WHERE child_run_id=@child_run_id)
      THROW 51408,'Conflicting replay of immutable copy receipt.',1;
   COMMIT; RETURN;
  END;
  DECLARE @source_path nvarchar(400),@pk nvarchar(400),@wm sysname,@mode varchar(11),@date varchar(10);
  SELECT @source_path=bronze_relative_path,@pk=primary_key_columns,@wm=watermark_column,@mode=load_type
  FROM ingestion.table_config WITH(HOLDLOCK) WHERE config_id=@config_id AND is_active=1;
  SET @date=SUBSTRING(@bronze_path,LEN(@source_path)+LEN('/load_type='+LOWER(@mode)+'/ingestion_date=')+1,10);
  IF @source_path IS NULL OR @mode<>@load_type OR TRY_CONVERT(date,@date,23) IS NULL
     OR @bronze_path COLLATE Latin1_General_100_BIN2 <>
        (@source_path+'/load_type='+LOWER(@mode)+'/ingestion_date='+@date+'/run_id='+@child_run_id) COLLATE Latin1_General_100_BIN2
     THROW 51403,'Destination differs from canonical source/mode/date/run path.',1;
  IF @mode='INCREMENTAL'
  BEGIN
   IF @watermark_lower IS NULL OR @watermark_upper IS NULL OR @version IS NULL
      OR ISNULL(ISJSON(@watermark_lower_key_json),0)<>1 OR ISNULL(ISJSON(@watermark_upper_key_json),0)<>1
      THROW 51404,'Incremental cursor evidence missing.',1;
   IF NOT EXISTS(SELECT 1 FROM ingestion.watermark_state WITH(UPDLOCK,HOLDLOCK)
       WHERE config_id=@config_id AND watermark_value=@watermark_lower AND row_version=@version)
      THROW 51405,'Planned watermark is stale.',1;
  END
  ELSE IF @mode='FULL'
  BEGIN
   IF @watermark_lower IS NOT NULL OR @watermark_upper IS NOT NULL OR @version IS NOT NULL
      OR @watermark_lower_key_json IS NOT NULL OR @watermark_upper_key_json IS NOT NULL
      THROW 51406,'FULL cannot carry cursor evidence.',1;
  END
  ELSE THROW 51407,'Unsupported load mode.',1;
  INSERT ingestion.bronze_run_audit
   (config_id,parent_run_id,child_run_id,bronze_path,load_type,expected_rows,copied_rows,
    watermark_lower,watermark_lower_key_json,watermark_upper,watermark_upper_key_json,
    expected_row_version,primary_key_columns,watermark_column,status)
  VALUES(@config_id,@parent_run_id,@child_run_id,@bronze_path,@load_type,@expected_rows,@copied_rows,
    @watermark_lower,@watermark_lower_key_json,@watermark_upper,@watermark_upper_key_json,
    @version,@pk,@wm,'COPIED');
  COMMIT;
 END TRY
 BEGIN CATCH
  IF XACT_STATE()<>0 ROLLBACK;
  THROW;
 END CATCH;
END;
GO

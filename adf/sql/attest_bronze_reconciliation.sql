-- Only the trusted independent reconciler may execute this procedure.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE ingestion.attest_bronze_reconciliation
 @child_run_id nvarchar(100), @bronze_path nvarchar(1000), @reconciled_rows bigint,
 @manifest_json nvarchar(max), @manifest_sha256 varchar(64)
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 IF DB_NAME()<>'AdventureWorks' THROW 51500,'Incorrect database.',1;
 IF @reconciled_rows IS NULL OR @reconciled_rows<0 OR ISNULL(ISJSON(@manifest_json),0)<>1
    OR LEN(ISNULL(@manifest_sha256,''))<>64
    OR LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',CONVERT(varbinary(max),@manifest_json)),2))<>@manifest_sha256
    THROW 51501,'Invalid manifest or digest (SHA256 of UTF-16LE JSON).',1;
 BEGIN TRY
  BEGIN TRANSACTION;
  DECLARE @status varchar(20),@expected bigint,@copied bigint,@path nvarchar(1000),@hash varchar(64),@parent nvarchar(100),
    @mode varchar(11),@lower nvarchar(33),@upper nvarchar(33),@lk nvarchar(1000),@uk nvarchar(1000);
  SELECT @status=status,@expected=expected_rows,@copied=copied_rows,@path=bronze_path,
         @hash=manifest_sha256,@parent=parent_run_id,@mode=load_type,
         @lower=ISNULL(CONVERT(nvarchar(33),watermark_lower,126),N''),
         @upper=ISNULL(CONVERT(nvarchar(33),watermark_upper,126),N''),
         @lk=ISNULL(watermark_lower_key_json,N''),@uk=ISNULL(watermark_upper_key_json,N'')
  FROM ingestion.bronze_run_audit WITH(UPDLOCK,HOLDLOCK) WHERE child_run_id=@child_run_id;
  IF @status IS NULL OR @status NOT IN('COPIED','RECONCILED','COMMITTED')
     THROW 51502,'Run is not eligible for attestation.',1;
  IF @bronze_path IS NULL OR @path<>@bronze_path OR @expected<>@reconciled_rows OR @copied<>@expected
     OR ISNULL(JSON_VALUE(@manifest_json,'$.run_id'),'')<>@child_run_id
     OR ISNULL(JSON_VALUE(@manifest_json,'$.parent_run_id'),'')<>@parent
     OR ISNULL(JSON_VALUE(@manifest_json,'$.folder'),'')<>@path
     OR ISNULL(TRY_CONVERT(bigint,JSON_VALUE(@manifest_json,'$.reconciled_rows')),-1)<>@expected
     THROW 51503,'Manifest differs from immutable receipt.',1;
  IF ISNULL(JSON_VALUE(@manifest_json,'$.load_type'),'')<>@mode
     OR ISNULL(JSON_VALUE(@manifest_json,'$.lower'),'missing')<>@lower
     OR ISNULL(JSON_VALUE(@manifest_json,'$.upper'),'missing')<>@upper
     OR ISNULL(JSON_VALUE(@manifest_json,'$.lower_key_json'),'missing')<>@lk
     OR ISNULL(JSON_VALUE(@manifest_json,'$.upper_key_json'),'missing')<>@uk
     OR ISNULL(JSON_VALUE(@manifest_json,'$.filesystem'),'')<>'bronze'
     OR LEN(ISNULL(JSON_VALUE(@manifest_json,'$.account'),'')) NOT BETWEEN 3 AND 24
     OR JSON_VALUE(@manifest_json,'$.account') COLLATE Latin1_General_100_BIN2 LIKE '%[^a-z0-9]%'
     OR ISNULL(LEFT(JSON_QUERY(@manifest_json,'$.files'),1),'')<>'['
     OR ISNULL((SELECT SUM(TRY_CONVERT(bigint,JSON_VALUE([value],'$.rows'))) FROM OPENJSON(@manifest_json,'$.files')),0)<>@reconciled_rows
     OR EXISTS(SELECT 1 FROM OPENJSON(@manifest_json,'$.files') WHERE
        TRY_CONVERT(bigint,JSON_VALUE([value],'$.rows')) IS NULL
        OR TRY_CONVERT(bigint,JSON_VALUE([value],'$.rows'))<0
        OR ISNULL(TRY_CONVERT(bigint,JSON_VALUE([value],'$.bytes')),0)<=0
        OR LEN(ISNULL(JSON_VALUE([value],'$.sha256'),''))<>64
        OR NULLIF(JSON_VALUE([value],'$.etag'),'') IS NULL)
     THROW 51506,'Manifest contract or file evidence is inconsistent.',1;
  IF @status IN('RECONCILED','COMMITTED')
  BEGIN
   IF @hash IS NULL OR @hash<>@manifest_sha256 THROW 51505,'Conflicting reconciliation replay.',1;
   COMMIT; RETURN;
  END;
  UPDATE ingestion.bronze_run_audit SET reconciled_rows=@reconciled_rows,status='RECONCILED',
    manifest_json=@manifest_json,manifest_sha256=@manifest_sha256,attested_by=ORIGINAL_LOGIN(),
    reconciled_at_utc=SYSUTCDATETIME(),updated_at_utc=SYSUTCDATETIME()
  WHERE child_run_id=@child_run_id AND status='COPIED';
  IF @@ROWCOUNT<>1 THROW 51504,'Concurrent attestation.',1;
  COMMIT;
 END TRY
 BEGIN CATCH
  IF XACT_STATE()<>0 ROLLBACK;
  THROW;
 END CATCH;
END;
GO

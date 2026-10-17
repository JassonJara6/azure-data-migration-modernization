-- All eleven commits share this transaction. A failed child rolls all back.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE ingestion.commit_bronze_parent @parent_run_id nvarchar(100)
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 IF DB_NAME()<>'AdventureWorks' THROW 51600,'Incorrect database.',1;
 BEGIN TRY
  BEGIN TRANSACTION;
  IF EXISTS(SELECT 1 FROM ingestion.bronze_parent_audit WITH(UPDLOCK,HOLDLOCK) WHERE parent_run_id=@parent_run_id)
  BEGIN
   COMMIT; RETURN; -- Historical receipt, not a request to move state again.
  END;
  IF (SELECT COUNT(*) FROM ingestion.table_config WITH(HOLDLOCK) WHERE is_active=1)<>11
     OR (SELECT COUNT(*) FROM ingestion.table_config WHERE is_active=1 AND load_type='INCREMENTAL')<>5
     OR (SELECT COUNT(*) FROM ingestion.bronze_run_audit WITH(UPDLOCK,HOLDLOCK) WHERE parent_run_id=@parent_run_id)<>11
     OR EXISTS(SELECT 1 FROM ingestion.bronze_run_audit AS a
       LEFT JOIN ingestion.table_config AS c ON c.config_id=a.config_id AND c.is_active=1
       WHERE a.parent_run_id=@parent_run_id AND
        (c.config_id IS NULL OR a.status<>'RECONCILED' OR a.manifest_sha256 IS NULL))
     THROW 51601,'Parent requires eleven independently reconciled active sources.',1;
  DECLARE @child nvarchar(100);
  DECLARE children CURSOR LOCAL FAST_FORWARD FOR
    SELECT child_run_id FROM ingestion.bronze_run_audit WHERE parent_run_id=@parent_run_id ORDER BY config_id;
  OPEN children;
  FETCH NEXT FROM children INTO @child;
  WHILE @@FETCH_STATUS=0
  BEGIN
   EXEC ingestion.commit_bronze_run @child_run_id=@child;
   FETCH NEXT FROM children INTO @child;
  END;
  CLOSE children;
  DEALLOCATE children;
  DECLARE @hashes nvarchar(max)=(SELECT child_run_id+N':'+manifest_sha256+N';'
      FROM ingestion.bronze_run_audit WHERE parent_run_id=@parent_run_id ORDER BY config_id
      FOR XML PATH(''),TYPE).value('.','nvarchar(max)');
  INSERT ingestion.bronze_parent_audit(parent_run_id,committed_children,manifest_sha256)
  VALUES(@parent_run_id,11,LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',CONVERT(varbinary(max),@hashes)),2)));
  COMMIT;
 END TRY
 BEGIN CATCH
  IF XACT_STATE()<>0 ROLLBACK;
  THROW;
 END CATCH;
END;
GO

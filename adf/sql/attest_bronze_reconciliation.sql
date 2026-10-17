-- Trusted reconciler calls this only AFTER independently reading the exact
-- child-run Parquet files and validating row-level metadata. Do not grant
-- EXECUTE to ADF ingestion identity or other untrusted callers.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE ingestion.attest_bronze_reconciliation
 @child_run_id nvarchar(100),
 @bronze_path nvarchar(1000),
 @reconciled_rows bigint
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 IF DB_NAME()<>N'AdventureWorks' THROW 51500,'Incorrect database.',1;
 IF NULLIF(LTRIM(RTRIM(@child_run_id)),N'') IS NULL
    OR @reconciled_rows IS NULL OR @reconciled_rows<0
    THROW 51501,'Invalid reconciliation attestation.',1;
 BEGIN TRY
   BEGIN TRANSACTION;
   DECLARE @status varchar(20),@expected bigint,@copied bigint,@stored_path nvarchar(1000);
   SELECT @status=status,@expected=expected_rows,@copied=copied_rows,
          @stored_path=bronze_path
   FROM ingestion.bronze_run_audit WITH(UPDLOCK,HOLDLOCK)
   WHERE child_run_id=@child_run_id;
   IF @status IS NULL OR @status<>'COPIED'
      THROW 51502,'Only COPIED runs can be reconciled.',1;
   IF @stored_path<>@bronze_path OR @expected<>@reconciled_rows
      OR @copied<>@expected
      THROW 51503,'Destination evidence does not match immutable plan.',1;
   UPDATE ingestion.bronze_run_audit
   SET reconciled_rows=@reconciled_rows,status='RECONCILED',
       updated_at_utc=SYSUTCDATETIME()
   WHERE child_run_id=@child_run_id AND status='COPIED';
   IF @@ROWCOUNT<>1 THROW 51504,'Concurrent reconciliation transition.',1;
   COMMIT;
 END TRY
 BEGIN CATCH
   IF XACT_STATE()<>0 ROLLBACK;
   THROW;
 END CATCH
END;
GO
-- Security: EXECUTE privilege ONLY for the separate trusted reconciler identity.
-- SQL alone cannot verify whether caller really read Parquet.

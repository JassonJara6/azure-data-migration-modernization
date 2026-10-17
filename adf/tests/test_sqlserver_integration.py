"""SQL Server 2022 integration tests, only in an explicitly isolated test container.

Never point these destructive fixtures at a user/source instance. The database
AdventureWorks in that disposable container contains only metadata test objects.
"""
import os
import subprocess
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
CONTAINER=os.environ.get('BRONZE_SQL_TEST_CONTAINER','')
pytestmark=pytest.mark.skipif(not CONTAINER,reason='Requires isolated SQL Server test container')


def sql(text,database='AdventureWorks'):
    if not CONTAINER.startswith('codex-bronze-sql-test') or not os.environ.get('BRONZE_SQL_TEST_PASSWORD'):
        raise ValueError('Refusing non-test SQL instance')
    env=dict(os.environ,SQLCMDPASSWORD=os.environ['BRONZE_SQL_TEST_PASSWORD'])
    result=subprocess.run(['docker','exec','-e','SQLCMDPASSWORD','-i',CONTAINER,
       '/opt/mssql-tools18/bin/sqlcmd','-S','localhost','-U','sa','-C','-I','-b','-r','1','-d',database],
       input=text,text=True,capture_output=True,env=env)
    if result.returncode:raise AssertionError(result.stdout+'\n'+result.stderr)
    return result.stdout


@pytest.fixture(scope='module',autouse=True)
def deploy():
    if not CONTAINER:return
    sql("IF DB_ID('AdventureWorks') IS NULL CREATE DATABASE AdventureWorks;",'master')
    paths=['sql/metadata/001_create_ingestion_metadata.sql','sql/metadata/002_seed_ingestion_metadata.sql',
      'adf/sql/preflight_bronze_commit.sql','adf/sql/create_bronze_run_audit.sql',
      'adf/sql/register_bronze_copy.sql','adf/sql/attest_bronze_reconciliation.sql',
      'adf/sql/commit_bronze_run.sql','adf/sql/commit_bronze_parent.sql']
    for path in paths:sql((ROOT/path).read_text())
    # Additive deployment is itself idempotent.
    sql((ROOT/'adf/sql/create_bronze_run_audit.sql').read_text())


@pytest.fixture(autouse=True)
def reset():
    if not CONTAINER:return
    sql("""DELETE ingestion.bronze_parent_audit; DELETE ingestion.bronze_run_audit;
    UPDATE w SET watermark_value='19000101',last_successful_run_id=NULL,last_successful_at_utc=NULL,
       last_successful_row_count=NULL,watermark_key_json=CASE WHEN JSON_VALUE(c.primary_key_columns,'$[1]') IS NULL
        THEN N'{"'+JSON_VALUE(c.primary_key_columns,'$[0]')+N'":0}' ELSE N'{"SalesOrderID":0,"SalesOrderDetailID":0}' END
    FROM ingestion.watermark_state w JOIN ingestion.table_config c ON c.config_id=w.config_id;""")


def receipts(parent,empty=False):
    return """
DECLARE @parent nvarchar(100)=N'%s',@empty bit=%s;
DECLARE @config int,@mode varchar(11),@prefix nvarchar(400),@child nvarchar(100),@path nvarchar(1000),
 @lower datetime2(7),@upper datetime2(7),@lk nvarchar(1000),@uk nvarchar(1000),@version varchar(18),@n bigint,@manifest nvarchar(max),@hash varchar(64);
DECLARE configs CURSOR LOCAL FAST_FORWARD FOR SELECT config_id,load_type,bronze_relative_path FROM ingestion.table_config ORDER BY config_id;
OPEN configs; FETCH NEXT FROM configs INTO @config,@mode,@prefix;
WHILE @@FETCH_STATUS=0
BEGIN
 SELECT @child=CONVERT(nvarchar(36),NEWID()),@lower=NULL,@upper=NULL,@lk=NULL,@uk=NULL,@version=NULL,@n=2;
 IF @mode='INCREMENTAL'
 BEGIN
  SELECT @lower=watermark_value,@lk=watermark_key_json,@version=CONVERT(varchar(18),CONVERT(binary(8),row_version),1)
   FROM ingestion.watermark_state WHERE config_id=@config;
  SELECT @upper=CASE WHEN @empty=1 THEN @lower ELSE CONVERT(datetime2(7),'2026-10-17T12:00:00.1234567') END,
    @uk=CASE WHEN @empty=1 THEN @lk ELSE REPLACE(@lk,':0',':1') END,@n=CASE WHEN @empty=1 THEN 0 ELSE 1 END;
 END;
 SET @path=@prefix+'/load_type='+LOWER(@mode)+'/ingestion_date=2026-10-17/run_id='+@child;
 EXEC ingestion.register_bronze_copy @config,@parent,@child,@path,@mode,@n,@n,@lower,@lk,@upper,@uk,@version;
 -- Exact copy receipt retries are safe before confirmation too.
 EXEC ingestion.register_bronze_copy @config,@parent,@child,@path,@mode,@n,@n,@lower,@lk,@upper,@uk,@version;
 SET @manifest=(SELECT @child AS run_id,@parent AS parent_run_id,@path AS folder,@n AS reconciled_rows,
 @mode AS load_type,ISNULL(CONVERT(nvarchar(33),@lower,126),'') AS lower,
 ISNULL(CONVERT(nvarchar(33),@upper,126),'') AS upper,ISNULL(@lk,'') AS lower_key_json,ISNULL(@uk,'') AS upper_key_json,
 'storage123' AS account,'bronze' AS filesystem,
 JSON_QUERY(CASE WHEN @n=0 THEN '[]' ELSE
 (SELECT 'part-00000.parquet' AS name,1 AS bytes,REPLICATE('a',64) AS sha256,'test-etag' AS etag,@n AS rows FOR JSON PATH) END) AS files
 FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
 SET @hash=LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',CONVERT(varbinary(max),@manifest)),2));
 EXEC ingestion.attest_bronze_reconciliation @child,@path,@n,@manifest,@hash;
 EXEC ingestion.attest_bronze_reconciliation @child,@path,@n,@manifest,@hash;
 FETCH NEXT FROM configs INTO @config,@mode,@prefix;
END;
CLOSE configs; DEALLOCATE configs;
GO
"""%(parent,1 if empty else 0)

P1='10000000-0000-0000-0000-000000000001'
P2='10000000-0000-0000-0000-000000000002'


def test_two_consecutive_runs_and_exact_parent_retry():
    sql(receipts(P1)+f"""
EXEC ingestion.commit_bronze_parent N'{P1}';
IF (SELECT COUNT(*) FROM ingestion.bronze_run_audit WHERE status='COMMITTED')<>11 THROW 52000,'First parent not complete.',1;
DECLARE @before nvarchar(max)=(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH);
EXEC ingestion.commit_bronze_parent N'{P1}';
DECLARE @child nvarchar(100)=(SELECT TOP 1 child_run_id FROM ingestion.bronze_run_audit WHERE parent_run_id=N'{P1}');
EXEC ingestion.commit_bronze_run @child;
IF @before<>(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH) THROW 52001,'Retry rewrote state.',1;
""")
    sql(receipts(P2,True)+f"""
DECLARE @before nvarchar(max)=(SELECT config_id,watermark_value,watermark_key_json FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH);
EXEC ingestion.commit_bronze_parent N'{P2}';
IF @before<>(SELECT config_id,watermark_value,watermark_key_json FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH) THROW 52002,'Empty run moved cursor.',1;
IF EXISTS(SELECT 1 FROM ingestion.watermark_state WHERE last_successful_row_count<>0) THROW 52003,'Empty run not audited.',1;
IF (SELECT COUNT(*) FROM ingestion.bronze_parent_audit)<>2 OR (SELECT COUNT(*) FROM ingestion.bronze_run_audit WHERE status='COMMITTED')<>22 THROW 52004,'Two parents not complete.',1;
-- Historical replay stays idempotent after a newer run commits.
DECLARE @state nvarchar(max)=(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH);
EXEC ingestion.commit_bronze_parent N'{P1}';
IF @state<>(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH) THROW 52005,'Historical retry regressed state.',1;
""")


def test_last_stale_cursor_rolls_back_entire_parent():
    sql(receipts(P1)+f"""
UPDATE ingestion.watermark_state SET watermark_value=watermark_value
 WHERE config_id=(SELECT MAX(config_id) FROM ingestion.table_config WHERE load_type='INCREMENTAL');
DECLARE @before nvarchar(max)=(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH);
BEGIN TRY
 EXEC ingestion.commit_bronze_parent N'{P1}';
 THROW 52006,'Stale parent unexpectedly succeeded.',1;
END TRY BEGIN CATCH IF ERROR_NUMBER()<>51215 THROW; END CATCH;
IF @before<>(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH) THROW 52007,'Partial watermark commit.',1;
IF EXISTS(SELECT 1 FROM ingestion.bronze_run_audit WHERE status<>'RECONCILED') OR EXISTS(SELECT 1 FROM ingestion.bronze_parent_audit) THROW 52008,'Partial audit commit.',1;
""")


@pytest.mark.parametrize('mutation', ['duplicate_keys','backward','empty_advance','changed_contract','missing_manifest'])
def test_corrupt_evidence_rejected_without_state_change(mutation):
    changes={
      'duplicate_keys':"UPDATE ingestion.bronze_run_audit SET watermark_upper_key_json=N'{\"SalesOrderID\":1,\"SalesOrderID\":2}' WHERE config_id=@c;",
      'backward':"UPDATE ingestion.bronze_run_audit SET watermark_upper='18990101' WHERE config_id=@c;",
      'empty_advance':"UPDATE ingestion.bronze_run_audit SET expected_rows=0,copied_rows=0,reconciled_rows=0 WHERE config_id=@c;",
      'changed_contract':"UPDATE ingestion.bronze_run_audit SET primary_key_columns=N'[\"changed\"]' WHERE config_id=@c;",
      'missing_manifest':"UPDATE ingestion.bronze_run_audit SET manifest_json=NULL,manifest_sha256=NULL WHERE config_id=@c;"}
    sql(receipts(P1)+f"""
DECLARE @c int=(SELECT config_id FROM ingestion.table_config WHERE source_table='SalesOrderDetail');
{changes[mutation]}
DECLARE @before nvarchar(max)=(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH),@rejected bit=0;
BEGIN TRY EXEC ingestion.commit_bronze_parent N'{P1}'; END TRY BEGIN CATCH SET @rejected=1; END CATCH;
IF @rejected<>1 THROW 52010,'Corrupt evidence accepted.',1;
IF @before<>(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH) THROW 52011,'Rejected parent mutated state.',1;
""")


def test_full_without_watermark_and_missing_child_rejection():
    sql(receipts(P1)+f"""
DECLARE @child nvarchar(100)=(SELECT TOP 1 child_run_id FROM ingestion.bronze_run_audit WHERE load_type='FULL');
EXEC ingestion.commit_bronze_run @child;
IF (SELECT COUNT(*) FROM ingestion.watermark_state)<>5 THROW 52012,'FULL created watermark.',1;
DELETE ingestion.bronze_run_audit WHERE child_run_id=@child;
BEGIN TRY EXEC ingestion.commit_bronze_parent N'{P1}'; THROW 52013,'Missing child accepted.',1;
END TRY BEGIN CATCH IF ERROR_NUMBER()<>51601 THROW; END CATCH;
""")


def test_adf_cannot_attest_or_write_watermarks():
    sql("""
IF USER_ID('bronze_adf_test') IS NULL CREATE USER bronze_adf_test WITHOUT LOGIN;
GRANT SELECT ON ingestion.table_config TO bronze_adf_test;
GRANT SELECT ON ingestion.watermark_state TO bronze_adf_test;
GRANT EXECUTE ON ingestion.register_bronze_copy TO bronze_adf_test;
EXECUTE AS USER='bronze_adf_test';
DECLARE @denied bit=0;
BEGIN TRY UPDATE ingestion.watermark_state SET watermark_value=watermark_value; END TRY BEGIN CATCH IF ERROR_NUMBER()=229 SET @denied=1; ELSE THROW; END CATCH;
IF @denied<>1 THROW 52014,'ADF can update watermarks.',1;
SET @denied=0;
BEGIN TRY EXEC ingestion.attest_bronze_reconciliation 'x','x',0,N'{}','x'; END TRY BEGIN CATCH IF ERROR_NUMBER()=229 SET @denied=1; ELSE THROW; END CATCH;
IF @denied<>1 THROW 52015,'ADF can attest.',1;
REVERT;
""")


def test_copy_and_attestation_replays_after_commit_and_conflicts():
    sql(receipts(P1)+f"""
EXEC ingestion.commit_bronze_parent N'{P1}';
DECLARE @c int,@p nvarchar(100),@r nvarchar(100),@path nvarchar(1000),@m varchar(11),@n bigint,
 @l datetime2(7),@u datetime2(7),@lk nvarchar(1000),@uk nvarchar(1000),@v varchar(18),@manifest nvarchar(max),@hash varchar(64);
SELECT TOP 1 @c=config_id,@p=parent_run_id,@r=child_run_id,@path=bronze_path,@m=load_type,@n=expected_rows,
 @l=watermark_lower,@u=watermark_upper,@lk=watermark_lower_key_json,@uk=watermark_upper_key_json,
 @v=CONVERT(varchar(18),expected_row_version,1),@manifest=manifest_json,@hash=manifest_sha256
 FROM ingestion.bronze_run_audit WHERE load_type='INCREMENTAL';
DECLARE @before nvarchar(max)=(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH);
EXEC ingestion.register_bronze_copy @c,@p,@r,@path,@m,@n,@n,@l,@lk,@u,@uk,@v;
EXEC ingestion.attest_bronze_reconciliation @r,@path,@n,@manifest,@hash;
SET @n=@n+1;
BEGIN TRY EXEC ingestion.register_bronze_copy @c,@p,@r,@path,@m,@n,@n,@l,@lk,@u,@uk,@v;
 THROW 52020,'Conflicting replay accepted.',1;
END TRY BEGIN CATCH IF ERROR_NUMBER()<>51408 THROW; END CATCH;
IF @before<>(SELECT * FROM ingestion.watermark_state ORDER BY config_id FOR JSON PATH) THROW 52021,'Replay mutated state.',1;
""")


def capture_plan(config_id):
    template=(ROOT/'adf/sql/plan_bronze_batch.sql').read_text().replace('__CONFIG_ID__',str(config_id)).replace("'","''")
    return """DECLARE @plan table(plan_contract_version int,config_id int,source_schema sysname,source_table sysname,
 load_type varchar(11),bronze_relative_path nvarchar(400),expected_rows varchar(20),watermark_lower nvarchar(33),
 lower_key_json nvarchar(1000),watermark_upper nvarchar(33),upper_key_json nvarchar(1000),expected_row_version varchar(18),extract_query nvarchar(max));
 INSERT @plan EXEC sys.sp_executesql N'%s';
 IF (SELECT COUNT(*) FROM @plan)<>1 THROW 52022,'Planner result is not exactly one row.',1;
 IF EXISTS(SELECT 1 FROM @plan WHERE LEN(expected_row_version)<>18 OR LEFT(expected_row_version,2)<>'0x') THROW 52023,'Invalid rowversion encoding.',1;
 """%template


def test_real_planner_single_result_and_copy_excludes_new_upper_tuples():
    sql("""
IF SCHEMA_ID('Sales') IS NULL EXEC('CREATE SCHEMA Sales');
IF OBJECT_ID('Sales.SalesOrderDetail') IS NULL CREATE TABLE Sales.SalesOrderDetail
 (SalesOrderID int NOT NULL,SalesOrderDetailID int NOT NULL,ModifiedDate datetime2(7) NOT NULL,
 PRIMARY KEY(SalesOrderID,SalesOrderDetailID));
DELETE Sales.SalesOrderDetail;
INSERT Sales.SalesOrderDetail VALUES(100,999,'2026-10-17T12:00:00.1234567'),
 (102,9,'2026-10-17T12:00:00.1234567'),(102,10,'2026-10-17T12:00:00.1234567');
GO
"""+capture_plan(10)+"""
IF EXISTS(SELECT 1 FROM @plan WHERE expected_rows<>'3' OR JSON_VALUE(upper_key_json,'$.SalesOrderID')<>'102'
 OR JSON_VALUE(upper_key_json,'$.SalesOrderDetailID')<>'10' OR watermark_upper<>'2026-10-17T12:00:00.1234567') THROW 52024,'Wrong maximal tuple.',1;
INSERT Sales.SalesOrderDetail VALUES(102,11,'2026-10-17T12:00:00.1234567'),
 (103,1,'2026-10-17T12:00:00.1234567'),(1,1,'2026-10-17T12:00:00.1234568');
DECLARE @query nvarchar(max)=(SELECT extract_query FROM @plan);
IF LEFT(@query,7)<>'SELECT ' OR CHARINDEX('USE ',@query)>0 THROW 52025,'Copy is not a single SELECT.',1;
DECLARE @copied table(SalesOrderID int,SalesOrderDetailID int,ModifiedDate datetime2(7));
INSERT @copied EXEC sys.sp_executesql @query;
IF (SELECT COUNT(*) FROM @copied)<>3 OR EXISTS(SELECT 1 FROM @copied WHERE SalesOrderID>102 OR
 (SalesOrderID=102 AND SalesOrderDetailID>10) OR ModifiedDate>'2026-10-17T12:00:00.1234567')
 THROW 52026,'Copy exceeded fixed lexicographic upper tuple.',1;
""")


def test_real_empty_plan_keeps_complete_lower_cursor():
    sql("""
IF SCHEMA_ID('Production') IS NULL EXEC('CREATE SCHEMA Production');
IF OBJECT_ID('Production.Product') IS NULL CREATE TABLE Production.Product(ProductID int NOT NULL PRIMARY KEY,ModifiedDate datetime2(7) NOT NULL);
DELETE Production.Product;
GO
"""+capture_plan(5)+"""
IF EXISTS(SELECT 1 FROM @plan WHERE expected_rows<>'0' OR watermark_lower<>watermark_upper OR lower_key_json<>upper_key_json)
 THROW 52027,'Empty plan invented progress.',1;
""")


def test_trusted_identity_commits_parent_without_direct_child_or_table_writes():
    sql(receipts(P1)+f"""
IF USER_ID('bronze_trusted_test') IS NULL CREATE USER bronze_trusted_test WITHOUT LOGIN;
GRANT EXECUTE ON ingestion.commit_bronze_parent TO bronze_trusted_test;
EXECUTE AS USER='bronze_trusted_test';
IF ISNULL(HAS_PERMS_BY_NAME('ingestion.watermark_state','OBJECT','UPDATE'),0)<>0 THROW 52028,'Trusted identity has direct write.',1;
EXEC ingestion.commit_bronze_parent N'{P1}';
REVERT;
IF (SELECT COUNT(*) FROM ingestion.bronze_run_audit WHERE status='COMMITTED')<>11 THROW 52029,'Ownership-chain commit failed.',1;
""")

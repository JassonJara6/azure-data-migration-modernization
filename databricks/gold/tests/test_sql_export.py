"""Strict MOCK SQL responses, real Arrow/Parquet; not live SQL validation."""
import copy
import json
from decimal import Decimal
from pathlib import Path
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from admm_silver.contracts import TABLES,SilverError,ticks
from local.fixture_data import rows
from local import sql_export as exporter
from local.sql_export import Settings,export,select_sql,upper_sql,pk_sql,schema_sql,table_name,ExportError
from local.sql_manifest import silver_input,checksum,load


def metadata(table):
    result=[]
    for c in table['columns']:
        ty=c['type'];p=s=0
        if ty=='opaque':system=declared='geography'
        elif ty=='timestamp':system=declared='datetime'
        elif ty=='int':system=declared='int'
        elif ty=='smallint':system=declared='tinyint' if c['sql_type']=='tinyint' else 'smallint'
        elif ty=='boolean':system=declared='bit'
        elif ty.startswith('decimal'):
            system=declared='decimal';p,s=map(int,ty[8:-1].split(','))
        elif c['sql_type']=='XML':system=declared='xml'
        elif c['sql_type']=='uniqueidentifier':system=declared='uniqueidentifier'
        else:system=declared='nvarchar'
        result.append((c['name'],system,declared,p,s))
    return result


class MockConnection:
    """Reject any statement not in the complete, read-only expected query set."""
    def __init__(self,case=None):
        self.case=case;self.calls=[];self.rollbacks=0;self.closed=False;self.commits=0
        self.reader=MockCursor(self)
    def cursor(self):return self.reader
    def rollback(self):self.rollbacks+=1
    def commit(self):self.commits+=1;pytest.fail('Exporter must NEVER commit SQL')
    def close(self):self.closed=True
    def __enter__(self):pytest.fail('pyodbc context exit can commit: never use it')


class MockCursor:
    def __init__(self,connection):self.connection=connection;self.values=[];self.description=[];self.closed=False
    def execute(self,sql):
        import re
        assert not re.search(r'\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|EXEC|COMMIT)\b',sql,re.I)
        assert ';' not in sql
        self.connection.calls.append(sql);case=self.connection.case
        if sql in ('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE','SET LOCK_TIMEOUT 60000'):self.values=[];return self
        if sql=="SELECT DB_NAME(),CAST(SERVERPROPERTY('EngineEdition') AS int)":
            self.values=[('wrong' if case=='database' else 'AdventureWorks',5 if case=='azure-engine' else 3)];return self
        for table in TABLES:
            data=rows(table,1)
            if table['source']=='Person.Address':data[0]['SpatialLocation']=b'opaque-geography-serialize-test'
            if sql==schema_sql(table):
                self.values=metadata(table)
                if case=='schema':self.values.pop()
                if case=='type':self.values[0]=(self.values[0][0],'float','float',0,0)
                return self
            if sql==pk_sql(table):
                self.values=[(k,) for k in table['pk']]
                if case=='pk':self.values=[('WrongKey',)]
                return self
            if sql=='SELECT COUNT_BIG(*) FROM '+table_name(table):
                self.values=[(len(data)+(case=='short-read'),)];return self
            keys=(['ModifiedDate'] if table['mode']=='INCREMENTAL' else [])+table['pk']
            data=sorted(data,key=lambda r:tuple(r[k] for k in keys))
            if sql==upper_sql(table):
                self.values=[tuple(data[-1][k] for k in ['ModifiedDate',*table['pk']])];return self
            if sql==select_sql(table):
                self.values=[tuple(row[c['name']] for c in table['columns']) for row in data]
                self.description=[(c['name'],) for c in table['columns']]
                if case=='read':raise RuntimeError('synthetic-private-details')
                return self
        pytest.fail('Unknown SQL; mock does not accept arbitrary commands')
    def fetchone(self):return self.values.pop(0) if self.values else None
    def fetchall(self):data=self.values;self.values=[];return data
    def fetchmany(self,n):data=self.values[:n];self.values=self.values[n:];return data
    def close(self):self.closed=True


def exported(tmp_path,case=None,chunk=2,expected=None):
    connection=MockConnection(case)
    manifest=export(tmp_path,Settings('SYNTHETIC_USER','SYNTHETIC_PASSWORD'),chunk_rows=chunk,
        expected_counts=expected,connector=lambda settings:connection)
    return manifest,connection


def test_mock_sql_exports_all_eleven_with_bounded_parts_and_no_remote_receipt(tmp_path):
    manifest,connection=exported(tmp_path)
    assert manifest['status']=='LOCAL_EXPORTED_VERIFIED' and manifest['source']['remote_commit_confirmed'] is False
    assert manifest['source']['read_transaction_end']=='ROLLBACK'
    assert '"COMMITTED"' not in json.dumps(manifest)
    assert connection.rollbacks==1 and connection.commits==0 and connection.closed and connection.reader.closed
    counts={t['source']:t['source_rows'] for t in manifest['tables']}
    assert len(counts)==11 and sum(counts.values())==25
    assert counts['Production.Product']==3 and counts['Sales.SalesOrderDetail']==5
    assert all(f['rows']<=2 for t in manifest['tables'] for f in t['files'])
    before=copy.deepcopy(manifest);bundle,bronze=silver_input(load(tmp_path/'local-sql-export.json'),tmp_path)
    assert manifest==before and bundle['evidence_kind']=='LOCAL_SQL_EXPORT' and not bundle['remote_commit_confirmed']
    assert len(bundle['tables'])==11
    address=next(t for t in manifest['tables'] if t['source']=='Person.Address')
    arrow=pq.ParquetFile(bronze/address['folder']/address['files'][0]['name']).read()
    assert arrow.schema.field('ModifiedDate').type==pa.timestamp('ns')
    exact=arrow['ModifiedDate'].cast(pa.int64())[0].as_py()
    assert exact==ticks('2020-02-01T00:00:00.1234567')*100
    assert arrow['SpatialLocation'][0].as_py()==b'opaque-geography-serialize-test'
    assert arrow.schema.metadata[b'admm.remote_commit_confirmed']==b'false'

@pytest.mark.parametrize('case,code',[('database','LOCAL_SQL_DATABASE_IDENTITY_INVALID'),('azure-engine','LOCAL_SQL_DATABASE_IDENTITY_INVALID'),
 ('schema','LOCAL_SQL_SCHEMA_COLUMNS_CHANGED'),('type','LOCAL_SQL_SCHEMA_TYPE_CHANGED'),('pk','LOCAL_SQL_PRIMARY_KEY_CHANGED'),
 ('short-read','LOCAL_SQL_ROW_RECONCILIATION_FAILED'),('read','LOCAL_SQL_EXPORT_FAILED')])
def test_mock_sql_failures_rollback_and_never_publish_success(tmp_path,case,code):
    connection=MockConnection(case)
    with pytest.raises(ExportError,match=code):
        export(tmp_path,Settings('SYNTHETIC_USER','SYNTHETIC_PASSWORD'),connector=lambda s:connection)
    assert connection.rollbacks==1 and connection.closed and connection.commits==0
    assert not (tmp_path/'local-sql-export.json').exists()


def test_user_count_baseline_checked_before_payload(tmp_path):
    connection=MockConnection()
    with pytest.raises(ExportError,match='LOCAL_SQL_EXPECTED_COUNT_MISMATCH') as caught:
        export(tmp_path,Settings('SYNTHETIC_USER','SYNTHETIC_PASSWORD'),connector=lambda s:connection,
            expected_counts={'Production.Product':504,'Sales.SalesOrderDetail':121317})
    assert caught.value.stage=='EXPECTED_COUNTS' and caught.value.source_table=='Production.Product'
    assert not any(sql==select_sql(t) for sql in connection.calls for t in TABLES)

@pytest.mark.parametrize('server',['example.database.windows.net','localhost;Database=master','127.0.0.1,1433;UID=x','localhost,65536','127.0.0.1,0'])
def test_nonlocal_or_injected_endpoint_rejected_before_connect(tmp_path,server):
    with pytest.raises(ExportError,match='LOCAL_SQL_LOOPBACK_REQUIRED'):
        export(tmp_path,Settings('SYNTHETIC_USER','SYNTHETIC_PASSWORD',server=server),connector=lambda *a:pytest.fail('No connection'))


def test_odbc_credentials_with_delimiters_are_braced_and_repr_hides_them():
    settings=Settings('SYNTHETIC;USER}','SYNTHETIC;P}ASS!$')
    value=settings.connection_string()
    assert 'UID={SYNTHETIC;USER}}}' in value and 'PWD={SYNTHETIC;P}}ASS!$}' in value
    assert 'SYNTHETIC' not in repr(settings)
    assert 'DATABASE={AdventureWorks}' in value


def test_sql_projection_is_explicit_and_preserves_datetime_money_and_opaque_types():
    person=next(t for t in TABLES if t['source']=='Person.Person')
    detail=next(t for t in TABLES if t['source']=='Sales.SalesOrderDetail')
    address=next(t for t in TABLES if t['source']=='Person.Address')
    assert 'CONVERT(varchar(27),CONVERT(datetime2(7),[ModifiedDate]),126)' in select_sql(person)
    assert 'CONVERT(nvarchar(max),[Demographics])' in select_sql(person)
    assert 'CONVERT(decimal(38,6),[LineTotal])' in select_sql(detail)
    assert '[SpatialLocation].Serialize() AS [SpatialLocation]' in select_sql(address)
    assert 'ORDER BY [ModifiedDate],[SalesOrderID],[SalesOrderDetailID]' in select_sql(detail)
    assert 'SELECT *' not in select_sql(person) and 'USE ' not in select_sql(person)

@pytest.mark.parametrize('case',['hash','path','remote','file','extra-file'])
def test_reused_manifest_and_actual_files_are_verified_before_replay(tmp_path,case):
    manifest,_=exported(tmp_path);bundle,bronze=silver_input(manifest,tmp_path)
    if case=='hash':manifest['manifest_sha256']='0'*64
    elif case=='path':manifest['tables'][0]['folder']='../escape';manifest['manifest_sha256']=checksum(manifest)
    elif case=='remote':manifest['source']['remote_commit_confirmed']=True;manifest['manifest_sha256']=checksum(manifest)
    elif case=='file':next(bronze.rglob('*.parquet')).write_bytes(b'tampered')
    else:(next(bronze.rglob('*.parquet')).parent/'part-extra.parquet').write_bytes(b'extra')
    with pytest.raises((ExportError,SilverError)):silver_input(manifest,tmp_path)


def test_mock_sql_to_actual_spark_silver_gold_and_replay(tmp_path,spark):
    from local.run import execute
    manifest,_=exported(tmp_path,chunk=2);bundle,bronze=silver_input(manifest,tmp_path)
    # Data and connection are MOCKED. Only Spark/Delta IO is real in this test.
    result=execute(spark,bundle,bronze,'localsql',tmp_path,data_kind='MOCK_SQL_EXPORT_TEST')
    assert len(result['silver']['metrics'])==11 and all(m['rejected_rows']==0 for m in result['silver']['metrics'].values())
    assert result['gold']['metrics']['fact_sales']['rows']==5
    assert Decimal(result['gold']['metrics']['sales_reconciliation']['net_sales'])==Decimal('115.5')
    assert result['replay']['verified'] and result['replay']['versions_before']==result['replay']['versions_after']
    assert sum(len(v) for v in result['replay']['versions_after'].values())==22 and not result['azure_validated']


def test_mock_cli_new_export_then_replay_without_sql_or_password(tmp_path,monkeypatch,capsys):
    from types import SimpleNamespace as NS
    from local import run_sql as cli
    monkeypatch.setenv('ADMM_SQL_USER','SYNTHETIC_USER');monkeypatch.setenv('ADMM_SQL_PASSWORD','SYNTHETIC_PASSWORD')
    calls=[]
    def create(root,settings,**kw):
        calls.append('export')
        return export(root,settings,**kw,connector=lambda s:MockConnection())
    def session(root):
        assert 'ADMM_SQL_PASSWORD' not in cli.os.environ
        return NS(version='3.5.3',sparkContext=NS(_jvm=NS(java=NS(lang=NS(System=NS(getProperty=lambda key:'17.0.17'))))),stop=lambda:None)
    def execute(spark,bundle,bronze,account,root,**kw):
        from admm_gold.model import KEYS
        assert kw['data_kind']=='LOCAL_SQL_SERVER_EXPORT'
        return {'status':'SUCCEEDED','azure_validated':False,'silver':{'metrics':{t['source']:{'input_rows':len(rows(t,1)),
            'target_rows':len(rows(t,1)),'rejected_rows':0} for t in TABLES}},'gold':{'metrics':{name:{'rows':0} for name in KEYS}}}
    monkeypatch.setattr(cli,'export',create);monkeypatch.setattr(cli,'session',session);monkeypatch.setattr(cli,'execute',execute)
    monkeypatch.setattr(cli.sys,'argv',['run_sql.py','--root',str(tmp_path),'--expected-product-rows','3','--expected-detail-rows','5'])
    assert cli.main()==0 and calls==['export']
    manifest=(tmp_path/'local-sql-export.json').read_bytes()
    assert cli.main()==0 and calls==['export'] and (tmp_path/'local-sql-export.json').read_bytes()==manifest
    report=json.loads(next(tmp_path.glob('sql-acceptance-*.json')).read_text())
    assert report['export_reused'] and not report['sql_queried_this_invocation'] and not report['remote_commit_confirmed']
    assert 'SYNTHETIC_PASSWORD' not in capsys.readouterr().out
    assert 'SYNTHETIC_PASSWORD' not in json.dumps(report)


def test_cli_does_not_overwrite_synthetic_lake_or_expose_connection_error(tmp_path,monkeypatch,capsys):
    from local import run_sql as cli
    monkeypatch.setattr(cli.sys,'argv',['run_sql.py','--root',str(tmp_path)])
    (tmp_path/'provenance.json').write_text(json.dumps({'data_kind':'SYNTHETIC_FIXTURES'}))
    monkeypatch.setattr(cli,'export',lambda *a,**k:pytest.fail('No SQL/export permitted'))
    assert cli.main()==1
    assert 'LOCAL_SQL_LAKE_PROVENANCE_CHANGED' in capsys.readouterr().err


def test_hidden_prompt_never_falls_back_to_echo(monkeypatch):
    import warnings
    from local import run_sql as cli
    monkeypatch.delenv('ADMM_SQL_PASSWORD',raising=False)
    def unsafe_prompt(*a):warnings.warn('Cannot hide input',cli.getpass.GetPassWarning)
    monkeypatch.setattr(cli.getpass,'getpass',unsafe_prompt)
    with pytest.raises(ExportError,match='LOCAL_SQL_HIDDEN_PROMPT_UNAVAILABLE'):cli.password_from_environment(True)


def test_sql_errors_only_expose_sqlstate_stage_and_contract_table(tmp_path,monkeypatch,capsys):
    from local import run_sql as cli
    monkeypatch.setenv('ADMM_SQL_PASSWORD','SYNTHETIC_SECRET')
    monkeypatch.setattr(cli.sys,'argv',['run_sql.py','--root',str(tmp_path)])
    def fail(*a,**k):
        error=exporter.safe_error('LOCAL_SQL_EXPORT_FAILED',Exception('42000','SYNTHETIC_SECRET credentials'))
        error.stage='SCHEMA';error.source_table='Person.Person';raise error
    monkeypatch.setattr(cli,'export',fail)
    assert cli.main()==1
    output=capsys.readouterr().err;report=json.loads(output)
    assert report['sqlstate']=='42000' and report['stage']=='SCHEMA' and report['source_table']=='Person.Person'
    assert 'SYNTHETIC_SECRET' not in output and 'SYNTHETIC_SECRET' not in (tmp_path/'validation-status.json').read_text()

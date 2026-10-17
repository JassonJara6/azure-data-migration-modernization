"""Safe diagnostics and ODBC boundary regression tests; no Azure access."""
import json
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import bronze_connection as connection
from bronze_diagnostics import BronzeFailure,report_error,step,close_resources
from commit_reconciled_bronze import SqlStore
import commit_bronze_parent as parent

PARENT='10000000-0000-0000-0000-000000000001'
SECRET="sensitive;PWD={a}}b};token=private"

@pytest.mark.parametrize('state,native,expected',[
 ('28000',18456,'SQL_LOGIN_FAILED'),('42000',4060,'SQL_DATABASE_UNAVAILABLE'),
 ('42000',229,'SQL_PERMISSION_DENIED'),('42000',2812,'SQL_CONTRACT_MISMATCH'),
 ('42000',51601,'SQL_GUARD_REJECTED'),('08001',10061,'SQL_CONNECTION_FAILED'),
 ('HYT00',0,'SQL_TIMEOUT'),('40001',1205,'SQL_DEADLOCK'),
 ('IM002',0,'ODBC_DRIVER_UNAVAILABLE'),('HY104',0,'SQL_PARAMETER_BINDING_FAILED')])
def test_allowlisted_sql_diagnostics(state,native,expected):
    error=Exception(state,f'{SECRET} [{state}] ({native}) SQL and row data')
    report=report_error(error,'sql_attest_child')
    assert report['code']==expected and report['sqlstate']==state
    assert SECRET not in json.dumps(report) and 'SQL and row data' not in json.dumps(report)


def test_certificate_and_http_diagnostics_do_not_print_messages():
    assert report_error(Exception('08001','SSL Provider certificate '+SECRET))['code']=='SQL_TLS_FAILED'
    error=Exception(SECRET);error.status_code=403
    report=report_error(error,'azure_verify_parent')
    assert report['http_status']==403 and SECRET not in json.dumps(report)
    assert SECRET not in json.dumps(report_error(ValueError(SECRET)))


def test_nested_commit_failure_is_unknown_and_context_is_allowlisted():
    with pytest.raises(BronzeFailure) as caught:
        with step('sql_commit_parent',parent_run_id=PARENT,password=SECRET):
            with step('sql_execute'):raise Exception('08S01',SECRET)
    report=caught.value.report
    assert report['parent_commit_outcome']=='UNKNOWN' and report['parent_run_id']==PARENT
    assert SECRET not in json.dumps(report)


def test_password_values_are_braced_and_not_shell_interpolated(monkeypatch):
    password="a;} {'\"$!`&\\ space"
    value=connection.build_connection('127.0.0.1,1433','bronze_reconciler',password,trust_local_certificate=True)
    assert 'PWD={'+password.replace('}','}}')+'};' in value
    assert 'DATABASE={AdventureWorks};' in value and 'Encrypt={yes};' in value
    monkeypatch.setenv('BRONZE_SQL_CONNECTION_STRING','stale connection')
    monkeypatch.setattr(connection,'prompt_password',lambda:password)
    assert connection.configured_connection(interactive=True,trust_local_certificate=True)==value

@pytest.mark.parametrize('server',['remote.example,1433','127.0.0.1;Server=remote'])
def test_certificate_exception_cannot_target_remote_sql(server):
    with pytest.raises(ValueError):connection.build_connection(server,'user','password',trust_local_certificate=True)


def test_secure_prompt_refuses_echo_fallback(monkeypatch):
    def fallback(*args):warnings.warn('cannot hide',connection.getpass.GetPassWarning)
    monkeypatch.setattr(connection.getpass,'getpass',fallback)
    with pytest.raises(BronzeFailure) as caught:connection.prompt_password()
    assert caught.value.report['code']=='SECURE_PROMPT_UNAVAILABLE'


class Cursor:
    description=[('database_ok',)]
    def execute(self,*args):return self
    def fetchall(self):return [(0,)]
    def nextset(self):raise Exception('08S01',SECRET)
    def close(self):raise Exception('cleanup '+SECRET)


def test_deferred_odbc_error_is_not_ignored_or_masked_by_cleanup():
    store=SqlStore(NS(cursor=lambda:Cursor()))
    with pytest.raises(BronzeFailure) as caught:store.commit_parent(PARENT)
    assert caught.value.report['stage']=='sql_commit_parent'
    assert caught.value.report['parent_commit_outcome']=='UNKNOWN'
    assert SECRET not in json.dumps(caught.value.report)
    close_resources(Cursor())


def test_select_one_does_not_validate_database():
    with pytest.raises(BronzeFailure) as caught:SqlStore(NS(cursor=lambda:Cursor())).validate_session()
    assert caught.value.report['code']=='SQL_WRONG_DATABASE'

@pytest.mark.parametrize('change',[{'committed_children':10},{'manifest_sha256':'invalid'},{'parent_run_id':'other'},{'committed_children':True}])
def test_invalid_historical_receipt_never_reports_committed(change):
    receipt=dict(parent_run_id=PARENT,committed_children=11,manifest_sha256='a'*64);receipt.update(change)
    store=NS(parent_receipt=lambda _:receipt)
    with pytest.raises(BronzeFailure):parent.commit_parent(store,None,'account','bronze',PARENT,None)


def test_lost_postcommit_receipt_is_unknown(monkeypatch):
    calls=[]
    configs=[dict(config_id=i,load_type='INCREMENTAL' if i<5 else 'FULL') for i in range(11)]
    runs=[dict(config_id=i,child_run_id=f'00000000-0000-0000-0000-{i:012d}',status='COPIED') for i in range(11)]
    def receipt(_):
        if calls:raise Exception('08S01',SECRET)
        return None
    store=NS(parent_receipt=receipt,active_configs=lambda:configs,parent_runs=lambda _:runs,commit_parent=lambda _:calls.append('commit'))
    monkeypatch.setattr(parent,'attest_run',lambda *args:{})
    monkeypatch.setattr(parent,'verify_unchanged',lambda *args:None)
    with pytest.raises(BronzeFailure) as caught:parent.commit_parent(store,None,'account','bronze',PARENT,lambda *args:None)
    assert calls==['commit'] and caught.value.report['parent_commit_outcome']=='UNKNOWN'
    assert caught.value.report['stage']=='sql_verify_parent_receipt'


def test_subscription_discovery_captures_cli_output(monkeypatch):
    monkeypatch.delenv('AZURE_SUBSCRIPTION_ID',raising=False)
    monkeypatch.setattr(parent.shutil,'which',lambda _:'az')
    def cli(args,**kwargs):
        assert args==['az','account','show','--query','id','--output','tsv'] and kwargs['capture_output']
        return NS(returncode=0,stdout=PARENT+'\n')
    monkeypatch.setattr(parent.subprocess,'run',cli)
    assert parent.discover_subscription(True)==PARENT


def test_factory_discovery_reads_existing_run_only(monkeypatch):
    monkeypatch.delenv('ADF_FACTORY',raising=False);monkeypatch.delenv('BRONZE_ADF_FACTORY',raising=False)
    calls=[]
    adf=NS(factories=NS(list_by_resource_group=lambda group:[NS(name='existing-factory')]),
           pipeline_runs=NS(get=lambda *args:calls.append(args)))
    assert parent.discover_factory(adf,'existing-group',PARENT,True)=='existing-factory'
    assert calls==[('existing-group','existing-factory',PARENT)]


def test_repeated_activity_page_token_is_rejected():
    from datetime import datetime,timezone
    now=datetime.now(timezone.utc)
    adf=NS(pipeline_runs=NS(get=lambda *args:NS(pipeline_name='pl_adventureworks_bronze',status='Failed',run_start=now,run_end=now)),
      activity_runs=NS(query_by_pipeline_run=lambda *args:NS(value=[],continuation_token='same')))
    with pytest.raises(ValueError,match='Repeated'):parent.verify_adf_parent(adf,'group','factory',PARENT,[])


def test_only_exact_versioned_validation_reasons_are_disclosed():
    assert report_error(ValueError('Destination row outside bounded cursor'))['code']=='PARQUET_CURSOR_OUT_OF_BOUNDS'
    report=report_error(ValueError('Destination row outside bounded cursor '+SECRET))
    assert report['code']=='BRONZE_VALIDATION_FAILED' and SECRET not in json.dumps(report)


def test_cli_reports_connection_stage_without_traceback_or_password(monkeypatch,capsys):
    import azure.identity
    import pyodbc
    monkeypatch.setattr(sys,'argv',['commit_bronze_parent.py','--account','storage123',
      '--resource-group','existing-group','--parent-run-id',PARENT,'--interactive'])
    monkeypatch.setattr(parent,'configured_connection',lambda **kwargs:SECRET)
    monkeypatch.setattr(azure.identity,'DefaultAzureCredential',lambda **kwargs:NS(close=lambda:None))
    def failed_connections(*args,**kwargs):
        with step('sql_connect'):raise pyodbc.OperationalError('08001',SECRET+' certificate verify failed')
    monkeypatch.setattr(parent,'connections',failed_connections)
    assert parent.main()==1
    captured=capsys.readouterr();report=json.loads(captured.err)
    assert not captured.out and report['stage']=='sql_connect' and report['code']=='SQL_TLS_FAILED'
    assert SECRET not in captured.err and 'Traceback' not in captured.err


def test_cli_historical_receipt_needs_no_azure_context_or_read(monkeypatch,capsys):
    import azure.identity
    import azure.mgmt.datafactory
    receipt=dict(parent_run_id=PARENT,committed_children=11,manifest_sha256='a'*64)
    monkeypatch.setattr(sys,'argv',['commit_bronze_parent.py','--account','storage123',
      '--resource-group','existing-group','--parent-run-id',PARENT])
    monkeypatch.setattr(parent,'configured_connection',lambda **kwargs:SECRET)
    monkeypatch.setattr(azure.identity,'DefaultAzureCredential',lambda **kwargs:NS(close=lambda:None))
    def forbidden(*args,**kwargs):pytest.fail('Historical replay must not read Azure')
    monkeypatch.setattr(azure.mgmt.datafactory,'DataFactoryManagementClient',forbidden)
    monkeypatch.setattr(parent,'connections',lambda *args,**kwargs:(NS(close=lambda:None),NS(close=lambda:None)))
    monkeypatch.setattr(parent,'SqlStore',lambda _:NS(parent_receipt=lambda _:receipt))
    assert parent.main()==0
    captured=capsys.readouterr()
    assert json.loads(captured.out)['replayed'] is True and not captured.err

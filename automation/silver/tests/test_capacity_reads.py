"""PR #32 regression: bounded pagination, stage-safe timeouts, legacy readers."""
import json
import subprocess
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from types import SimpleNamespace as NS
import pytest
from capacity_reads import read_sku_catalog,SKU_URL
from check_capacity import inspect_capacity
from plan_guard import Blocked
from test_compute_capacity import inputs,PRIMARY,SCHEMA
from diagnose import SUBSCRIPTION,TENANT


def page_url(token):return SKU_URL+'&$skiptoken='+token


def test_pages_are_complete_before_return_and_progress_has_no_urls():
    calls=[];events=[]
    def read(argv,stage,timeout):
        calls.append(argv)
        assert stage=='AZURE_SKUS' and 0<timeout<=60
        return {'value':[{'name':'first'}],'nextLink':page_url('synthetic-private-token')} if len(calls)==1 else {'value':[{'name':'last'}]}
    assert read_sku_catalog(read,progress=events.append)==[{'name':'first'},{'name':'last'}]
    assert len(calls)==2 and all(c[:4]==['az','rest','--method','get'] for c in calls)
    assert 'synthetic-private-token' not in json.dumps(events) and 'https' not in json.dumps(events)


def test_transient_page_timeout_retried_only_on_same_get():
    calls=[]
    def read(argv,*args):
        calls.append(argv)
        if len(calls)==1:raise Blocked('READ_TIMEOUT_AT_AZURE_SKUS')
        return {'value':[{'name':'complete'}]}
    assert read_sku_catalog(read)==[{'name':'complete'}]
    assert calls[0]==calls[1]


def test_partial_data_never_returned_after_failed_second_page():
    calls=[]
    def read(*args):
        calls.append(args)
        if len(calls)==1:return {'value':[{'name':'partial'}],'nextLink':page_url('next')}
        raise Blocked('READ_TIMEOUT_AT_AZURE_SKUS')
    with pytest.raises(Blocked,match='READ_TIMEOUT_AT_AZURE_SKUS'):read_sku_catalog(read)
    assert len(calls)==3 # Page one and exactly two attempts at page two.

@pytest.mark.parametrize('link',['https://evil.invalid/skus','http://management.azure.com/x','https://management.azure.com/subscriptions/other/providers/Microsoft.Compute/skus?api-version=2021-07-01',SKU_URL+'#fragment'])
def test_bad_pagination_link_never_followed(link):
    calls=[]
    def read(*args):calls.append(args);return {'value':[],'nextLink':link}
    with pytest.raises(Blocked,match='INVALID_AZURE_SKU_NEXT_LINK'):read_sku_catalog(read)
    assert len(calls)==1

@pytest.mark.parametrize('payload',[None,[],{'value':None},{'value':[None]},{'value':[],'nextLink':3}])
def test_malformed_page_blocks(payload):
    with pytest.raises(Blocked,match='INVALID_AZURE_SKU_'):read_sku_catalog(lambda *a:payload)


def test_repeated_link_and_page_limit_block():
    with pytest.raises(Blocked,match='REPEATED_AZURE_SKU_NEXT_LINK'):
        read_sku_catalog(lambda *a:{'value':[],'nextLink':SKU_URL})
    with pytest.raises(Blocked,match='AZURE_SKU_PAGE_LIMIT_EXCEEDED'):
        read_sku_catalog(lambda *a:{'value':[],'nextLink':page_url('next')},max_pages=1)


def test_total_budget_expiry_even_with_successful_partial_read():
    times=iter([0,0,181])
    with pytest.raises(Blocked,match='READ_TIMEOUT_AT_AZURE_SKUS'):
        read_sku_catalog(lambda *a:{'value':[]},clock=lambda:next(times))


def test_access_denied_is_not_retried():
    calls=[]
    def denied(*args):calls.append(args);raise Blocked('ACCESS_DENIED_AT_AZURE_SKUS')
    with pytest.raises(Blocked):read_sku_catalog(denied)
    assert len(calls)==1


def data_reader(fail_stage=None):
    a=inputs()
    for sku in a[2]:sku['family']='testFamily'
    cluster=dict(node_type_id=PRIMARY,policy_id='p',spark_version='15.4.x-scala2.12',num_workers=0,data_security_mode='SINGLE_USER',azure_attributes={'availability':'ON_DEMAND_AZURE'})
    def read(argv):
        if argv[1]=='rest':return ({'value':a[2]},None) if fail_stage is None else (None,{'code':fail_stage})
        if argv[1:3]==['account','show']:return {'id':SUBSCRIPTION,'tenantId':TENANT},None
        if argv[1:3]==['jobs','get']:return {'settings':{'job_clusters':[{'new_cluster':cluster}]}},None
        if argv[1:3]==['bundle','schema']:return SCHEMA,None
        if argv[1:3]==['vm','list-usage']:return [{'name':{'value':k},'limit':8,'currentValue':0} for k in ('cores','testFamily')],None
        if argv[1:3]==['api','get']:return (a[1] if 'list-node-types' in argv[3] else a[3]),None
        pytest.fail('Unexpected command')
    return read


def test_legacy_injected_reader_unchanged_signature():
    assert inspect_capacity('synthetic',1,data_reader())['status']=='ELIGIBLE_SNAPSHOT'


def test_default_reader_resolved_at_call_time_and_gets_timeouts(monkeypatch):
    import check_capacity
    calls=[];reader=data_reader()
    def actual(argv,*,timeout):calls.append(timeout);return reader(argv)
    monkeypatch.setattr(check_capacity,'read_json',actual)
    assert inspect_capacity('synthetic',1)['status']=='ELIGIBLE_SNAPSHOT'
    assert 120 in calls and any(t<=60 for t in calls) and 90 in calls

@pytest.mark.parametrize('payload',[None,[],{'settings':None},{'settings':{'job_clusters':None}}])
def test_bad_job_shapes_have_static_diagnostic(payload):
    base=data_reader()
    def read(argv):return (payload,None) if argv[1:3]==['jobs','get'] else base(argv)
    with pytest.raises(Blocked,match='INVALID_'):inspect_capacity('synthetic',1,read)


def test_unsafe_reader_error_never_echoed():
    with pytest.raises(Blocked) as exc:inspect_capacity('synthetic',1,data_reader('synthetic-secret'))
    assert str(exc.value)=='READ_FAILED_AT_AZURE_SKUS'


def test_real_cli_wrapper_timeout_and_invalid_json_do_not_echo(monkeypatch):
    import diagnose
    monkeypatch.setattr(diagnose.shutil,'which',lambda _:sys.executable)
    def slow(*args,**kwargs):
        assert kwargs['timeout']==17
        raise subprocess.TimeoutExpired('synthetic-secret',17,output='synthetic-secret',stderr='synthetic-secret')
    monkeypatch.setattr(diagnose.subprocess,'run',slow)
    assert diagnose.read_json(['az'],timeout=17)==(None,{'code':'READ_TIMEOUT'})
    monkeypatch.setattr(diagnose.subprocess,'run',lambda *a,**k:NS(returncode=0,stdout='synthetic-secret not JSON',stderr=''))
    assert diagnose.read_json(['az'])==(None,{'code':'INVALID_JSON_RESPONSE'})


def test_real_slow_cli_is_killed_and_output_not_exposed():
    from diagnose import read_json
    value,error=read_json([sys.executable,'-c',"import time; print('synthetic-secret',flush=True); time.sleep(2)"],timeout=0.1)
    assert value is None and error=={'code':'READ_TIMEOUT'}


def test_timeout_stage_and_malformed_error_code_are_safe():
    with pytest.raises(Blocked,match='^READ_TIMEOUT_AT_AZURE_SKUS$'):
        inspect_capacity('synthetic',1,data_reader('READ_TIMEOUT'))
    with pytest.raises(Blocked,match='^READ_FAILED_AT_AZURE_SKUS$'):
        inspect_capacity('synthetic',1,data_reader(['synthetic-secret']))


def test_cli_main_outputs_stage_without_sensitive_exception(monkeypatch,capsys):
    import check_capacity
    def failure(*a,**k):raise Blocked('READ_TIMEOUT_AT_AZURE_SKUS')
    monkeypatch.setattr(check_capacity,'inspect_capacity',failure)
    monkeypatch.setattr(sys,'argv',['check_capacity.py'])
    assert check_capacity.main()==1
    assert json.loads(capsys.readouterr().out)['code']=='READ_TIMEOUT_AT_AZURE_SKUS'


def test_launch_subprocess_timeout_is_safe(monkeypatch):
    import launch
    def slow(*a,**k):
        assert k['timeout']==5
        raise subprocess.TimeoutExpired('synthetic-secret',5,output='synthetic-secret')
    monkeypatch.setattr(launch.subprocess,'run',slow)
    with pytest.raises(Blocked,match='^READ_TIMEOUT$'):
        launch.command(['az','rest'],timeout=5)

"""Issue #33: scoped provider evidence, no full ARM SKU catalog dependency."""
import copy
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pytest
from capacity_reads import read_scoped_skus,ScopedSkuEvidence,APPROVED_SKUS,SOURCE
from check_capacity import inspect_capacity
from compute_capacity import select_alternates
from plan_guard import Blocked
from test_compute_capacity import inputs,PRIMARY,SCHEMA,scoped_nodes,workspace_response,scoped_quotas
from diagnose import SUBSCRIPTION,TENANT


def binding_reader(argv,stage,timeout):
    assert timeout==90 and 'rest' not in argv and 'list-skus' not in argv
    if argv[1:3]==['resource','show']:return workspace_response()
    if argv[1:3]==['api','get']:return {'workspace_id':7405617270738925}
    pytest.fail('No catalog or writes permitted')


def test_complete_scoped_snapshot_without_full_catalog():
    evidence=read_scoped_skus(binding_reader,scoped_nodes())
    assert isinstance(evidence,ScopedSkuEvidence) and evidence.requested==APPROVED_SKUS
    a=inputs();reports=[]
    assert select_alternates(PRIMARY,scoped_nodes(),evidence,a[3],SCHEMA,scoped_quotas(),reporter=reports.append)==list(APPROVED_SKUS[1:])
    assert reports[0]['source']==SOURCE and reports[0]['primary_quota']['family']['headroom']==8
    assert all('restrictions' not in r for r in evidence.records) # Never fabricate ARM restrictions.

@pytest.mark.parametrize('change,code',[
    ('missing','SCOPED_SKU_MISSING_OR_AMBIGUOUS'),('duplicate','SCOPED_SKU_MISSING_OR_AMBIGUOUS'),
    ('missing_info','SCOPED_PROVIDER_STATUS_MISSING'),('missing_status','SCOPED_PROVIDER_STATUS_MISSING'),
    ('null_status','SCOPED_PROVIDER_STATUS_MISSING'),('unknown_status','SCOPED_PROVIDER_STATUS_UNKNOWN'),
    ('wrong_instance','SCOPED_INSTANCE_TYPE_MISMATCH'),('wrong_size','SCOPED_DIMENSIONS_INCOMPATIBLE')])
def test_missing_or_ambiguous_scoped_evidence_blocks(change,code):
    nodes=scoped_nodes();n=nodes['node_types'][1]
    if change=='missing':nodes['node_types'].pop()
    if change=='duplicate':nodes['node_types'].append(copy.deepcopy(n))
    if change=='missing_info':n.pop('node_info')
    if change=='missing_status':n['node_info']={}
    if change=='null_status':n['node_info']['status']=None
    if change=='unknown_status':n['node_info']['status']=['synthetic-secret-status']
    if change=='wrong_instance':n['instance_type_id']='different'
    if change=='wrong_size':n['num_cores']=8
    with pytest.raises(Blocked,match=code):read_scoped_skus(binding_reader,nodes)

@pytest.mark.parametrize('status,reason',[('NotAvailableInRegion','PROVIDER_NOT_AVAILABLE_IN_REGION'),('NotEnabledOnSubscription','PROVIDER_NOT_ENABLED_ON_SUBSCRIPTION')])
def test_explicit_provider_restriction_never_eligible(status,reason):
    nodes=scoped_nodes();nodes['node_types'][1]['node_info']['status']=[status]
    evidence=read_scoped_skus(binding_reader,nodes);reports=[]
    result=select_alternates(PRIMARY,nodes,evidence,inputs()[3],SCHEMA,scoped_quotas(),reporter=reports.append)
    assert result==[APPROVED_SKUS[2]]
    assert reason in reports[0]['candidates'][0]['reasons']

@pytest.mark.parametrize('case',['region','id','assignment','malformed'])
def test_workspace_binding_is_mandatory(case):
    def read(argv,*args):
        if argv[0]=='az':
            value=workspace_response()
            if case=='region':value['location']='westus'
            if case=='id':value['id']='different'
            if case=='malformed':return []
            return value
        return {'workspace_id':0 if case=='assignment' else 7405617270738925}
    with pytest.raises(Blocked):read_scoped_skus(read,scoped_nodes())


def test_partial_typed_response_rejected_by_selector():
    complete=read_scoped_skus(binding_reader,scoped_nodes())
    partial=ScopedSkuEvidence(complete.records[1:],APPROVED_SKUS,'eastus')
    with pytest.raises(Blocked,match='INCOMPLETE_SCOPED_SKU_EVIDENCE'):
        select_alternates(PRIMARY,scoped_nodes(),partial,inputs()[3],SCHEMA,scoped_quotas())


def test_quota_denial_no_fabricated_headroom():
    evidence=read_scoped_skus(binding_reader,scoped_nodes());quotas=scoped_quotas()
    for q in quotas:q['limit']=0
    with pytest.raises(Blocked,match='NO_ELIGIBLE'):
        select_alternates(PRIMARY,scoped_nodes(),evidence,inputs()[3],SCHEMA,quotas)


def data_reader(failure=None):
    a=inputs()
    cluster=dict(node_type_id=PRIMARY,policy_id='p',spark_version='15.4.x-scala2.12',num_workers=0,data_security_mode='SINGLE_USER',azure_attributes={'availability':'ON_DEMAND_AZURE'})
    def read(argv):
        # Permanently unresponsive catalog: production code must never call it.
        if 'list-skus' in argv or any('/skus' in str(v) for v in argv):
            pytest.fail('Full SKU catalog endpoint is forbidden, not retried')
        if argv[1:3]==['account','show']:return {'id':SUBSCRIPTION,'tenantId':TENANT},None
        if argv[1:3]==['jobs','get']:return {'settings':{'job_clusters':[{'new_cluster':cluster}]}},None
        if argv[1:3]==['bundle','schema']:return SCHEMA,None
        if argv[1:3]==['resource','show']:return (None,{'code':failure}) if failure else (workspace_response(),None)
        if argv[1:3]==['vm','list-usage']:return scoped_quotas(),None
        if argv[1:3]==['api','get']:
            return ({'workspace_id':7405617270738925} if 'current-metastore' in argv[3] else scoped_nodes() if 'list-node-types' in argv[3] else a[3]),None
        pytest.fail('Unexpected command')
    return read


def test_permanently_unresponsive_catalog_does_not_block_scoped_route():
    assert inspect_capacity('synthetic',1,data_reader())['status']=='ELIGIBLE_SNAPSHOT'


def test_default_reader_timeout_and_legacy_injection(monkeypatch):
    import check_capacity
    calls=[];reader=data_reader()
    def actual(argv,*,timeout):calls.append(timeout);return reader(argv)
    monkeypatch.setattr(check_capacity,'read_json',actual)
    assert inspect_capacity('synthetic',1)['status']=='ELIGIBLE_SNAPSHOT'
    assert 120 in calls and 90 in calls

@pytest.mark.parametrize('error',['READ_TIMEOUT','ACCESS_DENIED','INVALID_JSON_RESPONSE'])
def test_read_failure_static_stage_and_no_retry(error):
    with pytest.raises(Blocked,match='^'+error+'_AT_AZURE_WORKSPACE$'):
        inspect_capacity('synthetic',1,data_reader(error))

@pytest.mark.parametrize('payload',[None,[],{'settings':None},{'settings':{'job_clusters':None}}])
def test_bad_job_shapes_have_static_diagnostic(payload):
    base=data_reader()
    def read(argv):return (payload,None) if argv[1:3]==['jobs','get'] else base(argv)
    with pytest.raises(Blocked,match='INVALID_'):inspect_capacity('synthetic',1,read)


def test_unsafe_reader_error_never_echoed():
    with pytest.raises(Blocked) as exc:inspect_capacity('synthetic',1,data_reader('synthetic-secret'))
    assert str(exc.value)=='READ_FAILED_AT_AZURE_WORKSPACE'


def test_real_slow_cli_is_killed_and_output_not_exposed():
    from diagnose import read_json
    value,error=read_json([sys.executable,'-c',"import time; print('synthetic-secret',flush=True); time.sleep(2)"],timeout=0.1)
    assert value is None and error=={'code':'READ_TIMEOUT'}


def test_launch_subprocess_timeout_is_safe(monkeypatch):
    import launch
    def slow(*a,**k):
        assert k['timeout']==5
        raise subprocess.TimeoutExpired('synthetic-secret',5,output='synthetic-secret')
    monkeypatch.setattr(launch.subprocess,'run',slow)
    with pytest.raises(Blocked,match='^READ_TIMEOUT$'):launch.command(['az','resource','show'],timeout=5)

@pytest.mark.parametrize('change',['source','region','family'])
def test_scoped_contract_rejects_forged_provenance_and_family(change):
    from dataclasses import replace
    evidence=read_scoped_skus(binding_reader,scoped_nodes())
    if change=='source':evidence=replace(evidence,source='untrusted')
    if change=='region':evidence=replace(evidence,region='westus')
    if change=='family':
        records=copy.deepcopy(evidence.records);records[0]['family']='inventedFamily'
        evidence=replace(evidence,records=records)
    with pytest.raises(Blocked,match='INVALID_SCOPED_'):
        select_alternates(PRIMARY,scoped_nodes(),evidence,inputs()[3],SCHEMA,scoped_quotas())


def test_primary_unknown_quota_and_disabled_provider_block():
    from capacity_reads import FAMILIES
    evidence=read_scoped_skus(binding_reader,scoped_nodes())
    quota=[q for q in scoped_quotas() if q['name']['value']!=FAMILIES[PRIMARY]]
    with pytest.raises(Blocked,match='SCOPED_PRIMARY_QUOTA_MISSING'):
        select_alternates(PRIMARY,scoped_nodes(),evidence,inputs()[3],SCHEMA,quota)
    nodes=scoped_nodes();nodes['node_types'][0]['node_info']['status']=['NotEnabledOnSubscription']
    evidence=read_scoped_skus(binding_reader,nodes)
    with pytest.raises(Blocked,match='SCOPED_PRIMARY_PROVIDER_STATUS_NOT_CLEAR'):
        select_alternates(PRIMARY,nodes,evidence,inputs()[3],SCHEMA,scoped_quotas())

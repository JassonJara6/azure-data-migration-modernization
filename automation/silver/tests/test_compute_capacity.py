import copy
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from compute_capacity import ALTERNATES,select_alternates
from plan_guard import Blocked
from bundle_overlay import ephemeral_bundle
import yaml

PRIMARY='Standard_D4ds_v5'
IDS=[PRIMARY,*ALTERNATES[PRIMARY]]
SCHEMA={'properties':{'new_cluster':{'properties':{'driver_node_type_flexibility':{}}}}}

def inputs():
    return [PRIMARY,{'node_types':[{'node_type_id':n} for n in IDS]},[
        {'name':n,'resourceType':'virtualMachines','locations':['eastus'],'capabilities':[{'name':'vCPUs','value':'4'},{'name':'MemoryGB','value':'16'}],'restrictions':[]} for n in IDS],
        {'definition':json.dumps({'node_type_id':{'type':'fixed','value':PRIMARY}})},copy.deepcopy(SCHEMA)]


def test_bounded_driver_fallback_overlay():
    args=inputs();alternates=select_alternates(*args)
    assert alternates==['Standard_D4ads_v5','Standard_D4ds_v4']
    source=Path(__file__).resolve().parents[3]/'databricks/silver/databricks.yml'
    spec=ephemeral_bundle(yaml.safe_load(source.read_text()),dict(policy_id='p',runner_application_id='sp',operator='op',node_type_id=PRIMARY,node_type_id_unused='ignored',driver_alternate_node_type_ids=alternates),'https://example.invalid')
    cluster=spec['resources']['jobs']['silver']['job_clusters'][0]['new_cluster']
    assert cluster['driver_node_type_flexibility']=={'alternate_node_type_ids':alternates}
    assert cluster['node_type_id']==PRIMARY and cluster['num_workers']==0
    assert cluster['spark_version']=='15.4.x-scala2.12'
    assert cluster['azure_attributes']['availability']=='ON_DEMAND_AZURE'
    assert 'worker_node_type_flexibility' not in cluster
    assert cluster['policy_id']=='p'

@pytest.mark.parametrize('failure',['old-cli','unknown-primary','empty-skus','wrong-region','larger','restricted','missing-workspace','policy-primary','policy-driver','policy-flex','policy-family'])
def test_fail_closed(failure):
    a=inputs()
    if failure=='old-cli':a[4]={}
    if failure=='unknown-primary':a[0]='Standard_D8ds_v5'
    if failure=='empty-skus':a[2]=[]
    if failure=='missing-workspace':a[1]['node_types']=[]
    for s in a[2]:
        if failure=='wrong-region':s['locations']=['westus']
        if failure=='larger':s['capabilities'][0]['value']='8'
        if failure=='restricted':s['restrictions']=[{'type':'Location','reasonCode':'NotAvailableForSubscription'}]
    definition=json.loads(a[3]['definition'])
    if failure=='policy-primary':definition['node_type_id']['value']='different'
    if failure=='policy-driver':definition['driver_node_type_id']={'type':'fixed','value':'different'}
    if failure=='policy-flex':definition['driver_node_type_flexibility.alternate_node_type_ids']={'type':'fixed','value':''}
    if failure=='policy-family':a[3]['policy_family_id']='unresolved'
    a[3]['definition']=json.dumps(definition)
    with pytest.raises(Blocked):select_alternates(*a)


def test_restricted_candidate_removed_not_replaced_by_larger_type():
    a=inputs();a[2][1]['restrictions']=[{'type':'Location'}]
    assert select_alternates(*a)==['Standard_D4ds_v4']


def test_existing_fixed_flex_policy_must_match_order():
    a=inputs();definition=json.loads(a[3]['definition'])
    definition['driver_node_type_flexibility.alternate_node_type_ids']={'type':'fixed','value':','.join(ALTERNATES[PRIMARY])}
    a[3]['definition']=json.dumps(definition)
    assert select_alternates(*a)==ALTERNATES[PRIMARY]
    definition['driver_node_type_flexibility.alternate_node_type_ids']['value']=','.join(reversed(ALTERNATES[PRIMARY]))
    a[3]['definition']=json.dumps(definition)
    with pytest.raises(Blocked):select_alternates(*a)

@pytest.mark.parametrize('regional,family,expected',[(4,4,True),(3,4,False),(4,3,False)])
def test_snapshot_quota(regional,family,expected):
    a=inputs()
    for sku in a[2]:sku['family']='testFamily'
    quotas=[{'name':{'value':'cores'},'limit':regional,'currentValue':0},{'name':{'value':'testFamily'},'limit':family,'currentValue':0}]
    if expected:assert select_alternates(*a,quotas)==ALTERNATES[PRIMARY]
    else:
        with pytest.raises(Blocked):select_alternates(*a,quotas)


def test_missing_quota_evidence_blocks():
    with pytest.raises(Blocked):select_alternates(*inputs(),[])

@pytest.mark.parametrize('code',['CLOUD_PROVIDER_RESOURCE_STOCKOUT','SkuNotAvailable'])
def test_failure_diagnostic_never_echoes_raw_state(code):
    from compute_capacity import check_run_result
    with pytest.raises(Blocked,match='SILVER_COMPUTE_STOCKOUT') as exc:
        check_run_result({'result_state':'FAILED','state_message':code+' synthetic-sensitive-detail'})
    assert 'synthetic-sensitive-detail' not in str(exc.value)


def test_launcher_read_only_preflight_propagates_selected_driver_list(monkeypatch):
    import launch
    from types import SimpleNamespace
    a=inputs()
    for sku in a[2]:sku['family']='testFamily'
    quotas=[{'name':{'value':key},'limit':4,'currentValue':0} for key in ('cores','testFamily')]
    calls=[]
    def command(argv,**kwargs):
        calls.append(argv)
        if argv[1:3]==['bundle','schema']:return SCHEMA
        if argv[1]=='rest':return {'value':a[2]}
        if argv[1:3]==['vm','list-usage']:return quotas
        pytest.fail('Unexpected CLI mutation')
    def api(method,path):
        assert method=='get'
        if path.endswith('metastore_summary'):return {'metastore_id':'m'}
        if path.endswith('spark-versions'):return {'versions':[{'key':'15.4.x-scala2.12'}]}
        if path.endswith('list-node-types'):return a[1]
        if '/policies/' in path:return a[3]
        if '/permissions/' in path:return {'privilege_assignments':[{'principal':'runner','privileges':['READ_VOLUME']}]}
        pytest.fail('Unexpected API')
    monkeypatch.setattr(launch,'command',command)
    obj=object.__new__(launch.Launcher);obj.env={};obj.ws_env={};obj.api=api
    uc=dict(metastore_id='m',node_type_id=PRIMARY,policy_id='p',catalog='admm_dev',runner_application_id='runner',operator='op')
    obj.check_workspace(uc)
    assert uc['driver_alternate_node_type_ids']==ALTERNATES[PRIMARY]
    assert len(calls)==3

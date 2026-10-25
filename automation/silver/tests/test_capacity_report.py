"""Offline issue #30 regressions using the exact launcher selection function."""
import json
import pytest
from test_compute_capacity import inputs,PRIMARY,ALTERNATES,SCHEMA
from compute_capacity import select_alternates
from plan_guard import Blocked


def evaluate(a,quotas=None):
    reports=[]
    try:select_alternates(*a,quotas,reporter=reports.append)
    except Blocked:pass
    return reports[0]


def quotas(family=' TestFamily '):
    return [{'name':{'value':key},'limit':'8','currentValue':'4'} for key in (' CORES ',family)]


def test_named_quota_normalization_and_headroom():
    a=inputs()
    for sku in a[2]:sku['family']='testfamily'
    r=evaluate(a,quotas())
    assert r['status']=='ELIGIBLE_SNAPSHOT'
    assert all(c['regional_quota']['headroom']==4 and c['family_quota']['headroom']==4 for c in r['candidates'])

@pytest.mark.parametrize('scope,expected',[('eastus',False),('westus',True),(None,False)])
def test_location_restriction_scope(scope,expected):
    a=inputs()
    for sku in a[2]:
        sku['restrictions']=[{'type':'Location',**({'restrictionInfo':{'locations':[scope]}} if scope else {})}]
    r=evaluate(a)
    assert (r['status']=='ELIGIBLE_SNAPSHOT')==expected
    if not expected:assert all('EASTUS_LOCATION_RESTRICTED' in c['reasons'] for c in r['candidates'])


def test_zone_restrictions_do_not_block_non_zonal_compute():
    a=inputs()
    for sku in a[2]:sku['restrictions']=[{'type':'Zone','values':['eastus'],'restrictionInfo':{'zones':['1']}}]
    assert evaluate(a)['status']=='ELIGIBLE_SNAPSHOT'


def test_every_candidate_has_all_reasons_and_static_actions():
    a=inputs();a[1]['node_types']=[{'node_type_id':PRIMARY}]
    for sku in a[2]:
        sku['family']='testfamily';sku['locations']=['westus']
        sku['capabilities'][0]['value']='8'
        sku['restrictions']=[{'type':'Location','values':['eastus'],'reasonCode':'synthetic-secret'}]
    r=evaluate(a,[])
    assert len(r['candidates'])==2
    for c in r['candidates']:
        assert set(c['reasons'])=={'NOT_IN_WORKSPACE_CATALOG','NOT_IN_EASTUS','VCPU_NOT_4','EASTUS_LOCATION_RESTRICTED','REGIONAL_QUOTA_MISSING','FAMILY_QUOTA_MISSING'}
        assert c['actions'] and not c['eligible']
    assert 'synthetic-secret' not in json.dumps(r)


def test_policy_mismatch_emitted_not_silently_removed():
    a=inputs();a[3]['definition']=json.dumps({'node_type_id':{'type':'fixed','value':'other'}})
    r=evaluate(a)
    assert r['blocker']=='SILVER_PRIMARY_POLICY_MISMATCH'
    assert all(not c['eligible'] and 'SILVER_PRIMARY_POLICY_MISMATCH' in c['reasons'] for c in r['candidates'])


def test_family_headroom_distinguished_from_missing_key():
    a=inputs()
    for sku in a[2]:sku['family']='testfamily'
    usage=quotas();usage[1]['currentValue']='7'
    r=evaluate(a,usage)
    assert all(c['family_quota']['headroom']==1 and 'FAMILY_QUOTA_INSUFFICIENT' in c['reasons'] for c in r['candidates'])
    r=evaluate(a,[usage[0]])
    assert all('FAMILY_QUOTA_MISSING' in c['reasons'] for c in r['candidates'])


def test_readonly_entrypoint_only_uses_read_commands():
    from check_capacity import inspect_capacity
    from diagnose import SUBSCRIPTION,TENANT
    a=inputs()
    for sku in a[2]:sku['family']='testfamily'
    calls=[]
    cluster=dict(node_type_id=PRIMARY,policy_id='p',spark_version='15.4.x-scala2.12',num_workers=0,data_security_mode='SINGLE_USER',azure_attributes={'availability':'ON_DEMAND_AZURE'})
    def reader(argv):
        calls.append(argv)
        if argv[1:3]==['account','show']:return {'id':SUBSCRIPTION,'tenantId':TENANT},None
        if argv[1:3]==['jobs','get']:return {'settings':{'job_clusters':[{'new_cluster':cluster}]}},None
        if argv[1:3]==['bundle','schema']:return SCHEMA,None
        if argv[1:3]==['vm','list-skus']:return a[2],None
        if argv[1:3]==['vm','list-usage']:return quotas(),None
        if argv[1:3]==['api','get']:return (a[1] if 'list-node-types' in argv[3] else a[3]),None
        pytest.fail('Unexpected command')
    assert inspect_capacity('admm-workspace',210884055971910,reader)['status']=='ELIGIBLE_SNAPSHOT'
    assert len(calls)==7
    assert not any(token in ('deploy','apply','run-now','create','update') for argv in calls for token in argv)


def test_null_live_quota_response_never_enables_catalog_only_mode(monkeypatch):
    import launch
    from types import SimpleNamespace as NS
    obj=object.__new__(launch.Launcher);obj.env={};obj.ws_env={}
    def api(method,path):
        if path.endswith('metastore_summary'):return {'metastore_id':'m'}
        if path.endswith('spark-versions'):return {'versions':[{'key':'15.4.x-scala2.12'}]}
        if path.endswith('list-node-types'):return inputs()[1]
        pytest.fail('Must block before policy read')
    def command(argv,**kwargs):
        if argv[1:3]==['bundle','schema']:return SCHEMA
        if argv[1:3]==['vm','list-skus']:return inputs()[2]
        if argv[1:3]==['vm','list-usage']:return None
        pytest.fail('Unexpected command')
    obj.api=api;monkeypatch.setattr(launch,'command',command)
    with pytest.raises(Blocked,match='INVALID_AZURE_QUOTAS_RESPONSE'):
        obj.check_workspace(dict(metastore_id='m',node_type_id=PRIMARY))


def test_launcher_emits_per_candidate_report_on_selection_failure(monkeypatch,capsys):
    import launch
    a=inputs()
    obj=object.__new__(launch.Launcher);obj.env={};obj.ws_env={}
    def api(method,path):
        if path.endswith('metastore_summary'):return {'metastore_id':'m'}
        if path.endswith('spark-versions'):return {'versions':[{'key':'15.4.x-scala2.12'}]}
        if path.endswith('list-node-types'):return a[1]
        if '/policies/' in path:return a[3]
        pytest.fail('Must block before volume checks')
    def command(argv,**kwargs):
        if argv[1:3]==['bundle','schema']:return SCHEMA
        if argv[1:3] in (['vm','list-skus'],['vm','list-usage']):return []
        pytest.fail('No writes permitted')
    obj.api=api;monkeypatch.setattr(launch,'command',command)
    with pytest.raises(Blocked,match='NO_ELIGIBLE'):
        obj.check_workspace(dict(metastore_id='m',node_type_id=PRIMARY,policy_id='p'))
    report=json.loads(capsys.readouterr().out)
    assert len(report['candidates'])==2
    assert all(c['reasons']==['NOT_IN_AZURE_SKU_CATALOG'] and c['actions'] for c in report['candidates'])

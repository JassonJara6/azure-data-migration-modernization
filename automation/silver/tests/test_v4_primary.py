"""Synthetic live-response contracts; no Azure calls or compute allocation."""
import copy
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from plan_guard import Blocked
from single_node_v4 import PRIMARY,FAMILY,NO_FALLBACK,integer,quotas,read_evidence,validate_policy,validate_cluster
from capacity_reads import WORKSPACE_RESOURCE_ID
from diagnose import SUBSCRIPTION,TENANT
from bundle_overlay import ephemeral_bundle
from v4_transition import target_clusters,review_policy_plan,canonical_sha


def node():return {'node_types':[{'node_type_id':PRIMARY,'instance_type_id':PRIMARY,'num_cores':4.0,'memory_mb':16384}]}
def usages():return [{'name':{'value':n},'limit':'4','current_value':'0'} for n in ('cores',FAMILY)]
def policy_definition(primary=PRIMARY):
    d={k:{'type':'fixed','value':v} for k,v in {'node_type_id':primary,'num_workers':0,'spark_version':'15.4.x-scala2.12',
        'data_security_mode':'SINGLE_USER','single_user_name':'runner','spark_conf.spark.databricks.cluster.profile':'singleNode',
        'spark_conf.spark.master':'local[*]','azure_attributes.availability':'ON_DEMAND_AZURE','cluster_type':'job'}.items()}
    if primary==PRIMARY:d.update({k:{'type':'fixed','value':v} for k,v in {'driver_node_type_id':PRIMARY,
        'driver_node_type_flexibility.alternate_node_type_ids':'','worker_node_type_flexibility.alternate_node_type_ids':''}.items()})
    return d


def responses():
    return {'V4_ACCOUNT':{'id':SUBSCRIPTION,'tenantId':TENANT,'state':'Enabled'},
        'V4_SUBSCRIPTION':{'subscriptionId':SUBSCRIPTION,'state':'Enabled','subscriptionPolicies':{'quotaId':'FreeTrial_2014-09-01','spendingLimit':'On'}},
        'V4_WORKSPACE':{'id':WORKSPACE_RESOURCE_ID,'location':'eastus','properties':{'workspaceId':'7405617270738925'}},
        'V4_ASSIGNMENT':{'workspace_id':7405617270738925},
        'V4_ARM_SKUS':{'value':[{'name':PRIMARY,'resourceType':'virtualMachines','locations':['eastus'],'family':FAMILY,'restrictions':[],
            'capabilities':[{'name':k,'value':v} for k,v in {'vCPUs':'4','MemoryGB':'16','CpuArchitectureType':'x64','PremiumIO':'True'}.items()]}]},
        'V4_QUOTAS':usages()}


def reader(data,calls):
    def read(argv,stage,timeout):
        calls.append(argv)
        assert argv[0] in ('az','databricks') and not any(a in argv for a in ('create','update','apply','run-now','register'))
        return copy.deepcopy(data[stage])
    return read


def test_live_arm_positive_is_distinct_from_missing_provider_status():
    calls=[];report=read_evidence(reader(responses(),calls),node())
    assert report['status']=='ELIGIBLE_SNAPSHOT' and report['databricks_provider']=={'status':'NOT_REPORTED'}
    assert report['physical_capacity']=='NOT_VERIFIED' and report['driver_alternates']==[]
    assert report['quotas']['cores']['headroom']==4 and report['quotas'][FAMILY]['original']['limit']=='4'
    assert sum('/Microsoft.Compute/skus?' in ' '.join(c) for c in calls)==1

@pytest.mark.parametrize('case',['paid','spending','account','tenant','workspace','workspace-id','location','family','missing-restrictions',
    'restriction','missing-sku','duplicate-sku','memory','cpu','architecture','premium','duplicate-capability','bad-page','untrusted-next','quota-zero','quota-missing'])
def test_bad_or_incomplete_live_evidence_blocks(case):
    d=responses();sku=d['V4_ARM_SKUS']['value'][0]
    if case=='paid':d['V4_SUBSCRIPTION']['subscriptionPolicies']['quotaId']='PayAsYouGo_2014-09-01'
    if case=='spending':d['V4_SUBSCRIPTION']['subscriptionPolicies']['spendingLimit']='Off'
    if case=='account':d['V4_ACCOUNT']['id']='other'
    if case=='tenant':d['V4_ACCOUNT']['tenantId']='other'
    if case=='workspace':d['V4_WORKSPACE']['id']='other'
    if case=='workspace-id':d['V4_ASSIGNMENT']['workspace_id']=1
    if case=='location':sku['locations']=['westus']
    if case=='family':sku['family']='other'
    if case=='missing-restrictions':sku.pop('restrictions')
    if case=='restriction':sku['restrictions']=[{'type':'Location','values':['eastus']}]
    if case=='missing-sku':d['V4_ARM_SKUS']['value']=[]
    if case=='duplicate-sku':d['V4_ARM_SKUS']['value'].append(copy.deepcopy(sku))
    if case in ('memory','cpu','architecture','premium'):
        key={'memory':'MemoryGB','cpu':'vCPUs','architecture':'CpuArchitectureType','premium':'PremiumIO'}[case]
        next(c for c in sku['capabilities'] if c['name']==key)['value']='bad'
    if case=='duplicate-capability':sku['capabilities'].append(copy.deepcopy(sku['capabilities'][0]))
    if case=='bad-page':d['V4_ARM_SKUS']['value']=None
    if case=='untrusted-next':d['V4_ARM_SKUS']['nextLink']='https://untrusted.invalid/secret'
    if case=='quota-zero':d['V4_QUOTAS'][1]['limit']='0'
    if case=='quota-missing':d['V4_QUOTAS'].pop()
    with pytest.raises(Blocked):read_evidence(reader(d,[]),node())

@pytest.mark.parametrize('status',[['NotEnabledOnSubscription'],['NotAvailableInRegion'],['unknown'],'',{},[None]])
def test_provider_rejection_or_malformed_flags_never_overridden_by_arm(status):
    n=node();n['node_types'][0]['node_info']={'status':status}
    with pytest.raises(Blocked):read_evidence(reader(responses(),[]),n)


def test_full_pagination_and_timeout_fail_closed():
    d=responses();calls=[]
    url='https://management.azure.com/subscriptions/'+SUBSCRIPTION+'/providers/Microsoft.Compute/skus?next=2'
    def read(argv,stage,timeout):
        calls.append(stage)
        if stage=='V4_ARM_SKUS' and calls.count(stage)==1:return {'value':[],'nextLink':url}
        return copy.deepcopy(d[stage])
    assert read_evidence(read,node())['arm_pages']==2
    def timed(argv,stage,timeout):
        if stage=='V4_ARM_SKUS':raise Blocked('READ_TIMEOUT')
        return d[stage]
    with pytest.raises(Blocked,match='READ_TIMEOUT'):read_evidence(timed,node())

@pytest.mark.parametrize('value',[True,None,'NaN','Infinity','-1','1.5','4_000','1e100000',''])
def test_bad_quota_values_not_zero(value):
    with pytest.raises(Blocked):integer(value)

@pytest.mark.parametrize('value',[4,'4','4.0',' 4 ','4e0'])
def test_numeric_strings_and_originals_preserved(value):
    rows=usages();rows[0]['limit']=value;rows[0]['currentValue']='0.0'
    result=quotas(rows)
    assert result['cores']['headroom']==4 and result['cores']['original']['limit']==value

@pytest.mark.parametrize('change',['duplicate','conflict','insufficient'])
def test_ambiguous_and_insufficient_quotas_block(change):
    rows=usages()
    if change=='duplicate':rows.append(copy.deepcopy(rows[0]))
    if change=='conflict':rows[0]['currentValue']='1'
    if change=='insufficient':rows[0]['current_value']='1'
    with pytest.raises(Blocked):quotas(rows)


def settings():
    import yaml
    original=yaml.safe_load((Path(__file__).resolve().parents[3]/'databricks/silver/databricks.yml').read_text())
    uc={'node_type_id':'Standard_D4ds_v5','policy_id':'policy','runner_application_id':'runner','operator':'op',
        'driver_alternate_node_type_ids':[PRIMARY]}
    return ephemeral_bundle(original,uc,'https://example.invalid')['resources']['jobs']['silver'],uc


def test_exact_transition_and_v4_overlay_disable_both_fallbacks():
    before,uc=settings();saved=copy.deepcopy(before)
    target=target_clusters(before,uc)
    validate_cluster(target[0]['new_cluster'])
    assert target[0]['new_cluster']['worker_node_type_flexibility']==NO_FALLBACK
    assert before==saved and canonical_sha(before)==canonical_sha(saved)
    after=copy.deepcopy(before);after['job_clusters']=target
    assert target_clusters(after,uc)==target
    import yaml
    spec=yaml.safe_load((Path(__file__).resolve().parents[3]/'databricks/silver/databricks.yml').read_text())
    uc['node_type_id']=PRIMARY
    validate_cluster(ephemeral_bundle(spec,uc,'https://example.invalid')['resources']['jobs']['silver']['job_clusters'][0]['new_cluster'])

@pytest.mark.parametrize('change',['sku','fallback','policy','runner','workers','autoscale','spark','init','trigger','timeout','retries','tasks'])
def test_transition_rejects_unrelated_drift(change):
    before,uc=settings();c=before['job_clusters'][0]['new_cluster']
    if change=='sku':c['node_type_id']='Standard_D4as_v5'
    if change=='fallback':c['driver_node_type_flexibility']={'alternate_node_type_ids':['unapproved']}
    if change=='policy':c['policy_id']='other'
    if change=='runner':before['run_as']['service_principal_name']='other'
    if change=='workers':c['num_workers']=1
    if change=='autoscale':c['autoscale']={'min_workers':1,'max_workers':2}
    if change=='spark':c['spark_conf']['spark.master']='local[1]'
    if change=='init':c['init_scripts']=[{}]
    if change=='trigger':before['schedule']={'quartz_cron_expression':'x'}
    if change=='timeout':before['timeout_seconds']=9000
    if change=='retries':before['tasks'][0]['max_retries']=1
    if change=='tasks':before['tasks'].append({})
    with pytest.raises(Blocked):target_clusters(before,uc)


def plan():
    before={'id':'policy','name':'silver','definition':json.dumps(policy_definition('Standard_D4ds_v5'))}
    after=dict(before,definition=json.dumps(policy_definition()))
    return {'resource_changes':[{'address':'databricks_cluster_policy.silver[0]','change':{'actions':['update'],'before':before,'after':after}}]}


def test_saved_plan_only_changes_existing_policy_rules():
    assert review_policy_plan(plan(),'uc-workspace')==[{'address':'databricks_cluster_policy.silver[0]','actions':['update']}]

@pytest.mark.parametrize('change',['create','replace','identity','permission','policy-id','policy-name','policy-rule','output'])
def test_saved_plan_rejects_every_unrelated_change(change):
    p=plan();r=p['resource_changes'][0]
    if change=='create':r['change']['actions']=['create']
    if change=='replace':r['change']['actions']=['delete','create']
    if change=='identity':r['address']='databricks_service_principal.runner'
    if change=='permission':r['address']='databricks_permissions.policy_runner[0]'
    if change=='policy-id':r['change']['after']['id']='other'
    if change=='policy-name':r['change']['after']['name']='other'
    if change=='policy-rule':
        d=json.loads(r['change']['after']['definition']);d['num_workers']['value']=1;r['change']['after']['definition']=json.dumps(d)
    if change=='output':p['output_changes']={'new-output':{'actions':['create'],'before':None,'after':1}}
    with pytest.raises(Blocked):review_policy_plan(p,'uc-workspace')


def test_native_windows_azure_command_bypasses_batch(tmp_path):
    from azure_cli import native_argv
    directory=tmp_path/'Azure CLI'/'wbin';directory.mkdir(parents=True);exe=directory/'az.cmd';exe.touch()
    with pytest.raises(Blocked):native_argv(str(exe),['az','rest'],windows=True)
    python=directory.parent/'python.exe';python.touch()
    args=['az','rest','--url','https://management.azure.com/x?a=1&b=%27eastus%27']
    assert native_argv(str(exe),args,windows=True)==[str(python),'-X','utf8','-IBm','azure.cli',*args[1:]]


def test_v4_shared_checker_requires_real_cluster_and_policy_without_schema_fallback():
    from check_capacity import inspect_capacity
    before,uc=settings();cluster=target_clusters(before,uc)[0]['new_cluster'];data=responses()
    def read(argv):
        if argv[1:3]==['account','show']:return data['V4_ACCOUNT'],None
        if argv[1:3]==['jobs','get']:return {'settings':{'job_clusters':[{'new_cluster':cluster}]}},None
        if 'list-node-types' in ' '.join(argv):return node(),None
        if 'policies/clusters/get?' in ' '.join(argv):return {'definition':policy_definition()},None
        if 'current-metastore-assignment' in ' '.join(argv):return data['V4_ASSIGNMENT'],None
        if argv[1:3]==['resource','show']:return data['V4_WORKSPACE'],None
        if argv[1:3]==['vm','list-usage']:return data['V4_QUOTAS'],None
        if '/Microsoft.Compute/skus?' in ' '.join(argv):return data['V4_ARM_SKUS'],None
        if argv[1:3]==['rest','--method']:return data['V4_SUBSCRIPTION'],None
        pytest.fail('No schema/fallback or mutations required for single SKU')
    assert inspect_capacity('profile',210884055971910,read)['status']=='ELIGIBLE_SNAPSHOT'


def test_v4_launcher_uses_live_evidence_and_readonly_input_permissions(monkeypatch,tmp_path):
    import launch
    obj=object.__new__(launch.Launcher);obj.deploy_only=False;obj.ws_env={};obj.env={}
    uc={'node_type_id':PRIMARY,'policy_id':'policy','runner_application_id':'runner','operator':'op','catalog':'admm_dev','metastore_id':'meta'}
    data=responses()
    def api(method,path):
        assert method=='get'
        if path.endswith('metastore_summary'):return {'metastore_id':'meta'}
        if path.endswith('spark-versions'):return {'versions':[{'key':'15.4.x-scala2.12'}]}
        if path.endswith('list-node-types'):return node()
        if 'policies/clusters/get?' in path:return {'definition':policy_definition()}
        if '/permissions/volume/' in path:return {'privilege_assignments':[{'principal':'runner','privileges':['READ_VOLUME']}]}
        pytest.fail('No writes')
    obj.api=api
    def command(argv,**kw):
        if argv[1:3]==['bundle','schema']:return {} # Not used as an eligibility claim.
        stages={'account':'V4_ACCOUNT','resource':'V4_WORKSPACE','vm':'V4_QUOTAS'}
        if argv[1] in stages:return data[stages[argv[1]]]
        if 'current-metastore-assignment' in ' '.join(argv):return data['V4_ASSIGNMENT']
        if '/Microsoft.Compute/skus?' in ' '.join(argv):return data['V4_ARM_SKUS']
        if argv[1]=='rest':return data['V4_SUBSCRIPTION']
        pytest.fail('No writes or legacy fallback reads')
    monkeypatch.setattr(launch,'command',command)
    obj.check_workspace(uc)
    assert uc['driver_alternate_node_type_ids']==[]

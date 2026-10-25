from pathlib import Path
import copy
import pytest
s=(Path(__file__).resolve().parents[1]/'silver-capacity-evidence.sh').read_text(encoding='utf-8').split("exec python - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]
ns={'__name__':'script_test'};exec(compile(s,'silver-capacity-evidence.sh','exec'),ns)

@pytest.mark.parametrize('node,expected',[
 ({},'UNKNOWN_FIELD_ABSENT'),({'node_info':{}},'UNKNOWN_FIELD_ABSENT'),
 ({'node_info':{'status':None}},'UNKNOWN_FIELD_ABSENT'),
 ({'node_info':{'status':[]}},'EXPLICIT'),
 ({'node_info':{'status':['NotEnabledOnSubscription']}},'EXPLICIT'),
 ({'node_info':{'status':['private-secret']}},'UNKNOWN_PROVIDER_FLAG')])
def test_provider_status(node,expected):assert ns['status'](node)['status']==expected

@pytest.mark.parametrize('limit,used,status',[(4,0,'OBSERVED'),(0,0,'OBSERVED'),(True,0,'INVALID'),(float('inf'),0,'INVALID'),(4,None,'INVALID')])
def test_quotas(limit,used,status):
 q=ns['quota_index']([{'name':{'value':'cores'},'limit':limit,'currentValue':used}])
 assert q['cores']['status']==status
 if limit==4 and used==0:assert q['cores']['headroom']==4

def test_duplicate_quota():
 u={'name':{'value':'cores'},'limit':4,'currentValue':0}
 assert ns['quota_index']([u,u])['cores']['status']=='AMBIGUOUS'

@pytest.mark.parametrize('restrictions,expected',[
 (None,'UNKNOWN_FIELD_ABSENT'),([], 'NO_LOCATION_RESTRICTION_REPORTED'),
 ([{'type':'Location','reasonCode':'NotAvailableForSubscription','values':['eastus']}],'BLOCKED_LOCATION'),
 ([{'type':'Location'}],'UNKNOWN_RESTRICTION'),
 ([{'type':'Zone','values':['eastus'],'restrictionInfo':{'locations':['eastus'],'zones':['1']}}],'NO_LOCATION_RESTRICTION_REPORTED')])
def test_arm_restrictions(restrictions,expected):
 s={'name':'X','family':'F','locations':['eastus']}
 if restrictions is not None:s['restrictions']=restrictions
 assert ns['arm_sku']('X',[s])['restriction_status']==expected

def test_snapshot_uses_only_supported_region_filter_and_get():
 def reader(argv,timeout):
  assert argv[:4]==['az','rest','--method','get']
  url=argv[argv.index('--url')+1]
  assert '%24filter=location+eq+%27eastus%27' in url and 'name+eq' not in url
  return {'value':[]},None
 assert ns['sku_snapshot'](reader)[1]['status']=='COMPLETE'

def test_untrusted_pagination_never_followed():
 calls=[]
 def reader(argv,timeout):
  calls.append(argv);return {'value':[],'nextLink':'https://untrusted.example/private'},None
 assert ns['sku_snapshot'](reader)[1]['code']=='UNEXPECTED_NEXT_LINK'
 assert len(calls)==1

def test_timeout_is_not_empty_success():
 assert ns['sku_snapshot'](lambda *a:(None,'READ_TIMEOUT'))[1]['status']=='INCOMPLETE'

def test_policy_family_not_resolved_as_empty_allowed():
 assert ns['safe_policy']({'policy_family_id':'p'})['status']=='POLICY_FAMILY_REQUIRES_EFFECTIVE_REVIEW'

def test_readonly_full_collection_with_fixtures(monkeypatch):
 calls=[];n={'node_type_id':'Standard_D4ds_v5','num_cores':4,'memory_mb':16384,'node_info':{'status':['NotEnabledOnSubscription']}}
 def reader(argv,timeout=90):
  calls.append(argv)
  if argv[:3]==['az','account','show']:return {'id':ns['SUB'],'tenantId':ns['TENANT'],'state':'Enabled'},None
  if argv[:3]==['az','resource','show']:return {'id':ns['RESOURCE'],'location':'eastus','properties':{'workspaceId':ns['WORKSPACE_ID']}},None
  if 'current-metastore-assignment' in ' '.join(argv):return {'workspace_id':ns['WORKSPACE_ID']},None
  if argv[:3]==['az','rest','--method']:return {'subscriptionPolicies':{'quotaId':'PayAsYouGo_2014-09-01','spendingLimit':'Off'}},None
  if argv[:3]==['az','provider','show']:return {'registrationState':'Registered'},None
  if argv[:3]==['az','vm','list-usage']:return [{'name':{'value':'cores'},'limit':4,'currentValue':0}],None
  if 'list-node-types' in ' '.join(argv):return {'node_types':[n]},None
  if 'spark-versions' in ' '.join(argv):return {'versions':[{'key':'15.4.x-scala2.12'}]},None
  if argv[1:3]==['jobs','get']:
   return {'settings':{'max_concurrent_runs':1,'tasks':[{}],'job_clusters':[{'new_cluster':{'node_type_id':n['node_type_id'],'num_workers':0,'spark_conf':{'spark.databricks.cluster.profile':'singleNode','spark.master':'local[*]'}}}]}},None
  pytest.fail('Unexpected command: no writes allowed')
 monkeypatch.setitem(ns,'read',reader)
 monkeypatch.setitem(ns,'sku_snapshot',lambda:([],{'status':'INCOMPLETE','code':'READ_TIMEOUT'}))
 report=ns['run']()
 assert report['identity_binding']=='VERIFIED'
 assert report['cluster']['single_node_confirmed']
 assert report['regional_quota']['headroom']==4
 by_name={n['node_type']:n for n in report['nodes']}
 assert by_name['Standard_D4ds_v5']['provider_assessment']=='REJECTED'
 assert by_name['Standard_D4ds_v5']['additional_vm_quota_cores_if_single_node']==4
 assert report['arm_sku_snapshot']['status']=='INCOMPLETE'
 assert by_name['Standard_D4ds_v4']['provider_assessment']=='UNKNOWN'
 assert not any(v in ('apply','create','deploy','run-now','update','register') for argv in calls for v in argv)

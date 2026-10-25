import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
SOURCE=Path(__file__).resolve().parents[1]/'silver-capacity-evidence.sh'
FAKE=r'''
import json, os, sys
from pathlib import Path
assert os.environ['MSYS_NO_PATHCONV']=='1'
assert os.environ['MSYS2_ARG_CONV_EXCL']=='*'
assert os.environ['PYTHONUTF8']=='1'
assert os.environ['PYTHONIOENCODING']=='utf-8'
a=sys.argv[1:]
with open(os.environ['CALL_LOG'],'a',encoding='utf-8') as stream:stream.write(json.dumps([Path(sys.argv[0]).name,*a])+'\n')
sub='b3d627f1-966e-4bd4-861a-e606b963875d';tenant='2c60929f-5f02-4969-8e91-62fdac1de9d4';wid='7405617270738925'
runner='7d5adada-f25c-47f2-ab76-9880b52d5490'
resource=f'/subscriptions/{sub}/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/workspaces/dbw-admm-dev-f6d99ba4'
if os.environ.get('FAKE_FAIL')=='read':
 print('Forbidden synthetic-private-token-123',file=sys.stderr);sys.exit(1)
if Path(sys.argv[0]).name=='az':
 if a[:2]==['account','show']:v={'id':sub,'tenantId':tenant,'state':'Enabled','user':{'name':'private@example.invalid'}}
 elif a[:2]==['resource','show']:v={'id':resource,'location':'eastus','properties':{'workspaceId':wid}}
 elif a[:2]==['provider','show']:v={'registrationState':'Registered'}
 elif a[:2]==['vm','list-usage']:v=[{'name':{'value':n},'limit':4,'currentValue':0} for n in ['cores','standardDDSv5Family','standardDADSv5Family','standardDDSv4Family']]
 elif a[:3]==['rest','--method','get']:
  url=a[a.index('--url')+1]
  if '/skus?' in url:v={'value':[]}
  else:v={'subscriptionPolicies':{'quotaId':'PayAsYouGo_2014-09-01','spendingLimit':'Off'},'token':'synthetic-private-token-123'}
 else:raise AssertionError('Unexpected mutation or query')
else:
 assert a[-2:]==['--profile','admm-workspace']
 if a[:2]==['jobs','get']:
  cluster={'node_type_id':'Standard_D4ds_v5','num_workers':0,'policy_id':'policy1','spark_version':'15.4.x-scala2.12','data_security_mode':'SINGLE_USER','spark_conf':{'spark.databricks.cluster.profile':'singleNode','spark.master':'local[*]','private-secret':'synthetic-private-token-123'}}
  v={'settings':{'tasks':[{}],'max_concurrent_runs':1,'job_clusters':[{'new_cluster':cluster}],'run_as':{'service_principal_name':runner}}}
  if os.environ.get('FAKE_FAIL')=='shape':v={'settings':[]}
 elif a[:2]==['permissions','get']:v={'access_control_list':[{'service_principal_name':runner,'all_permissions':[{'permission_level':'CAN_USE'}]}]}
 elif a[:2]==['api','get']:
  path=a[2]
  if path.endswith('current-metastore-assignment'):v={'workspace_id':wid}
  elif path.endswith('list-node-types'):
   v={'node_types':[{'node_type_id':n,'num_cores':4,'memory_mb':16384,**({'node_info':{'status':['NotEnabledOnSubscription']}} if n!='Standard_D4ds_v4' else {})} for n in ['Standard_D4ds_v5','Standard_D4ads_v5','Standard_D4ds_v4']]}
  elif path.endswith('spark-versions'):v={'versions':[{'key':'15.4.x-scala2.12'}]}
  elif path.startswith('/api/2.0/policies/clusters/get?'):v={'definition':json.dumps({'node_type_id':{'type':'fixed','value':'Standard_D4ds_v5'},'spark_conf.private-secret':{'type':'fixed','value':'synthetic-private-token-123'}})}
  else:raise AssertionError('Unexpected mutation or query')
 else:raise AssertionError('Unexpected mutation or query')
print(json.dumps(v))
'''
@pytest.mark.skipif(os.name=='nt',reason='POSIX fake executable fixture; native Windows execution is not claimed')
@pytest.mark.parametrize('failure',[None,'read','shape'])
def test_complete_bash_entrypoint_in_spaced_unicode_path(tmp_path,failure):
 repo=tmp_path/'OneDrive prueba ñ'/'Proyecto Silver';(repo/'automation/silver').mkdir(parents=True)
 script=repo/'automation/silver/silver-capacity-evidence.sh';shutil.copyfile(SOURCE,script)
 binaries=tmp_path/'fake-cli';binaries.mkdir()
 for name in ('az','databricks'):
  p=binaries/name;p.write_text('#!'+sys.executable+'\n'+FAKE,encoding='utf-8');p.chmod(0o700)
 env=dict(os.environ,PATH=str(binaries)+os.pathsep+os.environ['PATH'],CALL_LOG=str(tmp_path/'calls.jsonl'))
 if failure:env['FAKE_FAIL']=failure
 result=subprocess.run(['bash',str(script)],cwd=tmp_path,env=env,capture_output=True,text=True,encoding='utf-8',timeout=30)
 report_path=repo/'work/silver-capacity-evidence.json'
 assert report_path.is_file()
 report=json.loads(report_path.read_text(encoding='utf-8'))
 assert json.loads(result.stdout)==report
 assert not (tmp_path/'work/silver-capacity-evidence.json').exists()
 for text in (result.stdout,result.stderr,report_path.read_text()):
  assert 'synthetic-private-token-123' not in text and 'private@example.invalid' not in text and 'Traceback' not in text
 if failure=='shape':assert result.returncode==1 and report['status']=='COLLECTION_FAILED'
 else:
  assert result.returncode==0
  assert report['status']=='READ_ONLY_EVIDENCE_NOT_RUN_AUTHORIZATION'
  if failure=='read':assert report['identity_binding']=='NOT_VERIFIED' and report['read_errors']
  else:
   assert report['cluster']['single_node_confirmed']
   assert report['regional_quota']['headroom']==4
   by_id={v['node_type']:v for v in report['nodes']}
   assert by_id['Standard_D4ds_v5']['provider_assessment']=='REJECTED'
   assert by_id['Standard_D4ds_v4']['provider_assessment']=='UNKNOWN'
 calls=[json.loads(line) for line in (tmp_path/'calls.jsonl').read_text().splitlines()]
 for call in calls:
  assert not any(a in ('apply','deploy','run-now','create','update','register','set','delete','start') for a in call)
  assert call[1:3] in [['account','show'],['resource','show'],['provider','show'],['vm','list-usage'],['rest','--method'],['jobs','get'],['api','get'],['permissions','get']]


def test_windows_azure_cli_bypasses_cmd_and_preserves_url(tmp_path):
 text=SOURCE.read_text().split("exec python - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]
 ns={'__name__':'test'};exec(compile(text,'script','exec'),ns)
 root=tmp_path/'Microsoft SDKs'/'Azure'/'CLI2';(root/'wbin').mkdir(parents=True)
 cmd=root/'wbin'/'az.cmd';cmd.touch();(root/'python.exe').touch()
 url="https://management.azure.com/subscriptions/X/providers/Microsoft.Compute/skus?api-version=2021-07-01&%24filter=location+eq+%27eastus%27"
 args=['az','rest','--method','get','--url',url]
 argv,error=ns['native_command'](str(cmd),args,windows=True)
 assert error is None
 assert argv==[str(root/'python.exe'),'-IBm','azure.cli',*args[1:]]
 assert '.cmd' not in argv[0] and argv[-1]==url
 (root/'python.exe').unlink()
 assert ns['native_command'](str(cmd),args,windows=True)==(None,'AZURE_CLI_PACKAGED_PYTHON_NOT_FOUND')
 assert ns['native_command'](str(cmd),['databricks','api','get','/path'],windows=True)==(None,'NATIVE_DATABRICKS_CLI_REQUIRED')

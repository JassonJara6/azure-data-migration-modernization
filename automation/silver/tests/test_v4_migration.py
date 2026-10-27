"""Whole saved-plan + exact Job update flow; all external commands simulated."""
import copy
import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import launch
import migrate_v4 as migration
from plan_guard import Blocked,sha
from v4_transition import JOB_ID
from test_v4_primary import settings,plan,responses,node,policy_definition


def harness(tmp_path,monkeypatch,*,apply=True,change=None):
    obj=object.__new__(migration.MigrationLauncher)
    obj.work=tmp_path/'deployment';obj.work.mkdir();obj.env={};obj.ws_env={};obj.deploy=apply;obj.stage='silver'
    before,uc=settings();uc.update(provisioning_mode='workspace',metastore_id='meta',operator='op',node_type_id='Standard_D4ds_v5')
    obj.c={'runner_application_id':'runner'};obj.outputs=lambda root:{'deployment':{'value':copy.deepcopy(uc)}}
    (obj.work/'job-id.json').write_text(json.dumps({'job_id':JOB_ID,'host':'https://example.invalid'}))
    (obj.work/'bundle').mkdir()
    import yaml
    (obj.work/'bundle/databricks.yml').write_text(yaml.safe_dump({'resources':{'jobs':{'silver':copy.deepcopy(before)}}}))
    remote=copy.deepcopy(before);calls=[];saved=[];policy={'definition':policy_definition('Standard_D4ds_v5')}
    def api(method,path):
        assert method=='get';calls.append(('get',path))
        if path.startswith('/api/2.1/jobs/get?'):return {'job_id':int(JOB_ID),'settings':copy.deepcopy(remote)}
        if 'active_only' in path:return {'runs':[{'run_id':1}]} if change=='active' else {}
        if path.endswith('list-node-types'):return node()
        if path.endswith('spark-versions'):return {'versions':[]} if change=='runtime' else {'versions':[{'key':'15.4.x-scala2.12'}]}
        if 'policies/clusters/get?' in path:return policy
        pytest.fail('Unexpected API read')
    obj.api=api
    obj.verify_workspace_run_as=lambda *a:calls.append(('run-as',))
    def read(argv,stage,timeout):return copy.deepcopy(responses()[stage])
    obj.read=read
    def command(argv,**kw):
        if argv[1:3]==['bundle','summary']:return {'resources':{'jobs':{'silver':{'id':JOB_ID}}}}
        assert argv[:4]==['databricks','api','post','/api/2.1/jobs/update']
        payload=json.loads(argv[argv.index('--json')+1])
        assert payload['job_id']==int(JOB_ID) and set(payload['new_settings'])=={'job_clusters'}
        calls.append(('job-update',));remote['job_clusters']=payload['new_settings']['job_clusters']
        return {}
    monkeypatch.setattr(migration,'command',command)
    p=plan()
    def tf(root,*args,**kwargs):
        assert root==launch.UC_WORKSPACE
        if args[0]=='plan':
            path=Path(next(a[5:] for a in args if a.startswith('-out=')));path.write_bytes(b'exact-saved-plan');saved.append(path)
        elif args[0]=='show':return copy.deepcopy(p) if kwargs.get('json_output') else 'One in-place policy update'
        elif args[0]=='apply':
            assert Path(args[-1])==saved[0] and saved[0].read_bytes()==b'exact-saved-plan'
            calls.append(('policy-apply',));policy['definition']=policy_definition()
        else:pytest.fail('No other Terraform operations')
    obj.tf=tf
    def tf_approval(label,digest):
        calls.append(('policy-approval',label));assert sha(saved[0])==digest
        if change=='plan-bytes':saved[0].write_bytes(b'changed')
        if change=='before-policy':remote['timeout_seconds']=9000
        if change=='job-review-before-policy':obj.review_path.write_text('{}')
    monkeypatch.setattr(launch,'approval',tf_approval)
    def job_approval(label,digest):
        calls.append(('job-approval',label));assert digest==obj.fingerprint
        if change=='before-job':remote['timeout_seconds']=9000
        if change=='policy-race':policy['name']='changed'
        if change=='bundle-race':obj.bundle_file.write_text('changed')
        if change=='review-file':obj.review_path.write_text('{}')
    monkeypatch.setattr(migration,'approval',job_approval)
    variables={'node_type_id':'Standard_D4ds_v4','workspace_host':'https://example.invalid','workspace_id':'7405617270738925','workspace_resource_id':'arm'}
    return obj,variables,uc,remote,calls


def test_saved_policy_then_exact_job_update_no_run_no_permissions(tmp_path,monkeypatch):
    obj,variables,uc,remote,calls=harness(tmp_path,monkeypatch)
    assert obj.plan(launch.UC_WORKSPACE,'uc-workspace',variables)
    obj.update_job(obj.workspace,uc)
    assert ('policy-apply',) in calls and ('job-update',) in calls
    assert calls.index(('policy-apply',))<calls.index(('job-update',))
    assert remote['job_clusters'][0]['new_cluster']['node_type_id']=='Standard_D4ds_v4'
    assert len([c for c in calls if c[0]=='job-update'])==1
    assert all(c[0] in ('get','run-as','job-update','policy-apply','policy-approval','job-approval') for c in calls)


def test_preview_cannot_apply_or_update_job(tmp_path,monkeypatch):
    obj,variables,uc,remote,calls=harness(tmp_path,monkeypatch,apply=False)
    assert obj.plan(launch.UC_WORKSPACE,'uc-workspace',variables) is False
    assert obj.review_path.is_file()
    assert not any(c[0] in ('job-update','policy-apply','job-approval','policy-approval') for c in calls)

@pytest.mark.parametrize('change',['active','plan-bytes','before-policy','before-job','review-file','policy-race','bundle-race','job-review-before-policy','runtime'])
def test_races_and_tampering_fail_before_job_update(tmp_path,monkeypatch,change):
    obj,variables,uc,remote,calls=harness(tmp_path,monkeypatch,change=change)
    with pytest.raises(Blocked):
        obj.plan(launch.UC_WORKSPACE,'uc-workspace',variables)
        obj.update_job(obj.workspace,uc)
    assert ('job-update',) not in calls
    if change in ('active','plan-bytes','before-policy','job-review-before-policy','runtime'):assert ('policy-apply',) not in calls


def test_retry_after_policy_applied_does_not_apply_again(tmp_path,monkeypatch):
    obj,variables,uc,remote,calls=harness(tmp_path,monkeypatch)
    obj.plan(launch.UC_WORKSPACE,'uc-workspace',variables);obj.update_job(obj.workspace,uc)
    # Same target Job is an idempotent update check with no second API write.
    obj.before=copy.deepcopy(remote);obj.target=copy.deepcopy(remote['job_clusters'])
    calls.clear();obj.update_job(obj.workspace,uc)
    assert ('job-update',) not in calls and ('policy-apply',) not in calls


def test_migration_entrypoint_plan_only_does_not_call_publish(tmp_path,monkeypatch):
    work=tmp_path/'work';work.mkdir();config=work/'dev.json'
    config.write_text(json.dumps({'uc_mode':'workspace','node_type_id':'Standard_D4ds_v5'}));original=config.read_bytes()
    class Fake:
        def __init__(self,c,deploy,stage):assert not deploy and stage=='silver' and c['node_type_id']=='Standard_D4ds_v4'
        def provision(self):return None
        def update_job(self,*args):pytest.fail('Plan only cannot update Job')
    monkeypatch.setattr(migration,'ROOT',tmp_path);monkeypatch.setattr(migration,'MigrationLauncher',Fake)
    monkeypatch.setattr(sys,'argv',['migrate_v4.py','--config',str(config)])
    assert migration.main()==0 and config.read_bytes()==original


@pytest.mark.parametrize('changed',[False,True])
def test_apply_entrypoint_changes_only_local_node_after_verified_update(tmp_path,monkeypatch,changed):
    work=tmp_path/'work';work.mkdir();config=work/'dev.json'
    original={'uc_mode':'workspace','node_type_id':'Standard_D4ds_v5','existing':'preserved'}
    config.write_text(json.dumps(original));calls=[]
    class Fake:
        def __init__(self,c,deploy,stage):assert deploy and stage=='silver' and c['node_type_id']=='Standard_D4ds_v4'
        def provision(self):
            if changed:config.write_text('{}')
            return {},{}
        def update_job(self,*args):calls.append('verified-update')
    monkeypatch.setattr(migration,'ROOT',tmp_path);monkeypatch.setattr(migration,'MigrationLauncher',Fake)
    monkeypatch.setattr(sys,'argv',['migrate_v4.py','--config',str(config),'--apply'])
    assert migration.main()==(1 if changed else 0)
    if changed:assert calls==[]
    else:
        assert calls==['verified-update']
        assert json.loads(config.read_text())==dict(original,node_type_id='Standard_D4ds_v4')

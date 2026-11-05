"""Reported Jobs GET empty-object serialization; edit_mode stays functional."""
import copy
import json
from pathlib import Path
import sys
import pytest
import yaml
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from plan_guard import Blocked
from single_node_v4 import validate_cluster
from v4_transition import canonical_sha,target_clusters,verify_settings
from test_v4_primary import settings,policy_definition
from test_v4_migration import harness
import launch
import reconcile_v4_local as reconciliation


def pair():
    before,uc=settings();before['edit_mode']='UI_LOCKED'
    expected=copy.deepcopy(before);expected['job_clusters']=target_clusters(before,uc)
    actual=copy.deepcopy(expected)
    for key in ('driver_node_type_flexibility','worker_node_type_flexibility'):
        actual['job_clusters'][0]['new_cluster'][key]={}
    return before,uc,expected,actual,{'definition':policy_definition()}


def test_both_empty_objects_equivalent_only_with_verified_empty_policy():
    before,uc,expected,actual,policy=pair();saved=copy.deepcopy(actual)
    assert actual!=expected  # reproduces the old equality failure
    verify_settings(actual,expected,policy)
    validate_cluster(actual['job_clusters'][0]['new_cluster'],policy)
    assert target_clusters(actual,uc,policy)==expected['job_clusters']
    assert actual==saved
    policy['definition']['driver_node_type_flexibility.alternate_node_type_ids']['value']='Standard_D4ds_v5'
    with pytest.raises(Blocked):verify_settings(actual,expected,policy)

@pytest.mark.parametrize('key',['driver_node_type_flexibility','worker_node_type_flexibility'])
@pytest.mark.parametrize('bad',[None,[],{'alternate_node_type_ids':None},{'alternate_node_type_ids':{}},
    {'alternate_node_type_ids':['Standard_D4ds_v5']},{'unexpected':[]},'MISSING'])
def test_never_normalize_missing_null_alternates_or_unknown_fields(key,bad):
    _,_,expected,actual,policy=pair();cluster=actual['job_clusters'][0]['new_cluster']
    if bad=='MISSING':cluster.pop(key)
    else:cluster[key]=bad
    with pytest.raises(Blocked):verify_settings(actual,expected,policy)
    with pytest.raises(Blocked):validate_cluster(cluster,policy)

@pytest.mark.parametrize('mode',['EDITABLE','SYSTEM_MANAGED',None,'unknown'])
def test_edit_mode_difference_remains_blocked(mode):
    _,_,expected,actual,policy=pair();actual['edit_mode']=mode
    with pytest.raises(Blocked,match='V4_EDIT_MODE_CHANGED'):verify_settings(actual,expected,policy)

@pytest.mark.parametrize('key,value',[('num_workers',1),('node_type_id','Standard_D4ds_v5'),('single_user_name','other')])
def test_functional_cluster_drift_still_blocks(key,value):
    _,_,expected,actual,policy=pair();actual['job_clusters'][0]['new_cluster'][key]=value
    with pytest.raises(Blocked):verify_settings(actual,expected,policy)


def test_post_update_accepts_empty_objects_without_repeat_update(tmp_path,monkeypatch):
    obj,variables,uc,remote,calls=harness(tmp_path,monkeypatch)
    remote['edit_mode']='UI_LOCKED'
    base_command=__import__('migrate_v4').command
    def preserve_mode(argv,**kw):
        if argv[1:3]==['api','post']:
            payload=json.loads(argv[argv.index('--json')+1])
            assert payload['new_settings']['edit_mode']=='UI_LOCKED'
        return base_command(argv,**kw)
    monkeypatch.setattr(__import__('migrate_v4'),'command',preserve_mode)
    obj.plan(launch.UC_WORKSPACE,'uc-workspace',variables)
    original=obj.snapshot
    def snapshot():
        result=original()
        if result['job_clusters'][0]['new_cluster']['node_type_id']=='Standard_D4ds_v4':
            for key in ('driver_node_type_flexibility','worker_node_type_flexibility'):
                result['job_clusters'][0]['new_cluster'][key]={}
        return result
    obj.snapshot=snapshot;obj.update_job(obj.workspace,uc)
    assert calls.count(('job-update',))==1


def local_fixture(tmp_path,monkeypatch,edit=False):
    before,uc,expected,actual,policy=pair()
    if edit:actual['edit_mode']='EDITABLE'
    work=tmp_path/'work';bundle_path=work/'silver-deployment/bundle/databricks.yml'
    bundle_path.parent.mkdir(parents=True)
    bundle={'resources':{'jobs':{'silver':copy.deepcopy(before)}}}
    bundle_path.write_text(yaml.safe_dump(bundle))
    config=work/'silver-dev.json';config.write_text(json.dumps({'uc_mode':'workspace','node_type_id':'Standard_D4ds_v5',
        'runner_application_id':uc['runner_application_id'],'metastore_id':'meta','preserved':'value'}))
    record={'job_id':'210884055971910','host':'https://example.invalid'}
    (bundle_path.parent.parent/'job-id.json').write_text(json.dumps(record))
    transition={'job_id':record['job_id'],'workspace_host':record['host'],'policy_id':'0004947D67FAA646',
        'before_settings':before,'after_job_clusters':expected['job_clusters']}
    # Existing fixture policy ID differs; align all reviewed inputs, not just the target.
    for value in (before,expected,actual,bundle['resources']['jobs']['silver']):
        value['job_clusters'][0]['new_cluster']['policy_id']=transition['policy_id']
    bundle_path.write_text(yaml.safe_dump(bundle))
    path=work/('v4-job-transition-'+canonical_sha(transition)+'.json');path.write_text(json.dumps(transition))
    calls=[]
    def command(argv,**kwargs):
        assert argv[:3]==['databricks','api','get']  # no remote writes, TF, bundle or Jobs execution
        calls.append(argv[3])
        if 'runs/list' in argv[3]:return {}
        if 'policies/clusters' in argv[3]:return copy.deepcopy(policy)
        if 'jobs/get' in argv[3]:return {'job_id':int(record['job_id']),'settings':copy.deepcopy(actual)}
        pytest.fail('Unexpected read')
    monkeypatch.setattr(reconciliation,'ROOT',tmp_path)
    monkeypatch.setattr(reconciliation,'command',command)
    monkeypatch.setattr(reconciliation,'verify_assignment',lambda *a:None)
    monkeypatch.setattr(reconciliation,'approval',lambda *a:None)
    return config,path,bundle_path,calls


def test_local_preview_write_and_replay_have_no_remote_writes(tmp_path,monkeypatch):
    config,path,bundle,calls=local_fixture(tmp_path,monkeypatch)
    original=(config.read_bytes(),bundle.read_bytes())
    reconciliation.reconcile(config,path)
    assert original==(config.read_bytes(),bundle.read_bytes())
    reconciliation.reconcile(config,path,True)
    assert json.loads(config.read_text())['node_type_id']=='Standard_D4ds_v4'
    assert json.loads(config.read_text())['preserved']=='value'
    updated=(config.read_bytes(),bundle.read_bytes())
    reconciliation.reconcile(config,path,True)
    assert updated==(config.read_bytes(),bundle.read_bytes())


def test_edit_mode_mismatch_blocks_local_writes(tmp_path,monkeypatch):
    config,path,bundle,calls=local_fixture(tmp_path,monkeypatch,edit=True)
    original=(config.read_bytes(),bundle.read_bytes())
    with pytest.raises(Blocked,match='V4_EDIT_MODE_CHANGED'):reconciliation.reconcile(config,path,True)
    assert original==(config.read_bytes(),bundle.read_bytes())


@pytest.mark.parametrize('failure',['hash','policy','settings','local','approval-race'])
def test_reconciliation_rejects_tampering_and_drift(tmp_path,monkeypatch,failure):
    config,path,bundle,calls=local_fixture(tmp_path,monkeypatch)
    if failure=='hash':
        t=json.loads(path.read_text());t['job_id']='other';path.write_text(json.dumps(t))
    elif failure=='local':
        b=yaml.safe_load(bundle.read_text());b['resources']['jobs']['silver']['job_clusters'][0]['new_cluster']['num_workers']=2
        bundle.write_text(yaml.safe_dump(b))
    elif failure in ('policy','settings'):
        original=reconciliation.command
        def command(argv,**kw):
            value=original(argv,**kw)
            if failure=='policy' and 'policies/clusters' in argv[3]:
                value['definition']['num_workers']['value']=1
            if failure=='settings' and 'jobs/get' in argv[3]:value['settings']['timeout_seconds']=1
            return value
        monkeypatch.setattr(reconciliation,'command',command)
    else:monkeypatch.setattr(reconciliation,'approval',lambda *a:config.write_text('{}'))
    original_bundle=bundle.read_bytes()
    with pytest.raises(Blocked):reconciliation.reconcile(config,path,True)
    assert bundle.read_bytes()==original_bundle

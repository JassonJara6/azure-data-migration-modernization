"""No cloud calls: publication must remain distinct from execution/capacity."""
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import launch
from plan_guard import Blocked


def test_publish_preflight_never_claims_capacity_or_reads_skus(monkeypatch,capsys):
    obj=object.__new__(launch.Launcher);obj.deploy_only=True
    def api(method,path):
        if path.endswith('metastore_summary'):return {'metastore_id':'m'}
        if path.endswith('spark-versions'):return {'versions':[{'key':'15.4.x-scala2.12'}]}
        if path.endswith('list-node-types'):return {'node_types':[{'node_type_id':'Standard_D4ds_v5'}]} # Status missing, as observed.
        if '/permissions/' in path:return {'privilege_assignments':[{'principal':'runner','privileges':['READ_VOLUME']}]}
        pytest.fail('No unknown-capacity reads permitted during artifact publication')
    obj.api=api
    monkeypatch.setattr(launch,'command',lambda *a,**k:pytest.fail('No capacity CLI'))
    obj.check_workspace(dict(metastore_id='m',node_type_id='Standard_D4ds_v5',catalog='admm_dev',runner_application_id='runner',operator='op'))
    report=json.loads(capsys.readouterr().out)
    assert report['capacity']=='NOT_VERIFIED' and report['execution']=='BLOCKED'

@pytest.mark.parametrize('argv',[
    ['--deploy-only'],['--deploy-only','--deploy','--stage','all'],
    ['--deploy-only','--deploy','--stage','silver','--uc-mode','account']])
def test_invalid_release_flags_block_before_any_external_call(monkeypatch,argv,capsys):
    monkeypatch.setattr(sys,'argv',['launch.py',*argv])
    monkeypatch.setattr(launch,'load_config',lambda *a,**k:pytest.fail('No config mutation'))
    assert launch.main()==1
    assert 'DEPLOY_ONLY_REQUIRES' in capsys.readouterr().err


def test_collector_redacts_receipt_and_task_messages(monkeypatch):
    import collect_workspace as collector
    calls=[]
    def read(argv):
        calls.append(argv)
        if argv[1:3]==['jobs','get']:return {'settings':{'tasks':[{'libraries':[{'whl':'/Workspace/artifacts/admm_silver-0.1.0+e86a2e0b709b-py3-none-any.whl'}]}]}},None
        if argv[1:3]==['api','get']:return {'node_types':[{'node_type_id':'Standard_D4ds_v5','num_cores':4,'memory_mb':16384,'node_info':{}}]},None
        if argv[1:3]==['fs','cat']:return {'parent_run_id':'synthetic-secret'},None
        if argv[1:3]==['jobs','get-run-output']:return {'error':'SILVER_RUNTIME_FAILED synthetic-secret','error_trace':'synthetic-secret'},None
        if argv[:3]==['az','account','show']:return None,{'code':'READ_TIMEOUT'}
        pytest.fail('No writes, cluster or Job execution allowed')
    result=collector.collect(reader=read,artifact_verifier=lambda profile: (_ for _ in ()).throw(Blocked('WORKSPACE_EXPORT_SIZE_LIMIT')))
    assert result['artifacts']=={'status':'NOT_VERIFIED','code':'WORKSPACE_EXPORT_SIZE_LIMIT','error_type':'Blocked'}
    encoded=json.dumps(result)
    assert 'synthetic-secret' not in encoded and result['receipt']=='CONTRACT_INVALID'
    assert result['old_task_code']=='RUNTIME_FAILED'
    assert result['provider_metadata'][0]['status_present'] is False
    assert result['wheel_version_matches_repository'] is False
    assert len([a for a in calls if a[1:3]==['jobs','get']])==1
    assert not any(a in ('run-now','create','deploy','apply','update') for argv in calls for a in argv)


def test_git_bash_wrapper_disables_path_conversion():
    text=(Path(__file__).resolve().parents[1]/'collect_workspace.sh').read_text()
    assert 'MSYS_NO_PATHCONV=1' in text and "MSYS2_ARG_CONV_EXCL='*'" in text
    assert 'PYTHONUTF8=1' in text and '--deploy' not in text


def test_safe_task_metadata_parser_ignores_messages_and_paths():
    from runtime_report import safe_task_failure
    detail={'stage':'committed_evidence','error_type':'PermissionError','location':{'file':'admm_silver/evidence.py','line':8},'message':'synthetic-secret'}
    result=safe_task_failure({'error':'SILVER_RUNTIME_FAILED '+json.dumps(detail),'error_trace':'synthetic-secret'})
    assert result['error_type']=='PermissionError' and result['location']==detail['location']
    assert 'synthetic-secret' not in json.dumps(result)
    detail['location']['file']='/Users/synthetic-secret/path.py'
    assert safe_task_failure({'error':'SILVER_RUNTIME_FAILED '+json.dumps(detail)})['location'] is None
    assert safe_task_failure({'error':'SILVER_RUNTIME_FAILED'})['error_type'] is None

@pytest.mark.parametrize('change',[{'driver_node_type_id':'Standard_D32ds_v5'},{'autoscale':{'min_workers':1,'max_workers':2}},{'instance_pool_id':'p'},{'driver_node_type_flexibility':{'alternate_node_type_ids':['Standard_D32ds_v5']}}])
def test_publish_only_preserves_cost_guards(tmp_path,monkeypatch,change):
    from bundle_overlay import ephemeral_bundle
    import yaml
    uc=dict(runner_application_id='runner',operator='op',policy_id='p',node_type_id='Standard_D4ds_v5')
    spec=ephemeral_bundle(yaml.safe_load((launch.ROOT/'databricks/silver/databricks.yml').read_text()),uc,'https://example.invalid')
    settings=spec['resources']['jobs']['silver'];settings['job_clusters'][0]['new_cluster'].update(change)
    obj=object.__new__(launch.Launcher);obj.work=tmp_path;obj.deploy_only=True;obj.c={'uc_mode':'workspace'}
    (tmp_path/'job-id.json').write_text(json.dumps({'job_id':1,'host':'https://example.invalid'}))
    obj.api=lambda *a:{'settings':settings}
    monkeypatch.setattr(launch,'build_wheel',lambda *a:pytest.fail('Must block before build/deploy'))
    monkeypatch.setattr(launch,'command',lambda *a,**k:pytest.fail('No deployment'))
    with pytest.raises(Blocked):obj.deploy_job({'workspace_host':'https://example.invalid'},uc)


def test_malformed_task_metadata_is_not_a_secondary_failure():
    from runtime_report import safe_task_failure
    value={'stage':{},'error_type':[],'location':{'file':'synthetic-secret','line':'synthetic-secret'}}
    result=safe_task_failure({'error':'SILVER_RUNTIME_FAILED '+json.dumps(value)})
    assert result['stage'] is None and result['error_type'] is None and result['location'] is None


def test_collection_artifact_reader_is_optional_and_passes_profile():
    import collect_workspace as collector
    def read(argv):return None,{'code':'ACCESS_DENIED'}
    calls=[]
    def verify(profile):
        calls.append(profile)
        return {'status':'ARTIFACT_BYTES_VERIFIED','execution':'NOT_STARTED'}
    report=collector.collect('synthetic-profile',read,verify)
    assert calls==['synthetic-profile']
    assert report['artifacts']['execution']=='NOT_STARTED'


@pytest.mark.parametrize('message,expected',[
    ('{"code":"ACCESS_DENIED"}','ACCESS_DENIED'),
    ('secret:/Users/private','LOCAL_OR_REMOTE_READ_FAILED'),
    ('LOCAL_STAGING_REQUIRED','LOCAL_STAGING_REQUIRED')])
def test_collection_artifact_failure_never_leaks_raw_details(message,expected):
    import collect_workspace as collector
    def verify(profile):raise Blocked(message)
    report=collector.collect(reader=lambda argv:(None,{'code':'ACCESS_DENIED'}),artifact_verifier=verify)
    assert report['artifacts']['status']=='NOT_VERIFIED'
    assert report['artifacts']['code']==expected
    assert 'secret' not in json.dumps(report) and '/Users/private' not in json.dumps(report)


def test_readonly_artifact_check_requires_original_local_stage(tmp_path,monkeypatch):
    import collect_workspace as collector
    monkeypatch.setattr(collector,'ROOT',tmp_path)
    monkeypatch.setattr(launch,'command',lambda *a,**kw:pytest.fail('No remote calls without original local staging'))
    with pytest.raises(Blocked,match='LOCAL_STAGING_REQUIRED'):
        collector.verify_staged_artifacts('admm-workspace')

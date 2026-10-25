"""Deployed artifact preflight; synthetic CLI only, no cloud calls."""
import copy
import sys
from pathlib import Path
import pytest
import yaml
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from notebook_guard import verify_deployed_task
from plan_guard import Blocked

ROOT='/Workspace/Shared/admm_dev/admm-silver/dev'
TASK={'task_key':'transform_and_verify','notebook_task':{'source':'WORKSPACE','notebook_path':ROOT+'/files/notebooks/run_silver'},'libraries':[{'whl':ROOT+'/artifacts/pkg/code.whl'}]}

@pytest.mark.parametrize('failure',[None,'missing','file-not-notebook','wrong-path','wrong-wheel','wrong-source','missing-acl','other-principal'])
def test_preflight(failure):
    task=copy.deepcopy(TASK);calls=[]
    if failure=='wrong-path':task['notebook_task']['notebook_path']=ROOT+'/files/absent'
    if failure=='wrong-source':task['notebook_task']['source']='GIT'
    if failure=='wrong-wheel':task['libraries'][0]['whl']='/Workspace/elsewhere/code.whl'
    def cli(args,**kwargs):
        calls.append(args)
        if args[1:3]==['jobs','get']:return {'settings':{'tasks':[task]}}
        if args[1:3]==['workspace','get-status']:
            if failure=='missing':raise Blocked('COMMAND_FAILED')
            return {'object_type':'FILE' if args[3].endswith('.whl') or failure=='file-not-notebook' else 'NOTEBOOK','object_id':7}
        return {'access_control_list':[] if failure=='missing-acl' else [{'service_principal_name':'other' if failure=='other-principal' else 'runner','all_permissions':[{'permission_level':'CAN_READ','inherited':True}]}]}
    if failure:
        with pytest.raises(Blocked):verify_deployed_task(cli,{},1,ROOT,'runner')
    else:
        verify_deployed_task(cli,{},1,ROOT,'runner')
        assert len([c for c in calls if c[1:3]==['workspace','get-status']])==2
    assert not any('run-now' in c for c in calls)


def test_source_bundle_explicitly_syncs_notebook():
    repo=Path(__file__).resolve().parents[3]
    spec=yaml.safe_load((repo/'databricks/silver/databricks.yml').read_text())
    assert spec['sync']['include']==['notebooks/run_silver.py']
    task=spec['resources']['jobs']['silver']['tasks'][0]
    assert task['notebook_task']['notebook_path']=='./notebooks/run_silver.py'
    assert task['notebook_task']['source']=='WORKSPACE'
    assert (repo/'databricks/silver/notebooks/run_silver.py').read_text().startswith('# Databricks notebook source')

@pytest.mark.parametrize('failure',[None,'old-wheel','changed-bytes','old-notebook','magic'])
def test_exact_release_artifacts_required(tmp_path,failure):
    wheel=tmp_path/'code.whl';wheel.write_bytes(b'fresh bytes')
    source=tmp_path/'run_silver.py';source.write_text('x=1\n')
    task=copy.deepcopy(TASK)
    if failure=='old-wheel':task['libraries'][0]['whl']=ROOT+'/artifacts/pkg/old.whl'
    def cli(argv,**kwargs):
        if argv[1:3]==['jobs','get']:return {'settings':{'tasks':[task]}}
        if argv[1:3]==['workspace','get-status']:return {'object_type':'FILE' if argv[3].endswith('.whl') else 'NOTEBOOK','object_id':7}
        if argv[1:3]==['permissions','get']:return {'access_control_list':[{'service_principal_name':'runner','all_permissions':[{'permission_level':'CAN_READ'}]}]}
        if argv[1:3]==['workspace','export']:
            dest=Path(argv[argv.index('--file')+1])
            if argv[argv.index('--format')+1]=='RAW':dest.write_bytes(b'old' if failure=='changed-bytes' else b'fresh bytes')
            else:dest.write_text('x=2\n' if failure=='old-notebook' else 'x=1\n# MAGIC %sql SELECT 1\n' if failure=='magic' else '# Databricks notebook source\n\nx = 1\n')
            return
        pytest.fail('No run or mutation permitted')
    if failure:
        with pytest.raises(Blocked):verify_deployed_task(cli,{},1,ROOT,'runner',expected_wheel=wheel,expected_notebook=source)
    else:verify_deployed_task(cli,{},1,ROOT,'runner',expected_wheel=wheel,expected_notebook=source)


def test_cell_boundary_changes_are_not_accepted_as_whitespace():
    from notebook_guard import notebook_signature
    assert notebook_signature('x=1\ny=2\n')!=notebook_signature('x=1\n# COMMAND ----------\ny=2\n')
    with pytest.raises(Blocked,match='INVALID_PYTHON'):notebook_signature('try:\n# COMMAND ----------\nx=1\n')

@pytest.mark.parametrize('failure',['parameters','defaults','run-as','concurrency','retry','trigger'])
def test_deployed_settings_drift_blocks_before_artifact_or_execution(failure):
    expected={'tasks':[copy.deepcopy(TASK)],'run_as':{'service_principal_name':'runner'},'max_concurrent_runs':1,'timeout_seconds':7200,'parameters':[{'name':'parent_run_id','default':'expected-parent'}]}
    expected['tasks'][0]['notebook_task']['base_parameters']={'catalog':'admm_dev'}
    actual=copy.deepcopy(expected)
    if failure=='parameters':actual['tasks'][0]['notebook_task']['base_parameters']['catalog']='other'
    if failure=='defaults':actual['parameters'][0]['default']='other'
    if failure=='run-as':actual['run_as']['service_principal_name']='other'
    if failure=='concurrency':actual['max_concurrent_runs']=2
    if failure=='retry':actual['tasks'][0]['max_retries']=1
    if failure=='trigger':actual['schedule']={'pause_status':'UNPAUSED'}
    def cli(argv,**kwargs):
        if argv[1:3]==['jobs','get']:return {'settings':actual}
        pytest.fail('Must block before any export/run/mutation')
    with pytest.raises(Blocked):verify_deployed_task(cli,{},1,ROOT,'runner',expected_settings=expected)

@pytest.mark.parametrize('changed',['wheel','notebook'])
def test_verification_uses_frozen_approved_inputs(tmp_path,changed):
    from notebook_guard import notebook_signature
    from plan_guard import sha
    wheel=tmp_path/'code.whl';wheel.write_bytes(b'approved')
    notebook=tmp_path/'run_silver.py';notebook.write_text('x=1\n')
    approved_sha=sha(wheel);approved_cells=notebook_signature(notebook.read_text())
    if changed=='wheel':wheel.write_bytes(b'changed after approval')
    else:notebook.write_text('x=2\n')
    def cli(argv,**kwargs):
        if argv[1:3]==['jobs','get']:return {'settings':{'tasks':[copy.deepcopy(TASK)]}}
        if argv[1:3]==['workspace','get-status']:return {'object_type':'FILE' if argv[3].endswith('.whl') else 'NOTEBOOK','object_id':7}
        if argv[1:3]==['permissions','get']:return {'access_control_list':[{'service_principal_name':'runner','all_permissions':[{'permission_level':'CAN_READ'}]}]}
        if argv[1:3]==['workspace','export']:
            destination=Path(argv[argv.index('--file')+1])
            destination.write_bytes(wheel.read_bytes() if argv[argv.index('--format')+1]=='RAW' else notebook.read_bytes())
            return
        pytest.fail('No run')
    with pytest.raises(Blocked):verify_deployed_task(cli,{},1,ROOT,'runner',expected_wheel=wheel,expected_notebook=notebook,expected_wheel_sha=approved_sha,expected_cells=approved_cells)

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

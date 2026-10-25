"""Fail closed before run-now if the deployed task has inaccessible artifacts."""
import ast
import re
from pathlib import Path
import tempfile
import shutil
from plan_guard import require,sha


def notebook_signature(text):
    require(not any(line.lstrip().startswith('# MAGIC') for line in text.splitlines()),'UNEXPECTED_NOTEBOOK_MAGIC')
    try:
        cells=[ast.parse(cell) for cell in re.split(r'(?m)^# COMMAND -+\s*$',text)]
    except SyntaxError:
        require(False,'DEPLOYED_NOTEBOOK_INVALID_PYTHON')
    return [ast.dump(cell) for cell in cells if cell.body]


def verify_deployed_task(command, env, job_id, root, runner,*,expected_wheel=None,expected_notebook=None,expected_settings=None,expected_wheel_sha=None,expected_cells=None):
    job=command(['databricks','jobs','get',str(job_id),'--output','json'],env=env,json_output=True)
    tasks=job.get('settings',{}).get('tasks',[])
    require(len(tasks)==1 and tasks[0].get('task_key')=='transform_and_verify','DEPLOYED_TASK_MISMATCH')
    task=tasks[0]
    if expected_settings is not None:
        settings=job['settings'];expected_task=expected_settings['tasks'][0]
        require(settings.get('max_concurrent_runs',1)==1 and settings.get('timeout_seconds')==7200 and
            task.get('max_retries',0)==0 and not any(settings.get(k) for k in ('schedule','continuous','trigger')),'DEPLOYED_JOB_EXECUTION_GUARDS_MISMATCH')
        require(settings.get('run_as',{}).get('service_principal_name')==runner,'DEPLOYED_RUN_AS_MISMATCH')
        require(task.get('notebook_task',{}).get('base_parameters')==expected_task['notebook_task']['base_parameters'],'DEPLOYED_NOTEBOOK_PARAMETERS_MISMATCH')
        def defaults(values):
            require(isinstance(values,list) and all(isinstance(v,dict) and isinstance(v.get('name'),str) for v in values),'DEPLOYED_JOB_PARAMETERS_INVALID')
            require(len({v['name'] for v in values})==len(values),'DEPLOYED_JOB_PARAMETERS_INVALID')
            return {v['name']:v.get('default') for v in values}
        require(defaults(settings.get('parameters',[]))==defaults(expected_settings.get('parameters',[])),'DEPLOYED_JOB_PARAMETERS_MISMATCH')
    notebook=task.get('notebook_task',{})
    require(notebook.get('source','WORKSPACE')=='WORKSPACE','NOTEBOOK_SOURCE_NOT_WORKSPACE')
    root='/'+root.removeprefix('/Workspace').strip('/')
    path=notebook.get('notebook_path','').removeprefix('/Workspace')
    require(path==root+'/files/notebooks/run_silver','DEPLOYED_NOTEBOOK_PATH_MISMATCH')
    libraries=task.get('libraries',[])
    require(len(libraries)==1 and set(libraries[0])=={'whl'},'DEPLOYED_WHEEL_MISMATCH')
    wheel=libraries[0]['whl'].removeprefix('/Workspace')
    require(wheel.startswith(root+'/artifacts/') and wheel.endswith('.whl') and '/..' not in wheel,'DEPLOYED_WHEEL_PATH_MISMATCH')
    for path,kind,permission_kind in ((path,'NOTEBOOK','notebooks'),(wheel,'FILE','files')):
        obj=command(['databricks','workspace','get-status',path,'--output','json'],env=env,json_output=True)
        require(obj.get('object_type')==kind and obj.get('object_id'),'DEPLOYED_ARTIFACT_MISSING_OR_WRONG_TYPE')
        acl=command(['databricks','permissions','get',permission_kind,str(obj['object_id']),'--output','json'],env=env,json_output=True)
        require(any(entry.get('service_principal_name')==runner and any(p.get('permission_level') in ('CAN_READ','CAN_RUN','CAN_EDIT','CAN_MANAGE') for p in entry.get('all_permissions',[])) for entry in acl.get('access_control_list',[])),'RUNNER_ARTIFACT_READ_NOT_VERIFIED')

    if expected_wheel is not None or expected_notebook is not None:
        require(expected_wheel is not None and expected_notebook is not None,'EXPECTED_RELEASE_ARTIFACTS_REQUIRED')
        require(Path(wheel).name==expected_wheel.name,'DEPLOYED_WHEEL_VERSION_MISMATCH')
        # System temp, never delete a OneDrive bundle directory.
        directory=tempfile.mkdtemp(prefix='admm-verify-release-')
        try:
            local_wheel=Path(directory)/'remote.whl';local_notebook=Path(directory)/'remote.py'
            command(['databricks','workspace','export',wheel,'--format','RAW','--file',str(local_wheel)],env=env)
            require(sha(local_wheel)==(expected_wheel_sha or sha(expected_wheel)),'DEPLOYED_WHEEL_SHA256_MISMATCH')
            notebook_path=notebook['notebook_path']
            command(['databricks','workspace','export',notebook_path,'--format','SOURCE','--file',str(local_notebook)],env=env)
            remote=local_notebook.read_text(encoding='utf-8')
            # SOURCE may normalize whitespace; preserve executable code AND cell boundaries.
            require(notebook_signature(remote)==(expected_cells if expected_cells is not None else notebook_signature(expected_notebook.read_text(encoding='utf-8'))),'DEPLOYED_NOTEBOOK_CODE_MISMATCH')
        finally:
            try:shutil.rmtree(directory)
            except OSError:pass # Temporary cleanup cannot hide an integrity failure.

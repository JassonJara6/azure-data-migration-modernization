"""Fail closed before run-now if the deployed task has inaccessible artifacts."""
from plan_guard import require


def verify_deployed_task(command, env, job_id, root, runner):
    job=command(['databricks','jobs','get',str(job_id),'--output','json'],env=env,json_output=True)
    tasks=job.get('settings',{}).get('tasks',[])
    require(len(tasks)==1 and tasks[0].get('task_key')=='transform_and_verify','DEPLOYED_TASK_MISMATCH')
    task=tasks[0]
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

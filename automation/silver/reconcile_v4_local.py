"""Remote GETs only; preview by default, hash-approved LOCAL reconciliation.

Never Terraform, DAB deployment, Job update/run, Azure writes or permission writes.
An edit_mode change remains blocked until its exact semantics are reviewed.
"""
import argparse
import copy
import json
from pathlib import Path
import sys
import yaml
from launch import ROOT,command,approval
from plan_guard import require,Blocked,sha
from v4_transition import JOB_ID,canonical_sha,target_clusters,verify_settings
from single_node_v4 import PRIMARY,validate_policy
from workspace_uc import profile_environment,verify_assignment
import os


def reconcile(config_path,transition_path,write=False):
    config_path=Path(config_path).resolve();transition_path=Path(transition_path).resolve()
    require(all(p.is_relative_to(ROOT/'work') and p.suffix=='.json' and p.is_file()
                for p in (config_path,transition_path)),'V4_PRIVATE_FILES_REQUIRED')
    config=json.loads(config_path.read_text(encoding='utf-8'))
    transition=json.loads(transition_path.read_text(encoding='utf-8'))
    fingerprint=canonical_sha(transition)
    require(transition_path.name=='v4-job-transition-'+fingerprint+'.json','V4_TRANSITION_HASH_MISMATCH')
    require(str(transition.get('job_id'))==JOB_ID,'V4_JOB_ID_MISMATCH')
    require(config.get('uc_mode')=='workspace' and not config.get('allow_create_metastore',False),'V4_WORKSPACE_MODE_REQUIRED')
    require(config.get('node_type_id') in ('Standard_D4ds_v5',PRIMARY),'V4_LOCAL_NODE_UNEXPECTED')
    require(transition.get('policy_id')=='0004947D67FAA646','V4_POLICY_ID_MISMATCH')
    work=ROOT/'work/silver-deployment';bundle_path=work/'bundle/databricks.yml'
    owned=json.loads((work/'job-id.json').read_text(encoding='utf-8'))
    require(str(owned.get('job_id'))==JOB_ID and owned.get('host')==transition['workspace_host'],'V4_EXISTING_JOB_RECORD_MISMATCH')
    workspace={'workspace_host':transition['workspace_host'],'workspace_id':'7405617270738925'}
    env=profile_environment(os.environ,config.get('workspace_profile') or 'admm-workspace',workspace)
    def get(path):return command(['databricks','api','get',path],env=env,json_output=True,timeout=90)
    uc={'policy_id':transition['policy_id'],'runner_application_id':config['runner_application_id']}
    expected=copy.deepcopy(transition['before_settings'])
    # Recompute the intended transition, never trust a filename hash alone.
    require(target_clusters(expected,uc)==transition['after_job_clusters'],'V4_TRANSITION_TARGET_MISMATCH')
    expected['job_clusters']=transition['after_job_clusters']
    def snapshot():
        verify_assignment(lambda method,path:get(path),workspace,config['metastore_id'],config.get('operator_user_name'))
        job=get('/api/2.1/jobs/get?job_id='+JOB_ID)
        require(str(job.get('job_id'))==JOB_ID,'V4_JOB_ID_MISMATCH')
        active=get('/api/2.1/jobs/runs/list?job_id='+JOB_ID+'&active_only=true')
        require(isinstance(active,dict) and isinstance(active.get('runs',[]),list) and not active.get('runs'),'V4_ACTIVE_JOB_RUN')
        policy=get('/api/2.0/policies/clusters/get?policy_id='+uc['policy_id'])
        validate_policy(policy,uc['runner_application_id'])
        verify_settings(job['settings'],expected,policy)
        target_clusters(job['settings'],uc,policy)
        return canonical_sha({'job':job['settings'],'policy':policy})
    remote_sha=snapshot()
    bundle=yaml.safe_load(bundle_path.read_text(encoding='utf-8'))
    local=bundle['resources']['jobs']['silver']['job_clusters']
    require(local in (transition['before_settings']['job_clusters'],transition['after_job_clusters']),
            'V4_LOCAL_BUNDLE_CLUSTER_CHANGED')
    updated=copy.deepcopy(config);updated['node_type_id']=PRIMARY
    if local==transition['after_job_clusters'] and config==updated:
        print(json.dumps({'status':'V4_LOCAL_ALREADY_RECONCILED','execution':'NOT_STARTED'}));return
    before={str(p):sha(p) for p in (config_path,transition_path,bundle_path)}
    review=canonical_sha({'local_files':before,'remote_sha256':remote_sha,'transition_sha256':fingerprint})
    print(json.dumps({'status':'V4_LOCAL_RECONCILIATION_REVIEW','sha256':review,'execution':'NOT_STARTED'}))
    if not write:return
    approval('RECONCILE-V4-LOCAL',review)
    require(all(sha(Path(p))==h for p,h in before.items()),'V4_LOCAL_FILES_CHANGED')
    require(snapshot()==remote_sha,'V4_REMOTE_CHANGED_AFTER_REVIEW')
    bundle['resources']['jobs']['silver']['job_clusters']=copy.deepcopy(transition['after_job_clusters'])
    # A partial local write is recoverable: each file independently accepts old/target.
    if local!=transition['after_job_clusters']:
        bundle_path.write_text(yaml.safe_dump(bundle,sort_keys=False),encoding='utf-8')
    if config!=updated:config_path.write_text(json.dumps(updated,indent=2),encoding='utf-8')
    print(json.dumps({'status':'V4_LOCAL_RECONCILED','remote_writes':False,'execution':'NOT_STARTED'}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='work/silver-dev.json')
    parser.add_argument('--transition',required=True)
    parser.add_argument('--write-local',action='store_true')
    args=parser.parse_args()
    try:reconcile(args.config,args.transition,args.write_local);return 0
    except Exception as exc:
        print(json.dumps({'status':'BLOCKED','code':str(exc) if isinstance(exc,Blocked) else 'V4_LOCAL_OR_READ_FAILURE',
                          'error_type':type(exc).__name__}),file=sys.stderr);return 1

if __name__=='__main__':raise SystemExit(main())

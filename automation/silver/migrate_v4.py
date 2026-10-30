"""Plan-only by default. --apply requests hash approvals; NEVER runs Silver.

Existing workspace/UC/policy/Job only. No identity or permission writes.
"""
import argparse
import copy
import json
from pathlib import Path
import sys
import yaml
from launch import Launcher,ROOT,UC_WORKSPACE,command,approval
from plan_guard import require,Blocked,sha
from single_node_v4 import PRIMARY,read_evidence,validate_policy
from v4_transition import JOB_ID,target_clusters,canonical_sha


class MigrationLauncher(Launcher):
    def read(self,argv,stage,timeout=90):
        return command(argv,env=self.ws_env if argv[0]=='databricks' else self.env,json_output=True,timeout=timeout)

    def snapshot(self):
        result=self.api('get','/api/2.1/jobs/get?job_id='+JOB_ID)
        require(str(result.get('job_id'))==JOB_ID,'V4_JOB_ID_MISMATCH')
        active=self.api('get','/api/2.1/jobs/runs/list?job_id='+JOB_ID+'&active_only=true')
        require(isinstance(active,dict) and isinstance(active.get('runs',[]),list) and not active.get('runs'),'V4_ACTIVE_JOB_RUN')
        return result['settings']

    def recheck(self):
        self.verify_workspace_run_as(self.workspace,self.current_uc['metastore_id'],self.current_uc['operator'],self.current_uc['runner_application_id'])
        require(canonical_sha(self.snapshot())==canonical_sha(self.before),'V4_JOB_CHANGED_AFTER_REVIEW')
        require(sha(self.bundle_file)==self.bundle_sha,'V4_BUNDLE_CHANGED_AFTER_REVIEW')
        require(canonical_sha(json.loads(self.review_path.read_text(encoding='utf-8')))==self.fingerprint,'V4_REVIEW_FILE_CHANGED')
        versions=self.api('get','/api/2.0/clusters/spark-versions')
        require(isinstance(versions,dict) and isinstance(versions.get('versions'),list) and any(isinstance(v,dict) and v.get('key')=='15.4.x-scala2.12' for v in versions['versions']),'V4_DBR_15_4_UNAVAILABLE')
        nodes=self.api('get','/api/2.0/clusters/list-node-types')
        return read_evidence(self.read,nodes)

    def plan(self,root,stage,variables,*,before_apply=None,policy_only=False):
        require(root==UC_WORKSPACE and stage=='uc-workspace' and variables.get('node_type_id')==PRIMARY,'V4_WORKSPACE_ONLY')
        self.current_uc=self.outputs(UC_WORKSPACE)['deployment']['value']
        uc=self.current_uc
        require(uc['provisioning_mode']=='workspace' and uc['runner_application_id']==self.c['runner_application_id'],'V4_EXISTING_UC_REQUIRED')
        record=self.work/'job-id.json'
        require(record.is_file(),'V4_EXISTING_JOB_RECORD_REQUIRED')
        owned=json.loads(record.read_text(encoding='utf-8'))
        require(str(owned.get('job_id'))==JOB_ID and owned.get('host')==variables['workspace_host'],'V4_EXISTING_JOB_RECORD_MISMATCH')
        # DAB state must already own this same Job; never adopt or create.
        summary=command(['databricks','bundle','summary','--output','json'],cwd=self.work/'bundle',env=self.ws_env,json_output=True,timeout=90)
        require(str(summary['resources']['jobs']['silver'].get('id'))==JOB_ID,'V4_BUNDLE_JOB_ID_MISMATCH')
        self.workspace={'workspace_host':variables['workspace_host'],'workspace_id':variables['workspace_id'],
                        'workspace_resource_id':variables['workspace_resource_id']}
        self.bundle_file=self.work/'bundle/databricks.yml'
        require(self.bundle_file.is_file(),'V4_EXISTING_BUNDLE_FILE_REQUIRED')
        self.bundle_sha=sha(self.bundle_file)
        self.before=self.snapshot()
        self.target=target_clusters(self.before,uc)
        self.transition={'job_id':JOB_ID,'workspace_host':variables['workspace_host'],'policy_id':uc['policy_id'],
                         'before_settings':self.before,'after_job_clusters':self.target,'bundle_yaml_sha256':self.bundle_sha}
        self.fingerprint=canonical_sha(self.transition)
        self.review_path=self.work/('v4-job-transition-'+self.fingerprint+'.json')
        self.review_path.write_text(json.dumps(self.transition,indent=2),encoding='utf-8')
        print(json.dumps({'status':'V4_JOB_TRANSITION_REVIEW','sha256':self.fingerprint,'review_file':str(self.review_path),
                          'job_id':JOB_ID,'execution':'NOT_STARTED'}))
        print(json.dumps(self.recheck(),sort_keys=True))
        return super().plan(root,stage,variables,before_apply=self.recheck,policy_only=True)

    def update_job(self,workspace,uc):
        self.recheck()
        policy=self.api('get','/api/2.0/policies/clusters/get?policy_id='+uc['policy_id'])
        validate_policy(policy,uc['runner_application_id'])
        policy_sha=canonical_sha(policy)
        if self.before['job_clusters']!=self.target:
            approval('UPDATE-SILVER-V4-JOB',self.fingerprint)
            require(canonical_sha(json.loads(self.review_path.read_text(encoding='utf-8')))==self.fingerprint,'V4_REVIEW_FILE_CHANGED')
            self.recheck()
            current=self.api('get','/api/2.0/policies/clusters/get?policy_id='+uc['policy_id'])
            require(canonical_sha(current)==policy_sha,'V4_POLICY_CHANGED_AFTER_REVIEW')
            payload={'job_id':int(JOB_ID),'new_settings':{'job_clusters':self.target}}
            command(['databricks','api','post','/api/2.1/jobs/update','--json',json.dumps(payload)],env=self.ws_env,json_output=True,timeout=90)
        after=self.snapshot();expected=copy.deepcopy(self.before);expected['job_clusters']=self.target
        require(after==expected,'V4_POST_UPDATE_SETTINGS_MISMATCH')
        require(sha(self.bundle_file)==self.bundle_sha,'V4_BUNDLE_CHANGED_AFTER_REVIEW')
        bundle=yaml.safe_load(self.bundle_file.read_text(encoding='utf-8'))
        bundle['resources']['jobs']['silver']['job_clusters']=copy.deepcopy(self.target)
        self.bundle_file.write_text(yaml.safe_dump(bundle,sort_keys=False),encoding='utf-8')
        self.bundle_sha=sha(self.bundle_file)
        print(json.dumps({'status':'V4_JOB_CONFIGURATION_VERIFIED','job_id':JOB_ID,'execution':'NOT_STARTED',
                          'permissions':'UNCHANGED','identities':'UNCHANGED','physical_capacity':'NOT_VERIFIED'}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='work/silver-dev.json')
    parser.add_argument('--apply',action='store_true',help='Apply only approved saved policy plan and exact existing Job transition; never run a Job')
    args=parser.parse_args()
    try:
        path=Path(args.config).resolve()
        require(path.is_relative_to(ROOT/'work') and path.suffix=='.json' and path.is_file(),'V4_EXISTING_PRIVATE_CONFIG_REQUIRED')
        config_sha=sha(path);original=json.loads(path.read_text(encoding='utf-8'));config=copy.deepcopy(original)
        require(config.get('uc_mode')=='workspace' and not config.get('allow_create_metastore',False),'V4_WORKSPACE_MODE_REQUIRED')
        config['node_type_id']=PRIMARY
        launch=MigrationLauncher(config,args.apply,stage='silver');launch.deploy_only=False
        result=launch.provision()
        if args.apply:
            require(result is not None,'V4_POLICY_NOT_READY')
            require(sha(path)==config_sha,'V4_CONFIG_CHANGED_AFTER_REVIEW')
            launch.update_job(*result)
            require(sha(path)==config_sha,'V4_CONFIG_CHANGED_AFTER_REVIEW')
            original['node_type_id']=PRIMARY
            path.write_text(json.dumps(original,indent=2),encoding='utf-8')
        else:print(json.dumps({'status':'V4_PLAN_ONLY','job_update':'NOT_APPLIED','execution':'NOT_STARTED'}))
        return 0
    except Exception as exc:
        code=str(exc) if isinstance(exc,Blocked) else 'V4_LOCAL_OR_READ_FAILURE'
        print(json.dumps({'status':'BLOCKED','code':code,'error_type':type(exc).__name__}),file=sys.stderr)
        return 1

if __name__=='__main__':raise SystemExit(main())

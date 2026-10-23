"""Adapt PR #18 in an ignored staging directory; never edit its source bundle."""
import copy
from plan_guard import require


def ephemeral_bundle(original,uc,host):
    value=copy.deepcopy(original)
    require(value.get('bundle',{}).get('name')=='adventureworks-silver','SILVER_BUNDLE_CONTRACT_CHANGED')
    job=value['resources']['jobs']['silver']
    require(len(job['tasks'])==1 and job['tasks'][0]['task_key']=='transform_and_verify','SILVER_JOB_CONTRACT_CHANGED')
    task=job['tasks'][0]
    require('existing_cluster_id' in task,'SILVER_COMPUTE_CONTRACT_CHANGED')
    task.pop('existing_cluster_id');value['variables'].pop('existing_cluster_id')
    task['job_cluster_key']='silver_ephemeral'
    for trigger in ('schedule','continuous','trigger'):job.pop(trigger,None)
    task['max_retries']=0
    job['max_concurrent_runs']=1
    job['timeout_seconds']=7200
    job['run_as']={'service_principal_name':uc['runner_application_id']}
    job['permissions']=[{'user_name':uc['operator'],'level':'CAN_MANAGE'}]
    job['job_clusters']=[{'job_cluster_key':'silver_ephemeral','new_cluster':{
      'policy_id':uc['policy_id'],'apply_policy_default_values':True,
      'spark_version':'15.4.x-scala2.12','node_type_id':uc['node_type_id'],'num_workers':0,
      'data_security_mode':'SINGLE_USER','single_user_name':uc['runner_application_id'],
      'spark_conf':{'spark.databricks.cluster.profile':'singleNode','spark.master':'local[*]'},
      'custom_tags':{'ResourceClass':'SingleNode','project':'admm','layer':'silver'},
      'azure_attributes':{'availability':'ON_DEMAND_AZURE'}}}]
    value['workspace']={'host':host,'root_path':'/Workspace/Shared/'+uc.get('catalog','admm_dev')+'/admm-silver/${bundle.target}'}
    # This staging copy has a separate local identity but stable deployment name.
    value['bundle']['name']='adventureworks-silver-infrastructure'
    value['artifacts']['silver_wheel']['dynamic_version']=True
    return value

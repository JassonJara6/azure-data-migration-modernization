"""Exact policy/Job transition, no allocation or identity/permission mutation."""
import copy
import hashlib
import json
from plan_guard import require,review
from single_node_v4 import PRIMARY,NO_FALLBACK,validate_cluster,validate_policy

JOB_ID='210884055971910'
OLD='Standard_D4ds_v5'


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def target_clusters(settings,uc):
    require(isinstance(settings,dict),'V4_JOB_SETTINGS_INVALID')
    require(settings.get('run_as',{}).get('service_principal_name')==uc['runner_application_id'] and
            settings.get('max_concurrent_runs')==1 and settings.get('timeout_seconds')==7200 and
            not any(settings.get(k) for k in ('schedule','continuous','trigger')),'V4_JOB_EXECUTION_GUARDS_MISMATCH')
    tasks=settings.get('tasks')
    require(isinstance(tasks,list) and len(tasks)==1 and tasks[0].get('task_key')=='transform_and_verify' and
            tasks[0].get('job_cluster_key')=='silver_ephemeral' and tasks[0].get('max_retries',0)==0 and
            not tasks[0].get('existing_cluster_id'),'V4_JOB_TASK_MISMATCH')
    clusters=settings.get('job_clusters')
    require(isinstance(clusters,list) and len(clusters)==1 and clusters[0].get('job_cluster_key')=='silver_ephemeral','V4_JOB_CLUSTER_MISMATCH')
    cluster=clusters[0].get('new_cluster')
    require(isinstance(cluster,dict) and cluster.get('policy_id')==uc['policy_id'] and
            cluster.get('single_user_name')==uc['runner_application_id'],'V4_JOB_POLICY_OR_RUNNER_MISMATCH')
    if cluster.get('node_type_id')==PRIMARY:
        validate_cluster(cluster)
        return copy.deepcopy(clusters)
    require(cluster.get('node_type_id')==OLD and cluster.get('driver_node_type_id',OLD)==OLD,'V4_ONLY_EXACT_V5_TRANSITION_SUPPORTED')
    require(cluster.get('driver_node_type_flexibility')=={'alternate_node_type_ids':[PRIMARY]} and
            cluster.get('worker_node_type_flexibility',NO_FALLBACK)==NO_FALLBACK,'V4_OLD_FALLBACK_MISMATCH')
    target=copy.deepcopy(clusters);updated=target[0]['new_cluster']
    updated.update(node_type_id=PRIMARY,driver_node_type_id=PRIMARY,
                   driver_node_type_flexibility=copy.deepcopy(NO_FALLBACK),worker_node_type_flexibility=copy.deepcopy(NO_FALLBACK))
    validate_cluster(updated)
    # Cluster attributes outside the four explicit node/flexibility keys stay byte-for-byte equivalent.
    return target


def review_policy_plan(plan,stage):
    require(stage=='uc-workspace','V4_WORKSPACE_POLICY_PLAN_ONLY')
    changes=review(plan,stage)
    for r in plan['resource_changes']:
        if r.get('mode')=='data' or r['change']['actions']==['no-op']:continue
        require(r['address']=='databricks_cluster_policy.silver[0]' and r['change']['actions']==['update'],'V4_ONLY_EXISTING_POLICY_UPDATE_ALLOWED')
        before=copy.deepcopy(r['change'].get('before'));after=copy.deepcopy(r['change'].get('after'))
        require(isinstance(before,dict) and isinstance(after,dict) and before.get('id') and before['id']==after.get('id'),'V4_POLICY_ID_CHANGED')
        bd=before.pop('definition',None);ad=after.pop('definition',None)
        require(before==after,'V4_UNEXPECTED_POLICY_ATTRIBUTE_CHANGE')
        bd=json.loads(bd) if isinstance(bd,str) else bd
        ad=json.loads(ad) if isinstance(ad,str) else ad
        require(isinstance(bd,dict) and isinstance(ad,dict),'V4_POLICY_DEFINITION_INVALID')
        require(bd.get('node_type_id',{}).get('value') in (OLD,PRIMARY),'V4_POLICY_PRIMARY_UNEXPECTED')
        expected=copy.deepcopy(bd)
        expected.update({'node_type_id':{'type':'fixed','value':PRIMARY},'driver_node_type_id':{'type':'fixed','value':PRIMARY},
            'driver_node_type_flexibility.alternate_node_type_ids':{'type':'fixed','value':''},
            'worker_node_type_flexibility.alternate_node_type_ids':{'type':'fixed','value':''}})
        require(ad==expected,'V4_UNEXPECTED_POLICY_RULE_CHANGE')
        validate_policy({'definition':ad})
    for name,change in plan.get('output_changes',{}).items():
        if change.get('actions')==['no-op']:continue
        require(name=='deployment' and isinstance(change.get('before'),dict) and isinstance(change.get('after'),dict),'V4_UNEXPECTED_OUTPUT_CHANGE')
        before=copy.deepcopy(change['before']);after=copy.deepcopy(change['after'])
        require(before.pop('node_type_id',None) in (OLD,PRIMARY) and after.pop('node_type_id',None)==PRIMARY and before==after,'V4_UNEXPECTED_OUTPUT_CHANGE')
    return changes

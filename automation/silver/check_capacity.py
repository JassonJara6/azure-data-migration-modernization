"""Read-only shared Silver preflight. Never deploy, export receipts or start compute."""
import argparse
import json
import re
import sys
from capacity_reads import read_scoped_skus
from single_node_v4 import PRIMARY,read_evidence,validate_policy,validate_cluster
from compute_capacity import select_alternates
from diagnose import read_json,SUBSCRIPTION,TENANT
from plan_guard import Blocked,require


def inspect_capacity(profile,job_id,reader=None,*,progress=None):
    def read(argv, stage, timeout=90):
        if argv[0]=='databricks':argv=[*argv,'--profile',profile]
        # Keep injected test readers backward compatible. Only the real CLI
        # reader receives the per-step timeout.
        value,error=(read_json(argv,timeout=timeout) if reader is None else reader(argv))
        if error is not None:
            code=error.get('code','READ_FAILED') if isinstance(error,dict) else 'READ_FAILED'
            # Only codes from our reader allowlist may reach output.
            allowed={'READ_TIMEOUT','INVALID_JSON_RESPONSE','CLI_OUTPUT_NOT_UTF8','TOOL_MISSING','TOOL_START_FAILED','ACCESS_DENIED','AUTHENTICATION_REQUIRED','AZURE_LOGIN_REQUIRED','CONNECTIVITY_FAILED','DNS_FAILED','TLS_TRUST_FAILED','ARM_AUTHORIZATION_DENIED','READ_FAILED'}
            require(False,(code if isinstance(code,str) and code in allowed else 'READ_FAILED')+'_AT_'+stage)
        require(isinstance(value,(dict,list)),'INVALID_RESPONSE_SHAPE_AT_'+stage)
        return value
    azure=read(['az','account','show','--output','json'],'AZURE_ACCOUNT')
    require(isinstance(azure,dict) and isinstance(azure.get('id'),str) and isinstance(azure.get('tenantId'),str),'INVALID_AZURE_ACCOUNT_RESPONSE')
    require(azure.get('id','').lower()==SUBSCRIPTION,'AZURE_SUBSCRIPTION_MISMATCH')
    require(azure.get('tenantId','').lower()==TENANT,'AZURE_TENANT_MISMATCH')
    job=read(['databricks','jobs','get',str(job_id),'--output','json'],'DATABRICKS_JOB')
    require(isinstance(job,dict) and isinstance(job.get('settings'),dict),'INVALID_DATABRICKS_JOB_RESPONSE')
    clusters=job['settings'].get('job_clusters')
    require(isinstance(clusters,list) and all(isinstance(c,dict) for c in clusters),'INVALID_DATABRICKS_JOB_CLUSTERS')
    require(len(clusters)==1,'SILVER_JOB_CLUSTER_CONTRACT_CHANGED')
    cluster=clusters[0].get('new_cluster',{})
    require(isinstance(cluster,dict) and isinstance(cluster.get('azure_attributes'),dict),'INVALID_DATABRICKS_CLUSTER_RESPONSE')
    require(cluster.get('spark_version')=='15.4.x-scala2.12' and cluster.get('num_workers')==0 and
        cluster.get('data_security_mode')=='SINGLE_USER' and cluster.get('azure_attributes',{}).get('availability')=='ON_DEMAND_AZURE','SILVER_COMPUTE_CONSTRAINTS_CHANGED')
    policy_id=cluster.get('policy_id','')
    require(isinstance(policy_id,str) and re.fullmatch(r'[A-Za-z0-9_-]+',policy_id),'INVALID_POLICY_ID')
    nodes=read(['databricks','api','get','/api/2.0/clusters/list-node-types'],'WORKSPACE_NODE_TYPES')
    if cluster.get('node_type_id')==PRIMARY:
        validate_cluster(cluster)
        report=read_evidence(read,nodes)
        policy=read(['databricks','api','get','/api/2.0/policies/clusters/get?policy_id='+policy_id],'COMPUTE_POLICY')
        validate_policy(policy,cluster.get('single_user_name'))
        return report
    schema=read(['databricks','bundle','schema'],'BUNDLE_SCHEMA')
    skus=read_scoped_skus(read,nodes,progress=progress)
    usages=read(['az','vm','list-usage','--subscription',SUBSCRIPTION,'--location','eastus','--output','json'],'AZURE_QUOTAS',120)
    require(isinstance(usages,list),'INVALID_AZURE_QUOTAS_RESPONSE')
    policy=read(['databricks','api','get','/api/2.0/policies/clusters/get?policy_id='+policy_id],'COMPUTE_POLICY')
    reports=[]
    try:select_alternates(cluster.get('node_type_id'),nodes,skus,policy,schema,usages,reporter=reports.append)
    except Blocked:pass # Report contains only static codes and numeric quota snapshots.
    return reports[0]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',default='admm-workspace')
    parser.add_argument('--job-id',type=int,default=210884055971910)
    args=parser.parse_args()
    try:
        report=inspect_capacity(args.profile,args.job_id,progress=lambda event:print(json.dumps(event),file=sys.stderr,flush=True))
        print(json.dumps(report,sort_keys=True))
        return 0 if report['status']=='ELIGIBLE_SNAPSHOT' else 1
    except Exception as exc:
        print(json.dumps({'status':'BLOCKED','code':str(exc) if isinstance(exc,Blocked) else 'READ_ONLY_DIAGNOSTIC_FAILED','error_type':type(exc).__name__}))
        return 1

if __name__=='__main__':raise SystemExit(main())

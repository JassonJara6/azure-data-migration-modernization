"""Read-only shared Silver preflight. Never deploy, export receipts or start compute."""
import argparse
import json
import re
from compute_capacity import select_alternates
from diagnose import read_json,SUBSCRIPTION,TENANT
from plan_guard import Blocked,require


def inspect_capacity(profile,job_id,reader=read_json):
    def read(argv):
        if argv[0]=='databricks':argv=[*argv,'--profile',profile]
        value,error=reader(argv)
        require(error is None,(error or {}).get('code','READ_FAILED'))
        return value
    azure=read(['az','account','show','--output','json'])
    require(azure.get('id','').lower()==SUBSCRIPTION,'AZURE_SUBSCRIPTION_MISMATCH')
    require(azure.get('tenantId','').lower()==TENANT,'AZURE_TENANT_MISMATCH')
    job=read(['databricks','jobs','get',str(job_id),'--output','json'])
    clusters=job.get('settings',{}).get('job_clusters',[])
    require(len(clusters)==1,'SILVER_JOB_CLUSTER_CONTRACT_CHANGED')
    cluster=clusters[0].get('new_cluster',{})
    require(cluster.get('spark_version')=='15.4.x-scala2.12' and cluster.get('num_workers')==0 and
        cluster.get('data_security_mode')=='SINGLE_USER' and cluster.get('azure_attributes',{}).get('availability')=='ON_DEMAND_AZURE','SILVER_COMPUTE_CONSTRAINTS_CHANGED')
    policy_id=cluster.get('policy_id','')
    require(isinstance(policy_id,str) and re.fullmatch(r'[A-Za-z0-9_-]+',policy_id),'INVALID_POLICY_ID')
    nodes=read(['databricks','api','get','/api/2.0/clusters/list-node-types'])
    schema=read(['databricks','bundle','schema'])
    skus=read(['az','vm','list-skus','--subscription',SUBSCRIPTION,'--location','eastus','--resource-type','virtualMachines','--all','--output','json'])
    usages=read(['az','vm','list-usage','--subscription',SUBSCRIPTION,'--location','eastus','--output','json'])
    require(isinstance(usages,list),'INVALID_AZURE_QUOTAS_RESPONSE')
    policy=read(['databricks','api','get','/api/2.0/policies/clusters/get?policy_id='+policy_id])
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
        report=inspect_capacity(args.profile,args.job_id)
        print(json.dumps(report,sort_keys=True))
        return 0 if report['status']=='ELIGIBLE_SNAPSHOT' else 1
    except Exception as exc:
        print(json.dumps({'status':'BLOCKED','code':str(exc) if isinstance(exc,Blocked) else 'READ_ONLY_DIAGNOSTIC_FAILED','error_type':type(exc).__name__}))
        return 1

if __name__=='__main__':raise SystemExit(main())

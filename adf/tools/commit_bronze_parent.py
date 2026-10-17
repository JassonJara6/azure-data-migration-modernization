"""Verify the stopped ADF parent, attest eleven runs, then commit atomically.

No Azure resources, pipeline publishing or triggers are created by this CLI.
Invoke only after a separately approved ADF execution. It reads ADF management
state, reads ADLS, and writes SQL through the trusted EXECUTE-only procedures.
"""
from __future__ import annotations
import argparse
import json
import sys
from datetime import timedelta
from commit_reconciled_bronze import SqlStore,attest_run,connections,run_id,verify_unchanged


def verify_adf_parent(adf,group,factory,parent,children):
    run=adf.pipeline_runs.get(group,factory,parent)
    if run.pipeline_name!='pl_adventureworks_bronze' or run.status!='Failed' or not run.run_end:
        raise ValueError('ADF parent is not terminal at the confirmation block')
    from azure.mgmt.datafactory.models import RunFilterParameters
    token=None;activities=[]
    while True:
        response=adf.activity_runs.query_by_pipeline_run(group,factory,parent,RunFilterParameters(
          last_updated_after=run.run_start-timedelta(minutes=5),last_updated_before=run.run_end+timedelta(minutes=5),continuation_token=token))
        activities.extend(response.value);token=response.continuation_token
        if not token:break
    failed=[a for a in activities if a.status=='Failed']
    if len(failed)!=1 or failed[0].activity_name!='WatermarkConfirmationBlocked' or (failed[0].error or {}).get('errorCode')!='WATERMARK_CONFIRMATION_BLOCKED':
        raise ValueError('ADF failed for another reason')
    required={'AcknowledgeLandingOnly','ReadActiveConfigurations','ValidateActiveConfigurations','ForEachSource'}
    if not required.issubset({a.activity_name for a in activities if a.status=='Succeeded'}):raise ValueError('ADF parent success gates missing')
    if any(a.status not in ('Succeeded','Failed') for a in activities):raise ValueError('Incomplete or cancelled ADF activity')
    executed=[a for a in activities if a.activity_name=='LandSource' and a.activity_type=='ExecutePipeline' and a.status=='Succeeded']
    ids=[(a.output or {}).get('pipelineRunId') for a in executed]
    if len(ids)!=11 or len(set(ids))!=11 or set(ids)!=set(children):raise ValueError('ADF children differ from SQL parent receipts')


def commit_parent(store,client,account,filesystem,parent,verify_parent):
    run_id(parent)
    receipt=store.parent_receipt(parent)
    if receipt:return dict(receipt,status='COMMITTED',replayed=True)
    configs=store.active_configs();runs=store.parent_runs(parent)
    if len(configs)!=11 or sum(c['load_type']=='INCREMENTAL' for c in configs)!=5 or sum(c['load_type']=='FULL' for c in configs)!=6:
        raise ValueError('Expected eleven active sources, five incremental and six full')
    if len(runs)!=11 or len({r['child_run_id'] for r in runs})!=11 or {r['config_id'] for r in runs}!={c['config_id'] for c in configs}:
        raise ValueError('Parent has missing, duplicate or unexpected source receipts')
    if any(r['status'] not in ('COPIED','RECONCILED') for r in runs):
        raise ValueError('Parent has partial or invalid prior confirmation without a parent receipt')
    verify_parent(parent,[r['child_run_id'] for r in runs])
    plans=[attest_run(store,client,account,filesystem,r['child_run_id']) for r in runs]
    verify_unchanged(client,plans)
    # All attestations must pass before the sole mutating watermark operation.
    store.commit_parent(parent)
    receipt=store.parent_receipt(parent)
    if not receipt or receipt['committed_children']!=11:raise RuntimeError('Missing durable parent commit receipt')
    return dict(receipt,status='COMMITTED',replayed=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['account','parent-run-id','subscription','resource-group','factory']:parser.add_argument('--'+name,required=True)
    parser.add_argument('--filesystem',default='bronze');args=parser.parse_args()
    sql=service=adf=None
    try:
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient
        sql,service=connections(args.account,args.filesystem)
        adf=DataFactoryManagementClient(DefaultAzureCredential(),args.subscription)
        result=commit_parent(SqlStore(sql),service.get_file_system_client(args.filesystem),args.account,args.filesystem,args.parent_run_id,
          lambda parent,children:verify_adf_parent(adf,args.resource_group,args.factory,parent,children))
        print(json.dumps(result,sort_keys=True));return 0
    except Exception as exc:
        print(json.dumps({'status':'FAILED','error_type':type(exc).__name__}),file=sys.stderr);return 1
    finally:
        if sql:sql.close()
        if service:service.close()
        if adf:adf.close()

if __name__=='__main__':sys.exit(main())

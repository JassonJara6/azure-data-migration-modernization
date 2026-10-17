"""Verify the stopped ADF parent, attest eleven runs, then commit atomically.

No Azure resources, pipeline publishing or triggers are created by this CLI.
Invoke only after a separately approved ADF execution. It reads ADF management
state, reads ADLS, and writes SQL through the trusted EXECUTE-only procedures.
"""
from __future__ import annotations
import argparse
import json
import sys
import os
import logging
import re
import shutil
import subprocess
from datetime import timedelta
from commit_reconciled_bronze import SqlStore,attest_run,connections,run_id,verify_unchanged
from bronze_diagnostics import step, report_error, close_resources
from bronze_connection import configured_connection


def verify_adf_parent(adf,group,factory,parent,children):
    run=adf.pipeline_runs.get(group,factory,parent)
    if run.pipeline_name!='pl_adventureworks_bronze' or run.status!='Failed' or not run.run_end:
        raise ValueError('ADF parent is not terminal at the confirmation block')
    from azure.mgmt.datafactory.models import RunFilterParameters
    token=None;activities=[];seen_tokens=set()
    while True:
        response=adf.activity_runs.query_by_pipeline_run(group,factory,parent,RunFilterParameters(
          last_updated_after=run.run_start-timedelta(minutes=5),last_updated_before=run.run_end+timedelta(minutes=5),continuation_token=token))
        activities.extend(response.value);token=response.continuation_token
        if not token:break
        if token in seen_tokens:raise ValueError('Repeated ADF activity continuation token')
        seen_tokens.add(token)
    failed=[a for a in activities if a.status=='Failed']
    if len(failed)!=1 or failed[0].activity_name!='WatermarkConfirmationBlocked' or (failed[0].error or {}).get('errorCode')!='WATERMARK_CONFIRMATION_BLOCKED':
        raise ValueError('ADF failed for another reason')
    required={'AcknowledgeLandingOnly','ReadActiveConfigurations','ValidateActiveConfigurations','ForEachSource'}
    if not required.issubset({a.activity_name for a in activities if a.status=='Succeeded'}):raise ValueError('ADF parent success gates missing')
    if any(a.status not in ('Succeeded','Failed') for a in activities):raise ValueError('Incomplete or cancelled ADF activity')
    executed=[a for a in activities if a.activity_name=='LandSource' and a.activity_type=='ExecutePipeline' and a.status=='Succeeded']
    ids=[(a.output or {}).get('pipelineRunId') for a in executed]
    if len(ids)!=11 or len(set(ids))!=11 or set(ids)!=set(children):raise ValueError('ADF children differ from SQL parent receipts')


def validate_receipt(receipt,parent):
    if not receipt or receipt.get('parent_run_id')!=parent or type(receipt.get('committed_children')) is not int or receipt['committed_children']!=11 or not re.fullmatch(r'[0-9a-f]{64}',str(receipt.get('manifest_sha256',''))):
        raise ValueError('Invalid durable parent receipt')
    return receipt


def commit_parent(store,client,account,filesystem,parent,verify_parent):
    run_id(parent)
    with step('sql_read_parent_receipt',parent_run_id=parent):receipt=store.parent_receipt(parent)
    if receipt:
        with step('validate_parent_receipt'):validate_receipt(receipt,parent)
        return dict(receipt,status='COMMITTED',replayed=True)
    with step('validate_parent_contract',parent_run_id=parent):
        configs=store.active_configs();runs=store.parent_runs(parent)
        if len(configs)!=11 or sum(c['load_type']=='INCREMENTAL' for c in configs)!=5 or sum(c['load_type']=='FULL' for c in configs)!=6:
            raise ValueError('Expected eleven active sources, five incremental and six full')
        if len(runs)!=11 or len({r['child_run_id'] for r in runs})!=11 or {r['config_id'] for r in runs}!={c['config_id'] for c in configs}:
            raise ValueError('Parent has missing, duplicate or unexpected source receipts')
        if any(r['status'] not in ('COPIED','RECONCILED') for r in runs):
            raise ValueError('Parent has partial or invalid prior confirmation without a parent receipt')
    with step('azure_verify_parent',parent_run_id=parent):verify_parent(parent,[r['child_run_id'] for r in runs])
    plans=[]
    for r in runs:
        with step('reconcile_child',child_run_id=r['child_run_id']):
            plans.append(attest_run(store,client,account,filesystem,r['child_run_id']))
    with step('adls_verify_immutability'):verify_unchanged(client,plans)
    # Drain all ODBC results and check a durable receipt. An interrupted commit
    # is UNKNOWN, never implicitly failed/succeeded; retry this same parent.
    with step('sql_commit_parent'):store.commit_parent(parent)
    with step('sql_verify_parent_receipt'):
        receipt=validate_receipt(store.parent_receipt(parent),parent)
    return dict(receipt,status='COMMITTED',replayed=False)


def discover_subscription(interactive):
    value=os.environ.get('AZURE_SUBSCRIPTION_ID')
    if not value and interactive:
        az=shutil.which('az')
        if az:
            try:
                result=subprocess.run([az,'account','show','--query','id','--output','tsv'],capture_output=True,text=True,timeout=30)
                if result.returncode==0:value=result.stdout.strip()
            except (OSError,subprocess.TimeoutExpired):pass
        if not value:value=input('ID de suscripción Azure (no contraseña): ').strip()
    if not value:raise ValueError('Azure subscription is required')
    return run_id(value)


def discover_factory(adf,group,parent,interactive):
    value=os.environ.get('ADF_FACTORY') or os.environ.get('BRONZE_ADF_FACTORY')
    if value:return value
    if not interactive:raise ValueError('ADF factory is required')
    try:
        matches=[]
        for factory in adf.factories.list_by_resource_group(group):
            try:adf.pipeline_runs.get(group,factory.name,parent)
            except Exception as exc:
                if getattr(exc,'status_code',None)==404:continue
                raise
            matches.append(factory.name)
        if len(matches)==1:return matches[0]
    except Exception as exc:
        # A factory-scoped identity may read a known factory but not list the RG.
        if getattr(exc,'status_code',None)!=403:raise
    value=input('Nombre de la fábrica ADF existente: ').strip()
    if not value:raise ValueError('ADF factory is required')
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['account','parent-run-id','resource-group']:parser.add_argument('--'+name,required=True)
    parser.add_argument('--subscription');parser.add_argument('--factory')
    parser.add_argument('--filesystem',default='bronze')
    parser.add_argument('--interactive',action='store_true',help='Prompt SQL password securely; resolve/prompt nonsecret Azure context')
    parser.add_argument('--sql-server',default='127.0.0.1,1433')
    parser.add_argument('--sql-user',default='bronze_reconciler')
    parser.add_argument('--trust-local-sql-certificate',action='store_true',help='Encrypted loopback SQL only; does not affect Azure TLS')
    args=parser.parse_args()
    sql=service=adf=credential=None
    # Identity warnings may include environment/account details. CLI diagnostics
    # are intentionally structured; never enable raw Azure HTTP/ODBC traces here.
    logging.getLogger('azure.identity').setLevel(logging.CRITICAL)
    try:
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient
        with step('configuration'):
            run_id(args.parent_run_id)
            connection=configured_connection(interactive=args.interactive,server=args.sql_server,user=args.sql_user,
                                             trust_local_certificate=args.trust_local_sql_certificate)
        credential=DefaultAzureCredential(exclude_interactive_browser_credential=not args.interactive)
        sql,service=connections(args.account,args.filesystem,connection_string=connection,credential=credential)
        store=SqlStore(sql)
        # Historical retries need only the durable SQL receipt, not Azure auth.
        with step('sql_read_parent_receipt'):receipt=store.parent_receipt(args.parent_run_id)
        if receipt:
            with step('validate_parent_receipt'):validate_receipt(receipt,args.parent_run_id)
            result=dict(receipt,status='COMMITTED',replayed=True)
        else:
            with step('azure_context'):
                subscription=args.subscription or discover_subscription(args.interactive)
                adf=DataFactoryManagementClient(credential,subscription)
                factory=args.factory or discover_factory(adf,args.resource_group,args.parent_run_id,args.interactive)
            result=commit_parent(store,service.get_file_system_client(args.filesystem),args.account,args.filesystem,args.parent_run_id,
              lambda parent,children:verify_adf_parent(adf,args.resource_group,factory,parent,children))
        print(json.dumps(result,sort_keys=True));return 0
    except Exception as exc:
        print(json.dumps(report_error(exc),ensure_ascii=True),file=sys.stderr);return 1
    finally:close_resources(adf,service,sql,credential)

if __name__=='__main__':sys.exit(main())

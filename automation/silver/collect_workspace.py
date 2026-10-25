"""Sanitized existing metadata collection. Never print receipt/task error bodies."""
import argparse
import json
from pathlib import Path
import re
import sys
from check_capacity import inspect_capacity
from diagnose import read_json
from plan_guard import Blocked
from wheel_artifact import source_version
from runtime_report import safe_task_failure

ROOT=Path(__file__).resolve().parents[2]
PARENT='20d65511-8190-41a0-8ad2-23d7568363d2'
RECEIPT='/Volumes/admm_dev/inputs/receipts/'+PARENT+'.json'


def collect(profile='admm-workspace',reader=None):
    cache={}
    def read(argv):
        key=tuple(argv)
        if key not in cache:cache[key]=read_json(argv,timeout=120 if argv[:3]==['az','vm','list-usage'] else 90) if reader is None else reader(argv)
        return cache[key]
    def db(*argv):return read(['databricks',*argv,'--profile',profile])
    expected='admm_silver-'+source_version(ROOT/'databricks/silver')+'-py3-none-any.whl'
    report={'status':'READ_ONLY_COLLECTION','expected_wheel':expected,'receipt_identity':'OPERATOR_PROFILE_NOT_JOB_RUNNER'}
    job,error=db('jobs','get','210884055971910','--output','json')
    if error or not isinstance(job,dict):report['job']='READ_FAILED'
    else:
        settings=job.get('settings')
        tasks=settings.get('tasks',[]) if isinstance(settings,dict) else []
        libraries=tasks[0].get('libraries',[]) if isinstance(tasks,list) and len(tasks)==1 and isinstance(tasks[0],dict) else []
        wheels=[Path(i['whl']).name for i in libraries if isinstance(i,dict) and isinstance(i.get('whl'),str)] if isinstance(libraries,list) else []
        report['wheel_version_matches_repository']=wheels==[expected]
        defaults={p.get('name'):p.get('default') for p in settings.get('parameters',[]) if isinstance(p,dict) and isinstance(p.get('name'),str)} if isinstance(settings,dict) and isinstance(settings.get('parameters'),list) else {}
        report['job_parent_matches_expected']=defaults.get('parent_run_id')==PARENT
        report['job_receipt_matches_expected']=defaults.get('receipt_file')==RECEIPT
        report['observed_wheels']=[n if re.fullmatch(r'admm_silver-0\.1\.0\+[0-9a-f]{12}-py3-none-any\.whl',n) else 'UNRECOGNIZED' for n in wheels]
    nodes,error=db('api','get','/api/2.0/clusters/list-node-types')
    from capacity_reads import APPROVED_SKUS
    report['provider_metadata']=[]
    for name in APPROVED_SKUS:
        matches=[n for n in nodes.get('node_types',[]) if isinstance(n,dict) and n.get('node_type_id')==name] if isinstance(nodes,dict) and isinstance(nodes.get('node_types'),list) else []
        item={'node_type':name,'matches':len(matches)}
        if len(matches)==1:
            n=matches[0];info=n.get('node_info')
            item['provider_info_present']=isinstance(info,dict)
            item['status_present']=isinstance(info,dict) and isinstance(info.get('status'),list)
            if item['status_present']:
                item['statuses']=[v if v in ('NotEnabledOnSubscription','NotAvailableInRegion') else 'UNKNOWN' for v in info['status']]
            for key in ('num_cores','memory_mb'):
                item[key]=n.get(key) if type(n.get(key)) in (int,float) else 'INVALID_OR_MISSING'
        report['provider_metadata'].append(item)
    try:report['capacity']=inspect_capacity(profile,210884055971910,read)
    except Exception as exc:report['capacity']={'status':'BLOCKED','code':str(exc) if isinstance(exc,Blocked) else 'READ_FAILED'}
    receipt,error=db('fs','cat','dbfs:'+RECEIPT)
    if error:report['receipt']='READ_FAILED'
    else:
        sys.path.insert(0,str(ROOT/'databricks/silver/src'))
        from admm_silver.contracts import validate_bundle,SilverError
        try:
            validate_bundle(receipt,'stadmmdevdlf6d99ba4')
            report['receipt']='CONTRACT_VALID' if receipt['parent_run_id']==PARENT else 'PARENT_MISMATCH'
        except Exception as exc:
            report['receipt']='CONTRACT_INVALID'
            report['receipt_error_type']=type(exc).__name__
            if isinstance(exc,SilverError) and re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',str(exc)):report['receipt_code']=str(exc)
    output,error=db('jobs','get-run-output','831744328908969','--output','json')
    report['old_task']='READ_FAILED' if error or not isinstance(output,dict) else 'ORIGINAL_EXCEPTION_NOT_RECOVERABLE_FROM_GENERIC_ERROR'
    if not error and isinstance(output,dict):
        report['old_task_diagnostic']=safe_task_failure(output)
        # Never print error/error_trace/notebook_output: they may contain rows/secrets.
        text=output.get('error','')
        if not isinstance(text,str):text=''
        match=re.search(r'SILVER_(RECEIPT_FILE_NOT_FOUND|RECEIPT_FILE_ACCESS_DENIED|RECEIPT_INVALID_UTF8|RECEIPT_FILE_IO_FAILED|RUNTIME_FAILED)\b',text)
        if match:report['old_task_code']=match[1]
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--profile',default='admm-workspace')
    args=parser.parse_args()
    try:print(json.dumps(collect(args.profile),sort_keys=True));return 0
    except Exception as exc:
        print(json.dumps({'status':'COLLECTION_FAILED','error_type':type(exc).__name__}));return 1

if __name__=='__main__':raise SystemExit(main())

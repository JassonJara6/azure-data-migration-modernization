#!/usr/bin/env bash
# Read-only Azure/Databricks metadata. Run with existing Azure CLI/OAuth sessions.
# No Terraform, deployments, permissions changes, receipt/data reads, or compute.
set -euo pipefail
# Anchor output to the repository even when invoked from another directory.
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8
exec python - <<'PY'
import concurrent.futures
import datetime
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import urlencode, urlsplit

SUB='b3d627f1-966e-4bd4-861a-e606b963875d'
TENANT='2c60929f-5f02-4969-8e91-62fdac1de9d4'
REGION='eastus'
PROFILE='admm-workspace'
WORKSPACE_ID='7405617270738925'
RESOURCE=f'/subscriptions/{SUB}/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/workspaces/dbw-admm-dev-f6d99ba4'
JOB='210884055971910'
FAMILIES={'Standard_D4ds_v5':'standardDDSv5Family','Standard_D4ads_v5':'standardDADSv5Family','Standard_D4ds_v4':'standardDDSv4Family'}
SKU_PATH=f'/subscriptions/{SUB}/providers/Microsoft.Compute/skus'
CODES={'NotEnabledOnSubscription','NotAvailableInRegion'}
# Deliberately no raw stdout/stderr, UPNs, hosts, connection strings or tokens in report.
def native_command(exe,args,windows=None):
    # Windows can implicitly invoke cmd.exe for .cmd even with shell=False.
    # Bypass az.cmd using the same bundled interpreter/module as the MSI launcher:
    # otherwise '&', '%' and spaces in REST URLs may be interpreted by cmd.exe.
    windows=(os.name=='nt') if windows is None else windows
    if windows and Path(exe).suffix.lower() in ('.cmd','.bat'):
        if args[0]!='az':return None,'NATIVE_DATABRICKS_CLI_REQUIRED'
        interpreter=Path(exe).resolve().parent.parent/'python.exe'
        if not interpreter.is_file():return None,'AZURE_CLI_PACKAGED_PYTHON_NOT_FOUND'
        return [str(interpreter),'-IBm','azure.cli',*args[1:]],None
    return [exe,*args[1:]],None

def read(args,timeout=90):
    try:
        exe=shutil.which(args[0])
        if not exe:return None,'TOOL_MISSING'
        if args[0]=='databricks':args=[*args,'--profile',PROFILE]
        argv,error=native_command(exe,args)
        if error:return None,error
        p=subprocess.run(argv,capture_output=True,text=True,encoding='utf-8',errors='strict',timeout=timeout,shell=False)
        if p.returncode:
            body=(p.stderr or '')+'\n'+(p.stdout or '')
            for needle,code in [('AADSTS','ENTRA_AUTH_FAILED'),('AuthorizationFailed','ACCESS_DENIED'),('PERMISSION_DENIED','ACCESS_DENIED'),('403','ACCESS_DENIED'),('401','AUTH_REQUIRED'),('Please run','LOGIN_REQUIRED')]:
                if needle in body:return None,code
            return None,'READ_FAILED'
        return json.loads(p.stdout),None
    except subprocess.TimeoutExpired:return None,'READ_TIMEOUT'
    except UnicodeError:return None,'CLI_OUTPUT_NOT_UTF8'
    except ValueError:return None,'INVALID_JSON'
    except OSError:return None,'CLI_START_FAILED'

def number(v):
    return v if type(v) in (int,float) and math.isfinite(v) and v>=0 else None

def safe_id(v):
    return v if isinstance(v,str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,120}',v) else 'INVALID_OR_MISSING'

def quota_index(usages):
    out={}
    if not isinstance(usages,list):return out
    for u in usages:
        if not isinstance(u,dict) or not isinstance(u.get('name'),dict):continue
        key=u['name'].get('value')
        if not isinstance(key,str):continue
        key=key.lower()
        # Duplicate quota keys never silently replace a previous observation.
        if key in out:out[key]={'status':'AMBIGUOUS'};continue
        limit=number(u.get('limit'));used=number(u.get('currentValue'))
        if limit is None or used is None or limit!=int(limit) or used!=int(used):out[key]={'status':'INVALID'}
        else:out[key]={'status':'OBSERVED','limit':int(limit),'used':int(used),'headroom':int(limit-used)}
    return out

def status(node):
    info=node.get('node_info')
    flags=info.get('status') if isinstance(info,dict) else None
    if not isinstance(flags,list):return {'status':'UNKNOWN_FIELD_ABSENT'}
    if any(not isinstance(f,str) or f not in CODES for f in flags):return {'status':'UNKNOWN_PROVIDER_FLAG'}
    return {'status':'EXPLICIT','flags':flags}

def safe_policy(value):
    if not isinstance(value,dict):return {'status':'INVALID_RESPONSE'}
    if value.get('policy_family_id'):return {'status':'POLICY_FAMILY_REQUIRES_EFFECTIVE_REVIEW'}
    try:d=value.get('definition');d=json.loads(d) if isinstance(d,str) else d
    except ValueError:return {'status':'INVALID_DEFINITION'}
    if not isinstance(d,dict):return {'status':'INVALID_DEFINITION'}
    projected={}
    for key in ('node_type_id','driver_node_type_id','num_workers','data_security_mode','spark_version','spark_conf.spark.databricks.cluster.profile','spark_conf.spark.master','azure_attributes.availability','cluster_type','driver_node_type_flexibility.alternate_node_type_ids','worker_node_type_flexibility.alternate_node_type_ids','instance_pool_id','driver_instance_pool_id'):
        r=d.get(key)
        if not isinstance(r,dict):continue
        # These non-secret compute constraint fields only; no arbitrary spark_conf/tags.
        item={}
        for field in ('type','value','defaultValue','values','minValue','maxValue','pattern'):
            v=r.get(field)
            if isinstance(v,(str,int,float,bool,list)) and len(json.dumps(v))<1500:item[field]=v
        projected[key]=item
    return {'status':'OBSERVED_RULES_NOT_AN_AUTHORIZATION','rules':projected}

def sku_snapshot(reader=read,budget=180):
    url='https://management.azure.com'+SKU_PATH+'?'+urlencode({'api-version':'2021-07-01','$filter':"location eq 'eastus'"})
    records=[];seen=set();start=time.monotonic();pages=0
    while url:
        parts=urlsplit(url)
        if (parts.scheme!='https' or parts.netloc!='management.azure.com' or parts.path.lower()!=SKU_PATH.lower() or parts.fragment):
            return records,{'status':'INCOMPLETE','code':'UNEXPECTED_NEXT_LINK','pages':pages}
        if url in seen or pages>=100:return records,{'status':'INCOMPLETE','code':'PAGINATION_LIMIT','pages':pages}
        remaining=budget-(time.monotonic()-start)
        if remaining<=0:return records,{'status':'INCOMPLETE','code':'TOTAL_TIME_LIMIT','pages':pages}
        seen.add(url)
        page,err=reader(['az','rest','--method','get','--url',url,'--subscription',SUB,'--output','json'],min(60,remaining))
        if err:return records,{'status':'INCOMPLETE','code':err,'pages':pages}
        if not isinstance(page,dict) or not isinstance(page.get('value'),list):return records,{'status':'INCOMPLETE','code':'INVALID_SKU_PAGE','pages':pages}
        records.extend(s for s in page['value'] if isinstance(s,dict) and s.get('resourceType')=='virtualMachines')
        pages+=1;url=page.get('nextLink')
        if url is not None and (not isinstance(url,str) or not url):return records,{'status':'INCOMPLETE','code':'INVALID_NEXT_LINK','pages':pages}
    return records,{'status':'COMPLETE','pages':pages,'physical_capacity':'NOT_VERIFIED'}

def arm_sku(name,records):
    matches=[s for s in records if s.get('name')==name]
    if len(matches)!=1:return {'status':'MISSING_OR_AMBIGUOUS','matches':len(matches)}
    s=matches[0];out={'status':'OBSERVED','family':safe_id(s.get('family'))}
    caps=s.get('capabilities')
    if isinstance(caps,list):
        allowed={'vCPUs','vCPUsAvailable','MemoryGB','CpuArchitectureType','PremiumIO','MaxResourceVolumeMB','VMDeploymentTypes'}
        out['capabilities']={safe_id(c.get('name')):str(c.get('value'))[:100] for c in caps if isinstance(c,dict) and c.get('name') in allowed}
    locs=s.get('locations');out['eastus_listed']=isinstance(locs,list) and REGION in [l.lower() for l in locs if isinstance(l,str)]
    restrictions=s.get('restrictions')
    if not isinstance(restrictions,list):out['restriction_status']='UNKNOWN_FIELD_ABSENT';return out
    projected=[];blocking=False;unknown=False
    for r in restrictions:
        if not isinstance(r,dict):unknown=True;continue
        kind=r.get('type');reason=r.get('reasonCode');info=r.get('restrictionInfo')
        locations=info.get('locations') if isinstance(info,dict) else None
        locations=locations or r.get('values')
        if kind=='Location':
            if not isinstance(locations,list) or not all(isinstance(l,str) for l in locations):unknown=True
            elif REGION in [l.lower() for l in locations]:blocking=True
        elif kind!='Zone':unknown=True
        projected.append({'type':kind if kind in ('Location','Zone') else 'UNKNOWN',
            'reason':reason if reason in ('NotAvailableForSubscription','QuotaId') else 'UNKNOWN',
            'eastus_in_scope':isinstance(locations,list) and REGION in [l.lower() for l in locations if isinstance(l,str)]})
    out['restrictions']=projected
    # Zone restrictions do not prove regional ineligibility for AUTO-zone compute.
    # They also are not proof that any particular zone has capacity.
    out['restriction_status']='BLOCKED_LOCATION' if blocking else 'UNKNOWN_RESTRICTION' if unknown else 'NO_LOCATION_RESTRICTION_REPORTED'
    return out

def run():
    print('Leyendo identidad, Job, cuotas y politicas (sin compute)...',file=sys.stderr,flush=True)
    commands={
      'account':['az','account','show','--subscription',SUB,'--output','json'],
      'subscription_arm':['az','rest','--method','get','--url',f'https://management.azure.com/subscriptions/{SUB}?api-version=2022-12-01','--subscription',SUB,'--output','json'],
      'compute_provider':['az','provider','show','--namespace','Microsoft.Compute','--subscription',SUB,'--output','json'],
      'databricks_provider':['az','provider','show','--namespace','Microsoft.Databricks','--subscription',SUB,'--output','json'],
      'workspace':['az','resource','show','--ids',RESOURCE,'--api-version','2024-05-01','--subscription',SUB,'--output','json'],
      'assignment':['databricks','api','get','/api/2.1/unity-catalog/current-metastore-assignment'],
      'job':['databricks','jobs','get',JOB,'--output','json'],
      'nodes20':['databricks','api','get','/api/2.0/clusters/list-node-types'],
      'nodes21':['databricks','api','get','/api/2.1/clusters/list-node-types'],
      'quotas':['az','vm','list-usage','--subscription',SUB,'--location',REGION,'--output','json'],
      'runtimes':['databricks','api','get','/api/2.0/clusters/spark-versions'],
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures={key:pool.submit(read,cmd,120 if key=='quotas' else 90) for key,cmd in commands.items()}
        observed={key:f.result() for key,f in futures.items()}
    report={'status':'READ_ONLY_EVIDENCE_NOT_RUN_AUTHORIZATION','captured_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
      'subscription':SUB,'region':REGION,'job_id':JOB,'physical_capacity':'NOT_VERIFIED',
      'read_errors':{k:e for k,(_,e) in observed.items() if e}}
    account=observed['account'][0];ws=observed['workspace'][0];assignment=observed['assignment'][0]
    bound=(isinstance(account,dict) and account.get('id','').lower()==SUB and account.get('tenantId','').lower()==TENANT and
        isinstance(ws,dict) and ws.get('id','').lower()==RESOURCE.lower() and ws.get('location','').lower()==REGION and
        isinstance(ws.get('properties'),dict) and str(ws['properties'].get('workspaceId'))==WORKSPACE_ID and
        isinstance(assignment,dict) and str(assignment.get('workspace_id'))==WORKSPACE_ID)
    report['identity_binding']='VERIFIED' if bound else 'NOT_VERIFIED'
    state=account.get('state') if isinstance(account,dict) else None
    report['subscription_state']=state if state in ('Enabled','Warned','Disabled','Deleted','PastDue') else 'UNKNOWN'
    subscription=observed['subscription_arm'][0]
    policies=subscription.get('subscriptionPolicies') if isinstance(subscription,dict) else None
    report['subscription_offer']={'quota_id':safe_id(policies.get('quotaId')),'spending_limit':safe_id(policies.get('spendingLimit'))} if isinstance(policies,dict) else {'status':'UNKNOWN'}
    report['resource_provider_registration']={}
    for key in ('compute_provider','databricks_provider'):
        value=observed[key][0]
        report['resource_provider_registration'][key]=safe_id(value.get('registrationState')) if isinstance(value,dict) else 'UNKNOWN'
    q=quota_index(observed['quotas'][0]);report['regional_vm_count_quota']=q.get('virtualmachines',{'status':'MISSING'});report['regional_quota']=q.get('cores',{'status':'MISSING'})
    report['reviewed_family_quotas']={family:q.get(family.lower(),{'status':'MISSING'}) for family in FAMILIES.values()}
    versions=observed['runtimes'][0]
    report['runtime_15_4_listed']=isinstance(versions,dict) and any(isinstance(v,dict) and v.get('key')=='15.4.x-scala2.12' for v in versions.get('versions',[]))
    job=observed['job'][0];settings=job.get('settings',{}) if isinstance(job,dict) else {}
    run_as=settings.get('run_as')
    report['job_runner_matches_expected']=isinstance(run_as,dict) and run_as.get('service_principal_name')=='7d5adada-f25c-47f2-ab76-9880b52d5490'
    clusters=settings.get('job_clusters');cluster={}
    if isinstance(clusters,list) and len(clusters)==1 and isinstance(clusters[0],dict) and isinstance(clusters[0].get('new_cluster'),dict):cluster=clusters[0]['new_cluster']
    spark_conf=cluster.get('spark_conf');spark_conf=spark_conf if isinstance(spark_conf,dict) else {}
    attr=cluster.get('azure_attributes');attr=attr if isinstance(attr,dict) else {}
    single=(cluster.get('num_workers')==0 and spark_conf.get('spark.databricks.cluster.profile')=='singleNode' and
        spark_conf.get('spark.master')=='local[*]' and not any(cluster.get(k) for k in ('autoscale','instance_pool_id','driver_instance_pool_id')))
    driver=cluster.get('driver_node_type_id') or cluster.get('node_type_id')
    report['cluster']={'single_node_confirmed':single,'driver':safe_id(driver),'worker_node_type':safe_id(cluster.get('node_type_id')),
      'worker_count':number(cluster.get('num_workers')),'runtime':safe_id(cluster.get('spark_version')),
      'access_mode':safe_id(cluster.get('data_security_mode')),'availability':safe_id(attr.get('availability')),
      'zone':safe_id(attr.get('zone_id')),'max_concurrent_runs':number(settings.get('max_concurrent_runs')),
      'task_count':len(settings.get('tasks',[])) if isinstance(settings.get('tasks'),list) else None}
    flex=cluster.get('driver_node_type_flexibility');alts=flex.get('alternate_node_type_ids',[]) if isinstance(flex,dict) else []
    report['cluster']['driver_alternates']=[safe_id(a) for a in alts] if isinstance(alts,list) else 'INVALID'
    policy_id=cluster.get('policy_id')
    if isinstance(policy_id,str) and re.fullmatch(r'[A-Za-z0-9_-]+',policy_id):
        policy,err=read(['databricks','api','get','/api/2.0/policies/clusters/get?policy_id='+policy_id])
        report['policy']=safe_policy(policy) if not err else {'status':'READ_FAILED','code':err}
        acl,err=read(['databricks','permissions','get','cluster-policies',policy_id,'--output','json'])
        runner='7d5adada-f25c-47f2-ab76-9880b52d5490'
        entries=acl.get('access_control_list',[]) if isinstance(acl,dict) else []
        report['runner_direct_policy_can_use_observed']=None if err else any(isinstance(e,dict) and e.get('service_principal_name')==runner and any(isinstance(p,dict) and p.get('permission_level') in ('CAN_USE','CAN_MANAGE') for p in e.get('all_permissions',[])) for e in entries)
        report['runner_policy_note']='Direct ACL observation only; false does not rule out group inheritance'
    else:report['policy']={'status':'MISSING_POLICY_ID'}
    nodes_by_version={}
    for key in ('nodes20','nodes21'):
        data=observed[key][0]
        nodes_by_version[key]=[n for n in data.get('node_types',[]) if isinstance(n,dict)] if isinstance(data,dict) and isinstance(data.get('node_types'),list) else []
    # Examine all workspace-offered small x86 CPU nodes. This is discovery, not an approval list.
    names=set(FAMILIES)
    for nodes in nodes_by_version.values():
        for n in nodes:
            cpu=number(n.get('num_cores'));ram=number(n.get('memory_mb'))
            if cpu is not None and 0<cpu<=4 and ram is not None and ram>=8192 and not n.get('is_hidden') and not n.get('is_deprecated') and not n.get('is_graviton') and not n.get('num_gpus'):
                name=n.get('node_type_id')
                if isinstance(name,str) and re.fullmatch(r'Standard_[A-Za-z0-9_-]+',name):names.add(name)
    records=[]
    if bound and report['subscription_state']=='Enabled':
        print('Leyendo Resource SKUs East US: una sola paginacion, limite total 180 s; un timeout quedara UNKNOWN.',file=sys.stderr,flush=True)
        records,snapshot=sku_snapshot()
    else:snapshot={'status':'NOT_READ','code':'IDENTITY_OR_SUBSCRIPTION_NOT_VERIFIED'}
    report['arm_sku_snapshot']=snapshot;results=[]
    for name in sorted(names):
        item={'node_type':name,'currently_reviewed':name in FAMILIES,'api_versions':{}}
        node=None
        for key,nodes in nodes_by_version.items():
            found=[n for n in nodes if n.get('node_type_id')==name]
            item['api_versions'][key]=status(found[0]) if len(found)==1 else {'status':'MISSING_OR_AMBIGUOUS'}
            if len(found)==1:
                node=found[0] if node is None else node
                item['api_versions'][key].update(vcpu=number(found[0].get('num_cores')),memory_mb=number(found[0].get('memory_mb')))
        arm=arm_sku(name,records);item['arm']=arm
        family=arm.get('family');item['family_quota']=q.get(family.lower(),{'status':'MISSING'}) if isinstance(family,str) else {'status':'MISSING'}
        item['fits_existing_4vcpu_16gib_envelope']=bool(node and number(node.get('num_cores'))==4 and number(node.get('memory_mb'))==16384)
        item['additional_vm_quota_cores_if_single_node']=number(node.get('num_cores')) if node and single else 'NOT_VERIFIED'
        if arm.get('status')=='OBSERVED':
            caps=arm.get('capabilities',{})
            # Constrained-vCPU VM families can charge quota using full vCPUs: never infer from active cores alone.
            item['azure_advertised_quota_vcpus']=caps.get('vCPUs','UNKNOWN')
        flags=[x for x in item['api_versions'].values() if x['status']=='EXPLICIT']
        rejected=any(f.get('flags') for f in flags)
        item['provider_assessment']='REJECTED' if rejected else 'EXPLICIT_CLEAR_REPORTED' if flags else 'UNKNOWN'
        # Deliberately NO 'eligible' flag: no live allocation, effective policy change or trust-boundary bypass.
        item['action']='REVIEW_EVIDENCE_NOT_PERMISSION_TO_CHANGE_SKU'
        results.append(item)
    report['nodes']=results
    return report

if __name__=='__main__':
    exit_code=0
    try:report=run()
    except Exception as exc:
        report={'status':'COLLECTION_FAILED','error_type':type(exc).__name__,'raw_error_suppressed':True}
        exit_code=1
    try:
        target=Path('work/silver-capacity-evidence.json')
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(json.dumps(report,indent=2,sort_keys=True),encoding='utf-8')
        print(json.dumps(report,indent=2,sort_keys=True))
        print('Informe sanitizado guardado en work/silver-capacity-evidence.json. No se inicio compute ni se modificaron recursos.',file=sys.stderr)
    except Exception as exc:
        print(json.dumps({'status':'REPORT_WRITE_FAILED','error_type':type(exc).__name__,'raw_error_suppressed':True}))
        exit_code=1
    sys.exit(exit_code)
PY

"""Windows/Git Bash launcher. Default = plan only. --deploy requires hash approvals.

No shell interpolation, PATs, account keys, terraform -target, backend migration,
ADF runs or Bronze writes. Python 3.11+, Azure CLI, Databricks CLI and Terraform.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import yaml
from plan_guard import Blocked,require,review,sha
from bundle_overlay import ephemeral_bundle
from diagnose import diagnose, safe_failure, SUBSCRIPTION, TENANT

ROOT=Path(__file__).resolve().parents[2]
PLATFORM=ROOT/'terraform/environments/dev'
UC=ROOT/'terraform/databricks/dev'
PARENT='20d65511-8190-41a0-8ad2-23d7568363d2'
SOURCES={'Person.Person','Person.Address','Person.StateProvince','Person.CountryRegion','Production.Product','Production.ProductCategory','Production.ProductSubcategory','Sales.Customer','Sales.SalesOrderHeader','Sales.SalesOrderDetail','Sales.SalesTerritory'}


def command(args,*,cwd=ROOT,env=None,json_output=False,allowed=(0,),interactive=False):
    # Never echo raw external error output: SDK/CLI messages may contain tokens.
    program=shutil.which(str(args[0])) or str(args[0])
    try:
        result=subprocess.run([program,*map(str,args[1:])],cwd=cwd,env=env,
           text=True,capture_output=not interactive)
    except FileNotFoundError:
        raise Blocked('TOOL_MISSING_'+Path(str(args[0])).stem.upper()) from None
    if result.returncode not in allowed:
        detail=safe_failure((result.stderr or '')+'\n'+(result.stdout or ''),
                            'COMMAND_FAILED_'+Path(str(args[0])).stem.upper())
        raise Blocked(json.dumps(detail,sort_keys=True))
    if interactive:return None
    return json.loads(result.stdout) if json_output else result.stdout


def deployment_sha(stage):
    paths=[stage/'databricks.yml',stage/'verify_artifact.py',*sorted((stage/'notebooks').glob('*.py')),*sorted((stage/'dist').glob('*.whl'))]
    payload=b''.join(str(p.relative_to(stage)).encode()+b'\0'+p.read_bytes()+b'\0' for p in paths)
    return hashlib.sha256(payload).hexdigest()


def approval(label,fingerprint):
    expected=label+' '+fingerprint
    print('Aprobación requerida; escriba exactamente: '+expected)
    require(input('> ').strip()==expected,'APPROVAL_NOT_GIVEN')


def uuid(value):
    require(isinstance(value,str) and re.fullmatch(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}',value),'INVALID_ID')
    return value


def state_resources(state):
    def walk(module):
        for resource in module.get('resources',[]):yield resource['address'],resource.get('values',{})
        for child in module.get('child_modules',[]):yield from walk(child)
    return dict(walk(state.get('values',{}).get('root_module',{})))


def choose_metastore(response,selected=None,allow_create=False):
    entries=[m.get('metastore_info',m) for m in response.get('metastores',[])]
    regional=[m for m in entries if m.get('region','').lower().replace(' ','')=='eastus']
    if selected:
        require(any(m['metastore_id']==selected for m in regional),'METASTORE_NOT_IN_EAST_US')
        return selected,False
    require(len(regional)<=1,'AMBIGUOUS_METASTORE_REQUIRE_ID')
    if regional:return regional[0]['metastore_id'],False
    require(allow_create,'ACCOUNT_ADMIN_METASTORE_REQUIRED')
    return None,True


def validate_result(result,parent):
    require(result.get('status')=='SUCCEEDED' and result.get('parent_run_id')==parent,'SILVER_RESULT_FAILED')
    require(result.get('replay_verified') is True,'IDEMPOTENCY_NOT_VERIFIED')
    metrics=result.get('metrics',{})
    require(set(metrics)==SOURCES,'ELEVEN_METRICS_REQUIRED')
    for value in metrics.values():
        require(value.get('rejected_rows')==0 and value.get('relationship_rejected_rows',0)==0,'QUALITY_REJECTED')
        require(type(value.get('input_rows')) is int and type(value.get('target_rows')) is int and
                value['input_rows']>=0 and value['target_rows']>=0 and type(value.get('target_version')) is int and value['target_version']>=0,'METRICS_INVALID')
    return metrics


class Launcher:
    def __init__(self,config,deploy,stage="all"):
        self.c=config;self.deploy=deploy;self.stage=stage
        require(not os.environ.get('DATABRICKS_TOKEN'),'PAT_NOT_SUPPORTED')
        require(re.fullmatch(r'[a-z][a-z0-9_]{0,62}',config.get('catalog_name','admm_dev')),'INVALID_CATALOG')
        self.work=ROOT/'work/silver-deployment';self.work.mkdir(parents=True,exist_ok=True)
        self.work.chmod(0o700)
        self.env=dict(os.environ)
        # Real backend continues to use Azure CLI / federated credential handling.
        azure=command(['az','account','show','-o','json'],json_output=True)
        subscription=config.get('subscription_id') or SUBSCRIPTION;uuid(subscription)
        uuid(config.get('tenant_id') or TENANT)
        require(azure['id'].lower()==subscription.lower(),'AZURE_SUBSCRIPTION_MISMATCH')
        require(azure['tenantId'].lower()==(config.get('tenant_id') or TENANT).lower(),'AZURE_TENANT_MISMATCH')
        self.env['ARM_SUBSCRIPTION_ID']=subscription
        self.env['ARM_TENANT_ID']=azure['tenantId']
        registered=command(['az','provider','show','--namespace','Microsoft.Databricks','--query','registrationState','-o','tsv'])
        require(registered.strip() in ('Registered','NotRegistered'),'AZURE_PROVIDER_REGISTRATION_PENDING')
        self.register_provider=registered.strip()!='Registered'
        self.env['TF_VAR_subscription_id']=subscription
        self.account_env={k:v for k,v in self.env.items() if k not in ('DATABRICKS_HOST','DATABRICKS_AUTH_TYPE','DATABRICKS_AZURE_RESOURCE_ID')}
        self.ws_env=None
    def account_api(self,method,path):
        try:return command(['databricks','api',method,path,'--profile',self.c.get('account_profile','admm-account')],env=self.account_env,json_output=True)
        except Blocked as exc:raise Blocked('ACCOUNT_ADMIN_OAUTH_OR_CONNECTIVITY_REQUIRED: '+str(exc)) from None
    def api(self,method,path):
        return command(['databricks','api',method,path],env=self.ws_env,json_output=True)
    def tf(self,root,*args,**kwargs):
        try:return command(['terraform','-chdir='+str(root),*args],env=self.env,**kwargs)
        except Blocked as exc:
            stage='PLATFORM' if root==PLATFORM else 'UC'
            raise Blocked(stage+'_'+str(args[0]).upper()+': '+str(exc)) from None
    def init(self,root):
        # No -reconfigure, -migrate-state, force-copy or backend overrides.
        self.tf(root,'init','-input=false','-lockfile=readonly')
        self.tf(root,'validate')
    def outputs(self,root):
        return self.tf(root,'output','-json',json_output=True)
    def plan(self,root,stage,variables):
        vars_path=self.work/(stage+'.auto.tfvars.json')
        vars_path.write_text(json.dumps(variables,indent=2),encoding='utf-8')
        saved=self.work/(stage+'.tfplan')
        self.tf(root,'plan','-input=false','-lock-timeout=120s','-out='+str(saved),'-var-file='+str(vars_path))
        plan=self.tf(root,'show','-json',str(saved),json_output=True)
        changes=review(plan,stage)
        human=self.work/(stage+'-plan.txt');human.write_text(self.tf(root,'show','-no-color',str(saved)),encoding='utf-8')
        fingerprint=sha(saved)
        (self.work/(stage+'-review.json')).write_text(json.dumps({'sha256':fingerprint,'changes':changes,'stage':stage},indent=2),encoding='utf-8')
        print(json.dumps({'stage':stage,'plan_sha256':fingerprint,'changes':changes,'review_file':str(human)},ensure_ascii=True))
        if changes and self.deploy:
            print('Revise el plan completo en el archivo indicado. Se rechazan destrucciones/reemplazos y cambios protegidos.')
            approval('APPLY-'+stage.upper(),fingerprint)
            require(sha(saved)==fingerprint,'PLAN_CHANGED_AFTER_APPROVAL')
            require(review(self.tf(root,'show','-json',str(saved),json_output=True),stage)==changes,'PLAN_REVIEW_CHANGED')
            self.tf(root,'apply','-input=false',str(saved))
        return not changes or self.deploy
    def provision(self):
        if self.deploy and getattr(self,"stage","all")=="all":
            require((ROOT/'databricks/silver/databricks.yml').is_file() and
                    (ROOT/'databricks/silver/tools/export_committed_parent.py').is_file(),'PR18_REQUIRED_BEFORE_DEPLOY')
        self.init(PLATFORM)
        state=self.tf(PLATFORM,'state','list').splitlines()
        require({'azurerm_resource_group.platform','azurerm_storage_account.data_lake','azurerm_data_factory.platform'}.issubset(state),'EXISTING_PLATFORM_STATE_REQUIRED')
        old=self.outputs(PLATFORM)
        require(old['resource_group_name']['value']=='rg-admm-dev-platform' and
                old['storage_account_name']['value']=='stadmmdevdlf6d99ba4','PLATFORM_IDENTITY_MISMATCH')
        register=self.register_provider or 'module.databricks[0].azurerm_resource_provider_registration.databricks[0]' in state
        applied=self.plan(PLATFORM,'platform',{'enable_databricks':True,'register_databricks_provider':register})
        values=self.outputs(PLATFORM)
        workspace=values.get('silver_workspace',{}).get('value')
        if not applied or not workspace:
            reason='DEFERRED_UNTIL_PLATFORM_PLAN_APPLIED' if not applied else 'DEFERRED_UNTIL_WORKSPACE_OUTPUT_EXISTS'
            print(json.dumps({'status':'PLAN_ONLY','uc_plan':reason}));return None
        if getattr(self,'stage','all')=='workspace':
            print(json.dumps({'status':'WORKSPACE_STAGE_COMPLETE','uc':'NOT_CONFIGURED_OR_VERIFIED',
                              'silver':'NOT_DEPLOYED','next':'Configure authorized Databricks account OAuth, then rerun --stage all'}))
            return None
        uuid(self.c.get('account_id'))
        require('@' in self.c.get('operator_user_name',''),'OPERATOR_UPN_REQUIRED')
        self.ws_env=dict(self.env,DATABRICKS_HOST=workspace['workspace_host'],DATABRICKS_AUTH_TYPE='azure-cli',
                        DATABRICKS_AZURE_RESOURCE_ID=workspace['workspace_resource_id'])
        self.init(UC)
        owned=state_resources(self.tf(UC,'show','-json',json_output=True))
        # Azure may auto-enable UC while creating the workspace. Re-discover
        # regional metastores before ever proposing a new shared object.
        self.metastores=self.account_api('get',f'/api/2.0/accounts/{self.c["account_id"]}/metastores')
        selected,create=choose_metastore(self.metastores,self.c.get('metastore_id'),self.c.get('allow_create_metastore',False))
        owned_meta=owned.get('databricks_metastore.regional[0]')
        if owned_meta:
            require(selected==owned_meta['id'],'REGIONAL_METASTORE_CHANGED');selected=None;create=True
        assigned=self.assignment_for(workspace['workspace_id'])
        selected_effective=selected or (owned_meta or {}).get('id')
        if assigned:require(assigned.get('metastore_id')==selected_effective,'EXISTING_ASSIGNMENT_MISMATCH')
        manage='databricks_metastore_assignment.workspace[0]' in owned or assigned is None
        if 'databricks_service_principal.runner' not in owned:
            name=self.c.get('catalog_name','admm_dev')+'-silver-job'
            query=urllib.parse.quote('displayName eq \"'+name+'\"',safe='')
            matches=self.account_api('get',f'/api/2.0/accounts/{self.c["account_id"]}/scim/v2/ServicePrincipals?filter='+query)
            require(matches.get('totalResults',len(matches.get('Resources',[])))==0,'UNOWNED_RUNNER_PRINCIPAL_ALREADY_EXISTS')
        variables=dict(account_id=self.c['account_id'],account_profile=self.c.get('account_profile','admm-account'),
            workspace_host=workspace['workspace_host'],workspace_id=str(workspace['workspace_id']),
            workspace_resource_id=workspace['workspace_resource_id'],access_connector_id=workspace['access_connector_id'],
            storage_account_name=workspace['storage_account_name'],operator_user_name=self.c['operator_user_name'],
            metastore_id=selected,create_metastore=create,manage_assignment=manage,
            catalog_name=self.c.get('catalog_name','admm_dev'),node_type_id=self.c.get('node_type_id','Standard_D4ds_v5'))
        require(self.c['operator_user_name'].lower()==self.api('get','/api/2.0/preview/scim/v2/Me')['userName'].lower(),'WORKSPACE_OPERATOR_IDENTITY_MISMATCH')
        if not self.plan(UC,'uc',variables):return None
        output=self.outputs(UC)['deployment']['value']
        self.check_workspace(output)
        return workspace,output
    def assignment_for(self,workspace_id):
        # List assignments on known metastores: no heuristic 404 handling and
        # no swallowing authorization/transport failures.
        matches=[]
        for item in self.metastores.get('metastores',[]):
            meta=item.get('metastore_info',item)
            result=self.account_api('get',f'/api/2.0/accounts/{self.c["account_id"]}/metastores/{meta["metastore_id"]}/workspaces')
            if str(workspace_id) in {str(i) for i in result.get('workspace_ids',[])}:matches.append(meta['metastore_id'])
        require(len(matches)<=1,'WORKSPACE_ASSIGNMENT_AMBIGUOUS')
        return {'metastore_id':matches[0]} if matches else None
    def check_workspace(self,uc):
        current=self.api('get','/api/2.1/unity-catalog/metastore_summary')
        require(current['metastore_id']==uc['metastore_id'],'WORKSPACE_METASTORE_MISMATCH')
        versions=self.api('get','/api/2.0/clusters/spark-versions')
        require(any(v['key']=='15.4.x-scala2.12' for v in versions['versions']),'DBR_15_4_UNAVAILABLE')
        nodes=self.api('get','/api/2.0/clusters/list-node-types')
        require(any(n['node_type_id']==uc['node_type_id'] for n in nodes['node_types']),'NODE_TYPE_UNAVAILABLE')
        for volume in ('bronze','receipts'):
            name=uc['catalog']+'.inputs.'+volume
            permissions=self.api('get','/api/2.1/unity-catalog/permissions/volume/'+name)
            grants=permissions.get('privilege_assignments',[])
            runner=next((g for g in grants if g['principal']==uc['runner_application_id']),None)
            require(runner and set(runner['privileges'])=={'READ_VOLUME'},'RUNNER_INPUT_PERMISSIONS_MISMATCH')
            require(not any('WRITE_VOLUME' in g.get('privileges',[]) and g['principal']!=uc['operator'] for g in grants),'UNEXPECTED_RECEIPT_OR_BRONZE_WRITER')
        # Notebook additionally validates the exact external-volume URI and real
        # file hashes under the RUN-AS principal, not this operator identity.
    def deploy_job(self,workspace,uc):
        stage=self.work/'bundle';source=ROOT/'databricks/silver'
        # Keep .databricks deployment identity across reruns; sync code only.
        shutil.copytree(source,stage,dirs_exist_ok=True,ignore=shutil.ignore_patterns('.databricks','build','dist','*.egg-info','__pycache__'))
        dist=stage/'dist'
        if dist.exists():shutil.rmtree(dist)
        source_hash=hashlib.sha256(b''.join(str(p.relative_to(stage)).encode()+b'\0'+p.read_bytes()+b'\0' for p in sorted((stage/'src').rglob('*')) if p.is_file() and '__pycache__' not in p.parts)).hexdigest()[:12]
        project=stage/'pyproject.toml'
        project.write_text(re.sub(r'(?m)^version = .*$', f'version = "0.1.0+{source_hash}"',project.read_text()))
        spec=ephemeral_bundle(yaml.safe_load((source/'databricks.yml').read_text()),uc,workspace['workspace_host'])
        parent=self.c.get('parent_run_id',PARENT);uuid(parent)
        uri=uc['receipt_volume']+'/'+parent+'.json'
        spec['variables']['catalog']['default']=uc['catalog'];spec['variables']['schema']['default']=uc['schema']
        spec['variables']['bronze_volume']['default']=uc['bronze_volume'];spec['variables']['receipt_file']['default']=uri
        spec['variables']['parent_run_id']['default']=parent
        spec['artifacts']['silver_wheel']['build']='python verify_artifact.py'
        spec['artifacts']['silver_wheel']['dynamic_version']=False
        (stage/'databricks.yml').write_text(yaml.safe_dump(spec,sort_keys=False),encoding='utf-8')
        epoch=command(['git','log','-1','--format=%ct']).strip()
        require(epoch.isdigit(),'REPRODUCIBLE_BUILD_EPOCH_REQUIRED')
        command([sys.executable,'-m','build','--wheel'],cwd=stage,env=dict(self.env,SOURCE_DATE_EPOCH=epoch))
        validated=command(['databricks','bundle','validate','--output','json'],cwd=stage,env=self.ws_env,json_output=True)
        wheels=list(dist.glob('*.whl'));require(len(wheels)==1,'EXACTLY_ONE_WHEEL_REQUIRED')
        wheel=wheels[0]
        verifier="from pathlib import Path\nimport hashlib\np=Path('dist')/"+repr(wheel.name)+"\nassert hashlib.sha256(p.read_bytes()).hexdigest()=="+repr(sha(wheel))+"\n"
        (stage/'verify_artifact.py').write_text(verifier)
        fingerprint=deployment_sha(stage)
        print('Job efímero: DBR 15.4, 1 nodo, timeout 2h, sin schedule; escribe solo Silver. Revise '+str(stage/'databricks.yml'))
        approval('DEPLOY-RUN-SILVER',fingerprint)
        receipt=self.work/(parent+'.json')
        if receipt.exists():receipt.unlink() # Re-export authoritative evidence; never fabricate/reuse edited JSON.
        command([sys.executable,str(source/'tools/export_committed_parent.py'),'--parent-run-id',parent,
            '--output',str(receipt),'--sql-server',self.c.get('sql_server','127.0.0.1,1433'),
            '--sql-user',self.c.get('sql_user','bronze_reconciler'),'--trust-local-sql-certificate'],env=self.env,interactive=True)
        # Validate exactly the same evidence contract used by the Job before upload.
        sys.path.insert(0,str(source/'src'))
        from admm_silver.contracts import validate_bundle
        evidence=json.loads(receipt.read_text());validate_bundle(evidence,'stadmmdevdlf6d99ba4')
        require(evidence['parent_run_id']==parent,'EXPORTED_PARENT_MISMATCH')
        # Never overwrite a committed evidence artifact. Exact retries reuse it.
        listing=command(['databricks','fs','ls','dbfs:'+uc['receipt_volume'],'--output','json'],env=self.ws_env,json_output=True)
        names=listing if isinstance(listing,list) else listing.get('files',[])
        if any(str(item.get('path',item.get('name',''))).rstrip('/').split('/')[-1]==receipt.name for item in names):
            existing=self.work/'uploaded-receipt.json'
            if existing.exists():existing.unlink()
            command(['databricks','fs','cp','dbfs:'+uri,str(existing)],env=self.ws_env)
            require(sha(existing)==sha(receipt),'EXISTING_RECEIPT_DIFFERS_NO_OVERWRITE')
        else:command(['databricks','fs','cp',str(receipt),'dbfs:'+uri],env=self.ws_env)
        require(deployment_sha(stage)==fingerprint,'BUNDLE_CHANGED_AFTER_APPROVAL')
        # Never create a second Job for the same namespace/name. A cleared
        # local deployment record requires explicit state recovery, not adoption.
        job_record=self.work/'job-id.json'
        owned=json.loads(job_record.read_text()) if job_record.exists() else {}
        require(not owned or owned['host']==workspace['workspace_host'],'DEPLOYMENT_WORKSPACE_CHANGED')
        name=validated['resources']['jobs']['silver']['name']
        token=None;seen=set();existing=[]
        while True:
            query='/api/2.1/jobs/list?name='+urllib.parse.quote(name,safe='')
            if token:query+='&page_token='+urllib.parse.quote(token,safe='')
            page=self.api('get',query);existing.extend(j['job_id'] for j in page.get('jobs',[]) if j['settings']['name']==name)
            token=page.get('next_page_token')
            if not token:break
            require(token not in seen,'JOB_LIST_PAGINATION_REPEATED');seen.add(token)
        require(not existing or (len(existing)==1 and str(existing[0])==str(owned.get('job_id'))),'EXISTING_JOB_REQUIRES_STATE_REVIEW')
        if existing:
            active=self.api('get','/api/2.1/jobs/runs/list?job_id='+str(existing[0])+'&active_only=true')
            require(not active.get('runs',[]),'ACTIVE_SILVER_RUN_USE_STOP_OR_WAIT')
        command(['databricks','bundle','deploy'],cwd=stage,env=self.ws_env)
        resources=command(['databricks','bundle','summary','--output','json'],cwd=stage,env=self.ws_env,json_output=True)
        job_id=resources['resources']['jobs']['silver']['id']
        job_record.write_text(json.dumps({'job_id':job_id,'host':workspace['workspace_host']}))
        # RUN AS needs read access to its notebook/wheel, not the operator's home.
        bundle_root=validated['workspace']['root_path'].removeprefix('/Workspace')
        folder=command(['databricks','workspace','get-status',bundle_root,'--output','json'],env=self.ws_env,json_output=True)
        require(folder['object_type']=='DIRECTORY','BUNDLE_ROOT_NOT_DIRECTORY')
        acl=json.dumps({'access_control_list':[{'service_principal_name':uc['runner_application_id'],'permission_level':'CAN_READ'}]})
        command(['databricks','permissions','update','directories',str(folder['object_id']),'--json',acl],env=self.ws_env)
        submitted=command(['databricks','jobs','run-now',str(job_id),'--no-wait','--output','json'],env=self.ws_env,json_output=True)
        run_id=submitted['run_id'];(self.work/'last-run.json').write_text(json.dumps({'run_id':run_id,'job_id':job_id,'host':workspace['workspace_host'],'workspace_resource_id':workspace['workspace_resource_id']}))
        deadline=time.monotonic()+7500
        while True:
            run=self.api('get','/api/2.1/jobs/runs/get?run_id='+str(run_id))
            state=run['state']
            if state['life_cycle_state'] in ('TERMINATED','SKIPPED','INTERNAL_ERROR'):break
            require(time.monotonic()<deadline,'JOB_WAIT_TIMEOUT_USE_STOP')
            print('Silver: '+state['life_cycle_state']);time.sleep(15)
        require(state.get('result_state')=='SUCCESS','DATABRICKS_JOB_FAILED_CHECK_RUN_UI')
        task=next(t for t in run['tasks'] if t['task_key']=='transform_and_verify')
        output=self.api('get','/api/2.1/jobs/runs/get-output?run_id='+str(task['run_id']))
        require(not output.get('notebook_output',{}).get('truncated',False),'JOB_OUTPUT_TRUNCATED')
        result=json.loads(output['notebook_output']['result']);validate_result(result,parent)
        acceptance={'status':'ACCEPTED','parent_run_id':parent,'run_id':run_id,'job_id':job_id,
           'replay_verified':True,'metrics':result['metrics'],'azure_r2':'NOT_EXECUTED',
           'evidence_sha256':sha(receipt),'bundle_sha256':fingerprint,'workspace':workspace['workspace_host']}
        (self.work/'acceptance.json').write_text(json.dumps(acceptance,indent=2),encoding='utf-8')
        print(json.dumps(acceptance,indent=2))


def load_config(path,workspace_only=False):
    path=Path(path).resolve()
    require(path.is_relative_to(ROOT/'work') and path.suffix=='.json','PRIVATE_CONFIG_PATH_REQUIRED')
    existed=path.exists()
    config=json.loads(path.read_text()) if existed else json.loads((ROOT/'automation/silver/config.example.json').read_text())
    if workspace_only:
        if not existed:
            config['account_id']=None;config['operator_user_name']=None
            config['subscription_id']=SUBSCRIPTION;config['tenant_id']=TENANT
    elif not existed or not config.get('account_id') or not config.get('operator_user_name'):
        print('Configuración de cuenta (sin contraseñas): Account ID, perfil OAuth y operador autorizado.')
        config['account_id']=uuid(input('Databricks Account ID: ').strip())
        config['account_profile']=input('Perfil OAuth de cuenta [admm-account]: ').strip() or 'admm-account'
        config['operator_user_name']=input('UPN del operador autorizado: ').strip()
        require('@' in config['operator_user_name'],'OPERATOR_UPN_REQUIRED')
        answer=input('Permitir proponer un metastore nuevo solo si no existe East US (admin de cuenta) [no]: ').strip().lower()
        require(answer in ('','no','si','sí','yes'),'INVALID_METASTORE_CHOICE')
        config['allow_create_metastore']=answer in ('si','sí','yes')
    if not existed or not workspace_only:
        path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(config,indent=2),encoding='utf-8');path.chmod(0o600)
    return config


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='work/silver-dev.json')
    parser.add_argument('--diagnose',action='store_true',help='Read-only Azure identity/context/RBAC diagnosis; no login prompts or Terraform')
    parser.add_argument('--graph',action='store_true',help='Optional directory/security-defaults checks; requires existing delegated permissions')
    parser.add_argument('--correlation-id',help='Optional Entra sign-in correlation UUID; requires --graph and AuditLog.Read.All')
    parser.add_argument('--stage',choices=('workspace','all'),default='workspace',help='workspace excludes all Databricks account/UC/Job operations')
    parser.add_argument('--deploy',action='store_true',help='Request saved-plan and deployment approvals interactively')
    parser.add_argument('--stop',action='store_true',help='Cancel only the last recorded Silver run')
    args=parser.parse_args()
    try:
        require(not (args.diagnose and (args.deploy or args.stop)),'DIAGNOSE_IS_READ_ONLY')
        require(not args.correlation_id or args.graph,'CORRELATION_REQUIRES_GRAPH')
        require(not args.graph or args.diagnose,'GRAPH_REQUIRES_DIAGNOSE')
        if args.diagnose:
            path=Path(args.config).resolve()
            require(path.is_relative_to(ROOT/'work') and path.suffix=='.json','PRIVATE_CONFIG_PATH_REQUIRED')
            config=json.loads(path.read_text()) if path.exists() else {}
            result=diagnose(config,graph=args.graph,correlation_id=args.correlation_id)
            print(json.dumps(result,indent=2))
            return 0 if result['status']=='AZURE_ACCESS_OBSERVED' else 1
        if args.stop:
            last=json.loads((ROOT/'work/silver-deployment/last-run.json').read_text())
            env=dict(os.environ,DATABRICKS_HOST=last['host'],DATABRICKS_AUTH_TYPE='azure-cli',DATABRICKS_AZURE_RESOURCE_ID=last['workspace_resource_id'])
            command(['databricks','jobs','cancel-run',str(last['run_id']),'--no-wait'],env=env);return 0
        launch=Launcher(load_config(args.config,workspace_only=args.stage=="workspace"),args.deploy,stage=args.stage)
        deployed=launch.provision()
        if deployed and args.deploy:launch.deploy_job(*deployed)
        return 0
    except KeyboardInterrupt:
        print('INTERRUPTED: use --stop to cancel a recorded running Job.',file=sys.stderr);return 130
    except Exception as exc:
        code=str(exc) if isinstance(exc,Blocked) else 'LOCAL_OR_RUNTIME_FAILURE'
        print(json.dumps({'status':'BLOCKED','code':code,'error_type':type(exc).__name__}),file=sys.stderr);return 1

if __name__=='__main__':raise SystemExit(main())

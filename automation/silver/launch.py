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

ROOT=Path(__file__).resolve().parents[2]
PLATFORM=ROOT/'terraform/environments/dev'
UC=ROOT/'terraform/databricks/dev'
PARENT='20d65511-8190-41a0-8ad2-23d7568363d2'
SOURCES={'Person.Person','Person.Address','Person.StateProvince','Person.CountryRegion','Production.Product','Production.ProductCategory','Production.ProductSubcategory','Sales.Customer','Sales.SalesOrderHeader','Sales.SalesOrderDetail','Sales.SalesTerritory'}


def command(args,*,cwd=ROOT,env=None,json_output=False,allowed=(0,),interactive=False):
    # Never echo raw external error output: SDK/CLI messages may contain tokens.
    program=shutil.which(str(args[0])) or str(args[0])
    result=subprocess.run([program,*map(str,args[1:])],cwd=cwd,env=env,
       text=True,capture_output=not interactive)
    require(result.returncode in allowed,'COMMAND_FAILED_'+Path(str(args[0])).stem.upper())
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
    def __init__(self,config,deploy):
        self.c=config;self.deploy=deploy
        require(not os.environ.get('DATABRICKS_TOKEN'),'PAT_NOT_SUPPORTED')
        uuid(config['account_id'])
        require(re.fullmatch(r'[a-z][a-z0-9_]{0,62}',config.get('catalog_name','admm_dev')),'INVALID_CATALOG')
        self.work=ROOT/'work/silver-deployment';self.work.mkdir(parents=True,exist_ok=True)
        self.work.chmod(0o700)
        self.env=dict(os.environ)
        # Real backend continues to use Azure CLI / federated credential handling.
        azure=command(['az','account','show','-o','json'],json_output=True)
        subscription=config.get('subscription_id') or azure['id'];uuid(subscription)
        require(azure['id'].lower()==subscription.lower(),'AZURE_SUBSCRIPTION_MISMATCH')
        registered=command(['az','provider','show','--namespace','Microsoft.Databricks','--query','registrationState','-o','tsv'])
        require(registered.strip() in ('Registered','NotRegistered'),'AZURE_PROVIDER_REGISTRATION_PENDING')
        self.register_provider=registered.strip()!='Registered'
        self.env['TF_VAR_subscription_id']=subscription
        self.account_env={k:v for k,v in self.env.items() if k not in ('DATABRICKS_HOST','DATABRICKS_AUTH_TYPE','DATABRICKS_AZURE_RESOURCE_ID')}
        self.metastores=self.account_api('get',f'/api/2.0/accounts/{config["account_id"]}/metastores')
        choose_metastore(self.metastores,config.get('metastore_id'),config.get('allow_create_metastore',False))
        self.ws_env=None
    def account_api(self,method,path):
        try:return command(['databricks','api',method,path,'--profile',self.c.get('account_profile','admm-account')],env=self.account_env,json_output=True)
        except Blocked:raise Blocked('ACCOUNT_ADMIN_OAUTH_OR_CONNECTIVITY_REQUIRED') from None
    def api(self,method,path):
        return command(['databricks','api',method,path],env=self.ws_env,json_output=True)
    def tf(self,root,*args,**kwargs):
        return command(['terraform','-chdir='+str(root),*args],env=self.env,**kwargs)
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
        if self.deploy:
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
            print(json.dumps({'status':'PLAN_ONLY','uc_plan':'DEFERRED_UNTIL_WORKSPACE_EXISTS'}));return None
        self.ws_env=dict(self.env,DATABRICKS_HOST=workspace['workspace_host'],DATABRICKS_AUTH_TYPE='azure-cli',
                        DATABRICKS_AZURE_RESOURCE_ID=workspace['workspace_resource_id'])
        self.init(UC);previous=self.outputs(UC).get('deployment',{}).get('value',{})
        # Azure may auto-enable UC while creating the workspace. Re-discover
        # regional metastores before ever proposing a new shared object.
        self.metastores=self.account_api('get',f'/api/2.0/accounts/{self.c["account_id"]}/metastores')
        selected,create=choose_metastore(self.metastores,self.c.get('metastore_id'),self.c.get('allow_create_metastore',False))
        if previous.get('created_metastore'):
            require(selected==previous['metastore_id'],'REGIONAL_METASTORE_CHANGED');selected=None;create=True
        assigned=self.assignment_for(workspace['workspace_id'])
        selected_effective=selected or previous.get('metastore_id')
        if assigned:require(assigned.get('metastore_id')==selected_effective,'EXISTING_ASSIGNMENT_MISMATCH')
        manage=previous.get('manages_assignment',False) or assigned is None
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


def load_config(path):
    path=Path(path).resolve()
    require(path.is_relative_to(ROOT/'work') and path.suffix=='.json','PRIVATE_CONFIG_PATH_REQUIRED')
    if not path.exists():
        config=json.loads((ROOT/'automation/silver/config.example.json').read_text())
        print('Configuración inicial (sin contraseñas): cuenta Databricks, perfil OAuth y operador autorizado.')
        config['account_id']=uuid(input('Databricks Account ID: ').strip())
        config['account_profile']=input('Perfil OAuth de cuenta [admm-account]: ').strip() or 'admm-account'
        config['operator_user_name']=input('UPN del operador autorizado: ').strip()
        require('@' in config['operator_user_name'],'OPERATOR_UPN_REQUIRED')
        answer=input('Permitir proponer un metastore nuevo solo si no existe East US (admin de cuenta) [no]: ').strip().lower()
        require(answer in ('','no','si','sí','yes'),'INVALID_METASTORE_CHOICE')
        config['allow_create_metastore']=answer in ('si','sí','yes')
        path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(config,indent=2),encoding='utf-8');path.chmod(0o600)
        print('Guardado en '+str(path)+'. No se habilita creación de metastore por defecto.')
    return json.loads(path.read_text())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--deploy',action='store_true',help='Request saved-plan and deployment approvals interactively')
    parser.add_argument('--stop',action='store_true',help='Cancel only the last recorded Silver run')
    args=parser.parse_args()
    try:
        if args.stop:
            last=json.loads((ROOT/'work/silver-deployment/last-run.json').read_text())
            env=dict(os.environ,DATABRICKS_HOST=last['host'],DATABRICKS_AUTH_TYPE='azure-cli',DATABRICKS_AZURE_RESOURCE_ID=last['workspace_resource_id'])
            command(['databricks','jobs','cancel-run',str(last['run_id']),'--no-wait'],env=env);return 0
        launch=Launcher(load_config(args.config),args.deploy)
        deployed=launch.provision()
        if deployed and args.deploy:launch.deploy_job(*deployed)
        return 0
    except KeyboardInterrupt:
        print('INTERRUPTED: use --stop to cancel a recorded running Job.',file=sys.stderr);return 130
    except Exception as exc:
        code=str(exc) if isinstance(exc,Blocked) else 'LOCAL_OR_RUNTIME_FAILURE'
        print(json.dumps({'status':'BLOCKED','code':code,'error_type':type(exc).__name__}),file=sys.stderr);return 1

if __name__=='__main__':raise SystemExit(main())

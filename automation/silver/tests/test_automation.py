"""Mocked/local tests only: no Azure/Databricks or SQL calls."""
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
import yaml
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'automation/silver'))
from plan_guard import Blocked,review,PLATFORM,UC,sha
from bundle_overlay import ephemeral_bundle
from launch import choose_metastore,validate_result,Launcher,approval,command,SOURCES


def plan(address,actions=('create',),after=None,**change):
    return {'resource_changes':[dict(address=address,mode='managed',change=dict(actions=list(actions),after=after or {},**change))]}

@pytest.mark.parametrize('actions',[['delete'],['delete','create'],['create','delete'],['forget'],['update','delete']])
def test_destroy_replace_and_unexpected_actions_blocked(actions):
    with pytest.raises(Blocked):review(plan('databricks_catalog.silver',actions),'uc')

@pytest.mark.parametrize('address',['azurerm_storage_account.data_lake','azurerm_data_factory.platform','azurerm_resource_group.platform',
 'azurerm_role_assignment.data_factory_bronze_contributor','azurerm_key_vault.platform'])
def test_existing_resources_cannot_be_changed(address):
    with pytest.raises(Blocked,match='PROTECTED'):review(plan(address,['update']),'platform')
    assert review(plan(address,['no-op']),'platform')==[]


def test_saved_plan_import_not_silently_adopted():
    with pytest.raises(Blocked,match='ADOPTION'):review(plan('databricks_catalog.silver',importing={'id':'existing'}),'uc')


def test_bronze_rbac_is_reader_at_container_only():
    address='module.databricks[0].azurerm_role_assignment.storage["bronze"]'
    after=dict(role_definition_name='Storage Blob Data Reader',scope='/subscriptions/test/blobServices/default/containers/bronze')
    assert len(review(plan(address,after=after),'platform'))==1
    for changed in [dict(role_definition_name='Storage Blob Data Contributor'),dict(scope='/subscriptions/test')]:
        bad=dict(after,**changed)
        with pytest.raises(Blocked):review(plan(address,after=bad),'platform')

@pytest.mark.parametrize('address',['databricks_storage_credential.storage["bronze"]','databricks_external_location.storage["bronze"]'])
def test_read_only_bronze_required(address):
    assert review(plan(address,after={'read_only':True}),'uc')
    with pytest.raises(Blocked):review(plan(address,after={'read_only':False}),'uc')

@pytest.mark.parametrize('address',['databricks_grant.bronze_runner','databricks_grant.receipts_runner'])
def test_no_input_volume_write_grants(address):
    assert review(plan(address,after={'privileges':['READ_VOLUME']}),'uc')
    with pytest.raises(Blocked):review(plan(address,after={'privileges':['READ_VOLUME','WRITE_VOLUME']}),'uc')


def test_do_not_create_duplicate_metastore():
    existing={'metastores':[{'metastore_info':{'metastore_id':'regional','region':'eastus'}}]}
    assert choose_metastore(existing,allow_create=True)==('regional',False)
    with pytest.raises(Blocked,match='METASTORE_REQUIRED'):choose_metastore({'metastores':[]})
    assert choose_metastore({'metastores':[]},allow_create=True)==(None,True)
    with pytest.raises(Blocked,match='AMBIGUOUS'):choose_metastore({'metastores':existing['metastores']*2})
    with pytest.raises(Blocked,match='NOT_IN_EAST_US'):choose_metastore(existing,'other')


def baseline_bundle():
    # Contract fixture from PR18; excludes its transformations and real receipts.
    return dict(bundle={'name':'adventureworks-silver'},variables={'existing_cluster_id':{}},
      artifacts={'silver_wheel':{'type':'whl'}},resources={'jobs':{'silver':{'tasks':[{
      'task_key':'transform_and_verify','existing_cluster_id':'${var.existing_cluster_id}',
      'notebook_task':{'notebook_path':'./notebooks/run_silver.py'}}]}}})


def test_ephemeral_overlay_keeps_original_unchanged():
    original=baseline_bundle();before=copy.deepcopy(original)
    uc=dict(runner_application_id='runner-id',policy_id='policy-id',node_type_id='Standard_D4ds_v5',operator='operator@example.invalid')
    spec=ephemeral_bundle(original,uc,'https://workspace.example.invalid')
    assert original==before
    job=spec['resources']['jobs']['silver'];compute=job['job_clusters'][0]['new_cluster']
    assert compute['num_workers']==0 and compute['spark_version']=='15.4.x-scala2.12'
    assert job['run_as']['service_principal_name']=='runner-id' and job['max_concurrent_runs']==1
    assert 'existing_cluster_id' not in job['tasks'][0] and 'schedule' not in job
    assert 'existing_cluster_id' not in spec['variables']


def result():
    return dict(status='SUCCEEDED',parent_run_id='parent',replay_verified=True,
                metrics={name:dict(input_rows=1,rejected_rows=0,target_rows=1,target_version=0) for name in sorted(SOURCES)})

@pytest.mark.parametrize('change',[lambda r:r.update(status='FAILED'),lambda r:r.update(replay_verified=False),
 lambda r:r['metrics'].pop('Person.Person'),lambda r:r['metrics']['Person.Person'].update(rejected_rows=1),lambda r:r['metrics']['Person.Person'].update(target_rows=-1)])
def test_acceptance_never_fabricated(change):
    r=result();change(r)
    with pytest.raises(Blocked):validate_result(r,'parent')


def test_acceptance_only_from_complete_success():assert len(validate_result(result(),'parent'))==11


def test_workspace_assignment_discovery_does_not_mask_permissions(monkeypatch):
    launch=object.__new__(Launcher);launch.c={'account_id':'account'}
    launch.metastores={'metastores':[{'metastore_info':dict(metastore_id='regional',region='eastus')}]}
    launch.account_api=lambda *args:{'workspace_ids':[123]}
    assert launch.assignment_for('123')=={'metastore_id':'regional'}
    def denied(*args):raise Blocked('DENIED')
    launch.account_api=denied
    with pytest.raises(Blocked,match='DENIED'):launch.assignment_for('123')


def test_approval_is_exact_and_hash_sensitive(monkeypatch):
    monkeypatch.setattr('builtins.input',lambda _: 'APPLY wrong')
    with pytest.raises(Blocked,match='APPROVAL_NOT_GIVEN'):approval('APPLY','correct')


def test_command_does_not_print_raw_failure_or_use_shell(monkeypatch,capsys):
    def run(args,**kwargs):
        assert not kwargs.get('shell',False)
        return NS(returncode=1,stdout='',stderr='private password token connection-string')
    monkeypatch.setattr('subprocess.run',run)
    with pytest.raises(Blocked):command(['terraform','plan'])
    assert 'private' not in capsys.readouterr().out


def test_backend_and_existing_files_untouched():
    backend=(ROOT/'terraform/environments/dev/backend.tf').read_text()
    assert 'key                  = "dev/platform.tfstate"' in backend
    uc_backend=(ROOT/'terraform/databricks/dev/backend.tf').read_text()
    assert 'dev/databricks-uc.tfstate' in uc_backend and 'admmdevtfstatef6d99ba4' in uc_backend
    main=(ROOT/'terraform/databricks/dev/main.tf').read_text()
    assert 'resource "databricks_cluster"' not in main
    assert '"cluster_type"' in main or 'cluster_type' in main
    assert 'force_destroy = true' not in main
    assert 'servicePrincipals/${databricks_service_principal.runner.application_id}' in main


def test_wheel_version_is_valid_toml():
    import re,tomllib
    # Same expression used by staging builder; no escaped quotes in TOML.
    value=re.sub(r'(?m)^version = .*$',f'version = "0.1.0+abcdef123456"','version = "0.1.0"')
    assert tomllib.loads(value)['version']=='0.1.0+abcdef123456'


def test_overlay_matches_actual_pr18_configuration():
    original=yaml.safe_load((Path(__file__).parent/'fixtures/pr18-bundle.yml').read_text())
    spec=ephemeral_bundle(original,dict(runner_application_id='00000000-0000-0000-0000-000000000001',policy_id='policy',node_type_id='Standard_D4ds_v5',operator='operator@example.invalid'),'https://workspace.example.invalid')
    task=spec['resources']['jobs']['silver']['tasks'][0]
    assert spec['sync']['include']==['notebooks/run_silver.py']
    assert task['notebook_task']['source']=='WORKSPACE'
    assert task['notebook_task']['notebook_path']=='./notebooks/run_silver.py'
    assert task['notebook_task']['base_parameters']['replay_check']=='true'
    assert 'whl' in task['libraries'][0]


def test_config_wizard_never_stores_passwords(tmp_path,monkeypatch):
    import launch
    monkeypatch.setattr(launch,'ROOT',tmp_path)
    source=tmp_path/'automation/silver';source.mkdir(parents=True)
    source.joinpath('config.example.json').write_text((ROOT/'automation/silver/config.example.json').read_text())
    replies=iter(['00000000-0000-0000-0000-000000000001','','operator@example.invalid',''])
    monkeypatch.setattr('builtins.input',lambda _:next(replies))
    config=launch.load_config(tmp_path/'work/config.json',overrides={'uc_mode':'account'})
    assert config['allow_create_metastore'] is False
    assert not any(key in config for key in ('password','token','client_secret'))
    with pytest.raises(Blocked):launch.load_config(tmp_path/'tracked-config.json')


def test_azure_provider_cannot_implicitly_register_on_plan():
    provider=(ROOT/'terraform/environments/dev/providers.tf').read_text()
    assert 'resource_provider_registrations = "none"' in provider


def mock_plan_launcher(tmp_path,deploy):
    launch=object.__new__(Launcher);launch.work=tmp_path;launch.deploy=deploy;calls=[]
    def tf(root,*args,**kwargs):
        calls.append(args)
        if args[0]=='plan':
            path=Path(next(a[5:] for a in args if a.startswith('-out=')));path.write_bytes(b'approved-plan')
        elif args[0]=='show':
            return plan('module.databricks[0].azurerm_databricks_workspace.silver') if '-json' in args else 'human-readable reviewed plan'
        return ''
    launch.tf=tf
    return launch,calls


def test_default_plan_never_applies_or_requests_approval(tmp_path,monkeypatch):
    monkeypatch.setattr('builtins.input',lambda _:pytest.fail('Plan-only must not request deployment'))
    launch,calls=mock_plan_launcher(tmp_path,False)
    assert launch.plan(ROOT,'platform',{}) is False
    assert not any(c[0]=='apply' for c in calls)


def test_changed_saved_plan_cannot_be_applied(tmp_path,monkeypatch):
    import launch as module
    instance,calls=mock_plan_launcher(tmp_path,True)
    monkeypatch.setattr(module,'approval',lambda *args:next(tmp_path.rglob('plan.tfplan')).write_bytes(b'changed'))
    with pytest.raises(Blocked,match='PLAN_CHANGED_AFTER_APPROVAL'):instance.plan(ROOT,'platform',{})
    assert not any(c[0]=='apply' for c in calls)


def test_declared_resource_provider_registration_is_explicit():
    address='module.databricks[0].azurerm_resource_provider_registration.databricks[0]'
    assert review(plan(address,after={'name':'Microsoft.Databricks'}),'platform')
    with pytest.raises(Blocked):review(plan(address,after={'name':'Microsoft.Other'}),'platform')


@pytest.mark.parametrize('workspace_mode',[False,True])
@pytest.mark.parametrize('locked_cleanup',[False,True])
def test_job_orchestration_with_simulated_cli_only(tmp_path,monkeypatch,workspace_mode,locked_cleanup):
    """Synthetic control-flow test; does not run SQL, build a real wheel or call Azure."""
    import launch as module
    from types import ModuleType
    parent='10000000-0000-0000-0000-000000000001'
    repo=tmp_path/'repo';source=repo/'databricks/silver';source.mkdir(parents=True)
    (source/'src').mkdir();(source/'src/example.py').write_text('# synthetic source')
    (source/'tools').mkdir();(source/'tools/export_committed_parent.py').write_text('# never executed')
    (source/'pyproject.toml').write_text('[project]\nname="synthetic"\nversion = "0.1.0"\n')
    (source/'databricks.yml').write_text((Path(__file__).parent/'fixtures/pr18-bundle.yml').read_text())
    instance=object.__new__(Launcher);instance.work=repo/'work';instance.work.mkdir()
    instance.c={'parent_run_id':parent};instance.env={};instance.ws_env={}
    if workspace_mode:instance.c['uc_mode']='workspace'
    uc=dict(catalog='admm_dev',schema='silver',bronze_volume='/Volumes/admm_dev/inputs/bronze',
      receipt_volume='/Volumes/admm_dev/inputs/receipts',metastore_id='synthetic-region',runner_application_id='runner',operator='operator@example.invalid',policy_id='policy',node_type_id='Standard_D4ds_v5')
    stage=instance.work/'bundle';(stage/'dist').mkdir(parents=True)
    (stage/'dist/stale.whl').write_bytes(b'old wheel must not be used')
    (stage/'.databricks').mkdir();(stage/'.databricks/state-marker').write_text('keep deployment identity')
    if locked_cleanup:
        def windows_denied(*args,**kwargs):raise PermissionError('[WinError 5] Acceso denegado')
        monkeypatch.setattr(module.shutil,'rmtree',windows_denied)
    seen=[]
    def cli(args,**kwargs):
        args=list(map(str,args));seen.append(args)
        if args[:2]==['git','log']:return '1700000000'
        if '-m' in args and 'build' in args:
            import tomllib
            assert tomllib.loads((Path(kwargs['cwd'])/'pyproject.toml').read_text())['project']['version'].startswith('0.1.0+')
            dist=Path(args[args.index('--outdir')+1]);(dist/'synthetic.whl').write_bytes(b'synthetic artifact, not a wheel')
        elif 'validate' in args:
            spec=yaml.safe_load((stage/'databricks.yml').read_text())
            selected=spec['artifacts']['silver_wheel']['files'][0]['source']
            assert selected.startswith('./dist/wheel-') and '*' not in selected
            assert spec['resources']['jobs']['silver']['tasks'][0]['libraries']==[{'whl':selected}]
            assert (stage/selected).read_bytes()==b'synthetic artifact, not a wheel'
            return dict(workspace={'root_path':'/Workspace/Shared/admm_dev/admm-silver/dev'},resources={'jobs':{'silver':{'name':'adventureworks-silver-dev'}}})
        elif any('export_committed_parent.py' in a for a in args):
            assert kwargs['interactive'] is True
            Path(args[args.index('--output')+1]).write_text(json.dumps({'parent_run_id':parent}))
        elif args[1:3]==['fs','ls']:return []
        elif args[1:3]==['bundle','summary']:return {'resources':{'jobs':{'silver':{'id':'7'}}}}
        elif args[1:3]==['jobs','get']:return {'settings':{'tasks':[{'task_key':'transform_and_verify','notebook_task':{'notebook_path':'/Workspace/Shared/admm_dev/admm-silver/dev/files/notebooks/run_silver'},'libraries':[{'whl':'/Workspace/Shared/admm_dev/admm-silver/dev/artifacts/code.whl'}]}]}}
        elif args[1:3]==['permissions','get']:return {'access_control_list':[{'service_principal_name':'runner','all_permissions':[{'permission_level':'CAN_READ','inherited':True}]}]}
        elif args[1:3]==['workspace','get-status']:return {'object_type':'NOTEBOOK' if args[3].endswith('run_silver') else 'FILE' if args[3].endswith('.whl') else 'DIRECTORY','object_id':1}
        elif args[1:3]==['jobs','run-now']:return {'run_id':37}
        return ''
    def api(method,path):
        if '/jobs/list?' in path:return {'jobs':[]}
        if '/runs/get?' in path:return {'state':{'life_cycle_state':'TERMINATED','result_state':'SUCCESS'},'tasks':[{'task_key':'transform_and_verify','run_id':38}]}
        if '/get-output?' in path:
            r=result();r['parent_run_id']=parent
            return {'notebook_output':{'result':json.dumps(r)}}
        pytest.fail('Unexpected simulated API')
    pkg=ModuleType('admm_silver');pkg.__path__=[];contract=ModuleType('admm_silver.contracts')
    contract.validate_bundle=lambda data,account:None # real contract belongs to PR18's own tests
    monkeypatch.setitem(sys.modules,'admm_silver',pkg);monkeypatch.setitem(sys.modules,'admm_silver.contracts',contract)
    monkeypatch.setattr(module,'ROOT',repo);monkeypatch.setattr(module,'command',cli)
    monkeypatch.setattr(module,'approval',lambda *args:None);instance.api=api
    instance.verify_workspace_run_as=lambda *args:seen.append(['run-as-recheck'])
    instance.deploy_job({'workspace_host':'https://workspace.example.invalid','workspace_resource_id':'synthetic-arm-id'},uc)
    accepted=json.loads((instance.work/'acceptance.json').read_text())
    assert (stage/'dist/stale.whl').read_bytes()==b'old wheel must not be used'
    assert (stage/'.databricks/state-marker').read_text()=='keep deployment identity'
    assert accepted['run_id']==37 and accepted['parent_run_id']==parent and len(accepted['metrics'])==11
    assert len([args for args in seen if args[1:4]==['permissions','update','directories']])==2
    status_paths=[args[3] for args in seen if args[1:3]==['workspace','get-status']]
    assert '/Shared/admm_dev/admm-silver/dev' not in status_paths
    preflight=max(i for i,args in enumerate(seen) if args[1:3]==['permissions','get'])
    submitted=next(i for i,args in enumerate(seen) if args[1:3]==['jobs','run-now'])
    assert preflight<submitted
    assert not any('--overwrite' in args for args in seen)
    if workspace_mode:
        delegated=next(i for i,c in enumerate(seen) if c==['run-as-recheck'])
        deployed=next(i for i,c in enumerate(seen) if c[1:3]==['bundle','deploy'])
        assert delegated<deployed
    assert not any('adf' in a.lower() for args in seen for a in args)


def test_deployment_approval_covers_notebook_code(tmp_path):
    from launch import deployment_sha
    (tmp_path/'databricks.yml').write_text(yaml.safe_dump({'artifacts':{'silver_wheel':{'files':[{'source':'./dist/code.whl'}]}},'resources':{'jobs':{'silver':{'tasks':[{'libraries':[{'whl':'./dist/code.whl'}]}]}}}}))
    (tmp_path/'verify_artifact.py').write_text('# fixed verifier')
    (tmp_path/'notebooks').mkdir();notebook=tmp_path/'notebooks/run.py';notebook.write_text('# approved code')
    (tmp_path/'dist').mkdir();(tmp_path/'dist/code.whl').write_bytes(b'synthetic artifact')
    before=deployment_sha(tmp_path);notebook.write_text('# edited after approval')
    assert deployment_sha(tmp_path)!=before


def test_partial_uc_state_can_be_recovered_without_outputs():
    from launch import state_resources
    # Terraform may not persist computed outputs after a failed partial apply.
    state={'values':{'root_module':{'resources':[
      {'address':'databricks_metastore.regional[0]','values':{'id':'regional'}},
      {'address':'databricks_metastore_assignment.workspace[0]','values':{'metastore_id':'regional'}}]}}}
    owned=state_resources(state)
    assert owned['databricks_metastore.regional[0]']['id']=='regional'
    assert 'databricks_metastore_assignment.workspace[0]' in owned
    assert state_resources({'format_version':'1.0'})=={}

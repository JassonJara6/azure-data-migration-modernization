"""Synthetic workspace-only orchestration and security checks. No cloud calls."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import launch
from plan_guard import Blocked, review
from workspace_uc import (ACCOUNT_OWNED, ensure_workspace_state, profile_environment,
                          verify_assignment, verify_runner, verify_run_as)

META = 'b67792ab-c53b-48b4-ae57-fc45b869ed7a'
APP = '00000000-0000-0000-0000-000000000003'
ACCOUNT = '00000000-0000-0000-0000-000000000004'
WS = {'workspace_id': '7405617270738925', 'workspace_host': 'https://workspace.example.invalid',
      'workspace_resource_id': 'synthetic-arm-workspace', 'access_connector_id': 'synthetic-connector',
      'storage_account_name': 'stadmmdevdlf6d99ba4'}
OP = 'operator@example.invalid'


def api_reader(*, assignment=None, me=None, runner=None, rules=None):
    calls = []
    def api(method, path):
        assert method == 'get'
        calls.append(path)
        if 'current-metastore-assignment' in path:
            return assignment if assignment is not None else {'workspace_id': WS['workspace_id'], 'metastore_id': META}
        if path.endswith('/Me'):
            return me if me is not None else {'userName': OP, 'active': True, 'groups': [{'display': 'admins'}]}
        if 'ServicePrincipals?' in path:
            return runner if runner is not None else {'totalResults': 1, 'Resources': [
                {'applicationId': APP, 'active': True, 'entitlements': [{'value': 'workspace-access'}]}]}
        if 'access-control/rule-sets?' in path:
            return rules if rules is not None else {'name': f'accounts/{ACCOUNT}/servicePrincipals/{APP}/ruleSets/default',
                'grant_rules': [{'role': 'roles/servicePrincipal.user', 'principals': ['users/' + OP]}]}
        pytest.fail('Unexpected synthetic endpoint: ' + path)
    return api, calls


def workspace_launcher(tmp_path, *, deploy=False, stage='uc', runner=None, owned=None):
    obj = object.__new__(launch.Launcher)
    obj.c = {'uc_mode': 'workspace', 'workspace_profile': 'admm-workspace', 'metastore_id': META,
             'runner_application_id': runner}
    obj.deploy = deploy; obj.stage = stage; obj.env = {}; obj.work = tmp_path; obj.register_provider = False
    obj.api, calls = api_reader()
    obj.init = lambda root: calls.append(('init', root))
    def tf(root, *args, **kwargs):
        calls.append((root, args))
        if args[:2] == ('state', 'list'):
            return '\n'.join(['azurerm_resource_group.platform','azurerm_storage_account.data_lake','azurerm_data_factory.platform'])
        if args[0] == 'show':
            return {'values': {'root_module': {'resources': [{'address': a} for a in (owned or [])]}}}
        pytest.fail('Unexpected Terraform command')
    obj.tf = tf
    def outputs(root):
        if root == launch.PLATFORM:
            return {'resource_group_name': {'value': 'rg-admm-dev-platform'},
                    'storage_account_name': {'value': 'stadmmdevdlf6d99ba4'}, 'silver_workspace': {'value': WS}}
        return {'deployment': {'value': dict(provisioning_mode='workspace', runner_application_id=runner, metastore_id=META)}}
    obj.outputs = outputs
    obj.plan = lambda root, stage, variables, **kwargs: calls.append(('plan', root, stage, variables)) or False
    obj.account_api = lambda *a: pytest.fail('Workspace path must never call account APIs')
    obj.deploy_job = lambda *a: pytest.fail('No Jobs in UC stage')
    return obj, calls


def test_uc_plan_uses_existing_workspace_no_azure_plan_or_account_id(tmp_path):
    obj, calls = workspace_launcher(tmp_path)
    assert obj.provision() is None
    plans = [c for c in calls if isinstance(c, tuple) and c[0] == 'plan']
    assert len(plans) == 1 and plans[0][1:3] == (launch.UC_WORKSPACE, 'uc-workspace')
    variables = plans[0][3]
    assert variables['operator_user_name'] == OP and variables['metastore_id'] == META
    assert variables['workspace_profile'] == 'admm-workspace'
    assert not {'account_id','create_metastore','manage_assignment'}.intersection(variables)
    assert all(not (isinstance(c, str) and '/accounts/' in c) for c in calls)


@pytest.mark.parametrize('bad', [dict(workspace_id='wrong',metastore_id=META), dict(workspace_id=WS['workspace_id'],metastore_id='wrong')])
def test_profile_and_metastore_mismatch_never_plan(bad):
    api, _ = api_reader(assignment=bad)
    with pytest.raises(Blocked): verify_assignment(api, WS, META)


def test_profile_identity_cannot_silently_change():
    api, _ = api_reader()
    with pytest.raises(Blocked, match='IDENTITY_MISMATCH'): verify_assignment(api, WS, META, 'different@example.invalid')


def test_workspace_api_denied_is_not_assumed_missing_or_empty():
    def denied(*args): raise Blocked('ACCESS_DENIED')
    with pytest.raises(Blocked, match='ACCESS_DENIED'): verify_assignment(denied, WS, META)


def test_workspace_profile_overrides_ambient_account_auth_without_tokens():
    env = profile_environment({'DATABRICKS_HOST': 'https://accounts.example.invalid', 'DATABRICKS_TOKEN': 'synthetic-secret',
                              'DATABRICKS_AUTH_TYPE': 'pat', 'DATABRICKS_ACCOUNT_ID': ACCOUNT, 'PATH': 'keep'}, 'admm-workspace', WS)
    assert env == {'PATH': 'keep', 'DATABRICKS_HOST': WS['workspace_host'], 'DATABRICKS_CONFIG_PROFILE': 'admm-workspace'}


def test_cli_receives_profile_for_all_workspace_commands(monkeypatch):
    seen=[]
    monkeypatch.setattr(launch.subprocess, 'run', lambda args, **kw: seen.append(args) or NS(returncode=0, stdout='{}'))
    launch.command(['databricks','api','get','/synthetic'],env={'DATABRICKS_CONFIG_PROFILE':'admm-workspace'},json_output=True)
    assert seen[0][-2:] == ['--profile','admm-workspace']


@pytest.mark.parametrize('address', sorted(ACCOUNT_OWNED))
def test_account_managed_state_never_switched_automatically(address):
    with pytest.raises(Blocked, match='MIGRATION'): ensure_workspace_state({address:{}})


def test_state_guard_runs_before_any_uc_plan(tmp_path):
    obj, calls = workspace_launcher(tmp_path, owned=['databricks_service_principal.runner'])
    with pytest.raises(Blocked, match='MIGRATION'): obj.provision()
    assert not any(isinstance(c, tuple) and c[0]=='plan' for c in calls)


def test_existing_foundation_state_is_reused():
    ensure_workspace_state({'databricks_catalog.silver':{}, 'databricks_volume.bronze':{}})


def test_foundation_apply_mode_does_not_require_runner_or_run_as(tmp_path, capsys):
    obj, calls = workspace_launcher(tmp_path, deploy=True)
    obj.plan = lambda root, stage, variables: True # Simulated approval/application, no Terraform invocation.
    assert obj.provision() is None
    assert json.loads(capsys.readouterr().out)['silver'].startswith('BLOCKED_EXISTING_RUNNER')
    assert not any(isinstance(c,str) and ('access-control/' in c or 'ServicePrincipals?' in c) for c in calls)


def test_silver_deploy_without_runner_is_blocked_before_plan(tmp_path):
    obj, calls = workspace_launcher(tmp_path, deploy=True, stage='silver')
    with pytest.raises(Blocked, match='EXISTING_RUNNER_REQUIRED'): obj.provision()
    assert not any(isinstance(c,tuple) and c[0]=='plan' for c in calls)


@pytest.mark.parametrize('response', [{}, {'totalResults':0,'Resources':[]},
 {'totalResults':2,'Resources':[]}, {'totalResults':1,'Resources':[{'applicationId':APP,'active':False}]},
 {'totalResults':1,'Resources':[{'applicationId':APP,'active':True}]},
 {'totalResults':1,'Resources':[{'applicationId':'wrong','active':True,'entitlements':[{'value':'workspace-access'}]}]}])
def test_runner_not_assumed_active_or_assigned(response):
    api,_=api_reader(runner=response)
    with pytest.raises(Blocked): verify_runner(api, APP)


def test_existing_runner_never_created_or_modified():
    api,calls=api_reader()
    assert verify_runner(api, APP)['applicationId']==APP
    assert len(calls)==1


def test_run_as_role_read_via_workspace_proxy_without_account_login():
    api,calls=api_reader()
    verify_run_as(api,ACCOUNT,APP,OP)
    assert calls[0].startswith('/api/2.0/preview/accounts/access-control/rule-sets?')
    assert 'name=accounts%2F' in calls[0]


@pytest.mark.parametrize('rules', [[], [{'role':'roles/servicePrincipal.manager','principals':['users/'+OP]}],
 [{'role':'roles/servicePrincipal.user','principals':['groups/admins']}],
 [{'role':'roles/servicePrincipal.user','principals':['users/different@example.invalid']}]])
def test_admin_manager_or_unproven_group_membership_does_not_substitute_run_as(rules):
    api,_=api_reader(rules={'name':f'accounts/{ACCOUNT}/servicePrincipals/{APP}/ruleSets/default','grant_rules':rules})
    with pytest.raises(Blocked,match='USER_GRANT_NOT_PROVEN'):verify_run_as(api,ACCOUNT,APP,OP)


def test_role_ruleset_identity_must_match_target():
    api,_=api_reader(rules={'name':'different','grant_rules':[{'role':'roles/servicePrincipal.user','principals':['users/'+OP]}]})
    with pytest.raises(Blocked,match='RULESET_IDENTITY'):verify_run_as(api,ACCOUNT,APP,OP)


def test_run_as_metadata_and_role_read_remain_explicit_blocks():
    api,_=api_reader()
    with pytest.raises(Blocked,match='NONSECRET_ACCOUNT_ID'):verify_run_as(api,None,APP,OP)
    def denied(*args):raise Blocked('ACCESS_DENIED')
    with pytest.raises(Blocked,match='DELEGATED_SP_MANAGER'):verify_run_as(denied,ACCOUNT,APP,OP)


def test_workspace_mode_config_never_prompts_for_account(tmp_path, monkeypatch):
    monkeypatch.setattr(launch,'ROOT',tmp_path)
    path=tmp_path/'work/dev.json';path.parent.mkdir();path.write_text(json.dumps({'account_id':None,'metastore_id':META,'node_type_id':'Standard_D4as_v5'}))
    monkeypatch.setattr('builtins.input',lambda _:pytest.fail('No account prompt'))
    c=launch.load_config(path,overrides={'uc_mode':'workspace','workspace_profile':'admm-workspace'})
    assert c['account_id'] is None and c['node_type_id']=='Standard_D4as_v5'


def saved_plan(address,actions,after=None,**extra):
    return {'resource_changes':[{'address':address,'mode':'managed','change':dict(actions=actions,after=after or {},**extra)}]}


@pytest.mark.parametrize('actions', [['delete'],['create','delete'],['delete','create']])
def test_workspace_destroy_replace_guard(actions):
    with pytest.raises(Blocked,match='DESTROY'):review(saved_plan('databricks_catalog.silver',actions),'uc-workspace')


def test_workspace_stage_cannot_manage_account_or_azure_resources():
    for address in (*ACCOUNT_OWNED,'azurerm_resource_group.platform'):
        with pytest.raises(Blocked,match='PROTECTED'):review(saved_plan(address,['create']),'uc-workspace')


def test_input_write_grants_and_noop_imports_stay_blocked():
    with pytest.raises(Blocked,match='INPUT_WRITE'):review(saved_plan('databricks_grant.bronze_runner[0]',['create'],{'privileges':['WRITE_VOLUME']}),'uc-workspace')
    with pytest.raises(Blocked,match='ADOPTION'):review(saved_plan('databricks_catalog.silver',['no-op'],importing={'id':'existing'}),'uc-workspace')
    assert review(saved_plan('databricks_grant.receipts_runner[0]',['create'],{'privileges':['READ_VOLUME']}),'uc-workspace')


def test_runner_uc_configuration_does_not_imply_verified_run_as(tmp_path,capsys):
    obj,calls=workspace_launcher(tmp_path,deploy=True,runner=APP)
    obj.plan=lambda *args:True
    obj.check_workspace=lambda output:None # Synthetic UC verification, not a real service response.
    assert obj.provision() is None
    assert json.loads(capsys.readouterr().out)['silver']=='NOT_DEPLOYED_RUN_AS_NOT_VERIFIED'
    assert not any(isinstance(c,str) and 'access-control/' in c for c in calls)


def test_silver_plan_and_deploy_role_preflight_are_separate(tmp_path):
    obj,calls=workspace_launcher(tmp_path,deploy=True,stage='silver',runner=APP)
    with pytest.raises(Blocked,match='NONSECRET_ACCOUNT_ID'):obj.provision()
    assert not any(isinstance(c,tuple) and c[0]=='plan' for c in calls)
    obj.c['account_id']=ACCOUNT
    assert obj.provision() is None # Real plan is mocked as changed/not applied.
    assert any(isinstance(c,str) and 'access-control/' in c for c in calls)
    assert any(isinstance(c,tuple) and c[0]=='plan' for c in calls)


def test_create_metastore_flag_cannot_leak_from_legacy_config(tmp_path):
    obj,calls=workspace_launcher(tmp_path)
    obj.c['allow_create_metastore']=True
    with pytest.raises(Blocked,match='CANNOT_CREATE_METASTORE'):obj.provision()
    assert not any(isinstance(c,tuple) and c[0]=='plan' for c in calls)


def test_missing_metastore_message_points_to_configuration(tmp_path):
    obj,_=workspace_launcher(tmp_path);obj.c['metastore_id']=None
    with pytest.raises(Blocked,match='USE_METASTORE_ID_OPTION'):obj.provision()


def test_workspace_saved_plan_hash_is_rechecked_before_apply(tmp_path,monkeypatch):
    obj=object.__new__(launch.Launcher);obj.work=tmp_path;obj.deploy=True;calls=[]
    def tf(root,*args,**kwargs):
        calls.append(args)
        if args[0]=='plan':
            Path(next(a[5:] for a in args if a.startswith('-out='))).write_bytes(b'original-synthetic-plan')
        elif args[0]=='show':
            return saved_plan('databricks_catalog.silver',['create']) if '-json' in args else 'synthetic review'
        return ''
    obj.tf=tf
    monkeypatch.setattr(launch,'approval',lambda *a:next(tmp_path.rglob('plan.tfplan')).write_bytes(b'changed'))
    with pytest.raises(Blocked,match='PLAN_CHANGED_AFTER_APPROVAL'):obj.plan(launch.UC_WORKSPACE,'uc-workspace',{})
    assert not any(c[0]=='apply' for c in calls)


def test_same_uc_backend_key_and_no_account_provider_in_new_root():
    root=Path(__file__).resolve().parents[3]
    text=(root/'terraform/databricks/workspace/providers.tf').read_text()
    assert 'alias     = "workspace"' in text and 'account_id' not in text and 'accounts.azuredatabricks.net' not in text
    backend=(root/'terraform/databricks/workspace/backend.tf').read_text()
    assert 'key                  = "dev/databricks-uc.tfstate"' in backend
    assert 'key                  = "dev/platform.tfstate"' not in backend
    lock=(root/'terraform/databricks/workspace/.terraform.lock.hcl').read_text()
    assert 'version = "1.110.0"' in text and 'version     = "1.110.0"' in lock
    assert 'version     = "1.95.0"' in (root/'terraform/databricks/dev/.terraform.lock.hcl').read_text()


def test_account_mode_cannot_take_over_partial_workspace_state(tmp_path):
    obj,calls=workspace_launcher(tmp_path,owned=['data.databricks_current_metastore.assigned','databricks_catalog.silver'])
    obj.c.update(uc_mode='account',account_id=ACCOUNT,operator_user_name=OP)
    with pytest.raises(Blocked,match='USE_WORKSPACE_MODE'):obj.provision()
    assert not any(isinstance(c,tuple) and c[0]=='plan' for c in calls)


def test_uc_stage_never_deploys_job_even_if_provision_returns_outputs(monkeypatch):
    monkeypatch.setattr(sys,'argv',['launch.py','--stage','uc','--deploy'])
    monkeypatch.setattr(launch,'load_config',lambda *a,**k:{})
    class UCOnly:
        def __init__(self,*args,**kwargs):pass
        def provision(self):return WS,{}
        def deploy_job(self,*args):pytest.fail('UC stage cannot run a Job')
    monkeypatch.setattr(launch,'Launcher',UCOnly)
    assert launch.main()==0


def test_stop_reuses_recorded_workspace_oauth_profile(tmp_path,monkeypatch):
    monkeypatch.setattr(launch,'ROOT',tmp_path)
    monkeypatch.setattr(sys,'argv',['launch.py','--stop'])
    folder=tmp_path/'work/silver-deployment';folder.mkdir(parents=True)
    folder.joinpath('last-run.json').write_text(json.dumps({'host':WS['workspace_host'],'run_id':37,
        'workspace_resource_id':WS['workspace_resource_id'],'workspace_profile':'admm-workspace'}))
    seen=[]
    monkeypatch.setattr(launch,'command',lambda args,**kw:seen.append((args,kw)))
    assert launch.main()==0
    assert seen[0][0][1:4]==['jobs','cancel-run','37']
    env=seen[0][1]['env']
    assert env['DATABRICKS_CONFIG_PROFILE']=='admm-workspace'
    assert env['DATABRICKS_HOST']==WS['workspace_host'] and 'DATABRICKS_AUTH_TYPE' not in env


def test_existing_uc_owners_cannot_be_silently_overwritten():
    with pytest.raises(Blocked,match='OWNER_MISMATCH'):
        ensure_workspace_state({'databricks_catalog.silver':{'owner':'other@example.invalid'}},OP)
    ensure_workspace_state({'databricks_catalog.silver':{'owner':OP.upper()}},OP)


def test_workspace_silver_rechecks_run_as_after_approval_callback(tmp_path):
    obj,calls=workspace_launcher(tmp_path,deploy=True,stage='silver',runner=APP)
    obj.c['account_id']=ACCOUNT
    original=obj.api;revoked=False
    def api(method,path):
        if revoked and 'access-control/rule-sets?' in path:
            return {'name':f'accounts/{ACCOUNT}/servicePrincipals/{APP}/ruleSets/default','grant_rules':[]}
        return original(method,path)
    obj.api=api
    def plan(root,stage,variables,*,before_apply=None):
        nonlocal revoked
        assert callable(before_apply)
        revoked=True # Synthetic revocation while the user reviewed the plan.
        before_apply()
        pytest.fail('Revocation must block before apply')
    obj.plan=plan
    with pytest.raises(Blocked,match='USER_GRANT_NOT_PROVEN'):obj.provision()


def test_artifact_publication_reads_existing_uc_without_plan_apply(tmp_path):
    obj,calls=workspace_launcher(tmp_path,deploy=True,stage='silver',runner=APP)
    obj.deploy_only=True;obj.c['account_id']=ACCOUNT
    original=obj.outputs
    obj.outputs=lambda root:original(root) if root==launch.PLATFORM else {'deployment':{'value':dict(runner_application_id=APP,metastore_id=META,node_type_id='Standard_D4ds_v5',catalog='admm_dev',operator=OP)}}
    obj.plan=lambda *a,**k:pytest.fail('No Terraform plan/apply for artifact-only mode')
    obj.check_workspace=lambda *a:None
    assert obj.provision()[0]==WS
    assert not any(isinstance(c,tuple) and c[0]=='plan' for c in calls)


def test_artifact_publication_config_drift_blocks(tmp_path):
    obj,calls=workspace_launcher(tmp_path,deploy=True,stage='silver',runner=APP)
    obj.deploy_only=True;obj.c['account_id']=ACCOUNT
    original=obj.outputs
    obj.outputs=lambda root:original(root) if root==launch.PLATFORM else {'deployment':{'value':dict(runner_application_id=APP,metastore_id=META,node_type_id='different',catalog='admm_dev',operator=OP)}}
    with pytest.raises(Blocked,match='EXISTING_UC_OUTPUT_CONFIG_MISMATCH'):obj.provision()

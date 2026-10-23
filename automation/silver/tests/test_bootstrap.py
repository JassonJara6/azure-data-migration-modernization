"""Offline identity/bootstrap regressions; responses below are synthetic."""
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import diagnose as doctor
import launch
from plan_guard import Blocked


def synthetic_reader(*, graph_denied=False, overrides=None):
    calls = []
    def reader(args):
        calls.append(args)
        key = ' '.join(args)
        for marker, response in (overrides or {}).items():
            if marker in key:
                return response
        if args[1:3] == ['account', 'show']:
            return {'id': doctor.SUBSCRIPTION, 'tenantId': doctor.TENANT, 'user': {'type': 'user'}}, None
        if 'graph.microsoft.com' in key:
            if graph_denied:
                return None, {'code': 'ACCESS_DENIED'}
            if '/me?' in key:
                return {'id': doctor.TENANT, 'userType': 'Guest'}, None
            if '/policies/' in key:
                return {'isEnabled': True}, None
            return {'value': [{'status': {'errorCode': 530035}}]}, None
        if '/permissions?' in key:
            return {'value': [{'actions': ['*'], 'notActions': []}]}, None
        if '/workspaces?' in key:
            return {'value': []}, None
        if '/providers/Microsoft.Databricks?' in key:
            return {'registrationState': 'NotRegistered'}, None
        if '/resourceGroups/' in key:
            return {'location': 'eastus'}, None
        return {'state': 'Enabled', 'tenantId': doctor.TENANT}, None
    return reader, calls


def test_arm_only_msa_does_not_depend_on_graph_or_databricks():
    reader, calls = synthetic_reader()
    report = doctor.diagnose(reader=reader)
    assert report['status'] == 'AZURE_ACCESS_OBSERVED'
    assert report['workspace_bootstrap_preflight'] == 'ALLOW_ACTIONS_OBSERVED'
    assert report['checks']['workspaces']['count_in_first_page'] == 0
    assert not report['checks']['databricks_provider']['registered']
    assert all(c[0] == 'az' for c in calls)
    assert all(c[1:3] in (['account', 'show'], ['rest', '--method']) for c in calls)
    assert all('graph.microsoft.com' not in ' '.join(c) for c in calls)
    assert not any('signed-in-user' in c or 'get-access-token' in c for c in calls)
    assert all(c[c.index('--method') + 1] == 'get' for c in calls if '--method' in c)


def test_optional_graph_denied_does_not_block_arm():
    reader, _ = synthetic_reader(graph_denied=True)
    report = doctor.diagnose(graph=True, reader=reader)
    assert report['status'] == 'AZURE_ACCESS_OBSERVED'
    assert report['checks']['directory_identity']['code'] == 'ACCESS_DENIED'
    assert report['checks']['security_defaults']['status'] == 'UNAVAILABLE'


def test_correlated_graph_event_no_raw_pii_or_status_text():
    reader, calls = synthetic_reader()
    report = doctor.diagnose(graph=True, correlation_id=doctor.TENANT, reader=reader)
    assert report['checks']['sign_in_events']['security_defaults_block_observed']
    query = next(c[c.index('--url') + 1] for c in calls if 'auditLogs/signIns?' in ' '.join(c))
    assert '%24top=5' in query and doctor.TENANT in query
    assert 'failureReason' not in json.dumps(report)


@pytest.mark.parametrize('field,value', [('id', doctor.TENANT), ('tenantId', doctor.SUBSCRIPTION)])
def test_context_mismatch_stops_before_arm(field, value):
    account = {'id': doctor.SUBSCRIPTION, 'tenantId': doctor.TENANT, field: value}
    reader, calls = synthetic_reader(overrides={'account show': (account, None)})
    report = doctor.diagnose(reader=reader)
    assert report['status'] == 'BLOCKED' and len(calls) == 1


def test_no_login_no_downstream_calls():
    reader, calls = synthetic_reader(overrides={'account show': (None, {'code': 'SECURITY_DEFAULTS_BLOCK'})})
    assert doctor.diagnose(reader=reader)['status'] == 'BLOCKED'
    assert len(calls) == 1


@pytest.mark.parametrize('state,tenant', [('Disabled', doctor.TENANT), ('Enabled', doctor.SUBSCRIPTION)])
def test_subscription_state_and_arm_tenant_must_match(state, tenant):
    reader, _ = synthetic_reader(overrides={'?api-version=2022-12-01': ({'state': state, 'tenantId': tenant}, None)})
    assert doctor.diagnose(reader=reader)['status'] == 'BLOCKED'


def test_missing_permission_never_claims_bootstrap_ready():
    reader, _ = synthetic_reader(overrides={'/permissions?': ({'value': [{'actions': ['*/read']}]}, None)})
    report = doctor.diagnose(reader=reader)
    assert report['status'] == 'AZURE_ACCESS_OBSERVED'
    assert report['workspace_bootstrap_preflight'] == 'NOT_VERIFIED'


def test_notactions_and_multiple_grants():
    grant = {'actions': ['Microsoft.Databricks/*'], 'notActions': ['*/write']}
    assert not doctor.permits([grant], 'Microsoft.Databricks/workspaces/write')
    assert doctor.permits([grant], 'Microsoft.Databricks/workspaces/read')
    assert doctor.permits([grant, {'actions': ['*']}], 'Microsoft.Databricks/workspaces/write')
    assert doctor.permits([{'actions': ['MICROSOFT.DATABRICKS/*']}], 'Microsoft.Databricks/workspaces/write')


@pytest.mark.parametrize('text,code', [
    ('password=secret token=private AADSTS530035', 'SECURITY_DEFAULTS_BLOCK'),
    ('AADSTS53003', 'ENTRA_AUTHENTICATION_FAILED'),
    ('AuthorizationFailed Bearer secret', 'ARM_AUTHORIZATION_DENIED'),
    ('InvalidAuthenticationTokenTenant', 'ARM_TOKEN_TENANT_MISMATCH'),
    ('SubscriptionNotFound', 'SUBSCRIPTION_NOT_VISIBLE'),
    ('opaque secret and https://private.example', 'COMMAND_FAILED'),
])
def test_allowlisted_error_codes_only(text, code):
    result = doctor.safe_failure(text)
    assert result['code'] == code
    assert all(s not in json.dumps(result) for s in ['secret', 'private', 'Bearer', 'https://'])


def test_subprocess_timeout_and_missing_cli_safe(monkeypatch):
    monkeypatch.setattr(doctor.shutil, 'which', lambda _: None)
    assert doctor.read_json(['az'])[1]['code'] == 'TOOL_MISSING'
    monkeypatch.setattr(doctor.shutil, 'which', lambda _: 'az')
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired('az', 60, output='secret', stderr='secret')
    monkeypatch.setattr(doctor.subprocess, 'run', timeout)
    assert doctor.read_json(['az'])[1]['code'] == 'READ_TIMEOUT'


def test_reader_does_not_leak_failed_stdout_or_invalid_json(monkeypatch):
    monkeypatch.setattr(doctor.shutil, 'which', lambda _: 'az')
    monkeypatch.setattr(doctor.subprocess, 'run', lambda *a, **k: NS(returncode=1, stdout='secret', stderr='AADSTS530035 password'))
    assert doctor.read_json(['az'])[1] == {'code': 'SECURITY_DEFAULTS_BLOCK', 'entra_codes': ['AADSTS530035']}
    monkeypatch.setattr(doctor.subprocess, 'run', lambda *a, **k: NS(returncode=0, stdout='private not json'))
    assert doctor.read_json(['az'])[1]['code'] == 'INVALID_JSON_RESPONSE'


def test_diagnose_no_config_wizard_or_terraform(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, 'argv', ['launch.py', '--diagnose'])
    monkeypatch.setattr('builtins.input', lambda _: pytest.fail('No prompts allowed'))
    monkeypatch.setattr(launch, 'command', lambda *a, **k: pytest.fail('No provisioning'))
    reader, _ = synthetic_reader()
    monkeypatch.setattr(launch, 'diagnose', lambda *a, **k: doctor.diagnose(reader=reader))
    assert launch.main() == 0
    assert not (tmp_path / 'work').exists()
    assert json.loads(capsys.readouterr().out)['status'] == 'AZURE_ACCESS_OBSERVED'


def test_diagnose_cannot_be_combined_with_deploy(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['launch.py', '--diagnose', '--deploy'])
    monkeypatch.setattr(launch, 'diagnose', lambda *a, **k: pytest.fail('Should not execute'))
    assert launch.main() == 1


def test_workspace_config_needs_no_account_or_identity_prompt(tmp_path, monkeypatch):
    original = launch.ROOT
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    source = tmp_path / 'automation/silver'; source.mkdir(parents=True)
    source.joinpath('config.example.json').write_text((original / 'automation/silver/config.example.json').read_text())
    monkeypatch.setattr('builtins.input', lambda _: pytest.fail('No account required yet'))
    c = launch.load_config(tmp_path / 'work/config.json', workspace_only=True)
    assert c['account_id'] is None and c['tenant_id'] == doctor.TENANT


def test_workspace_constructor_no_account_api(monkeypatch, tmp_path):
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    def cli(args, **kwargs):
        if args[1:3] == ['account', 'show']:
            return {'id': doctor.SUBSCRIPTION, 'tenantId': doctor.TENANT}
        if args[1:3] == ['provider', 'show']:
            return 'Registered'
        pytest.fail('Account API must not be required')
    monkeypatch.setattr(launch, 'command', cli)
    monkeypatch.delenv('DATABRICKS_TOKEN', raising=False)
    instance = launch.Launcher({'account_id': None}, False, stage='workspace')
    assert instance.stage == 'workspace'


def test_workspace_stage_exits_before_uc_even_when_workspace_exists(tmp_path, monkeypatch):
    instance = object.__new__(launch.Launcher)
    instance.stage = 'workspace'; instance.deploy = False; instance.register_provider = False
    instance.init = lambda root: None if root == launch.PLATFORM else pytest.fail('UC must be deferred')
    instance.tf = lambda root, *a, **k: '\n'.join(['azurerm_resource_group.platform', 'azurerm_storage_account.data_lake', 'azurerm_data_factory.platform'])
    instance.outputs = lambda root: {'resource_group_name': {'value': 'rg-admm-dev-platform'},
        'storage_account_name': {'value': 'stadmmdevdlf6d99ba4'}, 'silver_workspace': {'value': {'workspace_id': 'synthetic'}}}
    instance.plan = lambda *a: True
    assert instance.provision() is None


def test_launcher_error_retains_aadsts_without_raw_text(monkeypatch):
    monkeypatch.setattr(launch.subprocess, 'run', lambda *a, **k: NS(returncode=1, stdout='', stderr='AADSTS530035 secret-password'))
    with pytest.raises(Blocked, match='SECURITY_DEFAULTS_BLOCK') as exc:
        launch.command(['az', 'account', 'show'])
    assert 'secret-password' not in str(exc.value)


def test_bootstrap_config_resumes_account_wizard_without_overwriting_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    path = tmp_path / 'work/config.json'; path.parent.mkdir()
    path.write_text(json.dumps({'account_id': None, 'operator_user_name': None,
        'node_type_id': 'Standard_D4as_v5', 'tenant_id': doctor.TENANT, 'metastore_id': 'existing'}))
    replies = iter([doctor.TENANT, '', 'operator@example.invalid', ''])
    monkeypatch.setattr('builtins.input', lambda _: next(replies))
    c = launch.load_config(path)
    assert c['account_id'] == doctor.TENANT and c['node_type_id'] == 'Standard_D4as_v5'
    assert c['metastore_id'] == 'existing' and not c['allow_create_metastore']


def test_workspace_config_reused_without_any_rewrite(tmp_path, monkeypatch):
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    path = tmp_path / 'work/config.json'; path.parent.mkdir()
    original = '{"account_id": null, "custom": "keep"}\n'
    path.write_text(original)
    launch.load_config(path, workspace_only=True)
    assert path.read_text() == original


def test_graph_flags_cannot_silently_start_provisioning(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['launch.py', '--graph'])
    monkeypatch.setattr(launch, 'load_config', lambda *a, **k: pytest.fail('No provisioning'))
    assert launch.main() == 1


def test_launcher_missing_tool_has_actionable_safe_code(monkeypatch):
    def missing(*a, **k):
        raise FileNotFoundError('sensitive file info')
    monkeypatch.setattr(launch.subprocess, 'run', missing)
    with pytest.raises(Blocked, match='TOOL_MISSING_AZ'):
        launch.command(['az', 'account', 'show'])


def test_workspace_default_main_does_not_run_silver(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, 'argv', ['launch.py'])
    seen = []
    monkeypatch.setattr(launch, 'load_config', lambda path, **kwargs: seen.append(kwargs) or {})
    class WorkspaceOnly:
        def __init__(self, config, deploy, stage):
            assert not deploy and stage == 'workspace'
        def provision(self):
            return None
        def deploy_job(self, *args):
            pytest.fail('No SQL/Job invocation')
    monkeypatch.setattr(launch, 'Launcher', WorkspaceOnly)
    assert launch.main() == 0
    assert seen == [{'workspace_only': True}]


def test_tf_error_identifies_stage_without_disclosing_output(monkeypatch):
    instance = object.__new__(launch.Launcher); instance.env = {}
    def denied(*args, **kwargs):
        raise Blocked('ARM_AUTHORIZATION_DENIED')
    monkeypatch.setattr(launch, 'command', denied)
    with pytest.raises(Blocked, match='PLATFORM_PLAN: ARM_AUTHORIZATION_DENIED'):
        instance.tf(launch.PLATFORM, 'plan')

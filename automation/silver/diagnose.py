"""Read-only bootstrap diagnostics. Never log raw CLI output or acquire/export tokens."""
import fnmatch
import json
import os
import re
import shutil
import subprocess
import urllib.parse
from datetime import datetime, timezone

SUBSCRIPTION = 'b3d627f1-966e-4bd4-861a-e606b963875d'
TENANT = '2c60929f-5f02-4969-8e91-62fdac1de9d4'
GUID = r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}'


def safe_failure(text, default='COMMAND_FAILED'):
    """Strict allowlist, not redaction: arbitrary provider text is never returned."""
    codes = sorted(set(re.findall(r'\bAADSTS[0-9]{5,9}\b', text)))
    if codes:
        reason = 'SECURITY_DEFAULTS_BLOCK' if 'AADSTS530035' in codes else 'ENTRA_AUTHENTICATION_FAILED'
        return {'code': reason, 'entra_codes': codes}
    for marker, code in (
        ('MAX_NOTEBOOK_SIZE_EXCEEDED', 'WORKSPACE_EXPORT_SIZE_LIMIT'),
        ('RESOURCE_DOES_NOT_EXIST', 'WORKSPACE_OBJECT_NOT_FOUND'),
        ('Exporting a FILE asset', 'WORKSPACE_EXPORT_FORMAT_UNSUPPORTED'),
        ('PERMISSION_DENIED', 'ACCESS_DENIED'),
        ('AuthorizationFailed', 'ARM_AUTHORIZATION_DENIED'),
        ('AuthenticationFailed', 'AUTHENTICATION_FAILED'),
        ('InvalidAuthenticationTokenTenant', 'ARM_TOKEN_TENANT_MISMATCH'),
        ('SubscriptionNotFound', 'SUBSCRIPTION_NOT_VISIBLE'),
        ('AADSTS', 'ENTRA_AUTHENTICATION_FAILED'),
        ('Forbidden', 'ACCESS_DENIED'), ('403', 'ACCESS_DENIED'),
        ('401', 'AUTHENTICATION_REQUIRED'),
        ('Please run', 'AZURE_LOGIN_REQUIRED'),
        ('Insufficient privileges', 'GRAPH_PRIVILEGES_REQUIRED'),
        ('CERTIFICATE_VERIFY_FAILED', 'TLS_TRUST_FAILED'),
        ('ConnectionError', 'CONNECTIVITY_FAILED'),
        ('getaddrinfo', 'DNS_FAILED'),
    ):
        if marker in text:
            return {'code': code}
    return {'code': default}


def read_json(args, *, timeout=60):
    program = shutil.which(args[0])
    if not program:
        return None, {'code': 'TOOL_MISSING', 'tool': args[0]}
    try:
        result = subprocess.run([program, *args[1:]], text=True, encoding='utf-8', errors='strict', capture_output=True,
                                timeout=timeout, shell=False,
                                env=dict(os.environ,PYTHONUTF8='1',PYTHONIOENCODING='utf-8'))
    except subprocess.TimeoutExpired:
        return None, {'code': 'READ_TIMEOUT'}
    except UnicodeError:
        return None, {'code': 'CLI_OUTPUT_NOT_UTF8'}
    except OSError:
        return None, {'code': 'TOOL_START_FAILED'}
    if result.returncode:
        return None, safe_failure(result.stderr + '\n' + result.stdout)
    try:
        return json.loads(result.stdout), None
    except (ValueError, TypeError):
        return None, {'code': 'INVALID_JSON_RESPONSE'}


def permits(entries, action):
    """Allow/NotActions only. Deny assignments and data-plane access remain unproven."""
    return any(any(fnmatch.fnmatchcase(action.lower(), a.lower()) for a in e.get('actions', []))
               and not any(fnmatch.fnmatchcase(action.lower(), a.lower()) for a in e.get('notActions', []))
               for e in entries)


def diagnose(config=None, *, graph=False, correlation_id=None, reader=read_json):
    c = config or {}
    subscription = c.get('subscription_id') or SUBSCRIPTION
    tenant = c.get('tenant_id') or TENANT
    if not all(isinstance(v, str) and re.fullmatch(GUID, v) for v in (subscription, tenant)):
        raise ValueError('INVALID_AZURE_CONTEXT_IDS')
    if correlation_id is not None and not re.fullmatch(GUID, correlation_id):
        raise ValueError('INVALID_CORRELATION_ID')
    report = {'status': 'NOT_VERIFIED', 'observed_at': datetime.now(timezone.utc).isoformat(),
              'expected_subscription_id': subscription, 'expected_tenant_id': tenant,
              'checks': {}, 'limitations': ['NO_APPLY_OR_ROLE_POLICY_CHANGES',
              'BACKEND_DATA_ACCESS_NOT_TESTED', 'RBAC_ALLOW_ACTIONS_DO_NOT_PROVE_NO_DENY',
              'ACCOUNT_ADMIN_AND_UC_NOT_PROVEN', 'SILVER_AND_AZURE_R2_NOT_EXECUTED']}
    checks = report['checks']

    def check(name, args, project=lambda data: data):
        data, error = reader(args)
        if error:
            checks[name] = {'status': 'UNAVAILABLE', **error}
            return None
        try:
            checks[name] = {'status': 'OBSERVED', **project(data)}
        except (KeyError, TypeError, AttributeError, ValueError):
            checks[name] = {'status': 'UNAVAILABLE', 'code': 'UNEXPECTED_RESPONSE_SCHEMA'}
            return None
        return data

    def arm(name, path, project):
        return check(name, ['az', 'rest', '--method', 'get', '--url',
            'https://management.azure.com' + path, '--subscription', subscription,
            '--only-show-errors', '-o', 'json'], project)

    account = check('cli_session', ['az', 'account', 'show', '-o', 'json'], lambda d: {
        'tenant_matches': str(d['tenantId']).lower() == tenant.lower(),
        'subscription_matches': str(d['id']).lower() == subscription.lower(),
        'identity_kind': d.get('user', {}).get('type') if d.get('user', {}).get('type') in ('user', 'servicePrincipal') else 'unknown'})
    if not account:
        report['status'] = 'BLOCKED'
        return report
    if not checks['cli_session']['tenant_matches'] or not checks['cli_session']['subscription_matches']:
        report['status'] = 'BLOCKED'
        checks['context'] = {'status': 'UNAVAILABLE', 'code': 'AZURE_CONTEXT_MISMATCH'}
        return report
    base = '/subscriptions/' + subscription
    subscription_data = arm('subscription', base + '?api-version=2022-12-01', lambda d: {
        'enabled': d['state'] == 'Enabled', 'tenant_matches': str(d.get('tenantId', '')).lower() == tenant.lower()})
    rg = base + '/resourceGroups/rg-admm-dev-platform'
    arm('resource_group', rg + '?api-version=2021-04-01', lambda d: {'east_us': d['location'].lower().replace(' ', '') == 'eastus'})
    scopes = {'subscription': base, 'project': rg,
              **{n: rg + '/providers/Microsoft.Storage/storageAccounts/stadmmdevdlf6d99ba4/blobServices/default/containers/' + n
                 for n in ('bronze', 'silver', 'audit')}}
    for name, scope in scopes.items():
        actions = (['Microsoft.Resources/subscriptions/resourceGroups/write', 'Microsoft.Databricks/register/action'] if name == 'subscription' else
                   ['Microsoft.Databricks/workspaces/write', 'Microsoft.Databricks/accessConnectors/write'] if name == 'project' else
                   ['Microsoft.Authorization/roleAssignments/write'])
        arm(name + '_allow_actions', scope + '/providers/Microsoft.Authorization/permissions?api-version=2022-04-01',
            lambda d, actions=actions: {'allows': {a: permits(d['value'], a) for a in actions}})
    arm('databricks_provider', base + '/providers/Microsoft.Databricks?api-version=2021-04-01',
        lambda d: {'registered': d['registrationState'] == 'Registered'})
    arm('workspaces', rg + '/providers/Microsoft.Databricks/workspaces?api-version=2024-05-01',
        lambda d: {'count_in_first_page': len(d['value']), 'has_more': bool(d.get('nextLink'))})
    # Graph is optional; ARM does not depend on /me or directory privileges.
    if graph:
        def graph_read(name, path, project):
            return check(name, ['az', 'rest', '--method', 'get', '--url', 'https://graph.microsoft.com/v1.0/' + path,
                               '--only-show-errors', '-o', 'json'], project)
        graph_read('directory_identity', 'me?$select=id,userType', lambda d: {
            'object_id': d['id'] if re.fullmatch(GUID, d['id']) else 'unknown',
            'user_type': d.get('userType') if d.get('userType') in ('Member', 'Guest') else 'unknown'})
        graph_read('security_defaults', 'policies/identitySecurityDefaultsEnforcementPolicy', lambda d: {'enabled': d['isEnabled'] is True})
        if correlation_id:
            query = urllib.parse.urlencode({'$filter': "correlationId eq '" + correlation_id + "'", '$top': '5',
                '$select': 'createdDateTime,status,conditionalAccessStatus'})
            graph_read('sign_in_events', 'auditLogs/signIns?' + query, lambda d: {
                'events_in_first_page': len(d['value']), 'has_more': bool(d.get('@odata.nextLink')),
                'error_codes': sorted({int(e['status']['errorCode']) for e in d['value']}),
                'security_defaults_block_observed': any(e['status']['errorCode'] == 530035 for e in d['value'])})
    observed = subscription_data and checks['subscription']['enabled'] and checks['subscription']['tenant_matches']
    report['status'] = 'AZURE_ACCESS_OBSERVED' if observed else 'BLOCKED'
    report['workspace_bootstrap_preflight'] = 'ALLOW_ACTIONS_OBSERVED' if observed and all(
        checks.get(n + '_allow_actions', {}).get('status') == 'OBSERVED' and
        all(checks[n + '_allow_actions']['allows'].values()) for n in scopes) else 'NOT_VERIFIED'
    return report

"""Workspace-only UC preflight. Existing assignments/identities are read, never changed."""
import urllib.parse
from plan_guard import Blocked, require

ACCOUNT_OWNED = {
    'databricks_service_principal.runner', 'databricks_mws_permission_assignment.runner',
    'databricks_mws_permission_assignment.operator', 'databricks_access_control_rule_set.run_as',
    'databricks_metastore.regional[0]', 'databricks_metastore_assignment.workspace[0]',
}


def ensure_workspace_state(owned, operator=None):
    require(not ACCOUNT_OWNED.intersection(owned),
            'ACCOUNT_MANAGED_UC_STATE_REQUIRES_REVIEWED_MIGRATION_NO_AUTOMATIC_SWITCH')
    # Legacy runner resources use singleton addresses; workspace mode only has
    # optional indexed grants/policy and never manages identity/entitlements.
    require(not any(k in owned for k in ('databricks_cluster_policy.silver',
            'databricks_permissions.policy_runner', 'databricks_entitlements.runner')),
            'LEGACY_RUNNER_STATE_REQUIRES_REVIEWED_MIGRATION')
    if operator:
        require(not any(address.startswith('databricks_') and isinstance(value.get('owner'), str)
                        and value['owner'].lower() != operator.lower() for address, value in owned.items()),
                'UC_STATE_OWNER_MISMATCH_REQUIRES_REVIEWED_TRANSFER')


def profile_environment(env, profile, workspace):
    require(isinstance(profile, str) and profile and '\n' not in profile, 'WORKSPACE_PROFILE_REQUIRED')
    # Avoid ambient account host/auth/PAT overriding the selected workspace profile.
    result = {k: v for k, v in env.items() if not k.startswith('DATABRICKS_')}
    result.update(DATABRICKS_HOST=workspace['workspace_host'], DATABRICKS_CONFIG_PROFILE=profile)
    return result


def required_read(api, path, code):
    try:
        return api('get', path)
    except Blocked as exc:
        raise Blocked(code + ': ' + str(exc)) from None


def verify_assignment(api, workspace, metastore_id, operator=None):
    assigned = required_read(api, '/api/2.1/unity-catalog/current-metastore-assignment', 'WORKSPACE_ASSIGNMENT_READ_REQUIRED')
    require(str(assigned.get('workspace_id')) == str(workspace['workspace_id']), 'PROFILE_WORKSPACE_ID_MISMATCH')
    require(assigned.get('metastore_id') == metastore_id,
            'WORKSPACE_METASTORE_MISMATCH_ACCOUNT_ADMIN_ASSIGNMENT_REQUIRED')
    current = required_read(api, '/api/2.0/preview/scim/v2/Me', 'WORKSPACE_OPERATOR_READ_REQUIRED')
    name = current.get('userName', '')
    require(current.get('active', True) is True and '@' in name, 'WORKSPACE_OPERATOR_USER_REQUIRED')
    if operator:
        require(name.lower() == operator.lower(), 'WORKSPACE_OPERATOR_IDENTITY_MISMATCH')
    return name


def verify_runner(api, application_id):
    query = urllib.parse.urlencode({'filter': 'applicationId eq "' + application_id + '"', 'count': '2'})
    result = required_read(api, '/api/2.0/preview/scim/v2/ServicePrincipals?' + query, 'WORKSPACE_RUNNER_SCIM_READ_REQUIRED')
    entries = result.get('Resources', [])
    require(result.get('totalResults') == 1 and len(entries) == 1, 'EXISTING_WORKSPACE_RUNNER_REQUIRED')
    runner = entries[0]
    require(runner.get('applicationId') == application_id and runner.get('active') is True,
            'EXISTING_WORKSPACE_RUNNER_INACTIVE_OR_MISMATCH')
    require(any(e.get('value') == 'workspace-access' for e in runner.get('entitlements', [])),
            'RUNNER_WORKSPACE_ACCESS_NOT_PROVEN_NO_AUTOMATIC_ENTITLEMENT_CHANGE')
    return runner


def verify_run_as(api, account_id, application_id, operator):
    require(account_id, 'RUN_AS_VERIFICATION_REQUIRES_NONSECRET_ACCOUNT_ID_NOT_ACCOUNT_ADMIN')
    name = f'accounts/{account_id}/servicePrincipals/{application_id}/ruleSets/default'
    query = urllib.parse.urlencode({'name': name, 'etag': ''})
    try:
        result = api('get', '/api/2.0/preview/accounts/access-control/rule-sets?' + query)
    except Blocked as exc:
        raise Blocked('RUN_AS_RULESET_READ_REQUIRED_DELEGATED_SP_MANAGER_OR_ACCOUNT_ADMIN: ' + str(exc)) from None
    require(result.get('name') == name, 'RUN_AS_RULESET_IDENTITY_MISMATCH')
    require(any(rule.get('role') == 'roles/servicePrincipal.user' and 'users/' + operator in rule.get('principals', [])
                for rule in result.get('grant_rules', [])),
            'EXPLICIT_SERVICE_PRINCIPAL_USER_GRANT_NOT_PROVEN_MANAGER_OR_WORKSPACE_ADMIN_IS_NOT_SUFFICIENT')
    # Conservative: group-only grants are not expanded into account group membership.
    # A manager/admin must delegate an explicit user grant if direct proof is absent.

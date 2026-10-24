"""Fail-closed review of REAL saved Terraform plans; never infer safety from fmt.

The platform root owns existing ADF/storage/state. Only the opt-in module's declared
resource addresses are eligible for changes. UC is a new, isolated state key.
"""
import hashlib
import json
import re

PLATFORM={
 'module.databricks[0].azurerm_databricks_workspace.silver',
 'module.databricks[0].azurerm_resource_provider_registration.databricks[0]',
 'module.databricks[0].azurerm_databricks_access_connector.silver',
 *{'module.databricks[0].azurerm_role_assignment.storage["'+k+'"]' for k in ('bronze','silver','audit')}}
UC={
 'databricks_metastore.regional[0]','databricks_metastore_assignment.workspace[0]',
 'databricks_service_principal.runner','databricks_mws_permission_assignment.runner',
 'databricks_mws_permission_assignment.operator','databricks_access_control_rule_set.run_as',
 'databricks_catalog.silver','databricks_schema.silver','databricks_schema.inputs',
 'databricks_volume.bronze','databricks_volume.receipts','databricks_cluster_policy.silver',
 'databricks_permissions.policy_runner','databricks_entitlements.runner',
 *{'databricks_storage_credential.storage["'+k+'"]' for k in ('bronze','silver','audit')},
 *{'databricks_external_location.storage["'+k+'"]' for k in ('bronze','silver','audit')},
 *{'databricks_grant.'+k for k in ('catalog_runner','silver_runner','inputs_runner','bronze_runner','receipts_runner')}}

UC_WORKSPACE={
 'databricks_catalog.silver','databricks_schema.silver','databricks_schema.inputs',
 'databricks_volume.bronze','databricks_volume.receipts',
 'databricks_cluster_policy.silver[0]','databricks_permissions.policy_runner[0]',
 *{'databricks_storage_credential.storage["'+k+'"]' for k in ('bronze','silver','audit')},
 *{'databricks_external_location.storage["'+k+'"]' for k in ('bronze','silver','audit')},
 *{'databricks_grant.'+k+'[0]' for k in ('catalog_runner','silver_runner','inputs_runner','bronze_runner','receipts_runner')}}

class Blocked(RuntimeError):pass


def require(condition,code):
    if not condition:raise Blocked(code)


def review(plan,stage):
    require(stage in ('platform','uc','uc-workspace'),'UNKNOWN_PLAN_STAGE')
    require(plan.get('errored',False) is False,'PLAN_NOT_APPLYABLE')
    require('resource_changes' in plan,'PLAN_CHANGES_MISSING')
    allowed={'platform':PLATFORM,'uc':UC,'uc-workspace':UC_WORKSPACE}[stage]
    changes=[]
    for resource in plan['resource_changes']:
        actions=resource['change']['actions'];address=resource['address']
        if resource.get('mode')=='data':
            require(actions in (['read'],['no-op']),'UNEXPECTED_DATA_ACTION');continue
        require('delete' not in actions,'DESTROY_OR_REPLACE_BLOCKED')
        require(actions in (['create'],['update'],['no-op']),'UNEXPECTED_PLAN_ACTION')
        require(not resource['change'].get('importing'),'IMPLICIT_ADOPTION_BLOCKED')
        if actions==['no-op']:continue
        require(address in allowed,'PROTECTED_OR_UNEXPECTED_RESOURCE_CHANGE')
        after=resource['change'].get('after') or {}
        if 'azurerm_resource_provider_registration' in address:
            require(after.get('name')=='Microsoft.Databricks','UNEXPECTED_RESOURCE_PROVIDER')
        if 'azurerm_role_assignment.storage' in address:
            key=re.search(r'\["(.*?)"\]$',address)[1]
            role='Storage Blob Data Reader' if key=='bronze' else 'Storage Blob Data Contributor'
            require(after.get('role_definition_name')==role,'RBAC_ROLE_MISMATCH')
            require(str(after.get('scope','')).endswith('/blobServices/default/containers/'+key),'RBAC_SCOPE_MISMATCH')
        if address.removesuffix('[0]') in ('databricks_grant.bronze_runner','databricks_grant.receipts_runner'):
            require(after.get('privileges')==['READ_VOLUME'],'INPUT_WRITE_GRANT_BLOCKED')
        if address in ('databricks_storage_credential.storage["bronze"]','databricks_external_location.storage["bronze"]'):
            require(after.get('read_only') is True,'BRONZE_NOT_READ_ONLY')
        if address.startswith('databricks_grant.'):
            expected={'catalog_runner':{'USE_CATALOG'},'silver_runner':{'USE_SCHEMA','CREATE_TABLE','SELECT','MODIFY'},'inputs_runner':{'USE_SCHEMA'},'bronze_runner':{'READ_VOLUME'},'receipts_runner':{'READ_VOLUME'}}
            require(set(after.get('privileges',[]))==expected[address.split('.')[-1].removesuffix('[0]')],'UNEXPECTED_UC_PRIVILEGES')
        changes.append({'address':address,'actions':actions})
    return changes


def sha(path):
    with open(path,'rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def comparison_sha(plan):
    """Comparison only: full show JSON minus its top-level generation timestamp.

    Never approve/apply using this digest: it is not the saved binary's identity.
    Nested timestamps, values, unknowns, configuration and state remain included.
    """
    payload={key:value for key,value in plan.items() if key!='timestamp'}
    canonical=json.dumps(payload,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()

"""Read-only, bounded single-node capacity preflight. Never allocate/retry compute."""
import json
from decimal import Decimal, InvalidOperation
from plan_guard import require

# Directional compatibility, not interchangeable just because CPU/RAM match.
# https://learn.microsoft.com/azure/databricks/compute/flexible-node-type-instances
ALTERNATES={
    'Standard_D4ds_v5':['Standard_D4ads_v5','Standard_D4ds_v4'],
    'Standard_D4as_v5':['Standard_D4ads_v5','Standard_D4ds_v5','Standard_D4s_v5'],
}


def supports_driver_flexibility(schema):
    if isinstance(schema,dict):
        properties=schema.get('properties')
        return (isinstance(properties,dict) and 'driver_node_type_flexibility' in properties) or any(supports_driver_flexibility(v) for v in schema.values())
    if isinstance(schema,list):return any(supports_driver_flexibility(v) for v in schema)
    return False


def select_alternates(primary,nodes,skus,policy,schema,usages=None):
    require(supports_driver_flexibility(schema),'FLEXIBLE_NODES_REQUIRE_DATABRICKS_CLI_0_285_OR_NEWER')
    require(primary in ALTERNATES,'UNREVIEWED_SILVER_PRIMARY_NODE_TYPE')
    require(isinstance(nodes,dict) and isinstance(nodes.get('node_types'),list),'INVALID_NODE_TYPES_RESPONSE')
    require(isinstance(skus,list),'INVALID_AZURE_SKUS_RESPONSE')
    require(usages is None or isinstance(usages,list),'INVALID_AZURE_QUOTAS_RESPONSE')
    require(isinstance(policy,dict),'INVALID_COMPUTE_POLICY_RESPONSE')
    require(all(isinstance(n,dict) and isinstance(n.get('node_type_id'),str) for n in nodes['node_types']),'INVALID_NODE_TYPES_RESPONSE')
    offered={n['node_type_id']:n for n in nodes['node_types']}
    require(primary in offered,'NODE_TYPE_UNAVAILABLE')
    def number(value,code):
        require(isinstance(value,(int,float,str)) and not isinstance(value,bool),code)
        try:parsed=Decimal(str(value))
        except InvalidOperation:require(False,code)
        require(parsed.is_finite() and parsed>=0,code)
        return parsed
    quotas={}
    for u in usages or []:
        require(isinstance(u,dict) and isinstance(u.get('name'),dict) and isinstance(u['name'].get('value'),str),'INVALID_AZURE_QUOTA_NAME')
        key=u['name']['value'].lower()
        require(key not in quotas,'DUPLICATE_AZURE_QUOTA_NAME')
        quotas[key]=u
    def quota_available(sku):
        if usages is None:return True # Pure catalog validation; launcher always supplies quotas.
        def room(key):
            q=quotas.get(key.lower())
            if q is None:return False
            limit=number(q.get('limit'),'INVALID_AZURE_QUOTA_LIMIT')
            used=number(q.get('currentValue'),'INVALID_AZURE_QUOTA_CURRENT_VALUE')
            require(limit==limit.to_integral_value() and used==used.to_integral_value(),'INVALID_AZURE_QUOTA_CORES')
            return limit-used>=4
        family=sku.get('family')
        require(isinstance(family,str),'INVALID_AZURE_SKU_FAMILY')
        return room('cores') and room(family)
    available=set()
    for sku in skus:
        require(isinstance(sku,dict),'INVALID_AZURE_SKU_ITEM')
        # Unrelated SKU capabilities are not inputs to this bounded VM choice.
        if sku.get('name') not in ALTERNATES[primary]:continue
        capabilities=sku.get('capabilities')
        require(isinstance(capabilities,list) and all(isinstance(c,dict) and isinstance(c.get('name'),str) for c in capabilities),'INVALID_AZURE_SKU_CAPABILITIES')
        caps={c['name']:c.get('value') for c in capabilities}
        locations=sku.get('locations');restrictions=sku.get('restrictions')
        require(isinstance(locations,list) and isinstance(restrictions,list) and all(isinstance(r,dict) for r in restrictions),'INVALID_AZURE_SKU_LOCATIONS_OR_RESTRICTIONS')
        if (sku.get('resourceType')=='virtualMachines' and 'eastus' in locations and
            number(caps.get('vCPUs'),'INVALID_AZURE_SKU_VCPUS')==4 and
            number(caps.get('MemoryGB'),'INVALID_AZURE_SKU_MEMORY')==16 and
            not any(r.get('type')=='Location' for r in restrictions) and quota_available(sku)):
            available.add(sku['name'])
    candidates=[n for n in ALTERNATES[primary] if n in offered and n in available]
    require(candidates,'NO_ELIGIBLE_SILVER_FALLBACK_CHECK_EASTUS_SKUS_AND_QUOTAS')
    require(not policy.get('policy_family_id'),'POLICY_FAMILY_REQUIRES_EFFECTIVE_COMPUTE_REVIEW')
    definition=policy.get('definition')
    if isinstance(definition,str):
        try:definition=json.loads(definition)
        except ValueError:require(False,'INVALID_COMPUTE_POLICY_DEFINITION_JSON')
    require(isinstance(definition,dict) and all(isinstance(v,dict) for v in definition.values()),'INVALID_COMPUTE_POLICY_DEFINITION')
    node_rule=definition.get('node_type_id',{})
    require(node_rule.get('type')=='fixed' and node_rule.get('value')==primary,'SILVER_PRIMARY_POLICY_MISMATCH')
    driver=definition.get('driver_node_type_id')
    require(driver is None or (driver.get('type')=='fixed' and driver.get('value')==primary),'SILVER_DRIVER_POLICY_MISMATCH')
    for attr in ('driver_node_type_flexibility','worker_node_type_flexibility'):
        rule=definition.get(attr+'.alternate_node_type_ids')
        expected=','.join(candidates) if attr.startswith('driver') else ''
        require(rule is None or (rule.get('type')=='fixed' and rule.get('value')==expected),'FLEXIBLE_NODES_CONFLICT_WITH_EXISTING_POLICY')
    return candidates


def check_run_result(state):
    if state.get('result_state')=='SUCCESS':return
    message=state.get('state_message','')
    if any(code in message for code in ('CLOUD_PROVIDER_RESOURCE_STOCKOUT','SkuNotAvailable')):
        require(False,'SILVER_COMPUTE_STOCKOUT_CHECK_CAPACITY_GUIDE_NO_AUTOMATIC_RETRY')
    require(False,'DATABRICKS_JOB_FAILED_CHECK_RUN_UI')

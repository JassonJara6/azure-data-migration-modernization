"""Read-only, bounded single-node capacity preflight. Never allocate/retry compute."""
import json
from plan_guard import require

# Directional compatibility, not interchangeable just because CPU/RAM match.
# https://learn.microsoft.com/azure/databricks/compute/flexible-node-type-instances
ALTERNATES={
    'Standard_D4ds_v5':['Standard_D4ads_v5','Standard_D4ds_v4'],
    'Standard_D4as_v5':['Standard_D4ads_v5','Standard_D4ds_v5','Standard_D4s_v5'],
}


def supports_driver_flexibility(schema):
    if isinstance(schema,dict):
        return 'driver_node_type_flexibility' in schema.get('properties',{}) or any(supports_driver_flexibility(v) for v in schema.values())
    if isinstance(schema,list):return any(supports_driver_flexibility(v) for v in schema)
    return False


def select_alternates(primary,nodes,skus,policy,schema,usages=None):
    require(supports_driver_flexibility(schema),'FLEXIBLE_NODES_REQUIRE_DATABRICKS_CLI_0_285_OR_NEWER')
    require(primary in ALTERNATES,'UNREVIEWED_SILVER_PRIMARY_NODE_TYPE')
    offered={n['node_type_id']:n for n in nodes['node_types']}
    require(primary in offered,'NODE_TYPE_UNAVAILABLE')
    def allowed(sku):
        caps={c['name']:c['value'] for c in sku.get('capabilities',[])}
        return (sku.get('resourceType')=='virtualMachines' and 'eastus' in sku.get('locations',[]) and
                caps.get('vCPUs')=='4' and float(caps.get('MemoryGB','0'))==16 and
                not any(r.get('type')=='Location' for r in sku.get('restrictions',[])))
    quotas={u.get('name',{}).get('value','').lower():u for u in usages or []}
    def quota_available(sku):
        if usages is None:return True # Pure catalog validation; launcher always supplies quotas.
        def room(key):
            q=quotas.get(key.lower(),{})
            return q.get('limit',0)-q.get('currentValue',0)>=4
        return room('cores') and room(sku.get('family',''))
    available={s['name'] for s in skus if allowed(s) and quota_available(s)}
    candidates=[n for n in ALTERNATES[primary] if n in offered and n in available]
    require(candidates,'NO_ELIGIBLE_SILVER_FALLBACK_CHECK_EASTUS_SKUS_AND_QUOTAS')
    require(not policy.get('policy_family_id'),'POLICY_FAMILY_REQUIRES_EFFECTIVE_COMPUTE_REVIEW')
    definition=json.loads(policy.get('definition','{}'))
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

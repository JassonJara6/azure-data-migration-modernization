"""Read-only, bounded single-node capacity preflight. Never allocate/retry compute."""
import json
import re
from decimal import Decimal, InvalidOperation
from plan_guard import require,Blocked
from capacity_reads import ScopedSkuEvidence,APPROVED_SKUS,SOURCE,FAMILIES

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


def _select_alternates(primary,nodes,skus,policy,schema,usages,report):
    require(supports_driver_flexibility(schema),'FLEXIBLE_NODES_REQUIRE_DATABRICKS_CLI_0_285_OR_NEWER')
    require(primary in ALTERNATES,'UNREVIEWED_SILVER_PRIMARY_NODE_TYPE')
    require(isinstance(nodes,dict) and isinstance(nodes.get('node_types'),list),'INVALID_NODE_TYPES_RESPONSE')
    scoped=isinstance(skus,ScopedSkuEvidence)
    if scoped:
        require(skus.source==SOURCE and skus.region=='eastus' and skus.requested==APPROVED_SKUS,'INVALID_SCOPED_SKU_CONTRACT')
        skus=list(skus.records)
        require(len(skus)==len(APPROVED_SKUS) and {s.get('name') for s in skus if isinstance(s,dict)}==set(APPROVED_SKUS),'INCOMPLETE_SCOPED_SKU_EVIDENCE')
        require(primary in APPROVED_SKUS and all(n in APPROVED_SKUS for n in ALTERNATES[primary]),'UNSUPPORTED_SCOPED_PRIMARY')
        report['source']=SOURCE;report['scope']=list(APPROVED_SKUS)
        require(all(isinstance(s,dict) and s.get('family')==FAMILIES[s['name']] for s in skus),'INVALID_SCOPED_QUOTA_FAMILY')
        primary_record=next(s for s in skus if s['name']==primary)
        require(primary_record.get('provider_status')==[],'SCOPED_PRIMARY_PROVIDER_STATUS_NOT_CLEAR')
    require(isinstance(skus,list),'INVALID_AZURE_SKUS_RESPONSE')
    require(all(isinstance(s,dict) for s in skus),'INVALID_AZURE_SKU_ITEM')
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
        key=u['name']['value'].strip().lower()
        require(key not in quotas,'DUPLICATE_AZURE_QUOTA_NAME')
        quotas[key]=u
    def quota_info(key):
        if usages is None:return {'status':'NOT_CHECKED'}
        key=key.strip().lower()
        require(re.fullmatch(r'[a-z0-9]{1,80}',key) is not None,'INVALID_AZURE_QUOTA_KEY')
        q=quotas.get(key)
        if q is None:return {'status':'MISSING','key':key}
        limit=number(q.get('limit'),'INVALID_AZURE_QUOTA_LIMIT')
        used=number(q.get('currentValue'),'INVALID_AZURE_QUOTA_CURRENT_VALUE')
        require(limit==limit.to_integral_value() and used==used.to_integral_value(),'INVALID_AZURE_QUOTA_CORES')
        return {'status':'CHECKED','key':key,'limit':int(limit),'used':int(used),'headroom':int(limit-used)}
    if scoped:
        report['primary_quota']={'regional':quota_info('cores'),'family':quota_info(primary_record['family'])}
        require(all(q['status']=='CHECKED' for q in report['primary_quota'].values()),'SCOPED_PRIMARY_QUOTA_MISSING')
    available=set()
    for entry in report['candidates']:
        node=entry['node_type_id'];reasons=entry['reasons']
        if node not in offered:reasons.append('NOT_IN_WORKSPACE_CATALOG')
        matches=[s for s in skus if isinstance(s,dict) and s.get('name')==node]
        if not matches:
            reasons.append('NOT_IN_AZURE_SKU_CATALOG');continue
        try:
            require(len(matches)==1,'AMBIGUOUS_AZURE_SKU')
            sku=matches[0]
            capabilities=sku.get('capabilities')
            require(isinstance(capabilities,list) and all(isinstance(c,dict) and isinstance(c.get('name'),str) for c in capabilities),'INVALID_AZURE_SKU_CAPABILITIES')
            require(len({c['name'] for c in capabilities})==len(capabilities),'INVALID_AZURE_SKU_DUPLICATE_CAPABILITY')
            caps={c['name']:c.get('value') for c in capabilities}
            locations=sku.get('locations')
            if scoped:
                statuses=sku.get('provider_status')
                require(isinstance(statuses,list) and all(s in ('NotEnabledOnSubscription','NotAvailableInRegion') for s in statuses),'INVALID_SCOPED_PROVIDER_STATUS')
                if 'NotEnabledOnSubscription' in statuses:reasons.append('PROVIDER_NOT_ENABLED_ON_SUBSCRIPTION')
                if 'NotAvailableInRegion' in statuses:reasons.append('PROVIDER_NOT_AVAILABLE_IN_REGION')
                restrictions=[] # No ARM restriction assertion: provider flags above are the evidence.
            else:restrictions=sku.get('restrictions')
            require(isinstance(locations,list) and isinstance(restrictions,list) and all(isinstance(r,dict) for r in restrictions),'INVALID_AZURE_SKU_LOCATIONS_OR_RESTRICTIONS')
            if sku.get('resourceType')!='virtualMachines':reasons.append('NOT_VIRTUAL_MACHINE')
            if 'eastus' not in [str(v).lower() for v in locations]:reasons.append('NOT_IN_EASTUS')
            if number(caps.get('vCPUs'),'INVALID_AZURE_SKU_VCPUS')!=4:reasons.append('VCPU_NOT_4')
            if number(caps.get('MemoryGB'),'INVALID_AZURE_SKU_MEMORY')!=16:reasons.append('MEMORY_NOT_16_GIB')
            for restriction in restrictions:
                if restriction.get('type')=='Location':
                    info=restriction.get('restrictionInfo') or {}
                    require(isinstance(info,dict),'INVALID_AZURE_RESTRICTION_INFO')
                    affected=info.get('locations') or restriction.get('values')
                    require(affected is None or isinstance(affected,list),'INVALID_AZURE_RESTRICTION_LOCATIONS')
                    # Missing scope is unknown, never evidence that East US is allowed.
                    if not affected or 'eastus' in [str(v).lower() for v in affected]:
                        reasons.append('EASTUS_LOCATION_RESTRICTED')
                elif restriction.get('type')!='Zone':
                    reasons.append('UNKNOWN_AZURE_RESTRICTION')
            entry['regional_quota']=quota_info('cores')
            family=sku.get('family')
            if usages is not None:require(isinstance(family,str) and family.strip(),'INVALID_AZURE_SKU_FAMILY')
            entry['family_quota']=quota_info(family or '')
            for name in ('regional','family'):
                quota=entry[name+'_quota']
                if quota['status']=='MISSING':reasons.append(name.upper()+'_QUOTA_MISSING')
                if quota['status']=='CHECKED' and quota['headroom']<4:reasons.append(name.upper()+'_QUOTA_INSUFFICIENT')
        except Blocked as exc:
            reasons.append(str(exc))
        entry['eligible']=not reasons
        if entry['eligible']:available.add(node)
    invalid=next((r for e in report['candidates'] for r in e['reasons'] if r.startswith('INVALID_')),None)
    require(invalid is None,invalid)
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


def select_alternates(primary,nodes,skus,policy,schema,usages=None,*,reporter=None):
    report={'status':'BLOCKED','region':'eastus','primary':primary if primary in ALTERNATES else 'UNREVIEWED','physical_capacity':'NOT_VERIFIED',
        'candidates':[{'node_type_id':n,'eligible':False,'reasons':[]} for n in ALTERNATES.get(primary,[])],
        'action':'REVIEW_CANDIDATE_REASONS_NO_AUTOMATIC_RETRY'}
    try:
        result=_select_alternates(primary,nodes,skus,policy,schema,usages,report)
        report.update(status='ELIGIBLE_SNAPSHOT',selected=result,action='SNAPSHOT_ONLY_NOT_AUTHORIZATION_TO_RUN')
        return result
    except Blocked as exc:
        report['blocker']=str(exc)
        for entry in report['candidates']:
            if not entry['reasons']:entry['reasons'].append(str(exc))
            entry['eligible']=False
        raise
    finally:
        for entry in report['candidates']:
            entry['actions']=sorted({
                'REVIEW_EXPLICIT_PROVIDER_EVIDENCE_NO_PAID_RETRY' if reason.startswith('PROVIDER_') or 'PROVIDER_STATUS' in reason else
                'VERIFY_MACHINE_QUOTA_KEYS_AND_READER_ACCESS' if 'MISSING' in reason and 'QUOTA' in reason else
                'REVIEW_REGIONAL_OR_FAMILY_QUOTA_NO_AUTO_INCREASE' if 'QUOTA_INSUFFICIENT' in reason else
                'REVIEW_EASTUS_SUBSCRIPTION_RESTRICTIONS' if 'RESTRICT' in reason else
                'REVIEW_EXISTING_COMPUTE_POLICY_NO_AUTO_CHANGE' if 'POLICY' in reason else
                'VERIFY_WORKSPACE_NODE_CATALOG' if 'WORKSPACE_CATALOG' in reason else
                'VERIFY_AZURE_EASTUS_SKU_CATALOG' if 'SKU_CATALOG' in reason else
                'REVIEW_INVALID_OR_INCOMPATIBLE_EVIDENCE' for reason in entry['reasons']})
        if reporter is not None:reporter(report)


def check_run_result(state):
    if state.get('result_state')=='SUCCESS':return
    message=state.get('state_message','')
    if any(code in message for code in ('CLOUD_PROVIDER_RESOURCE_STOCKOUT','SkuNotAvailable')):
        require(False,'SILVER_COMPUTE_STOCKOUT_CHECK_CAPACITY_GUIDE_NO_AUTOMATIC_RETRY')
    require(False,'DATABRICKS_JOB_FAILED_CHECK_RUN_UI')

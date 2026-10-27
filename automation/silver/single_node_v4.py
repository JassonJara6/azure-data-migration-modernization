"""Live ARM eligibility + explicit quota for one reviewed Free Trial SKU.

No manual eligibility overrides, imported snapshots, allocation or cloud writes.
"""
import json
import re
import time
from decimal import Decimal,InvalidOperation
from urllib.parse import urlencode,urlsplit
from plan_guard import require,Blocked
from diagnose import SUBSCRIPTION,TENANT
from capacity_reads import WORKSPACE_RESOURCE_ID

PRIMARY='Standard_D4ds_v4'
FAMILY='standardDDSv4Family'
NO_FALLBACK={'alternate_node_type_ids':[]}


def integer(value):
    require(not isinstance(value,bool) and isinstance(value,(int,float,str)),'QUOTA_NUMBER_REQUIRED')
    text=str(value).strip()
    require(re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',text),'QUOTA_NUMBER_REQUIRED')
    try:n=Decimal(text)
    except InvalidOperation:raise Blocked('QUOTA_NUMBER_REQUIRED') from None
    require(n.is_finite() and n>=0 and n<=2**63-1 and n==n.to_integral_value(),'QUOTA_NONNEGATIVE_INTEGER_REQUIRED')
    return int(n)


def quotas(rows):
    require(isinstance(rows,list),'INVALID_AZURE_QUOTAS_RESPONSE')
    result={}
    for key in ('cores',FAMILY):
        matches=[r for r in rows if isinstance(r,dict) and isinstance(r.get('name'),dict) and
                 isinstance(r['name'].get('value'),str) and r['name']['value'].lower()==key.lower()]
        require(len(matches)==1,'V4_QUOTA_MISSING_OR_AMBIGUOUS')
        row=matches[0];limit=integer(row.get('limit'))
        values=[integer(row[k]) for k in ('currentValue','current_value') if k in row]
        require(values and len(set(values))==1,'V4_QUOTA_USAGE_MISSING_OR_CONFLICTING')
        original={k:row[k] for k in ('limit','currentValue','current_value') if k in row}
        result[key]={'limit':limit,'used':values[0],'headroom':limit-values[0],'original':original}
        require(limit-values[0]>=4,'V4_QUOTA_INSUFFICIENT_'+('REGIONAL' if key=='cores' else 'FAMILY'))
    return result


def validate_node(nodes):
    require(isinstance(nodes,dict) and isinstance(nodes.get('node_types'),list),'INVALID_NODE_TYPES_RESPONSE')
    matches=[n for n in nodes['node_types'] if isinstance(n,dict) and n.get('node_type_id')==PRIMARY]
    require(len(matches)==1,'V4_WORKSPACE_NODE_MISSING_OR_AMBIGUOUS')
    n=matches[0]
    require(n.get('instance_type_id')==PRIMARY and type(n.get('num_cores')) in (int,float) and n['num_cores']==4 and
            type(n.get('memory_mb')) is int and n['memory_mb']==16384,'V4_NODE_DIMENSIONS_MISMATCH')
    require(n.get('is_hidden',False) is False and n.get('is_deprecated',False) is False and
            n.get('is_graviton',False) is False and n.get('num_gpus',0)==0,'V4_NODE_UNSUPPORTED')
    info=n.get('node_info')
    require(info is None or isinstance(info,dict),'V4_PROVIDER_INFO_MALFORMED')
    status=info.get('status') if isinstance(info,dict) else None
    if status is None:return {'status':'NOT_REPORTED'}
    require(isinstance(status,list),'V4_PROVIDER_STATUS_MALFORMED')
    require(all(s in ('NotEnabledOnSubscription','NotAvailableInRegion') for s in status),'V4_PROVIDER_STATUS_UNKNOWN')
    require(not status,'V4_PROVIDER_REJECTED')
    return {'status':'EXPLICIT_CLEAR','flags':[]}


def read_evidence(read,nodes):
    """read(argv, stage, timeout) uses selected profile and never logs raw bodies."""
    account=read(['az','account','show','--subscription',SUBSCRIPTION,'--output','json'],'V4_ACCOUNT',90)
    require(isinstance(account,dict) and str(account.get('id','')).lower()==SUBSCRIPTION and
            str(account.get('tenantId','')).lower()==TENANT and account.get('state')=='Enabled','V4_ACCOUNT_BINDING_FAILED')
    subscription=read(['az','rest','--method','get','--url','https://management.azure.com/subscriptions/'+SUBSCRIPTION+'?api-version=2022-12-01',
                       '--subscription',SUBSCRIPTION,'--output','json'],'V4_SUBSCRIPTION',90)
    require(isinstance(subscription,dict) and isinstance(subscription.get('subscriptionPolicies'),dict),'V4_SUBSCRIPTION_RESPONSE_INVALID')
    require(str(subscription.get('subscriptionId','')).lower()==SUBSCRIPTION and subscription.get('state')=='Enabled' and
            subscription['subscriptionPolicies'].get('quotaId')=='FreeTrial_2014-09-01' and
            subscription['subscriptionPolicies'].get('spendingLimit')=='On','V4_FREE_TRIAL_REQUIRED')
    workspace=read(['az','resource','show','--ids',WORKSPACE_RESOURCE_ID,'--api-version','2024-05-01',
                    '--subscription',SUBSCRIPTION,'--output','json'],'V4_WORKSPACE',90)
    require(isinstance(workspace,dict) and isinstance(workspace.get('properties'),dict) and
            str(workspace.get('id','')).lower()==WORKSPACE_RESOURCE_ID.lower() and str(workspace.get('location','')).lower()=='eastus','V4_WORKSPACE_BINDING_FAILED')
    assigned=read(['databricks','api','get','/api/2.1/unity-catalog/current-metastore-assignment'],'V4_ASSIGNMENT',90)
    require(isinstance(assigned,dict) and str(assigned.get('workspace_id'))=='7405617270738925' and
            str(workspace['properties'].get('workspaceId'))=='7405617270738925','V4_WORKSPACE_ID_MISMATCH')
    provider=validate_node(nodes)
    path='/subscriptions/'+SUBSCRIPTION+'/providers/Microsoft.Compute/skus'
    url='https://management.azure.com'+path+'?'+urlencode({'api-version':'2021-07-01','$filter':"location eq 'eastus'"})
    deadline=time.monotonic()+180;seen=set();records=[];pages=0
    while url:
        parts=urlsplit(url)
        require(parts.scheme=='https' and parts.netloc=='management.azure.com' and parts.path.lower()==path.lower() and not parts.fragment,'V4_UNTRUSTED_PAGINATION')
        require(url not in seen and pages<100 and time.monotonic()<deadline,'V4_ARM_SNAPSHOT_INCOMPLETE')
        seen.add(url)
        page=read(['az','rest','--method','get','--url',url,'--subscription',SUBSCRIPTION,'--output','json'],
                  'V4_ARM_SKUS',min(60,max(1,deadline-time.monotonic())))
        require(isinstance(page,dict) and isinstance(page.get('value'),list) and all(isinstance(s,dict) for s in page['value']),'V4_ARM_PAGE_INVALID')
        records.extend(s for s in page['value'] if s.get('name')==PRIMARY);pages+=1
        url=page.get('nextLink')
        require(url is None or (isinstance(url,str) and url),'V4_ARM_NEXT_LINK_INVALID')
    require(len(records)==1,'V4_ARM_SKU_MISSING_OR_AMBIGUOUS')
    sku=records[0]
    require(sku.get('resourceType')=='virtualMachines' and isinstance(sku.get('locations'),list) and
            'eastus' in [str(l).lower() for l in sku['locations']] and str(sku.get('family','')).lower()==FAMILY.lower(),'V4_ARM_SKU_BINDING_FAILED')
    # Require explicit unrestricted ARM data. Missing restrictions is NOT [].
    require(sku.get('restrictions')==[],'V4_ARM_RESTRICTIONS_NOT_CLEAR')
    caps=sku.get('capabilities')
    require(isinstance(caps,list) and all(isinstance(c,dict) and isinstance(c.get('name'),str) for c in caps),'V4_ARM_CAPABILITIES_INVALID')
    require(len({c['name'] for c in caps})==len(caps),'V4_ARM_CAPABILITIES_AMBIGUOUS')
    caps={c['name']:c.get('value') for c in caps}
    require(integer(caps.get('vCPUs'))==4 and integer(caps.get('MemoryGB'))==16 and
            caps.get('CpuArchitectureType')=='x64' and str(caps.get('PremiumIO')).lower()=='true','V4_ARM_DIMENSIONS_MISMATCH')
    usage=read(['az','vm','list-usage','--subscription',SUBSCRIPTION,'--location','eastus','--output','json'],'V4_QUOTAS',120)
    return {'status':'ELIGIBLE_SNAPSHOT','primary':PRIMARY,'region':'eastus','source':'LIVE_ARM_RESOURCE_SKUS_V1',
            'arm_snapshot':'COMPLETE','arm_pages':pages,'arm_restrictions':[],
            'databricks_provider':provider,'quotas':quotas(usage),'driver_alternates':[],
            'physical_capacity':'NOT_VERIFIED','free_trial':True,'execution':'NOT_AUTHORIZED'}


def validate_policy(policy,runner=None):
    require(isinstance(policy,dict) and not policy.get('policy_family_id'),'V4_EFFECTIVE_POLICY_REQUIRED')
    definition=policy.get('definition')
    if isinstance(definition,str):
        try:definition=json.loads(definition)
        except ValueError:raise Blocked('V4_POLICY_JSON_INVALID') from None
    require(isinstance(definition,dict),'V4_POLICY_INVALID')
    expected={'node_type_id':PRIMARY,'driver_node_type_id':PRIMARY,'num_workers':0,
              'spark_version':'15.4.x-scala2.12','data_security_mode':'SINGLE_USER',
              'spark_conf.spark.databricks.cluster.profile':'singleNode','spark_conf.spark.master':'local[*]',
              'azure_attributes.availability':'ON_DEMAND_AZURE','cluster_type':'job',
              'driver_node_type_flexibility.alternate_node_type_ids':'',
              'worker_node_type_flexibility.alternate_node_type_ids':''}
    require(set(definition)<=set(expected)|{'single_user_name','custom_tags.ResourceClass'},'V4_POLICY_UNREVIEWED_RULE')
    identity=definition.get('single_user_name')
    require(isinstance(identity,dict) and identity.get('type')=='fixed' and isinstance(identity.get('value'),str) and identity['value'],'V4_POLICY_RUNNER_REQUIRED')
    if runner is not None:require(identity['value']==runner,'V4_POLICY_RUNNER_MISMATCH')
    for key,value in expected.items():require(definition.get(key)=={'type':'fixed','value':value},'V4_POLICY_CONSTRAINT_MISMATCH')


def validate_cluster(cluster):
    require(isinstance(cluster,dict),'V4_CLUSTER_INVALID')
    require(type(cluster.get('num_workers')) is int,'V4_WORKERS_INTEGER_REQUIRED')
    for key,value in {'node_type_id':PRIMARY,'driver_node_type_id':PRIMARY,'num_workers':0,
                      'spark_version':'15.4.x-scala2.12','data_security_mode':'SINGLE_USER',
                      'driver_node_type_flexibility':NO_FALLBACK,'worker_node_type_flexibility':NO_FALLBACK}.items():
        require(cluster.get(key)==value,'V4_CLUSTER_CONSTRAINT_MISMATCH')
    require(cluster.get('spark_conf')=={'spark.databricks.cluster.profile':'singleNode','spark.master':'local[*]'} and
            cluster.get('azure_attributes')=={'availability':'ON_DEMAND_AZURE'} and
            not any(cluster.get(k) for k in ('autoscale','instance_pool_id','driver_instance_pool_id','init_scripts','docker_image')),'V4_CLUSTER_COST_CONSTRAINTS_CHANGED')

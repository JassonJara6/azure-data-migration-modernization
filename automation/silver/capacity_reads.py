"""Scoped Databricks cloud-provider evidence; never enumerate ARM Compute SKUs."""
from dataclasses import dataclass
from diagnose import SUBSCRIPTION
from plan_guard import require

# Fixed project resource, not an operator-supplied URL or an authentication secret.
WORKSPACE_RESOURCE_ID='/subscriptions/'+SUBSCRIPTION+'/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/workspaces/dbw-admm-dev-f6d99ba4'
# Reviewed Azure VM family quota machine keys, not inferred from a display label.
FAMILIES={'Standard_D4ds_v5':'standardDDSv5Family',
          'Standard_D4ads_v5':'standardDADSv5Family',
          'Standard_D4ds_v4':'standardDDSv4Family'}
APPROVED_SKUS=tuple(FAMILIES)
SOURCE='DATABRICKS_CLOUD_PROVIDER_NODE_INFO_V1'

@dataclass(frozen=True)
class ScopedSkuEvidence:
    records: tuple
    requested: tuple
    region: str
    source: str=SOURCE


def read_scoped_skus(read,nodes,*,progress=None):
    """Use explicit provider status, never interpret omitted status as unrestricted.

    read(argv, stage, timeout) only invokes read-only CLI commands. The workspace
    ARM identity binds Databricks metadata to this subscription and East US.
    """
    if progress:progress({'stage':'SCOPED_SKUS','status':'READING_WORKSPACE_BINDING'})
    workspace=read(['az','resource','show','--ids',WORKSPACE_RESOURCE_ID,'--api-version','2024-05-01','--output','json'],'AZURE_WORKSPACE',90)
    require(isinstance(workspace,dict) and isinstance(workspace.get('properties'),dict),'INVALID_AZURE_WORKSPACE_RESPONSE')
    require(isinstance(workspace.get('id'),str) and workspace['id'].lower()==WORKSPACE_RESOURCE_ID.lower(),'SCOPED_WORKSPACE_RESOURCE_MISMATCH')
    require(isinstance(workspace.get('location'),str) and workspace['location'].lower()=='eastus','SCOPED_WORKSPACE_NOT_EASTUS')
    assigned=read(['databricks','api','get','/api/2.1/unity-catalog/current-metastore-assignment'],'WORKSPACE_IDENTITY',90)
    require(isinstance(assigned,dict) and isinstance(assigned.get('workspace_id'),(str,int)) and not isinstance(assigned.get('workspace_id'),bool),'INVALID_WORKSPACE_IDENTITY_RESPONSE')
    expected=workspace['properties'].get('workspaceId')
    require(expected is not None and str(expected).isdigit() and str(assigned['workspace_id'])==str(expected),'SCOPED_WORKSPACE_ID_MISMATCH')
    require(isinstance(nodes,dict) and isinstance(nodes.get('node_types'),list),'INVALID_NODE_TYPES_RESPONSE')
    records=[]
    for name,family in FAMILIES.items():
        if progress:progress({'stage':'SCOPED_SKUS','sku':name,'status':'VALIDATING_EXPLICIT_PROVIDER_EVIDENCE'})
        matches=[n for n in nodes['node_types'] if isinstance(n,dict) and n.get('node_type_id')==name]
        require(len(matches)==1,'SCOPED_SKU_MISSING_OR_AMBIGUOUS')
        n=matches[0];info=n.get('node_info')
        require(isinstance(info,dict) and isinstance(info.get('status'),list),'SCOPED_PROVIDER_STATUS_MISSING')
        status=info['status']
        require(all(s in ('NotEnabledOnSubscription','NotAvailableInRegion') for s in status),'SCOPED_PROVIDER_STATUS_UNKNOWN')
        require(n.get('instance_type_id')==name,'SCOPED_INSTANCE_TYPE_MISMATCH')
        require(type(n.get('num_cores')) in (int,float) and type(n.get('memory_mb')) is int,'SCOPED_DIMENSIONS_MISSING')
        require(n['num_cores']==4 and n['memory_mb']==16384,'SCOPED_DIMENSIONS_INCOMPATIBLE')
        require(n.get('is_deprecated',False) is False and n.get('is_hidden',False) is False,'SCOPED_NODE_DEPRECATED_OR_HIDDEN')
        records.append({'name':name,'resourceType':'virtualMachines','locations':[workspace['location'].lower()],
            'family':family,'provider_status':list(status),
            'capabilities':[{'name':'vCPUs','value':str(n['num_cores'])},{'name':'MemoryGB','value':str(n['memory_mb']/1024)}]})
    if progress:progress({'stage':'SCOPED_SKUS','status':'COMPLETE','requested':list(APPROVED_SKUS),'source':SOURCE})
    return ScopedSkuEvidence(tuple(records),APPROVED_SKUS,'eastus')

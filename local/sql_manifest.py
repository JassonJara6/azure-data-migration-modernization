"""Local SQL evidence, deliberately distinct from Azure/SQL COMMITTED receipts."""
import hashlib
import json
from pathlib import Path
import re
from admm_silver.contracts import TABLES,CONTRACT_HASH,digest,validate_bundle,uuid,ticks
from admm_silver.storage import verify_files

ACCOUNT='localsql'  # In-memory legacy contract binding, NOT an Azure storage account.
FORMAT='admm.local.sql-export.v1'


class ExportError(RuntimeError):
    def __init__(self,code,*,sqlstate=None):
        super().__init__(code);self.sqlstate=sqlstate;self.stage=None;self.source_table=None


def require(condition,code):
    if not condition:raise ExportError(code)


def checksum(value):
    body={k:v for k,v in value.items() if k!='manifest_sha256'}
    return hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')).hexdigest()


def silver_input(manifest,root,*,verify=True):
    """Translate verified LOCAL authority into the unchanged Silver function API.

    COMMITTED is an internal legacy enum only: this dictionary is never written
    as an Azure receipt. The external manifest uses LOCAL_EXPORTED_VERIFIED and
    explicitly remote_commit_confirmed=false; no SQL commit/watermark is changed.
    """
    require(manifest.get('format')==FORMAT and manifest.get('status')=='LOCAL_EXPORTED_VERIFIED','LOCAL_SQL_MANIFEST_FORMAT_INVALID')
    require(manifest.get('manifest_sha256')==checksum(manifest),'LOCAL_SQL_MANIFEST_HASH_MISMATCH')
    require(manifest.get('contract_sha256')==CONTRACT_HASH,'LOCAL_SQL_CONTRACT_CHANGED')
    source=manifest.get('source',{})
    require(source.get('kind')=='SQL_SERVER_LOCAL' and source.get('database')=='AdventureWorks' and
        type(source.get('engine_edition')) is int and source['engine_edition'] in (2,3,4) and
        source.get('isolation')=='SERIALIZABLE' and source.get('read_transaction_end')=='ROLLBACK' and
        source.get('remote_commit_confirmed') is False,'LOCAL_SQL_PROVENANCE_INVALID')
    parent=uuid(manifest['export_id']);ticks(manifest['export_completed_at_utc'])
    require(manifest.get('data_dir')=='bronze-'+parent,'LOCAL_SQL_DATA_ROOT_INVALID')
    bronze=(Path(root)/manifest['data_dir']).resolve()
    require(bronze.parent==Path(root).resolve(),'LOCAL_SQL_DATA_ROOT_INVALID')
    tables=manifest.get('tables')
    require(isinstance(tables,list) and len(tables)==11,'LOCAL_SQL_ELEVEN_SOURCES_REQUIRED')
    by_source={t['source']:t for t in tables}
    require(len(by_source)==11 and set(by_source)=={t['source'] for t in TABLES},'LOCAL_SQL_SOURCE_SET_INVALID')
    records=[]
    for index,table in enumerate(TABLES,1):
        item=by_source[table['source']]
        require(item['mode']==table['mode'] and item['pk']==table['pk'] and item.get('sql_schema_verified') is True,
                'LOCAL_SQL_SOURCE_CONTRACT_INVALID')
        require(type(item['source_rows']) is int and item['source_rows']>=0 and
            item['source_rows']==item['exported_rows'],'LOCAL_SQL_ROW_RECONCILIATION_FAILED')
        child=uuid(item['table_export_id'])
        m={k:item[k] for k in ('folder','lower','upper','lower_key_json','upper_key_json','files')}
        m.update(run_id=child,parent_run_id=parent,load_type=table['mode'],account=ACCOUNT,
            filesystem='bronze',reconciled_rows=item['exported_rows'])
        text=json.dumps(m,sort_keys=True,separators=(',',':'))
        records.append(dict(config_id=index,source=table['source'],child_run_id=child,status='COMMITTED',
            load_type=table['mode'],primary_key_columns=json.dumps(table['pk']),manifest_json=text,manifest_sha256=digest(text)))
    bundle=dict(format_version=1,status='COMMITTED',parent_run_id=parent,
        committed_at_utc=manifest['export_completed_at_utc'],committed_children=11,
        manifest_sha256=digest(''.join(r['child_run_id']+':'+r['manifest_sha256']+';' for r in records)),tables=records,
        evidence_kind='LOCAL_SQL_EXPORT',remote_commit_confirmed=False)
    # Original schema/cursor/path/hash controls stay enabled. File verification is
    # also repeated on replay, before the Silver cached-success shortcut.
    receipts,_=validate_bundle(bundle,ACCOUNT)
    if verify:
        for table in TABLES:verify_files(bronze,receipts[table['source']]['manifest'])
    return bundle,bronze


def load(path):
    from admm_silver.contracts import strict_json
    value=strict_json(Path(path).read_text(encoding='utf-8'))
    require(isinstance(value,dict),'LOCAL_SQL_MANIFEST_OBJECT_REQUIRED')
    return value

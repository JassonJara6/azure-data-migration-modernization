"""Versioned contracts, committed-evidence checks and exact SQL cursor parsing."""
import hashlib
import json
import re
from datetime import datetime
from importlib.resources import files
from uuid import UUID

CONTRACT=json.loads(files('admm_silver').joinpath('contract.json').read_text())
TABLES=CONTRACT['tables']
METADATA={'_pipeline_run_id','_parent_pipeline_run_id','_source_system','_load_type',
          '_watermark_lower','_watermark_upper','_watermark_lower_key_json',
          '_watermark_upper_key_json','_ingested_at_utc'}
CONTRACT_HASH=hashlib.sha256(json.dumps(CONTRACT,sort_keys=True).encode()).hexdigest()

class SilverError(ValueError):
    """Only static reason codes; never include source rows or credentials."""


def require(condition,code):
    if not condition:raise SilverError(code)


def identifier(value):
    require(isinstance(value,str) and re.fullmatch(r'[a-z_][a-z0-9_]{0,62}',value),'INVALID_IDENTIFIER')
    return value


def uuid(value):
    require(isinstance(value,str) and str(UUID(value))==value,'INVALID_RUN_ID')
    return value


def strict_json(text):
    def pairs(items):
        result={}
        for key,value in items:
            require(key not in result,'DUPLICATE_JSON_KEY');result[key]=value
        return result
    return json.loads(text,object_pairs_hook=pairs)


def digest(text):return hashlib.sha256(text.encode('utf-16le')).hexdigest()


def ticks(value):
    match=re.fullmatch(r'(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d{1,7}))?Z?',value)
    require(match is not None,'INVALID_SQL_TIMESTAMP')
    delta=datetime.fromisoformat(match[1]+'T'+match[2])-datetime(1970,1,1)
    return (delta.days*86400+delta.seconds)*10_000_000+int((match[3] or '').ljust(7,'0'))


def cursor(date,key,pk):
    keys=strict_json(key)
    require(set(keys)==set(pk) and all(type(v) is int and 0<=v<=2147483647 for v in keys.values()),'CURSOR_KEYS_INVALID')
    return (ticks(date),*(keys[k] for k in pk))


def validate_bundle(bundle,account):
    require(re.fullmatch(r'[a-z0-9]{3,24}',account) is not None,'INVALID_STORAGE_ACCOUNT')
    require(bundle['format_version']==1 and bundle['status']=='COMMITTED','UNCOMMITTED_BUNDLE')
    uuid(bundle['parent_run_id']);ticks(bundle['committed_at_utc'])
    rows=bundle['tables'];require(len(rows)==11,'ELEVEN_TABLES_REQUIRED')
    require(all(type(r['config_id']) is int and r['config_id']>0 for r in rows),'CONFIG_ID_INVALID')
    require(len({r['child_run_id'] for r in rows})==11 and len({r['config_id'] for r in rows})==11,'DUPLICATE_RECEIPT')
    by_source={r['source']:r for r in rows}
    require(set(by_source)=={t['source'] for t in TABLES},'SOURCE_SET_MISMATCH')
    for table in TABLES:
        row=by_source[table['source']];uuid(row['child_run_id'])
        require(row['status']=='COMMITTED' and row['load_type']==table['mode'] and
                strict_json(row['primary_key_columns'])==table['pk'],'RECEIPT_CONTRACT_MISMATCH')
        text=row['manifest_json'];require(digest(text)==row['manifest_sha256'],'MANIFEST_HASH_MISMATCH')
        manifest=strict_json(text)
        require(manifest['parent_run_id']==bundle['parent_run_id'] and manifest['run_id']==row['child_run_id'] and
                manifest['account']==account and manifest['filesystem']=='bronze' and manifest['load_type']==table['mode'],
                'MANIFEST_IDENTITY_MISMATCH')
        path=manifest['folder']
        require(re.fullmatch(re.escape(table['path'])+'/load_type='+table['mode'].lower()+
            r'/ingestion_date=\d{4}-\d\d-\d\d/run_id='+row['child_run_id'],path) is not None,'BRONZE_PATH_INVALID')
        n=manifest['reconciled_rows'];require(type(n) is int and n>=0,'INVALID_ROW_COUNT')
        require(len({f['name'] for f in manifest['files']})==len(manifest['files']),'DUPLICATE_FILE')
        for f in manifest['files']:
            require(re.fullmatch(r'part-[A-Za-z0-9_-]+\.parquet',f['name']) and
                    re.fullmatch(r'[0-9a-f]{64}',f['sha256']) and type(f['bytes']) is int and f['bytes']>0 and
                    type(f['rows']) is int and f['rows']>=0,'INVALID_FILE_EVIDENCE')
        require(sum(f['rows'] for f in manifest['files'])==n,'MANIFEST_COUNT_MISMATCH')
        if table['mode']=='INCREMENTAL':
            lower=cursor(manifest['lower'],manifest['lower_key_json'],table['pk'])
            upper=cursor(manifest['upper'],manifest['upper_key_json'],table['pk'])
            require(upper>=lower and (n!=0 or upper==lower),'CURSOR_WINDOW_INVALID')
        else:require(not any(manifest[k] for k in ('lower','upper','lower_key_json','upper_key_json')),'FULL_CURSOR_INVALID')
        row['manifest']=manifest
    hashes=''.join(r['child_run_id']+':'+r['manifest_sha256']+';' for r in sorted(rows,key=lambda r:r['config_id']))
    require(bundle['committed_children']==11 and digest(hashes)==bundle['manifest_sha256'],'PARENT_HASH_MISMATCH')
    # Computed only from authoritative receipt content, not mutable parsed copies.
    batch_digest=hashlib.sha256((bundle['parent_run_id']+bundle['manifest_sha256']+CONTRACT_HASH).encode()).hexdigest()
    return by_source,batch_digest


def check_sequence(table,manifest,progress,parent_time):
    """Separate Silver checkpoint; never reads/writes a Bronze watermark."""
    if progress:
        require(ticks(parent_time)>ticks(progress['committed_at_utc']),'OUT_OF_ORDER_PARENT')
    if table['mode']=='INCREMENTAL':
        lower=cursor(manifest['lower'],manifest['lower_key_json'],table['pk'])
        expected=cursor(progress['upper'],progress['upper_key_json'],table['pk']) if progress else (ticks('1900-01-01T00:00:00'),*([0]*len(table['pk'])))
        require(lower==expected,'SILVER_CURSOR_GAP_OR_OVERLAP')

"""Independent, read-only validation of the exact immutable Parquet run folder.

SQL attestation is a separate privileged action. No credentials or data rows are
printed. Timestamp comparisons preserve SQL datetime2's seven-digit precision.
"""
from __future__ import annotations
import argparse
import hashlib
import io
import json
import re
import sys
from datetime import datetime

REQUIRED_METADATA = {'_pipeline_run_id','_parent_pipeline_run_id','_source_system',
 '_load_type','_watermark_lower','_watermark_upper','_watermark_lower_key_json',
 '_watermark_upper_key_json','_ingested_at_utc'}


def strict_json(text):
    def pairs(items):
        result = {}
        for key,value in items:
            if key in result: raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=pairs)


def ticks(value):
    """Naive SQL datetime2 to integer 100-ns units, never floating point."""
    match = re.fullmatch(r'(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d{1,7}))?Z?',str(value))
    if not match: raise ValueError('Invalid SQL timestamp')
    date = datetime.fromisoformat(match[1]+'T'+match[2])
    delta = date-datetime(1970,1,1)
    return (delta.days*86400+delta.seconds)*10_000_000+int((match[3] or '').ljust(7,'0'))


def cursor_key(value, keys):
    result = strict_json(value)
    if not isinstance(result,dict) or set(result)!=set(keys): raise ValueError('Cursor key set differs from PK')
    values = tuple(result[k] for k in keys)
    if any(type(v) is not int or not 0<=v<=2147483647 for v in values): raise ValueError('Invalid integer cursor key')
    return values


def manifest_digest(text):
    # SQL HASHBYTES over NVARCHAR has exactly this byte representation.
    return hashlib.sha256(text.encode('utf-16le')).hexdigest()


def reconcile(files, expected_rows, run_id, plan=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if type(expected_rows) is not int or expected_rows<0 or not run_id: raise ValueError('Invalid reconciliation identity/count')
    names = [name for name,_ in files]
    if len(names)!=len(set(names)): raise ValueError('Duplicate file names')
    if any(not re.fullmatch(r'part-[A-Za-z0-9_-]+\.parquet',n) for n in names): raise ValueError('Unexpected object in run folder')
    if not files and expected_rows: raise ValueError('Missing nonempty destination')
    keys = strict_json(plan['primary_key_columns']) if plan else []
    if plan and (not isinstance(keys,list) or not keys or len(keys)!=len(set(keys))): raise ValueError('Invalid PK contract')
    mode = plan['load_type'] if plan else None
    if plan and mode not in ('FULL','INCREMENTAL'): raise ValueError('Invalid load mode')
    lower = upper = None
    if mode=='INCREMENTAL':
        lower=(ticks(plan['watermark_lower']),*cursor_key(plan['lower_key_json'],keys))
        upper=(ticks(plan['watermark_upper']),*cursor_key(plan['upper_key_json'],keys))
        if upper<lower or (expected_rows==0 and upper!=lower): raise ValueError('Invalid or invented cursor boundary')
    total=0; schemas=set(); seen=set(); manifests=[]; maximum=None
    for name,data in sorted(files):
        parquet=pq.ParquetFile(io.BytesIO(data)); schema=parquet.schema_arrow
        if len(schema.names)!=len(set(schema.names)): raise ValueError('Duplicate Parquet columns')
        required=REQUIRED_METADATA|set(keys)|({'ModifiedDate'} if mode=='INCREMENTAL' else set())
        if required-set(schema.names): raise ValueError('Missing source/ingestion columns')
        schemas.add(str(schema.remove_metadata())); file_rows=0
        for batch in parquet.iter_batches(batch_size=65536):
            # Do not materialize arbitrary source timestamps as Python datetime:
            # nanoseconds would require pandas or lose precision. Only metadata
            # and PKs become Python values; ModifiedDate is inspected as Arrow.
            rows=batch.select(sorted(REQUIRED_METADATA|set(keys))).to_pylist()
            for index,row in enumerate(rows):
                if row['_pipeline_run_id']!=run_id or row['_source_system']!='adventureworks': raise ValueError('Run/source mismatch')
                ticks(row['_ingested_at_utc'])
                if plan:
                    if row['_parent_pipeline_run_id']!=plan['parent_run_id'] or row['_load_type']!=mode: raise ValueError('Parent/mode mismatch')
                    pk=tuple(row[k] for k in keys)
                    if any(v is None for v in pk) or pk in seen: raise ValueError('Null or duplicate destination PK')
                    seen.add(pk)
                    if mode=='INCREMENTAL':
                        if any(type(v) is not int for v in pk): raise ValueError('Invalid incremental PK type')
                        if ticks(row['_watermark_lower'])!=lower[0] or ticks(row['_watermark_upper'])!=upper[0]: raise ValueError('Boundary metadata mismatch')
                        if cursor_key(row['_watermark_lower_key_json'],keys)!=lower[1:] or cursor_key(row['_watermark_upper_key_json'],keys)!=upper[1:]: raise ValueError('Boundary key metadata mismatch')
                        column=batch.column(batch.schema.get_field_index('ModifiedDate'))
                        scalar=column[index]
                        if scalar.is_valid and pa.types.is_timestamp(column.type):
                            ns=scalar.cast(pa.timestamp('ns')).cast(pa.int64()).as_py()
                            if ns%100: raise ValueError('Sub-datetime2 precision')
                            date_ticks=ns//100
                        elif scalar.is_valid: date_ticks=ticks(scalar.as_py())
                        else: raise ValueError('Null source watermark')
                        value=(date_ticks,*pk)
                        if not lower<value<=upper: raise ValueError('Destination row outside bounded cursor')
                        maximum=value if maximum is None else max(maximum,value)
                    elif any(row[field] not in ('',None) for field in ('_watermark_lower','_watermark_upper','_watermark_lower_key_json','_watermark_upper_key_json')):
                        raise ValueError('FULL row carries cursor metadata')
                total+=1;file_rows+=1
        manifests.append({'name':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'rows':file_rows})
    if len(schemas)>1: raise ValueError('Parquet schemas differ')
    if total!=expected_rows: raise ValueError('Destination count differs from plan')
    if mode=='INCREMENTAL' and expected_rows and maximum!=upper: raise ValueError('Destination does not reach captured upper tuple')
    result={'status':'RECONCILED_READ_ONLY','run_id':run_id,'expected_rows':expected_rows,
            'reconciled_rows':total,'files':manifests}
    if plan:
        result.update(parent_run_id=plan['parent_run_id'],folder=plan['bronze_path'],load_type=mode,
                      lower=plan['watermark_lower'],upper=plan['watermark_upper'],
                      lower_key_json=plan['lower_key_json'],upper_key_json=plan['upper_key_json'])
    return result


def snapshot(client,folder,expected_rows):
    from azure.core.exceptions import ResourceNotFoundError
    try: paths=list(client.get_paths(path=folder,recursive=True))
    except ResourceNotFoundError:
        if expected_rows==0: return {}
        raise ValueError('Missing nonempty run folder') from None
    result={}
    for p in paths:
        prefix=folder+'/'
        if p.is_directory or not p.name.startswith(prefix) or not re.fullmatch(r'part-[A-Za-z0-9_-]+\.parquet',p.name[len(prefix):]): raise ValueError('Unexpected destination object')
        if p.name in result: raise ValueError('Duplicate destination path')
        result[p.name]=(str(p.etag),p.content_length)
    return result


def read_and_reconcile(client,plan,account,filesystem):
    from azure.core import MatchConditions
    folder=plan['bronze_path']; run_id=plan['child_run_id']
    expected=plan['expected_rows']
    if filesystem!='bronze' or not re.fullmatch(r'[a-z0-9]{3,24}',account): raise ValueError('Invalid storage binding')
    if not folder.startswith('adventureworks/') or '/..' in folder or folder.split('/')[-1]!='run_id='+run_id: raise ValueError('Invalid exact run folder')
    before=snapshot(client,folder,expected)
    files=[]
    for name,(etag,size) in sorted(before.items()):
        data=client.get_file_client(name).download_file(etag=etag,match_condition=MatchConditions.IfNotModified).readall()
        if len(data)!=size: raise ValueError('Destination byte size changed')
        files.append((name[len(folder)+1:],data))
    evidence=reconcile(files,expected,run_id,plan)
    if snapshot(client,folder,expected)!=before: raise ValueError('Destination mutated during reconciliation')
    for f in evidence['files']: f['etag']=before[folder+'/'+f['name']][0]
    evidence.update(account=account,filesystem=filesystem)
    text=json.dumps(evidence,sort_keys=True,separators=(',',':'),ensure_ascii=True)
    return evidence,text,manifest_digest(text)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account',required=True);parser.add_argument('--filesystem',default='bronze')
    parser.add_argument('--plan-json',required=True,help='Protected SQL audit snapshot, not a manually invented plan')
    args=parser.parse_args()
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient
        with open(args.plan_json) as f: plan=json.load(f)
        with DataLakeServiceClient('https://'+args.account+'.dfs.core.windows.net',DefaultAzureCredential()) as service:
            evidence,_,_=read_and_reconcile(service.get_file_system_client(args.filesystem),plan,args.account,args.filesystem)
        print(json.dumps(evidence,sort_keys=True));return 0
    except Exception as exc:
        print(json.dumps({'status':'RECONCILIATION_FAILED','error_type':type(exc).__name__}),file=sys.stderr);return 1

if __name__=='__main__':sys.exit(main())

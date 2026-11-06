"""Read-only loopback SQL Server -> immutable typed Bronze Parquet + local manifest."""
from dataclasses import dataclass,field
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
from uuid import uuid4
import pyarrow as pa
import pyarrow.parquet as pq
from admm_silver.contracts import TABLES,CONTRACT_HASH,ticks
from local.sql_manifest import ExportError,require,checksum,FORMAT,silver_input


@dataclass(frozen=True)
class Settings:
    user:str=field(repr=False)
    password:str=field(repr=False)
    server:str='127.0.0.1,1433'
    driver:str='ODBC Driver 18 for SQL Server'

    def connection_string(self):
        match=re.fullmatch(r'(?:127\.0\.0\.1|localhost)(?:,([0-9]{1,5}))?',self.server,re.I)
        require(match is not None and (match[1] is None or 1<=int(match[1])<=65535),'LOCAL_SQL_LOOPBACK_REQUIRED')
        require(self.driver=='ODBC Driver 18 for SQL Server','LOCAL_SQL_DRIVER_18_REQUIRED')
        require(bool(self.user and self.password) and not any('\0' in v for v in (self.user,self.password)),
                'LOCAL_SQL_CREDENTIALS_REQUIRED')
        def quoted(value):return '{'+value.replace('}','}}')+'}'
        values={'DRIVER':self.driver,'SERVER':self.server,'DATABASE':'AdventureWorks','UID':self.user,'PWD':self.password}
        # Self-signed certificate trust is limited to the strictly loopback local
        # development endpoint. Never reuse this setting for a cloud endpoint.
        return ';'.join(k+'='+quoted(v) for k,v in values.items())+';Encrypt=yes;TrustServerCertificate=yes;ApplicationIntent=ReadOnly;'


def connect(settings):
    text=settings.connection_string()
    try:import pyodbc
    except ImportError:raise ExportError('LOCAL_SQL_PYODBC_REQUIRED') from None
    require(settings.driver in pyodbc.drivers(),'LOCAL_SQL_ODBC_DRIVER_MISSING')
    try:return pyodbc.connect(text,autocommit=False,timeout=15)
    except Exception as exc:raise safe_error('LOCAL_SQL_CONNECTION_FAILED',exc) from None


def safe_error(code,exc):
    value=exc.args[0] if getattr(exc,'args',()) else None
    state=value if isinstance(value,str) and re.fullmatch(r'[A-Z0-9]{5}',value) else None
    return ExportError(code,sqlstate=state)


def quote(name):
    require(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',name) is not None,'LOCAL_SQL_IDENTIFIER_INVALID')
    return '['+name+']'


def table_name(table):return '.'.join(quote(n) for n in table['source'].split('.'))


def expression(column):
    name=quote(column['name']);ty=column['type']
    if ty=='timestamp':value='CONVERT(varchar(27),CONVERT(datetime2(7),'+name+'),126)'
    elif ty.startswith('decimal'):value='CONVERT('+ty+','+name+')'
    elif ty=='opaque':value=name+'.Serialize()'
    elif column['sql_type']=='XML':value='CONVERT(nvarchar(max),'+name+')'
    elif column['sql_type']=='uniqueidentifier':value='CONVERT(nvarchar(36),'+name+')'
    else:value=name
    return value+' AS '+name


def select_sql(table):
    keys=(['ModifiedDate'] if table['mode']=='INCREMENTAL' else [])+table['pk']
    return 'SELECT '+','.join(expression(c) for c in table['columns'])+' FROM '+table_name(table)+' ORDER BY '+','.join(quote(k) for k in keys)


def upper_sql(table):
    modified=next(c for c in table['columns'] if c['name']=='ModifiedDate')
    return 'SELECT TOP (1) '+expression(modified)+','+','.join(quote(k) for k in table['pk'])+' FROM '+table_name(table)+\
        ' ORDER BY '+','.join(quote(k)+' DESC' for k in ['ModifiedDate',*table['pk']])


def schema_sql(table):
    return ("SELECT c.name,TYPE_NAME(c.system_type_id),TYPE_NAME(c.user_type_id),c.precision,c.scale "
        "FROM sys.columns c WHERE c.object_id=OBJECT_ID(N'"+table['source']+"') ORDER BY c.column_id")


def pk_sql(table):
    return ("SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id "
        "JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
        "WHERE i.is_primary_key=1 AND ic.key_ordinal>0 AND i.object_id=OBJECT_ID(N'"+table['source']+"') ORDER BY ic.key_ordinal")


def check_schema(table,columns,pk):
    require([c[0] for c in columns]==[c['name'] for c in table['columns']],'LOCAL_SQL_SCHEMA_COLUMNS_CHANGED')
    require([r[0] for r in pk]==table['pk'],'LOCAL_SQL_PRIMARY_KEY_CHANGED')
    for spec,actual in zip(table['columns'],columns):
        _,system,declared,precision,scale=actual;ty=spec['type']
        if ty=='opaque':valid=declared=='geography'
        elif ty=='timestamp':valid=system=='datetime'
        elif ty=='int':valid=system=='int'
        elif ty=='smallint':valid=system==('tinyint' if spec['sql_type']=='tinyint' else 'smallint')
        elif ty=='boolean':valid=system=='bit'
        elif ty.startswith('decimal'):
            p,s=map(int,ty[8:-1].split(','));valid=system in ('money','decimal','numeric') and (precision,scale)==(p,s)
        elif spec['sql_type']=='uniqueidentifier':valid=system=='uniqueidentifier'
        elif spec['sql_type']=='XML':valid=system=='xml'
        else:valid=system in ('nvarchar','varchar','nchar','char')
        require(valid,'LOCAL_SQL_SCHEMA_TYPE_CHANGED')


def arrow(table,records,metadata):
    arrays=[];names=[]
    for index,spec in enumerate(table['columns']):
        name,ty=spec['name'],spec['type'];values=[r[index] for r in records]
        if ty=='timestamp':
            values=[ticks(v)*100 if v is not None else None for v in values]
            value=pa.array(values,type=pa.int64()).cast(pa.timestamp('ns'))
        else:
            physical={'int':pa.int32(),'smallint':pa.int16(),'boolean':pa.bool_(),'string':pa.string(),'opaque':pa.binary()}.get(ty)
            if ty.startswith('decimal'):
                p,s=map(int,ty[8:-1].split(','));physical=pa.decimal128(p,s)
            value=pa.array(values,type=physical)
        names.append(name);arrays.append(value)
    for name,value in sorted(metadata.items()):names.append(name);arrays.append(pa.array([value]*len(records),type=pa.string()))
    return pa.Table.from_arrays(arrays,names=names).replace_schema_metadata({
        b'admm.provenance':b'LOCAL_SQL_SERVER_EXPORT',b'admm.database':b'AdventureWorks',b'admm.remote_commit_confirmed':b'false'})


def now():return datetime.now(timezone.utc).isoformat(timespec='microseconds').removesuffix('+00:00')


def export(root,settings,*,chunk_rows=5000,expected_counts=None,connector=connect):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    require(type(chunk_rows) is int and 1<=chunk_rows<=100000,'LOCAL_SQL_CHUNK_SIZE_INVALID')
    require(not (root/'local-sql-export.json').exists(),'LOCAL_SQL_EXPORT_ALREADY_EXISTS')
    settings.connection_string()  # Validate endpoint/credentials before injected or real I/O.
    parent=str(uuid4());data_dir='bronze-'+parent;bronze=root/data_dir;started=now()
    connection=None;cursor=None;rolled_back=False;items=[];stage='CONNECT';current_source=None
    try:
        connection=connector(settings);connection.timeout=120;cursor=connection.cursor()
        cursor.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
        cursor.execute('SET LOCK_TIMEOUT 60000')
        stage='IDENTITY'
        identity=cursor.execute("SELECT DB_NAME(),CAST(SERVERPROPERTY('EngineEdition') AS int)").fetchone()
        require(identity and identity[0]=='AdventureWorks' and identity[1] in (2,3,4),'LOCAL_SQL_DATABASE_IDENTITY_INVALID')
        counts={}
        # Acquire all row/range read locks before reading any table's payload.
        for table in TABLES:
            current_source=table['source'];stage='SCHEMA'
            columns=cursor.execute(schema_sql(table)).fetchall();pk=cursor.execute(pk_sql(table)).fetchall()
            check_schema(table,columns,pk)
            stage='COUNT'
            count=cursor.execute('SELECT COUNT_BIG(*) FROM '+table_name(table)).fetchone()[0]
            require(type(count) is int and count>=0,'LOCAL_SQL_COUNT_INVALID');counts[table['source']]=count
        for source,expected in (expected_counts or {}).items():
            stage='EXPECTED_COUNTS';current_source=source
            require(counts.get(source)==expected,'LOCAL_SQL_EXPECTED_COUNT_MISMATCH')
        for table in TABLES:
            current_source=table['source'];stage='UPPER_CURSOR'
            child=str(uuid4());inc=table['mode']=='INCREMENTAL'
            lower='1900-01-01T00:00:00' if inc else '';zero=json.dumps(dict.fromkeys(table['pk'],0)) if inc else ''
            high=cursor.execute(upper_sql(table)).fetchone() if inc else None
            upper=high[0] if high else lower;upper_key=json.dumps(dict(zip(table['pk'],high[1:]))) if high else zero
            if inc:
                require(isinstance(upper,str) and ticks(upper)>=ticks(lower),'LOCAL_SQL_CURSOR_INVALID')
                require((counts[table['source']]>0)==bool(high),'LOCAL_SQL_UPPER_COUNT_MISMATCH')
            item=dict(source=table['source'],mode=table['mode'],pk=table['pk'],sql_schema_verified=True,table_export_id=child,
                folder=table['path']+'/load_type='+table['mode'].lower()+'/ingestion_date='+started[:10]+'/run_id='+child,
                lower=lower,upper=upper,lower_key_json=zero,upper_key_json=upper_key,source_rows=counts[table['source']],exported_rows=0,files=[])
            metadata=dict(_pipeline_run_id=child,_parent_pipeline_run_id=parent,_source_system='adventureworks',_load_type=table['mode'],
                _watermark_lower=lower,_watermark_upper=upper,_watermark_lower_key_json=zero,_watermark_upper_key_json=upper_key,_ingested_at_utc=started)
            stage='READ_ROWS'
            cursor.execute(select_sql(table))
            require([d[0] for d in cursor.description]==[c['name'] for c in table['columns']],'LOCAL_SQL_RESULT_COLUMNS_CHANGED')
            while True:
                batch=cursor.fetchmany(chunk_rows)
                if not batch:break
                part=bronze/item['folder']/f"part-{len(item['files']):05d}.parquet";part.parent.mkdir(parents=True,exist_ok=True)
                stage='WRITE_PARQUET'
                with part.open('xb') as stream:pq.write_table(arrow(table,batch,metadata),stream,version='2.6')
                data=part.read_bytes()
                item['files'].append(dict(name=part.name,bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),rows=len(batch),etag='LOCAL_SQL_EXPORT'))
                item['exported_rows']+=len(batch);stage='READ_ROWS'
            require(item['exported_rows']==item['source_rows'],'LOCAL_SQL_ROW_RECONCILIATION_FAILED');items.append(item)
        # Success ends the READ transaction with ROLLBACK, never COMMIT.
        stage='ROLLBACK';current_source=None
        connection.rollback();rolled_back=True
    except Exception as exc:
        failure=exc if isinstance(exc,ExportError) else safe_error('LOCAL_SQL_EXPORT_FAILED',exc)
        failure.stage=stage;failure.source_table=current_source
        raise failure from None
    finally:
        if connection is not None:
            if not rolled_back:
                try:connection.rollback()
                except Exception:pass
            if cursor is not None:
                try:cursor.close()
                except Exception:pass
            try:connection.close()
            except Exception:pass
    manifest=dict(format=FORMAT,status='LOCAL_EXPORTED_VERIFIED',contract_sha256=CONTRACT_HASH,export_id=parent,
        export_started_at_utc=started,export_completed_at_utc=now(),data_dir=data_dir,tables=items,
        source=dict(kind='SQL_SERVER_LOCAL',database='AdventureWorks',engine_edition=int(identity[1]),isolation='SERIALIZABLE',
            read_transaction_end='ROLLBACK',remote_commit_confirmed=False,run_id_scope='LOCAL_EXPORT_UUID_NOT_ADF'))
    manifest['manifest_sha256']=checksum(manifest)
    silver_input(manifest,root)  # Validate every actual Parquet hash/schema/count before publishing the local manifest.
    # Exclusive publication, last. An interrupted JSON write is invalid and
    # fails closed; never adopt or overwrite another export's manifest.
    path=root/'local-sql-export.json'
    with path.open('x',encoding='utf-8') as stream:
        json.dump(manifest,stream,indent=2,sort_keys=True);stream.flush();os.fsync(stream.fileno())
    return manifest

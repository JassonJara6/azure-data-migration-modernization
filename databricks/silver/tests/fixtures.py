"""Synthetic fixture data only. Never an assertion about the real Bronze run."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from admm_silver.contracts import TABLES,METADATA,digest,ticks
PARENT='10000000-0000-0000-0000-000000000001'
DATE='2020-01-01T00:00:00.1234567'


def row(table):
    result={}
    for c in table['columns']:
        name=c['name'];ty=c['type']
        result[name]=None if c['nullable'] else (1 if ty in ('int','smallint') else True if ty=='boolean' else Decimal('1') if ty.startswith('decimal') else DATE if ty=='timestamp' else ' value ')
    for key in table['pk']:result[key]='US' if key=='CountryRegionCode' else 1
    for key in ('CountryRegionCode','StateProvinceID','TerritoryID','ProductCategoryID','ProductSubcategoryID','CustomerID','ProductID','SalesOrderID','BusinessEntityID','PersonID','BillToAddressID','ShipToAddressID'):
        if key in result:result[key]='US' if key=='CountryRegionCode' else 1
    if 'rowguid' in result:result['rowguid']='00000000-0000-0000-0000-000000000001'
    if table['source']=='Person.Person':result.update(PersonType='IN',EmailPromotion=0)
    if table['source']=='Sales.SalesOrderHeader':result.update(SubTotal=Decimal('10'),TaxAmt=Decimal('1'),Freight=Decimal('1'),TotalDue=Decimal('12'),Status=1)
    if table['source']=='Sales.SalesOrderDetail':result.update(OrderQty=1,UnitPrice=Decimal('10'),UnitPriceDiscount=Decimal('0'),LineTotal=Decimal('10'))
    return result


def manifest(table,parent=PARENT,index=1):
    child=f'20000000-0000-0000-0000-{index+int(parent[-12:])*100:012d}'
    incremental=table['mode']=='INCREMENTAL'
    return dict(run_id=child,parent_run_id=parent,folder=table['path']+'/load_type='+table['mode'].lower()+'/ingestion_date=2020-01-01/run_id='+child,
       reconciled_rows=1,load_type=table['mode'],lower='1900-01-01T00:00:00' if incremental else '',upper=DATE if incremental else '',
       lower_key_json=json.dumps(dict.fromkeys(table['pk'],0)) if incremental else '',upper_key_json=json.dumps(dict.fromkeys(table['pk'],1)) if incremental else '',
       account='storage123',filesystem='bronze',files=[])


def raw_rows(table,m,data=None):
    data=data if data is not None else [row(table)]
    for item in data:
        item.update(_pipeline_run_id=m['run_id'],_parent_pipeline_run_id=m['parent_run_id'],_source_system='adventureworks',_load_type=table['mode'],
         _watermark_lower=m['lower'],_watermark_upper=m['upper'],_watermark_lower_key_json=m['lower_key_json'],_watermark_upper_key_json=m['upper_key_json'],_ingested_at_utc=DATE)
    return data


def arrow(table,rows):
    arrays=[];names=[]
    for c in table['columns']:
        name=c['name'];ty=c['type'];values=[r[name] for r in rows]
        if ty=='timestamp':arr=pa.array([ticks(v)*100 if v is not None else None for v in values],type=pa.int64()).cast(pa.timestamp('ns'))
        else:
            physical={'int':pa.int32(),'smallint':pa.int16(),'boolean':pa.bool_(),'string':pa.string(),'opaque':pa.binary()}.get(ty)
            if ty.startswith('decimal'):
                precision,scale=map(int,ty[8:-1].split(','));physical=pa.decimal128(precision,scale)
            arr=pa.array(values,type=physical)
        names.append(name);arrays.append(arr)
    for name in sorted(METADATA):names.append(name);arrays.append(pa.array([r[name] for r in rows],type=pa.string()))
    return pa.Table.from_arrays(arrays,names=names)


def batch(root,parent=PARENT,mutate=None):
    records=[]
    for index,table in enumerate(TABLES,1):
        m=manifest(table,parent,index);rows=raw_rows(table,m)
        if mutate:mutate(table,rows)
        path=Path(root)/m['folder']/'part-0000.parquet';path.parent.mkdir(parents=True)
        pq.write_table(arrow(table,rows),path,version='2.6')
        content=path.read_bytes()
        m['files']=[dict(name=path.name,bytes=len(content),sha256=hashlib.sha256(content).hexdigest(),rows=len(rows),etag='synthetic')]
        m['reconciled_rows']=len(rows);text=json.dumps(m,sort_keys=True,separators=(',',':'))
        records.append(dict(config_id=index,source=table['source'],child_run_id=m['run_id'],status='COMMITTED',load_type=table['mode'],
           primary_key_columns=json.dumps(table['pk']),manifest_json=text,manifest_sha256=digest(text)))
    return dict(format_version=1,status='COMMITTED',parent_run_id=parent,committed_at_utc='2020-01-02T00:00:00',committed_children=11,
       manifest_sha256=digest(''.join(r['child_run_id']+':'+r['manifest_sha256']+';' for r in records)),tables=records)

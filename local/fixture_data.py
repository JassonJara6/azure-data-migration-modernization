"""Explicit SYNTHETIC AdventureWorks-shaped Parquet; never real Bronze evidence."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from admm_silver.contracts import TABLES,METADATA,digest,ticks

PARENTS={1:'30000000-0000-0000-0000-000000000001',2:'30000000-0000-0000-0000-000000000002'}
DATES={1:'2020-02-01T00:00:00.1234567',2:'2020-02-02T00:00:00.1234567'}


def rows(table,phase):
    source=table['source']
    ids={'Person.Person':[1,2],'Person.Address':[1,2],'Production.Product':list(range(1,4+(phase==2))),
        'Production.ProductSubcategory':[1,2],'Sales.Customer':[1,2,3,4 if phase==1 else 5],
        'Sales.SalesOrderHeader':[1,2,3],'Sales.SalesOrderDetail':[1,2,3,4,5]+([6] if phase==2 else [])}.get(source,[1])
    result=[]
    for index in ids:
        row={}
        for column in table['columns']:
            name,ty=column['name'],column['type']
            row[name]=(None if column['nullable'] else 1 if ty in ('int','smallint') else True if ty=='boolean'
                else Decimal('1') if ty.startswith('decimal') else DATES[phase] if ty=='timestamp' else 'Fixture')
        for key in table['pk']:row[key]='US' if key=='CountryRegionCode' else index
        if 'rowguid' in row:row['rowguid']=f'40000000-0000-0000-0000-{index:012d}'
        for name in ('StateProvinceID','TerritoryID','ProductCategoryID'):
            if name in row and name not in table['pk']:row[name]=1
        if 'CountryRegionCode' in row:row['CountryRegionCode']='US'
        if source=='Person.Person':row.update(PersonType='IN',EmailPromotion=0,FirstName='Fixture'+str(index)+('Updated' if phase==2 else ''),LastName='Person')
        if source=='Production.Product':
            row.update(Name='Fixture product '+str(index),ProductNumber='FX-'+str(index),
                ProductSubcategoryID=None if index==3 else 1,StandardCost=Decimal('1'),ListPrice=Decimal('10'))
        if source=='Production.ProductSubcategory':row['Name']='Fixture subcategory '+str(index)+(' updated' if phase==2 else '')
        if source=='Sales.Customer':row.update(PersonID=index if index<=2 else None,StoreID=None if index<=2 else 90+index,AccountNumber='FXC-'+str(index))
        if source=='Sales.SalesOrderHeader':
            subtotal={1:Decimal('38') if phase==1 else Decimal('48'),2:Decimal('60') if phase==1 else Decimal('70'),3:Decimal('17.5')}[index]
            row.update(CustomerID=index,OrderDate='2020-01-01T00:00:00' if index<=2 else '2020-01-03T00:00:00',
                DueDate='2020-01-05T00:00:00',BillToAddressID=1,ShipToAddressID=2,Status=5,
                SubTotal=subtotal,TaxAmt=Decimal('1'),Freight=Decimal('1'),TotalDue=subtotal+2)
        if source=='Sales.SalesOrderDetail':
            order,product,quantity,price,discount={1:(1,1,2 if phase==1 else 3,'10','0'),2:(1,2,1,'20','.1'),
                3:(2,2,3,'20','0'),4:(3,3,1,'7.5','0'),5:(3,1,1,'10','0'),6:(2,4,2,'5','0')}[index]
            row.update(SalesOrderID=order,ProductID=product,OrderQty=quantity,UnitPrice=Decimal(price),UnitPriceDiscount=Decimal(discount),
                LineTotal=Decimal(price)*(1-Decimal(discount))*quantity)
        result.append(row)
    if phase==2 and table['mode']=='INCREMENTAL':
        # Realistic partial change windows: leave untouched Silver keys in place.
        changed={'Person.Person':{1},'Person.Address':{1},'Production.Product':{1,4},
                 'Sales.SalesOrderHeader':{1,2},'Sales.SalesOrderDetail':{1,6}}[source]
        result=[r for r in result if r[table['pk'][-1]] in changed]
    return result


def arrow(table,data):
    arrays=[];names=[]
    for column in table['columns']:
        name,ty=column['name'],column['type'];values=[r[name] for r in data]
        if ty=='timestamp':value=pa.array([ticks(v)*100 if v else None for v in values],type=pa.int64()).cast(pa.timestamp('ns'))
        else:
            physical={'int':pa.int32(),'smallint':pa.int16(),'boolean':pa.bool_(),'string':pa.string(),'opaque':pa.binary()}.get(ty)
            if ty.startswith('decimal'):
                precision,scale=map(int,ty[8:-1].split(','));physical=pa.decimal128(precision,scale)
            value=pa.array(values,type=physical)
        names.append(name);arrays.append(value)
    for name in sorted(METADATA):names.append(name);arrays.append(pa.array([r[name] for r in data],type=pa.string()))
    return pa.Table.from_arrays(arrays,names=names)


def generate(root,phase=1):
    """Immutable deterministic fixtures; refuse to overwrite a different byte stream."""
    root=Path(root);records=[];parent=PARENTS[phase]
    for index,table in enumerate(TABLES,1):
        child=f'50000000-0000-0000-0000-{phase*100+index:012d}'
        data=rows(table,phase);incremental=table['mode']=='INCREMENTAL'
        upper={k:max(tuple(r[p] for p in table['pk']) for r in data)[i] for i,k in enumerate(table['pk'])}
        previous=rows(table,1);lower={k:max(tuple(r[p] for p in table['pk']) for r in previous)[i] for i,k in enumerate(table['pk'])}
        manifest=dict(run_id=child,parent_run_id=parent,folder=table['path']+'/load_type='+table['mode'].lower()+
            '/ingestion_date=2020-02-0'+str(phase)+'/run_id='+child,load_type=table['mode'],account='storage123',filesystem='bronze',
            lower=('1900-01-01T00:00:00' if phase==1 else DATES[1]) if incremental else '',upper=DATES[phase] if incremental else '',
            lower_key_json=json.dumps(dict.fromkeys(table['pk'],0) if phase==1 else lower) if incremental else '',
            upper_key_json=json.dumps(upper) if incremental else '',reconciled_rows=len(data),files=[])
        for row in data:row.update(_pipeline_run_id=child,_parent_pipeline_run_id=parent,_source_system='adventureworks',
            _load_type=table['mode'],_watermark_lower=manifest['lower'],_watermark_upper=manifest['upper'],
            _watermark_lower_key_json=manifest['lower_key_json'],_watermark_upper_key_json=manifest['upper_key_json'],_ingested_at_utc=DATES[phase])
        sink=pa.BufferOutputStream();pq.write_table(arrow(table,data),sink,version='2.6');content=sink.getvalue().to_pybytes()
        path=root/manifest['folder']/'part-0000.parquet';path.parent.mkdir(parents=True,exist_ok=True)
        if path.exists():
            if path.read_bytes()!=content:raise ValueError('SYNTHETIC_FIXTURE_CHANGED')
        else:path.write_bytes(content)
        manifest['files']=[dict(name=path.name,bytes=len(content),sha256=hashlib.sha256(content).hexdigest(),rows=len(data),etag='SYNTHETIC')]
        text=json.dumps(manifest,sort_keys=True,separators=(',',':'))
        records.append(dict(config_id=index,source=table['source'],child_run_id=child,status='COMMITTED',load_type=table['mode'],
            primary_key_columns=json.dumps(table['pk']),manifest_json=text,manifest_sha256=digest(text)))
    return dict(format_version=1,status='COMMITTED',parent_run_id=parent,committed_at_utc='2020-02-0'+str(phase+1)+'T00:00:00',
        committed_children=11,manifest_sha256=digest(''.join(r['child_run_id']+':'+r['manifest_sha256']+';' for r in records)),tables=records)

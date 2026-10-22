import copy
import hashlib
import json
from pathlib import Path
from decimal import Decimal
import pytest
from pyspark.sql import functions as F
from admm_silver.contracts import TABLES,validate_bundle,SilverError,check_sequence,ticks
from admm_silver.quality import transform,business_frame,missing_relationships
from admm_silver.storage import verify_files,DeltaStore
from admm_silver.pipeline import run
from fixtures import batch,manifest,raw_rows,arrow,DATE,PARENT


def raw(spark,tmp_path,table,rows):
    import pyarrow.parquet as pq
    path=tmp_path/'raw.parquet';pq.write_table(arrow(table,rows),path,version='2.6')
    return spark.read.parquet(str(path))


def test_registry_matches_existing_bronze_contract():
    import re
    root=Path(__file__).resolve().parents[3]
    seed=(root/'sql/metadata/002_seed_ingestion_metadata.sql').read_text()
    declared=re.findall(r"\(N'(\w+)', N'(\w+)', N'(FULL|INCREMENTAL)', N'(\[.*?\])', (?:N'ModifiedDate'|NULL), N'([^']+)'",seed)
    assert len(TABLES)==11 and sum(t['mode']=='INCREMENTAL' for t in TABLES)==5
    assert [(t['source'],t['mode'],t['pk'],t['path']) for t in TABLES]==[(s+'.'+n,m,json.loads(k),p) for s,n,m,k,p in declared]


def test_bundle_checks_real_hashes_files_and_receipts(tmp_path):
    b=batch(tmp_path);rows,_=validate_bundle(b,'storage123')
    for t in TABLES:assert len(verify_files(tmp_path,rows[t['source']]['manifest'])[0])==1
    f=next(tmp_path.rglob('*.parquet'));f.write_bytes(b'changed')
    matched=next(r['manifest'] for r in rows.values() if str(f.parent).endswith(r['child_run_id']))
    with pytest.raises(SilverError,match='FILE_SIZE_CHANGED'):verify_files(tmp_path,matched)

@pytest.mark.parametrize('change',[lambda b:b.update(status='COPIED'),lambda b:b.update(committed_children=10),
 lambda b:b['tables'].pop(),lambda b:b['tables'][0].update(manifest_sha256='a'*64),lambda b:b.update(manifest_sha256='b'*64)])
def test_invalid_evidence_fails_closed(tmp_path,change):
    b=batch(tmp_path);change(b)
    with pytest.raises(SilverError):validate_bundle(b,'storage123')


def test_checkpoint_gap_and_out_of_order_rejected():
    t=TABLES[0];m=manifest(t)
    check_sequence(t,m,None,'2020-01-02T00:00:00')
    p=dict(upper=m['upper'],upper_key_json=m['upper_key_json'],committed_at_utc='2020-01-02T00:00:00')
    with pytest.raises(SilverError,match='GAP'):check_sequence(t,m,p,'2020-01-03T00:00:00')
    with pytest.raises(SilverError,match='OUT_OF_ORDER'):check_sequence(t,m,p,'2020-01-01T00:00:00')

@pytest.mark.parametrize('index',range(11))
def test_each_table_preserves_business_and_100ns(spark,tmp_path,index):
    t=TABLES[index];m=manifest(t);rows=raw_rows(t,m)
    df=transform(raw(spark,tmp_path,t,rows),t,m,PARENT,{c['name']:'ns' for c in t['columns'] if c['type']=='timestamp'})
    result=df.first().asDict()
    assert not result['_quality_errors']
    assert result['ModifiedDate__ticks_100ns']==ticks(DATE)
    for c in t['columns']:
        if c['type'] not in ('timestamp',):assert result[c['name']]==rows[0][c['name']]


def test_duplicate_composite_keys_and_business_rejections(spark,tmp_path):
    t=next(t for t in TABLES if t['source']=='Sales.SalesOrderDetail');m=manifest(t)
    rows=raw_rows(t,m);rows.append(copy.deepcopy(rows[0]));rows[0]['OrderQty']=0
    df=transform(raw(spark,tmp_path,t,rows),t,m,PARENT,{'ModifiedDate':'ns'})
    records=df.collect()
    assert all('DUPLICATE_PK' in r['_quality_errors'] for r in records)
    assert 'BUSINESS_RULE_FAILED' in records[0]['_quality_errors']


def test_null_keys_and_schema_drift_are_not_dropped(spark,tmp_path):
    t=TABLES[0];m=manifest(t);rows=raw_rows(t,m);rows[0]['BusinessEntityID']=None
    df=raw(spark,tmp_path,t,rows)
    assert 'NULL_PK' in transform(df,t,m,PARENT,{'ModifiedDate':'ns'}).first()['_quality_errors']
    with pytest.raises(SilverError,match='SCHEMA_COLUMNS'):transform(df.withColumn('Unexpected',F.lit(1)),t,m,PARENT)


def test_delta_full_snapshot_and_incremental_merge(spark):
    spark.sql('CREATE DATABASE IF NOT EXISTS merge_test');store=DeltaStore(spark,'merge_test')
    def source(rows):return spark.createDataFrame(rows,'id int, value string, _silver_row_hash string')
    store.apply('sample',source([(1,'old','a'),(2,'keep','b')]),['id'],'FULL')
    store.apply('sample',source([(1,'updated','c'),(3,'new','d')]),['id'],'INCREMENTAL')
    assert sorted((r.id,r.value) for r in store.read('sample').collect())==[(1,'updated'),(2,'keep'),(3,'new')]
    store.apply('sample',source([(1,'updated','c')]),['id'],'FULL')
    assert [(r.id,r.value) for r in store.read('sample').collect()]==[(1,'updated')]


def test_complete_parent_and_replay_without_delta_writes(spark,tmp_path,monkeypatch):
    b=batch(tmp_path);spark.sql('CREATE DATABASE IF NOT EXISTS complete_test')
    original=DeltaStore.upsert_record;failed=[]
    def interrupt(self,target,record,key):
        if target=='_silver_progress' and not failed:
            failed.append(True);raise RuntimeError('Synthetic crash after Delta write before checkpoint')
        return original(self,target,record,key)
    monkeypatch.setattr(DeltaStore,'upsert_record',interrupt)
    with pytest.raises(RuntimeError,match='Synthetic crash'):run(spark,b,account='storage123',bronze_root=str(tmp_path),namespace='complete_test')
    result=run(spark,b,account='storage123',bronze_root=str(tmp_path),namespace='complete_test')
    assert result['status']=='SUCCEEDED' and len(result['metrics'])==11
    store=DeltaStore(spark,'complete_test');before={t['target']:store.version(t['target']) for t in TABLES}
    assert run(spark,b,account='storage123',bronze_root=str(tmp_path),namespace='complete_test')['replayed']
    assert before=={t['target']:store.version(t['target']) for t in TABLES}
    # Synthetic R2 only: existing incremental windows empty, FULL snapshots reload.
    import shutil
    from admm_silver.contracts import digest
    second=batch(tmp_path,parent='10000000-0000-0000-0000-000000000002')
    second['committed_at_utc']='2020-01-03T00:00:00'
    for record in second['tables']:
        m=json.loads(record['manifest_json'])
        if record['load_type']=='INCREMENTAL':
            m.update(lower=m['upper'],lower_key_json=m['upper_key_json'],files=[],reconciled_rows=0)
            shutil.rmtree(tmp_path/m['folder'])
            record['manifest_json']=json.dumps(m,sort_keys=True,separators=(',',':'))
            record['manifest_sha256']=digest(record['manifest_json'])
    second['manifest_sha256']=digest(''.join(r['child_run_id']+':'+r['manifest_sha256']+';' for r in second['tables']))
    r2=run(spark,second,account='storage123',bronze_root=str(tmp_path),namespace='complete_test')
    assert all(m['input_rows']==0 for name,m in r2['metrics'].items() if next(t for t in TABLES if t['source']==name)['mode']=='INCREMENTAL')
    assert all(m['target_rows']==1 for m in r2['metrics'].values())


def test_quality_failure_quarantines_and_does_not_publish(spark,tmp_path):
    def corrupt(t,rows):
        if t['source']=='Sales.SalesOrderDetail':rows[0]['UnitPriceDiscount']=Decimal('2')
    b=batch(tmp_path,mutate=corrupt);spark.sql('CREATE DATABASE IF NOT EXISTS rejected_test')
    with pytest.raises(SilverError,match='QUALITY_GATE_FAILED'):run(spark,b,account='storage123',bronze_root=str(tmp_path),namespace='rejected_test')
    store=DeltaStore(spark,'rejected_test')
    assert store.read('_quarantine').count()==1
    assert not any(store.exists(t['target']) for t in TABLES)
    assert store.record('_silver_batches','parent_run_id',PARENT)['status']=='FAILED'


def test_equal_timestamps_composite_upper_is_lexicographic(spark,tmp_path):
    t=next(t for t in TABLES if t['source']=='Sales.SalesOrderDetail');m=manifest(t)
    m['upper_key_json']=json.dumps({'SalesOrderID':2,'SalesOrderDetailID':1})
    rows=[]
    for order,detail in [(1,9),(2,1),(2,2),(3,0)]:
        item=raw_rows(t,m)[0];item.update(SalesOrderID=order,SalesOrderDetailID=detail);rows.append(item)
    result=transform(raw(spark,tmp_path,t,rows),t,m,PARENT,{'ModifiedDate':'ns'}).orderBy('SalesOrderID','SalesOrderDetailID').collect()
    assert ['CURSOR_OUT_OF_BOUNDS' in r['_quality_errors'] for r in result]==[False,False,True,True]


def test_lossy_decimal_cast_is_rejected(spark,tmp_path):
    t=next(t for t in TABLES if t['source']=='Sales.SalesOrderDetail');m=manifest(t)
    df=raw(spark,tmp_path,t,raw_rows(t,m)).withColumn('UnitPrice',F.lit('1.12345'))
    assert 'LOSSY_DECIMAL_CAST' in transform(df,t,m,PARENT,{'ModifiedDate':'ns'}).first()['_quality_errors']


def test_string_timestamp_keeps_seventh_digit(spark,tmp_path):
    t=TABLES[0];m=manifest(t)
    df=raw(spark,tmp_path,t,raw_rows(t,m)).withColumn('ModifiedDate',F.lit(DATE))
    result=transform(df,t,m,PARENT).first()
    assert result['ModifiedDate__ticks_100ns']==ticks(DATE)


def test_int96_submicroseconds_fail_instead_of_truncating(spark,tmp_path):
    import pyarrow.parquet as pq
    t=TABLES[0];m=manifest(t);folder=tmp_path/m['folder'];folder.mkdir(parents=True)
    path=folder/'part-000.parquet'
    pq.write_table(arrow(t,raw_rows(t,m)),path,use_deprecated_int96_timestamps=True)
    data=path.read_bytes();m['files']=[dict(name=path.name,bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),rows=1)]
    with pytest.raises(SilverError,match='INT96_SUBMICROSECOND_UNSUPPORTED'):verify_files(tmp_path,m)


def test_missing_relationship_is_detected(spark):
    from admm_silver.quality import RELATIONSHIPS
    candidates={}
    for t in TABLES:
        fields={key:'US' if key=='CountryRegionCode' else 1 for key in t['pk']}
        for child,fk,parent,pk in RELATIONSHIPS:
            if child==t['source']:fields[fk]='US' if fk=='CountryRegionCode' else 1
        candidates[t['source']]=spark.createDataFrame([fields])
    candidates['Sales.SalesOrderDetail']=candidates['Sales.SalesOrderDetail'].withColumn('ProductID',F.lit(999))
    rejected=[(source,df.count()) for source,df in missing_relationships(candidates)]
    assert sum(count for _,count in rejected)==1


def test_bundle_job_configuration_is_serial_and_secret_free():
    import yaml
    path=Path(__file__).resolve().parents[1]
    bundle=yaml.safe_load((path/'databricks.yml').read_text())
    job=bundle['resources']['jobs']['silver']
    assert job['max_concurrent_runs']==1 and len(job['tasks'])==1
    task=job['tasks'][0]
    assert 'new_cluster' not in task and 'existing_cluster_id' in task
    assert task['notebook_task']['base_parameters']['replay_check']=='true'
    assert not any(x in (path/'databricks.yml').read_text().lower() for x in ('password','token:','secret:'))


def test_databricks_volume_binding_must_match_real_container():
    from types import SimpleNamespace as NS
    from admm_silver.storage import verify_volume
    info=dict(volume_type='EXTERNAL',storage_location='abfss://bronze@storage123.dfs.core.windows.net/')
    spark=NS(sql=lambda query:NS(first=lambda:NS(asDict=lambda:info)))
    verify_volume(spark,'/Volumes/portfolio/landing/bronze','storage123')
    info['storage_location']='abfss://other@storage123.dfs.core.windows.net/'
    with pytest.raises(SilverError,match='BINDING_MISMATCH'):verify_volume(spark,'/Volumes/portfolio/landing/bronze','storage123')
    with pytest.raises(SilverError,match='EXTERNAL_VOLUME_REQUIRED'):verify_volume(spark,'/mnt/bronze','storage123')


def test_sql_export_is_read_only_and_collects_actual_receipts():
    import importlib.util
    path=Path(__file__).resolve().parents[1]/'tools/export_committed_parent.py'
    spec=importlib.util.spec_from_file_location('silver_export',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    assert 'bronze_parent_audit' in module.QUERY and 'bronze_run_audit' in module.QUERY
    import re
    assert not re.search(r'\b(UPDATE|INSERT|DELETE|EXEC|TRUNCATE|MERGE)\b',module.QUERY,re.I)
    assert 'watermark_state' not in module.QUERY

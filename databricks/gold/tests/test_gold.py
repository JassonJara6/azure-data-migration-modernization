"""Offline synthetic sales, Delta recovery and whole-lake idempotence."""
from datetime import datetime
from decimal import Decimal
import json
import pytest
from pyspark.sql import functions as F,types as T
from admm_silver.contracts import TABLES
from admm_silver.pipeline import run as silver_run
from admm_silver.storage import DeltaStore
from admm_gold.model import build,validate,GoldError,KEYS
from admm_gold.pipeline import run,publish
from local.fixture_data import rows,generate,PARENTS
from local.lake import register,history
from local.run import single_writer


def inputs(spark):
    result={}
    for table in TABLES:
        records=rows(table,1)
        for row in records:
            for column in table['columns']:
                if column['type']=='timestamp' and row[column['name']] is not None:
                    row[column['name']]=datetime.fromisoformat(row[column['name']])
        schema=T.StructType([T.StructField(c['name'],T.BinaryType() if c['type']=='opaque'
            else T._parse_datatype_string(c['type']),True) for c in table['columns']])
        result[table['target']]=spark.createDataFrame(records,schema)
    return result


def test_sales_model_counts_exact_measures_and_optional_members(spark):
    frames=inputs(spark);outputs=build(frames);metrics=validate(frames,outputs)
    assert {name:metrics[name]['rows'] for name in KEYS}==dict(dim_product=3,dim_customer=4,dim_date=3,
        fact_sales=5,agg_sales_daily=2,agg_product_month=3,agg_customer_month=3)
    assert Decimal(metrics['sales_reconciliation']['net_sales'])==Decimal('115.5')
    assert metrics['sales_reconciliation']['quantity']==8
    assert outputs['dim_product'].filter('product_key=3 AND category_id IS NULL').count()==1
    assert outputs['dim_customer'].filter("customer_key=3 AND customer_type='STORE' AND person_display_name IS NULL").count()==1
    assert outputs['dim_date'].filter("date='2020-01-02'").count()==1

@pytest.mark.parametrize('case,code',[('duplicate','GOLD_KEY_QUALITY_FAILED'),('null','GOLD_KEY_QUALITY_FAILED'),
    ('orphan','GOLD_FOREIGN_KEY_FAILED'),('subtotal','GOLD_HEADER_SUBTOTAL_MISMATCH'),('negative','GOLD_MEASURE_QUALITY_FAILED')])
def test_quality_failures_block_instead_of_dropping_rows(spark,case,code):
    frames=inputs(spark)
    if case=='duplicate':frames['sales_salesorderdetail']=frames['sales_salesorderdetail'].unionByName(frames['sales_salesorderdetail'])
    if case=='null':frames['sales_salesorderdetail']=frames['sales_salesorderdetail'].withColumn('ProductID',F.lit(None).cast('int'))
    if case=='orphan':frames['sales_salesorderdetail']=frames['sales_salesorderdetail'].withColumn('ProductID',F.lit(999))
    if case=='subtotal':frames['sales_salesorderheader']=frames['sales_salesorderheader'].withColumn('SubTotal',(F.col('SubTotal')+1).cast('decimal(19,4)'))
    if case=='negative':frames['sales_salesorderdetail']=frames['sales_salesorderdetail'].withColumn('OrderQty',F.lit(-1).cast('smallint'))
    with pytest.raises(GoldError,match=code) as caught:validate(frames,build(frames))
    assert caught.value.metrics

@pytest.mark.parametrize('case',['missing','type','sources'])
def test_schema_contract_fails_before_publication(spark,case):
    frames=inputs(spark)
    if case=='missing':frames['production_product']=frames['production_product'].drop('Name')
    if case=='type':frames['production_product']=frames['production_product'].withColumn('ProductID',F.col('ProductID').cast('string'))
    if case=='sources':frames.pop('person_address')
    with pytest.raises(GoldError):build(frames)


def test_sixth_decimal_digit_is_not_rounded_by_spark_subtraction(spark):
    frames=inputs(spark)
    frames['sales_salesorderdetail']=(frames['sales_salesorderdetail'].filter('SalesOrderDetailID=1')
        .withColumn('OrderQty',F.lit(1).cast('smallint')).withColumn('UnitPrice',F.lit('0.1234').cast('decimal(19,4)'))
        .withColumn('UnitPriceDiscount',F.lit('0.3333').cast('decimal(19,4)'))
        .withColumn('LineTotal',F.lit('0.082271').cast('decimal(38,6)')))
    line=build(frames)['fact_sales'].first()
    assert line.discount_amount==Decimal('0.041129') and line.net_sales==Decimal('0.082271')


def test_noop_snapshot_never_creates_even_an_empty_delta_merge(spark,tmp_path):
    register(spark,tmp_path,'noop_gold');store=DeltaStore(spark,'noop_gold')
    df=spark.createDataFrame([(1,'value')],'id int, value string')
    assert publish(spark,'noop_gold','sample',df,['id'])
    before=store.version('sample')
    assert publish(spark,'noop_gold','sample',df,['id']) is False
    assert store.version('sample')==before


def test_eleven_source_pipeline_recovery_replay_and_second_parent(spark,tmp_path,monkeypatch):
    for name in ('integration_silver','integration_gold'):register(spark,tmp_path,name)
    bronze=tmp_path/'bronze';bundle=generate(bronze)
    silver=silver_run(spark,bundle,account='storage123',bronze_root=str(bronze),namespace='integration_silver')
    assert len(silver['metrics'])==11 and all(m['rejected_rows']==0 for m in silver['metrics'].values())
    original=DeltaStore.upsert_record;crashed=[]
    def interrupt(self,target,record,key):
        if target=='_gold_progress' and not crashed:
            crashed.append(True);raise RuntimeError('SYNTHETIC_GOLD_INTERRUPT_AFTER_DELTA_WRITE')
        return original(self,target,record,key)
    monkeypatch.setattr(DeltaStore,'upsert_record',interrupt)
    with pytest.raises(RuntimeError,match='SYNTHETIC_GOLD_INTERRUPT'):
        run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[1])
    store=DeltaStore(spark,'integration_gold');written=store.version('dim_product')
    result=run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[1])
    assert store.version('dim_product')==written and result['status']=='SUCCEEDED'
    before={n:history(spark,n) for n in ('integration_silver','integration_gold')}
    assert silver_run(spark,bundle,account='storage123',bronze_root=str(bronze),namespace='integration_silver')['replayed']
    assert run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[1])['replayed']
    assert before=={n:history(spark,n) for n in before}
    second=generate(bronze,2)
    silver_run(spark,second,account='storage123',bronze_root=str(bronze),namespace='integration_silver')
    r2=run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[2])
    assert Decimal(r2['metrics']['sales_reconciliation']['net_sales'])==Decimal('135.5')
    assert r2['metrics']['sales_reconciliation']['quantity']==11
    assert store.read('fact_sales').count()==6 and store.read('dim_product').count()==4
    assert {r.customer_key for r in store.read('dim_customer').collect()}=={1,2,3,5}
    assert store.read('fact_sales').filter('sales_order_id=2 AND sales_order_detail_id=6').count()==1
    before={n:history(spark,n) for n in ('integration_silver','integration_gold')}
    assert silver_run(spark,second,account='storage123',bronze_root=str(bronze),namespace='integration_silver')['replayed']
    assert run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[2])['replayed']
    assert before=={n:history(spark,n) for n in before}
    with pytest.raises(GoldError,match='GOLD_SILVER_SNAPSHOT_NOT_CURRENT'):
        run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[1])
    # Independent edits cannot be adopted as an approved Gold replay.
    store.read('fact_sales').limit(1).write.format('delta').mode('append').saveAsTable(store.name('fact_sales'))
    with pytest.raises(GoldError,match='GOLD_CURRENT_SNAPSHOT_MOVED'):
        run(spark,silver_namespace='integration_silver',gold_namespace='integration_gold',parent_run_id=PARENTS[2])


def test_failed_or_missing_silver_parent_is_never_published(spark,tmp_path):
    for name in ('missing_silver','missing_gold'):register(spark,tmp_path,name)
    with pytest.raises(GoldError,match='GOLD_SILVER_BATCH_NOT_READY'):
        run(spark,silver_namespace='missing_silver',gold_namespace='missing_gold',parent_run_id=PARENTS[1])
    assert history(spark,'missing_gold')=={}


def test_local_single_writer_lock_fails_closed_and_is_removed(tmp_path):
    with single_writer(tmp_path):
        with pytest.raises(FileExistsError):
            with single_writer(tmp_path):pytest.fail('Second writer entered')
    assert not (tmp_path/'.writer.lock').exists()


def test_empty_sales_snapshot_is_valid_without_fabricated_calendar(spark):
    frames={name:df.limit(0) for name,df in inputs(spark).items()}
    output=build(frames);metrics=validate(frames,output)
    assert all(metrics[name]['rows']==0 for name in KEYS)
    assert metrics['sales_reconciliation']['net_sales']=='0'


def test_gold_cannot_overwrite_silver_namespace(spark):
    with pytest.raises(GoldError,match='GOLD_NAMESPACE_OVERLAP'):
        run(spark,silver_namespace='same',gold_namespace='same',parent_run_id=PARENTS[1])

# Databricks notebook source
# One Job invocation performs preflight, publication and a no-write replay check.
import json
from admm_silver.contracts import require,identifier,TABLES
from admm_silver.evidence import load_committed_evidence
from admm_silver.diagnostics import failure_report
from admm_silver.storage import verify_volume,DeltaStore
from admm_silver.pipeline import run

for name in ('catalog','schema','bronze_volume','receipt_file','account','parent_run_id','execution_id'):
    dbutils.widgets.text(name,'')
dbutils.widgets.text('replay_check','true')

# COMMAND ----------
stage='configuration'
try:
    catalog=identifier(dbutils.widgets.get('catalog'));schema=identifier(dbutils.widgets.get('schema'))
    namespace=catalog+'.'+schema
    bronze=dbutils.widgets.get('bronze_volume');receipt_file=dbutils.widgets.get('receipt_file')
    account=dbutils.widgets.get('account');parent=dbutils.widgets.get('parent_run_id')
    require(receipt_file.startswith('/Volumes/') and '..' not in receipt_file.split('/') and receipt_file.endswith('.json'),'RECEIPT_VOLUME_PATH_REQUIRED')
    require(dbutils.widgets.get('replay_check') in ('true','false'),'INVALID_REPLAY_SETTING')
    # Explicit session semantics; no credential or storage-key settings.
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.conf.set('spark.sql.caseSensitive','true')
    spark.conf.set('spark.sql.legacy.parquet.nanosAsLong','true')
    spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled','false')
    stage='unity_catalog_preflight'
    verify_volume(spark,bronze,account)
    require(catalog not in ('hive_metastore','spark_catalog'),'UNITY_CATALOG_REQUIRED')
    # Require existing schema with a managed storage location; never create infrastructure.
    spark.sql('DESCRIBE SCHEMA '+namespace).collect()
    stage='committed_evidence'
    bundle=load_committed_evidence(receipt_file,parent)
    stage='silver_apply'
    result=run(spark,bundle,account=account,bronze_root=bronze,namespace=namespace,execution_id=dbutils.widgets.get('execution_id'))
    if dbutils.widgets.get('replay_check')=='true':
        stage='idempotency_check';store=DeltaStore(spark,namespace)
        before={t['target']:store.version(t['target']) for t in TABLES}
        replay=run(spark,bundle,account=account,bronze_root=bronze,namespace=namespace,execution_id=dbutils.widgets.get('execution_id'))
        require(replay['replayed'] and before=={t['target']:store.version(t['target']) for t in TABLES},'REPLAY_CHANGED_DELTA')
        result['replay_verified']=True
    dbutils.jobs.taskValues.set(key='silver_status',value='SUCCEEDED')
except Exception as exc:
    # Hide raw Spark/cloud messages from notebook output (may contain row data).
    diagnostic=failure_report(exc,stage)
    code=diagnostic['error_code']
    print(json.dumps(diagnostic,sort_keys=True))
    # Include only this safe metadata in task error output as well as cell output.
    raise RuntimeError('SILVER_'+code+' '+json.dumps(diagnostic,sort_keys=True)) from None

# Keep exit outside the exception handler: Databricks exit may use a control exception.
dbutils.notebook.exit(json.dumps(result,sort_keys=True))

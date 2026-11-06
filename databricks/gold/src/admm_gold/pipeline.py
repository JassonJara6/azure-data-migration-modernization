"""Pinned Silver snapshot -> checked Gold snapshot, resumable per-table writes.

One local writer per lake is mandatory. There is no multi-table Delta transaction.
Consumers must check _gold_batches SUCCEEDED for the current snapshot.
"""
import hashlib
import json
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from admm_silver.contracts import TABLES,CONTRACT_HASH
from admm_silver.storage import DeltaStore
from .model import KEYS,build,validate,require,GoldError


def model_hash():
    root=Path(__file__).parent
    return hashlib.sha256(b''.join((root/name).read_bytes() for name in ('model.py','pipeline.py'))).hexdigest()


def versions(spark,namespace):
    store=DeltaStore(spark,namespace)
    return {name:store.version(name) for name in [*KEYS,'_gold_batches','_gold_progress'] if store.exists(name)}


def publish(spark,namespace,name,frame,keys):
    """FULL current-state refresh by key; skip even MERGE on identical relations."""
    store=DeltaStore(spark,namespace);table=store.name(name)
    data=frame.withColumn('_gold_row_hash',F.sha2(F.to_json(F.struct(*[F.col(c) for c in frame.columns]),
        options={'ignoreNullFields':'false'}),256))
    if not store.exists(name):data.write.format('delta').mode('errorifexists').saveAsTable(table);return True
    store.validate_target(name);current=store.read(name)
    require([(f.name,f.dataType) for f in current.schema]==[(f.name,f.dataType) for f in data.schema],'GOLD_TARGET_SCHEMA_DRIFT')
    if not data.exceptAll(current).take(1) and not current.exceptAll(data).take(1):return False
    (DeltaTable.forName(spark,table).alias('t').merge(data.alias('s'),
        ' AND '.join('t.`'+key+'` <=> s.`'+key+'`' for key in keys))
        .whenMatchedUpdateAll(condition='NOT (t._gold_row_hash <=> s._gold_row_hash)')
        .whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
    return True


def run(spark,*,silver_namespace,gold_namespace,parent_run_id):
    require(silver_namespace!=gold_namespace,'GOLD_NAMESPACE_OVERLAP')
    require(spark.conf.get('spark.sql.session.timeZone')=='UTC','GOLD_UTC_REQUIRED')
    require(spark.conf.get('spark.sql.caseSensitive')=='true','GOLD_CASE_SENSITIVE_REQUIRED')
    source=DeltaStore(spark,silver_namespace);target=DeltaStore(spark,gold_namespace)
    batch=source.record('_silver_batches','parent_run_id',parent_run_id)
    require(batch and batch['status']=='SUCCEEDED' and batch['contract_hash']==CONTRACT_HASH,'GOLD_SILVER_BATCH_NOT_READY')
    input_versions={};frames={}
    for table in TABLES:
        name=table['target'];checkpoint=source.record('_silver_progress','source',table['source'])
        require(checkpoint and checkpoint['parent_run_id']==parent_run_id and checkpoint['batch_digest']==batch['batch_digest'],
            'GOLD_SILVER_SNAPSHOT_NOT_CURRENT')
        source.validate_target(name);version=source.version(name)
        require(version==int(checkpoint['target_version']),'GOLD_SILVER_CHANGED_OUTSIDE_PIPELINE')
        input_versions[name]=version
        location=DeltaTable.forName(spark,source.name(name)).detail().select('location').first()[0]
        frames[name]=spark.read.format('delta').option('versionAsOf',version).load(location)
    binding={'parent':parent_run_id,'silver_namespace':silver_namespace,'silver_digest':batch['batch_digest'],
        'versions':input_versions,'model_hash':model_hash()}
    fingerprint=hashlib.sha256(json.dumps(binding,sort_keys=True).encode()).hexdigest()
    prior=target.record('_gold_batches','parent_run_id',parent_run_id)
    if prior:
        require(prior['fingerprint']==fingerprint,'GOLD_INPUT_OR_MODEL_CHANGED')
        if prior['status']=='SUCCEEDED':
            metrics=json.loads(prior['metrics'])
            require(all(target.exists(n) and target.version(n)==metrics[n]['target_version'] for n in KEYS),'GOLD_CURRENT_SNAPSHOT_MOVED')
            for name in KEYS:
                checkpoint=target.record('_gold_progress','target',name)
                require(checkpoint and checkpoint['fingerprint']==fingerprint and
                    int(checkpoint['target_version'])==metrics[name]['target_version'],'GOLD_CHECKPOINT_MISMATCH')
                metrics[name]['delta_written']=False
            return {'status':'SUCCEEDED','replayed':True,'parent_run_id':parent_run_id,'metrics':metrics}
    # Reject independent target modifications and allow an interrupted table write
    # carrying this exact fingerprint. Before adopting any existing table require
    # an existing progress record or our own interrupted commit marker.
    for name in KEYS:
        checkpoint=target.record('_gold_progress','target',name)
        if target.exists(name):
            target.validate_target(name)
            require((checkpoint and target.version(name)==int(checkpoint['target_version'])) or
                target.marker(name)==fingerprint+':'+name,'GOLD_TARGET_CHANGED_OUTSIDE_PIPELINE')
        else:require(not checkpoint,'GOLD_CHECKPOINT_TARGET_MISSING')
    audit={'parent_run_id':parent_run_id,'fingerprint':fingerprint,'status':'RUNNING','metrics':'{}',
        'input_versions':json.dumps(input_versions,sort_keys=True),'model_hash':binding['model_hash'],'error_code':''}
    target.upsert_record('_gold_batches',audit,'parent_run_id')
    outputs={};metrics={}
    try:
        outputs={name:df.cache() for name,df in build(frames).items()}
        metrics=validate(frames,outputs)
        require(all(source.version(name)==v for name,v in input_versions.items()),'GOLD_SILVER_CHANGED_DURING_READ')
        require(all(source.record('_silver_progress','source',t['source'])['parent_run_id']==parent_run_id
                    for t in TABLES),'GOLD_SILVER_CHANGED_DURING_READ')
        for name,df in outputs.items():
            checkpoint=target.record('_gold_progress','target',name)
            if checkpoint and checkpoint['fingerprint']==fingerprint:
                require(target.version(name)==int(checkpoint['target_version']),'GOLD_TARGET_CHANGED_OUTSIDE_PIPELINE')
                changed=False
            else:
                key='spark.databricks.delta.commitInfo.userMetadata';old=spark.conf.get(key,None)
                spark.conf.set(key,fingerprint+':'+name)
                try:changed=publish(spark,gold_namespace,name,df,KEYS[name])
                finally:
                    if old is None:spark.conf.unset(key)
                    else:spark.conf.set(key,old)
                target.upsert_record('_gold_progress',{'target':name,'fingerprint':fingerprint,
                    'parent_run_id':parent_run_id,'target_version':str(target.version(name))},'target')
            metrics[name].update(target_version=target.version(name),delta_written=changed)
        audit.update(status='SUCCEEDED',metrics=json.dumps(metrics,sort_keys=True))
        target.upsert_record('_gold_batches',audit,'parent_run_id')
        return {'status':'SUCCEEDED','replayed':False,'parent_run_id':parent_run_id,'metrics':metrics}
    except Exception as exc:
        audit.update(status='FAILED',error_code=str(exc) if isinstance(exc,GoldError) else 'GOLD_RUNTIME_FAILED',
            metrics=json.dumps(getattr(exc,'metrics',None) or metrics,sort_keys=True))
        target.upsert_record('_gold_batches',audit,'parent_run_id')
        raise
    finally:
        for df in outputs.values():df.unpersist()

"""Preflight all eleven, quarantine failures, then apply/resume table-atomic Delta writes.

Single writer per namespace is mandatory (Job max_concurrent_runs=1). Delta has
no eleven-table transaction; durable per-table progress makes repairs resumable.
"""
import json
from datetime import datetime,timezone
from pyspark.sql import functions as F
from .contracts import (TABLES,CONTRACT_HASH,SilverError,require,validate_bundle,check_sequence)
from .quality import transform,empty_schema,business_frame,missing_relationships,q
from .storage import DeltaStore,verify_files


def run(spark,bundle,*,account,bronze_root,namespace,execution_id="local"):
    require(spark.conf.get('spark.sql.session.timeZone')=='UTC','SPARK_TIMEZONE_MUST_BE_UTC')
    require(spark.conf.get('spark.sql.caseSensitive')=='true','SPARK_CASE_SENSITIVE_REQUIRED')
    require(spark.conf.get('spark.sql.legacy.parquet.nanosAsLong')=='true','SPARK_NANOS_AS_LONG_REQUIRED')
    receipts,batch_digest=validate_bundle(bundle,account)
    store=DeltaStore(spark,namespace);parent=bundle['parent_run_id']
    prior=store.record('_silver_batches','parent_run_id',parent)
    if prior:
        require(prior['batch_digest']==batch_digest,'BATCH_EVIDENCE_CHANGED')
        if prior['status']=='SUCCEEDED':return {'status':'SUCCEEDED','replayed':True,'parent_run_id':parent,'metrics':json.loads(prior['metrics'])}
    audit=dict(parent_run_id=parent,batch_digest=batch_digest,contract_hash=CONTRACT_HASH,status='RUNNING',error_code='',metrics='{}',execution_id=execution_id,
               started_at_utc=datetime.now(timezone.utc).isoformat(),finished_at_utc='')
    store.upsert_record('_silver_batches',audit,'parent_run_id')
    staged={};candidates={};metrics={};progress={};failures=[];cached=[]
    try:
        # Check every sequence/target before any business table write.
        for table in TABLES:
            target=table['target'];receipt=receipts[table['source']];manifest=receipt['manifest']
            checkpoint=store.record('_silver_progress','source',table['source']);progress[target]=checkpoint
            if checkpoint and checkpoint['parent_run_id']==parent:
                require(checkpoint['batch_digest']==batch_digest,'CHECKPOINT_EVIDENCE_CHANGED')
            else:check_sequence(table,manifest,checkpoint,bundle['committed_at_utc'])
            if store.exists(target):
                store.validate_target(target)
                if checkpoint:
                    require(store.version(target)==int(checkpoint['target_version']) or
                            store.marker(target)==batch_digest+':'+target,'TARGET_CHANGED_OUTSIDE_JOB')
                else:require(store.marker(target)==batch_digest+':'+target,'UNMANAGED_EXISTING_TARGET')
            elif checkpoint:require(False,'CHECKPOINT_TARGET_MISSING')
        for table in TABLES:
            target=table['target'];manifest=receipts[table['source']]['manifest']
            paths,units=verify_files(bronze_root,manifest)
            opaque_types={f.name:f.dataType for f in store.read(target).schema} if store.exists(target) else {}
            raw=spark.read.parquet(*paths) if paths else spark.createDataFrame([],empty_schema(table,opaque_types))
            try:df=transform(raw,table,manifest,parent,units)
            except SilverError as exc:
                failed=raw.select(F.to_json(F.struct(*[F.col(q(c)) for c in raw.columns]),options={'ignoreNullFields':'false'}).alias('_silver_raw_json'))\
                    .withColumn('_quality_errors',F.array(F.lit(str(exc))))
                store.quarantine([(table['source'],failed)],parent)
                raise
            df=df.cache();cached.append(df);count=df.count()
            require(count==manifest['reconciled_rows'],'BATCH_ROW_COUNT_MISMATCH')
            if count and table['mode']=='INCREMENTAL' and not df.filter(F.size('_quality_errors')>0).take(1):
                from .contracts import cursor
                maximum=df.select(F.max(F.struct(F.col('ModifiedDate__ticks_100ns'),*[F.col(k) for k in table['pk']])).alias('maximum')).first()[0]
                require(tuple(maximum)==cursor(manifest['upper'],manifest['upper_key_json'],table['pk']),'CAPTURED_UPPER_TUPLE_MISSING')
            rejected=df.filter(F.size('_quality_errors')>0)
            n=rejected.count();metrics[table['source']]={'input_rows':count,'rejected_rows':n}
            if n:failures.append((table['source'],rejected))
            staged[target]=df
            if store.exists(target) and table['mode']=='INCREMENTAL':
                existing=store.read(target)
                older=df.alias('s').join(existing.alias('t'),table['pk']).filter(F.col('s.ModifiedDate__ticks_100ns')<F.col('t.ModifiedDate__ticks_100ns'))
                require(not older.take(1),'SOURCE_UPDATE_REGRESSION')
                existing=existing.drop('_silver_row_hash','_silver_parent_id','_silver_batch_digest')
                existing=existing.withColumn('_quality_errors',F.array().cast('array<string>'))\
                    .withColumn('_silver_raw_json',F.to_json(F.struct(*[F.col(q(c)) for c in existing.columns])))
                candidates[table['source']]=existing.join(df.select(*table['pk']),table['pk'],'left_anti').unionByName(df)
            else:candidates[table['source']]=df
        if not failures:
            for source,rejected in missing_relationships(candidates):
                n=rejected.count()
                if n:
                    failures.append((source,rejected));metrics[source]['relationship_rejected_rows']=metrics[source].get('relationship_rejected_rows',0)+n
        if failures:
            # Combine same-source failures to replace one quarantine partition once.
            grouped={}
            for source,df in failures:
                output=df.select('_silver_raw_json','_quality_errors')
                grouped[source]=grouped[source].unionByName(output) if source in grouped else output
            store.quarantine(list(grouped.items()),parent)
            raise SilverError('QUALITY_GATE_FAILED')
        for source,df in candidates.items():
            df=df.cache();df.count();cached.append(df);candidates[source]=df
        # Hash checks again after Spark's lazy reads have been materialized.
        for table in TABLES:verify_files(bronze_root,receipts[table['source']]['manifest'])
        for table in TABLES:
            target=table['target'];checkpoint=progress[target];manifest=receipts[table['source']]['manifest']
            if not (checkpoint and checkpoint['parent_run_id']==parent):
                source=business_frame(staged[target],parent,batch_digest)
                marker_key='spark.databricks.delta.commitInfo.userMetadata'
                previous=spark.conf.get(marker_key,None)
                spark.conf.set(marker_key,batch_digest+':'+target)
                try:
                    if manifest['reconciled_rows'] or table['mode']=='FULL' or not store.exists(target):
                        store.apply(target,source,table['pk'],table['mode'])
                finally:
                    if previous is None:spark.conf.unset(marker_key)
                    else:spark.conf.set(marker_key,previous)
                store.upsert_record('_silver_progress',dict(source=table['source'],parent_run_id=parent,
                    batch_digest=batch_digest,committed_at_utc=bundle['committed_at_utc'],
                    upper=manifest['upper'],upper_key_json=manifest['upper_key_json'],
                    target_version=str(store.version(target))),'source')
            metrics[table['source']]['target_rows']=store.read(target).count()
            require(metrics[table['source']]['target_rows']==candidates[table['source']].count(),'TARGET_ROW_COUNT_MISMATCH')
            metrics[table['source']]['target_version']=store.version(target)
        audit.update(status='SUCCEEDED',metrics=json.dumps(metrics,sort_keys=True),finished_at_utc=datetime.now(timezone.utc).isoformat())
        store.upsert_record('_silver_batches',audit,'parent_run_id')
        return {'status':'SUCCEEDED','replayed':False,'parent_run_id':parent,'metrics':metrics}
    except Exception as exc:
        audit.update(status='FAILED',error_code=str(exc) if isinstance(exc,SilverError) else 'RUNTIME_FAILED',metrics=json.dumps(metrics,sort_keys=True),finished_at_utc=datetime.now(timezone.utc).isoformat())
        store.upsert_record('_silver_batches',audit,'parent_run_id')
        raise
    finally:
        for df in cached:df.unpersist()

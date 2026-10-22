"""UC volume/Parquet evidence verification and Delta table writes."""
import hashlib
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from .contracts import require,identifier


def verify_files(root,manifest):
    folder=Path(root)/manifest['folder']
    expected={f['name'] for f in manifest['files']}
    actual={p.name for p in folder.iterdir()} if folder.exists() else set()
    require(actual==expected,'DESTINATION_FILE_SET_CHANGED')
    units={};schemas=set();paths=[]
    for evidence in manifest['files']:
        path=folder/evidence['name'];require(path.is_file() and path.stat().st_size==evidence['bytes'],'FILE_SIZE_CHANGED')
        hasher=hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024),b''):hasher.update(block)
        require(hasher.hexdigest()==evidence['sha256'],'FILE_HASH_CHANGED')
        parquet=pq.ParquetFile(path);require(parquet.metadata.num_rows==evidence['rows'],'FILE_ROW_COUNT_CHANGED')
        schemas.add(str(parquet.schema_arrow.remove_metadata()))
        # Spark truncates INT96 to microseconds. Fail closed if Arrow observes
        # any finer component; native logical TIMESTAMP_NANOS is handled exactly.
        int96=[parquet.schema.column(i).name for i in range(len(parquet.schema)) if parquet.schema.column(i).physical_type=='INT96']
        if int96:
            for batch in parquet.iter_batches(columns=int96):
                for field,column in zip(batch.schema,batch.columns):
                    values=column.cast(pa.timestamp('ns')).cast(pa.int64()).to_pylist()
                    require(all(v is None or v%1000==0 for v in values),'INT96_SUBMICROSECOND_UNSUPPORTED')
        for field in parquet.schema_arrow:
            if pa.types.is_timestamp(field.type):
                require(field.type.tz in (None,'UTC'),'UNSUPPORTED_SOURCE_TIMEZONE')
                units[field.name]=field.type.unit
        paths.append(str(path))
    require(len(schemas)<=1,'PARQUET_SCHEMA_DRIFT_WITHIN_BATCH')
    return paths,units


def verify_volume(spark,volume_root,account):
    import re
    match=re.fullmatch(r'/Volumes/([a-z][a-z0-9_]*)/([a-z][a-z0-9_]*)/([a-z][a-z0-9_]*)',volume_root)
    require(match is not None,'EXTERNAL_VOLUME_REQUIRED')
    row=spark.sql('DESCRIBE VOLUME '+'.'.join(match.groups())).first().asDict()
    require(row.get('volume_type')=='EXTERNAL' and row.get('storage_location','').rstrip('/')==
            'abfss://bronze@'+account+'.dfs.core.windows.net','BRONZE_VOLUME_BINDING_MISMATCH')


class DeltaStore:
    def __init__(self,spark,namespace):
        for part in namespace.split('.'):identifier(part)
        self.spark=spark;self.namespace=namespace
    def name(self,target):return self.namespace+'.'+identifier(target)
    def exists(self,target):return self.spark.catalog.tableExists(self.name(target))
    def read(self,target):return self.spark.table(self.name(target))
    def version(self,target):
        from delta.tables import DeltaTable
        return DeltaTable.forName(self.spark,self.name(target)).history(1).select('version').first()[0]
    def marker(self,target):
        from delta.tables import DeltaTable
        return DeltaTable.forName(self.spark,self.name(target)).history(1).select('userMetadata').first()[0]
    def validate_target(self,target):
        from delta.tables import DeltaTable
        DeltaTable.forName(self.spark,self.name(target)) # Fail if provider differs.
    def apply(self,target,source,keys,mode):
        from delta.tables import DeltaTable
        from .quality import q
        name=self.name(target)
        if not self.exists(target):source.write.format('delta').mode('errorifexists').saveAsTable(name)
        else:
            self.validate_target(target)
            require(self.read(target).schema==source.schema or
                    [(f.name,f.dataType) for f in self.read(target).schema]==[(f.name,f.dataType) for f in source.schema],
                    'TARGET_SCHEMA_DRIFT')
            merge=DeltaTable.forName(self.spark,name).alias('t').merge(source.alias('s'),
                ' AND '.join('t.'+q(k)+' = s.'+q(k) for k in keys))\
                .whenMatchedUpdateAll(condition='NOT (t._silver_row_hash <=> s._silver_row_hash)')\
                .whenNotMatchedInsertAll()
            if mode=='FULL':merge=merge.whenNotMatchedBySourceDelete()
            merge.execute()
    def upsert_record(self,target,record,key):
        """Small control tables have explicit string schema, including null values."""
        from delta.tables import DeltaTable
        from pyspark.sql.types import StructType,StructField,StringType
        fields=sorted(record)
        df=self.spark.createDataFrame([tuple(record[k] for k in fields)],StructType([StructField(k,StringType()) for k in fields]))
        if not self.exists(target):df.write.format('delta').saveAsTable(self.name(target))
        else:DeltaTable.forName(self.spark,self.name(target)).alias('t').merge(df.alias('s'),'t.'+key+' = s.'+key).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    def record(self,target,key,value):
        from pyspark.sql import functions as F
        if not self.exists(target):return None
        rows=self.read(target).where(F.col(key)==value).take(2)
        require(len(rows)<=1,'CONTROL_KEY_DUPLICATE')
        return rows[0].asDict() if rows else None
    def quarantine(self,frames,parent):
        from pyspark.sql import functions as F
        for source,df in frames:
            output=df.select(F.lit(parent).alias('parent_run_id'),F.lit(source).alias('source'),
                  '_silver_raw_json',F.col('_quality_errors').alias('reason_codes'))
            # Replaces exactly one parent/source partition, retaining duplicates.
            output.write.format('delta').mode('overwrite').partitionBy('parent_run_id','source')\
                .option('replaceWhere',f"parent_run_id = '{parent}' AND source = '{source}'").saveAsTable(self.name('_quarantine'))

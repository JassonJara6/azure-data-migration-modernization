"""Local-only Spark/Delta session; persisted Delta paths, no Hive service required."""
import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def session(root):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    os.environ.setdefault('SPARK_LOCAL_IP','127.0.0.1')
    builder=(SparkSession.builder.master('local[2]').appName('admm-local-medallion')
        .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
        .config('spark.databricks.delta.snapshotPartitions','1').config('spark.driver.memory','2g')
        .config('spark.sql.warehouse.dir',(root/'warehouse').as_uri())
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.caseSensitive','true')
        .config('spark.sql.legacy.parquet.nanosAsLong','true'))
    jars=os.environ.get('SILVER_DELTA_JARS')
    builder=builder.config('spark.jars',jars) if jars else configure_spark_with_delta_pip(builder)
    spark=builder.getOrCreate();spark.sparkContext.setLogLevel('ERROR')
    return spark


def register(spark,root,namespace):
    from admm_silver.contracts import identifier
    identifier(namespace)
    path=Path(root).resolve()/namespace;path.mkdir(parents=True,exist_ok=True)
    uri=path.as_uri().replace("'","''")
    spark.sql('CREATE DATABASE IF NOT EXISTS '+namespace+" LOCATION '"+uri+"'")
    # A new process uses Spark's in-memory catalog but reuses actual Delta logs.
    for table in sorted(path.iterdir()):
        if table.is_dir() and (table/'_delta_log').is_dir():
            identifier(table.name)
            spark.sql('CREATE TABLE IF NOT EXISTS '+namespace+'.'+table.name+" USING DELTA LOCATION '"+
                table.as_uri().replace("'","''")+"'")


def history(spark,namespace):
    from admm_silver.storage import DeltaStore
    store=DeltaStore(spark,namespace)
    return {r.name:store.version(r.name) for r in spark.catalog.listTables(namespace)}

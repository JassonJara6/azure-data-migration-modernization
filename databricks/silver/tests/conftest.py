import os
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))

@pytest.fixture(scope='session')
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    os.environ.setdefault('SPARK_LOCAL_IP','127.0.0.1')
    root=tmp_path_factory.mktemp('spark')
    builder=SparkSession.builder.master('local[2]').appName('silver-offline-tests')\
      .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')\
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.driver.memory','2g')\
      .config('spark.sql.warehouse.dir',str(root/'warehouse'))\
      .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')\
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')\
      .config('spark.sql.session.timeZone','UTC').config('spark.sql.caseSensitive','true')\
      .config('spark.sql.legacy.parquet.nanosAsLong','true')
    jars=os.environ.get('SILVER_DELTA_JARS')
    builder=builder.config('spark.jars',jars) if jars else configure_spark_with_delta_pip(builder)
    session=builder.getOrCreate();session.sparkContext.setLogLevel('ERROR')
    yield session
    session.stop()

# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver | AdventureWorks Bronze -> Delta
# MAGIC This notebook deliberately requires complete source snapshots. Existing
# MAGIC incremental Bronze run folders are **not** complete snapshots.
# COMMAND ----------
dbutils.widgets.text("bronze_root", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.dropdown("confirmed_full_snapshots", "false", ["false", "true"])
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

root = dbutils.widgets.get("bronze_root").rstrip("/")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("silver_schema").strip()
confirmed = dbutils.widgets.get("confirmed_full_snapshots") == "true"
assert root.startswith("abfss://"), "Provide a valid abfss Bronze root"
assert catalog and catalog.replace("_", "").isalnum(), "Invalid catalog"
assert schema and schema.replace("_", "").isalnum(), "Invalid schema"
assert confirmed, "STOP: source folders must contain COMPLETE snapshots, not incremental deltas"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
# COMMAND ----------
# Supply an exact full-snapshot run folder for each source; never glob all
# historical run folders together.
dbutils.widgets.text("orders_path", "")
dbutils.widgets.text("details_path", "")
dbutils.widgets.text("products_path", "")
specs = [
    ("sales_order_header", "orders_path", ["SalesOrderID"], ["OrderDate"]),
    ("sales_order_detail", "details_path", ["SalesOrderID", "SalesOrderDetailID"], []),
    ("product", "products_path", ["ProductID"], []),
]
for target, widget, keys, dates in specs:
    path = dbutils.widgets.get(widget).rstrip("/")
    assert path.startswith(root + "/") and "/run_id=" in path, f"Exact run folder required: {widget}"
    df = spark.read.parquet(path)
    missing = set(keys) - set(df.columns)
    assert not missing, f"{target}: missing columns {missing}"
    for key in keys:
        assert df.filter(F.col(key).isNull()).limit(1).count() == 0, f"{target}: null key {key}"
    duplicates = df.groupBy(*keys).count().filter("count > 1").limit(1).count()
    assert duplicates == 0, f"{target}: duplicate primary key"
    for col in dates:
        df = df.withColumn(col, F.to_timestamp(F.col(col)))
    df = df.withColumn("_silver_processed_at", F.current_timestamp())
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"`{catalog}`.`{schema}`.`{target}`"))
    print(f"{target}: {df.count()} rows")

# Databricks notebook source
# MAGIC %md
# MAGIC # Gold | Sales analytics
# MAGIC Requires the Silver tables from 01_silver_adventureworks.py.
# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
catalog=dbutils.widgets.get("catalog").strip()
silver=dbutils.widgets.get("silver_schema").strip()
gold=dbutils.widgets.get("gold_schema").strip()
assert all(x and x.replace("_","").isalnum() for x in (catalog,silver,gold))
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{gold}`")
# COMMAND ----------
from pyspark.sql import functions as F
orders=spark.table(f"`{catalog}`.`{silver}`.`sales_order_header`")
details=spark.table(f"`{catalog}`.`{silver}`.`sales_order_detail`")
products=spark.table(f"`{catalog}`.`{silver}`.`product`")
fact=(details.alias("d")
      .join(orders.select("SalesOrderID","OrderDate").alias("o"),"SalesOrderID","inner")
      .join(products.select("ProductID","Name").alias("p"),"ProductID","left")
      .select("SalesOrderID","SalesOrderDetailID","ProductID",
              F.col("p.Name").alias("ProductName"),
              F.to_date("OrderDate").alias("OrderDate"),
              F.col("d.OrderQty").cast("long").alias("OrderQty"),
              F.col("d.UnitPrice").cast("decimal(19,4)").alias("UnitPrice"),
              F.col("d.LineTotal").cast("decimal(19,4)").alias("LineTotal")))
assert fact.count()==details.count(), "Missing orders or duplicated join keys"
assert fact.filter("ProductName IS NULL").count()==0, "Missing product references"
(fact.write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .saveAsTable(f"`{catalog}`.`{gold}`.`fact_sales`"))
# COMMAND ----------
monthly=(fact.groupBy(F.date_format("OrderDate","yyyy-MM").alias("order_month"))
         .agg(F.sum("LineTotal").alias("revenue"),F.sum("OrderQty").alias("units_sold"),
              F.count("*").alias("line_count")))
(monthly.write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .saveAsTable(f"`{catalog}`.`{gold}`.`sales_by_month`"))
top=(fact.groupBy("ProductID","ProductName")
     .agg(F.sum("LineTotal").alias("revenue"),F.sum("OrderQty").alias("units_sold")))
(top.write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .saveAsTable(f"`{catalog}`.`{gold}`.`sales_by_product`"))
assert monthly.agg(F.sum("revenue")).first()[0]==fact.agg(F.sum("LineTotal")).first()[0]
assert top.agg(F.sum("revenue")).first()[0]==fact.agg(F.sum("LineTotal")).first()[0]
display(monthly.orderBy("order_month"))

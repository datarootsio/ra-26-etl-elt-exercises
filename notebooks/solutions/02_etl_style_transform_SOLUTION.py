# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Question 1, the ETL way (SOLUTION)

# COMMAND ----------

dbutils.widgets.text("schema", "", "Your schema (e.g. ra26_elt_ex_nils)")
dbutils.widgets.text("catalog", "dtr_sandbox", "Catalog")

SCHEMA = dbutils.widgets.get("schema").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
assert SCHEMA

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders")
customers = spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers")

# COMMAND ----------

delivered = orders.filter(
    (F.col("order_status") == "delivered")
    & F.col("order_delivered_customer_date").isNotNull()
)

joined = delivered.join(customers, on="customer_id", how="inner")

with_delay = joined.withColumn(
    "delay_days",
    F.datediff(F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date")),
)

result = (
    with_delay.groupBy("customer_state")
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.avg("delay_days"), 1).alias("avg_delay_days"),
        F.round(
            100.0 * F.sum(F.when(F.col("delay_days") > 0, 1).otherwise(0)) / F.count("*"), 1
        ).alias("pct_late"),
    )
    .orderBy(F.desc("pct_late"))
)

display(result)

# COMMAND ----------

(
    result.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_delivery_performance_by_state_etl")
)

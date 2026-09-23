# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Q2 & Q3, PySpark versions (SOLUTION)
# MAGIC Reference only — students choose PySpark or dbt per question, so most
# MAGIC won't build both of these. Useful for checking their work either way.

# COMMAND ----------

dbutils.widgets.text("schema", "", "Your schema (e.g. ra26_elt_ex_nils)")
dbutils.widgets.text("catalog", "dtr_sandbox", "Catalog")
SCHEMA = dbutils.widgets.get("schema").strip()
CATALOG = dbutils.widgets.get("catalog").strip()

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Question 2 — PySpark

# COMMAND ----------

order_items = spark.table(f"{CATALOG}.{SCHEMA}.bronze_order_items")
products = spark.table(f"{CATALOG}.{SCHEMA}.bronze_products")
category_translation = spark.table(f"{CATALOG}.{SCHEMA}.bronze_category_translation")
orders = spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders")

items = (
    order_items
    .join(products, on="product_id", how="inner")
    .join(orders, on="order_id", how="inner")
    .filter(F.col("order_status") != "canceled")
)

translated = items.join(category_translation, on="product_category_name", how="left")

q2_result = (
    translated
    .withColumn("order_month", F.trunc("order_purchase_timestamp", "month"))
    .withColumn("category_english", F.coalesce("product_category_name_english", F.lit("unknown")))
    .groupBy("order_month", "category_english")
    .agg(
        F.round(F.sum("price"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .orderBy("order_month", "category_english")
)

(
    q2_result.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_monthly_revenue_by_category_etl")
)
display(q2_result)

# COMMAND ----------

# MAGIC %md ## Question 3 — PySpark

# COMMAND ----------

order_reviews = spark.table(f"{CATALOG}.{SCHEMA}.bronze_order_reviews")

delivered = orders.filter(
    (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull()
).withColumn(
    "delay_days",
    F.datediff(F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date")),
)

bucketed = delivered.join(order_reviews, on="order_id", how="inner").withColumn(
    "delay_bucket",
    F.when(F.col("delay_days") <= 0, "early_or_on_time")
     .when(F.col("delay_days") <= 3, "1_3_days_late")
     .when(F.col("delay_days") <= 7, "4_7_days_late")
     .otherwise("8_plus_days_late"),
)

q3_result = (
    bucketed.groupBy("delay_bucket")
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
    )
)

(
    q3_result.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_review_score_vs_delay_etl")
)
display(q3_result)

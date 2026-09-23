# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Question 1, the ETL way
# MAGIC
# MAGIC Same business question you'll also answer in dbt
# MAGIC (`gold_delivery_performance_by_state.sql`): **delivery delay and lateness
# MAGIC rate by customer state.**
# MAGIC
# MAGIC This time it's ETL-style: read the raw-ish Bronze tables, do ALL the
# MAGIC joining and aggregating here in PySpark, and only write the final,
# MAGIC already-business-ready result to Gold. Nothing gets loaded anywhere
# MAGIC queryable until the transform is done.
# MAGIC
# MAGIC Required output columns (must match the dbt version exactly, so you can
# MAGIC compare them at the end):
# MAGIC - `customer_state`
# MAGIC - `order_count`
# MAGIC - `avg_delay_days`
# MAGIC - `pct_late`

# COMMAND ----------

dbutils.widgets.text("schema", "", "Your schema (e.g. ra26_elt_ex_nils)")
dbutils.widgets.text("catalog", "dtr_sandbox", "Catalog")

SCHEMA = dbutils.widgets.get("schema").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
assert SCHEMA, "Set your schema in the widget above."

# Everything lives in your one schema now — bronze_* tables in, this
# notebook's gold_*_etl output back into the same place.

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders")
customers = spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build it
# MAGIC
# MAGIC Steps:
# MAGIC 1. Filter `orders` to `order_status == "delivered"` and
# MAGIC    `order_delivered_customer_date` not null
# MAGIC 2. Join to `customers` on `customer_id`
# MAGIC 3. Add a `delay_days` column: days between
# MAGIC    `order_delivered_customer_date` and `order_estimated_delivery_date`
# MAGIC    (positive = late). Look at `F.datediff`.
# MAGIC 4. Group by `customer_state` and aggregate: count, avg delay, % late

# COMMAND ----------

# TODO: step 1 — filter delivered orders with a non-null delivery date
delivered = orders  # placeholder, replace with your filter

# TODO: step 2 — join to customers
joined = delivered  # placeholder, replace with your join

# TODO: step 3 — add delay_days using F.datediff(delivered_col, estimated_col)
with_delay = joined.withColumn("delay_days", F.lit(None))  # placeholder

# TODO: step 4 — group by customer_state and compute order_count,
# avg_delay_days (rounded to 1 decimal), pct_late (rounded to 1 decimal)
result = (
    with_delay.groupBy("customer_state")
    .agg(
        F.count("*").alias("order_count"),
        F.lit(None).alias("avg_delay_days"),   # TODO
        F.lit(None).alias("pct_late"),          # TODO
    )
    .orderBy(F.desc("pct_late"))
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load — write the finished, business-ready table to Gold
# MAGIC
# MAGIC Notice this is the ONLY write in this whole notebook. Everything before
# MAGIC it was in-memory transformation. That's the "T before L" of ETL.

# COMMAND ----------

(
    result.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_delivery_performance_by_state_etl")
)

print(f"Written to {CATALOG}.{SCHEMA}.gold_delivery_performance_by_state_etl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare
# MAGIC
# MAGIC Once you've also run the dbt model, compare the two results — they
# MAGIC should match exactly. If they don't, that's a good debugging exercise
# MAGIC in itself (which one do you trust, and why?).
# MAGIC
# MAGIC There's also a THIRD version of this same question, written as a
# MAGIC tested Python function rather than notebook cells:
# MAGIC `src/olist_pipeline/transforms.py::delivery_performance_by_state`, with
# MAGIC its expected output asserted in `src/tests/test_transforms.py`. Once
# MAGIC you've built your own, it's worth reading that side by side and asking:
# MAGIC what would break first if the dataset changed? Which one would you
# MAGIC trust untouched for a year?
# MAGIC
# MAGIC ```sql
# MAGIC -- run this in a SQL cell/notebook once both exist:
# MAGIC SELECT 'etl' AS built_via, * FROM {catalog}.{schema}.gold_delivery_performance_by_state_etl
# MAGIC UNION ALL
# MAGIC SELECT 'elt' AS built_via, * FROM {catalog}.{schema}.gold_delivery_performance_by_state
# MAGIC ORDER BY customer_state, built_via
# MAGIC ```

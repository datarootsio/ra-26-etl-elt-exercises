# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Questions 2 & 3: your choice
# MAGIC
# MAGIC For each question below, decide: PySpark (ETL — transform before load)
# MAGIC or dbt (ELT — transform after load)? There's no "correct" answer — the
# MAGIC point is to make the call and be able to defend it in the wrap-up.
# MAGIC
# MAGIC If you pick **dbt** for a question, you don't need anything in this
# MAGIC notebook — go build the matching file in `dbt_project/models/marts/`.
# MAGIC
# MAGIC If you pick **PySpark**, build it in the empty cells below.

# COMMAND ----------

dbutils.widgets.text("schema", "", "Your schema (e.g. ra26_elt_ex_nils)")
dbutils.widgets.text("catalog", "dtr_sandbox", "Catalog")
SCHEMA = dbutils.widgets.get("schema").strip()
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2 — Monthly revenue by product category
# MAGIC
# MAGIC `order_items` + `products` + `category_translation` + `orders`, revenue
# MAGIC by month and category (English name).
# MAGIC
# MAGIC **Your choice:** PySpark / dbt  *(delete one)*
# MAGIC
# MAGIC **Why:** _(one sentence — e.g. data volume, who'd maintain this in a real
# MAGIC team, how often it needs to run, how comfortable you are in each tool)_

# COMMAND ----------

# If you picked PySpark for Q2, build it here.
# Required output columns: order_month, category_english, revenue, order_count
# (same shape as the dbt version in gold_monthly_revenue_by_category.sql)

# TODO (only if you picked PySpark): read bronze_order_items, bronze_products,
# bronze_category_translation, bronze_orders from {CATALOG}.{SCHEMA}; join;
# group by month + category; write to
# {CATALOG}.{SCHEMA}.gold_monthly_revenue_by_category_etl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3 — Does shipping speed drive satisfaction?
# MAGIC
# MAGIC Review score bucketed by delivery delay.
# MAGIC
# MAGIC **Your choice:** PySpark / dbt  *(delete one)*
# MAGIC
# MAGIC **Why:** _(one sentence)_

# COMMAND ----------

# If you picked PySpark for Q3, build it here.
# Required output columns: delay_bucket, order_count, avg_review_score
# (same shape as the dbt version in gold_review_score_vs_delivery_delay.sql)

# TODO (only if you picked PySpark): read bronze_orders, bronze_order_reviews
# from {CATALOG}.{SCHEMA}; compute delay_days and bucket it; join to reviews;
# group by bucket; write to
# {CATALOG}.{SCHEMA}.gold_review_score_vs_delay_etl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before the wrap-up discussion
# MAGIC
# MAGIC Jot down (doesn't need to be more than a sentence each):
# MAGIC - Which approach did you pick for Q2 and Q3, and why?
# MAGIC - Now that you've built Q1 both ways — which did you personally find
# MAGIC   easier to write? Which would you rather **debug at 2am in production**?
# MAGIC - If this dataset were 100x bigger, would either of your Q2/Q3 choices change?
# MAGIC - `src/olist_pipeline/` builds all three questions as tested Python
# MAGIC   functions, not notebook cells — would you ship what you just wrote
# MAGIC   in this notebook to run unattended every night? What's missing?

# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC
# MAGIC Goal: land the raw Olist CSVs into your own Bronze Delta tables. Minimal
# MAGIC transformation here — just schema application and maybe a type cast.
# MAGIC No business logic yet, that comes later.
# MAGIC
# MAGIC **Run the widget cell below first and set your schema name.**

# COMMAND ----------

dbutils.widgets.text("schema", "", "Your schema (e.g. ra26_elt_ex_nils)")
dbutils.widgets.text("catalog", "dtr_sandbox", "Catalog")
dbutils.widgets.text("volume", "raw_data", "Volume (holds your CSVs)")

SCHEMA = dbutils.widgets.get("schema").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
VOLUME = dbutils.widgets.get("volume").strip()
assert SCHEMA, "Set your schema in the widget above before running anything else."

# This is YOUR volume — the bundle provisioned it for you, and only you have
# access to it. Upload the Olist CSVs into it once (your teacher does this
# for everyone via scripts/upload_datasets.sh), then this path just works.
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# NOTE: Bronze tables live in the SAME schema as everything else you build
# (staging views, gold tables) — there's no separate bronze/gold schema
# split anymore. The "bronze_" prefix on the table name is what keeps things
# apart; that's why every table below is named bronze_<something>.
print(f"Bronze tables will land in {CATALOG}.{SCHEMA}, reading CSVs from {RAW_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: `bronze_customers`
# MAGIC
# MAGIC This one's done for you — use it as the pattern for the rest.

# COMMAND ----------

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_PATH}/olist_customers_dataset.csv")
)

(
    customers_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_customers")
)

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your turn: the other 7 tables
# MAGIC
# MAGIC Same pattern every time: read CSV with header + inferSchema, write as a
# MAGIC managed Delta table under `{CATALOG}.{SCHEMA}`, named `bronze_<table>`.
# MAGIC Fill in `ingest_csv_to_bronze` below, then call it once per table.
# MAGIC
# MAGIC While you do this, think about (you'll want an answer for the wrap-up
# MAGIC discussion): should any of these be partitioned? Does Bronze even need
# MAGIC partitioning, or does that decision belong further downstream? What
# MAGIC would you do differently for `order_items` (largest table) vs
# MAGIC `category_translation` (tiny lookup table)?
# MAGIC
# MAGIC (There's a fully worked, tested version of this exact ingestion step in
# MAGIC `src/olist_pipeline/pipeline.py::ingest_bronze` if you want to compare
# MAGIC notes afterwards — don't peek before you've had a go.)

# COMMAND ----------

def ingest_csv_to_bronze(csv_filename: str, table_name: str) -> None:
    """Read {RAW_PATH}/{csv_filename} and write it to {CATALOG}.{SCHEMA}.bronze_{table_name}.

    TODO: implement this using the same pattern as the customers example above.
    """
    # TODO: read the csv
    # TODO: write it as a managed delta table named bronze_{table_name}
    raise NotImplementedError("fill this in")


# TODO: call ingest_csv_to_bronze once for each of these
tables_to_ingest = [
    ("olist_orders_dataset.csv", "orders"),
    ("olist_order_items_dataset.csv", "order_items"),
    ("olist_order_payments_dataset.csv", "order_payments"),
    ("olist_order_reviews_dataset.csv", "order_reviews"),
    ("olist_products_dataset.csv", "products"),
    ("olist_sellers_dataset.csv", "sellers"),
    ("product_category_name_translation.csv", "category_translation"),
]

for csv_filename, table_name in tables_to_ingest:
    ingest_csv_to_bronze(csv_filename, table_name)
    print(f"landed bronze_{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC Before moving to `02_etl_style_transform.py`, confirm all 8 tables exist:

# COMMAND ----------

expected = {f"bronze_{t}" for t in [
    "customers", "orders", "order_items", "order_payments",
    "order_reviews", "products", "sellers", "category_translation",
]}

existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()}
missing = expected - existing

if missing:
    print(f"Still missing: {sorted(missing)}")
else:
    print("All 8 Bronze tables are in place. On to the next notebook.")

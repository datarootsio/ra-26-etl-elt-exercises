# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion (SOLUTION)

# COMMAND ----------

dbutils.widgets.text("schema", "", "Your schema (e.g. ra26_elt_ex_nils)")
dbutils.widgets.text("catalog", "dtr_sandbox", "Catalog")
dbutils.widgets.text("volume", "raw_data", "Volume (holds your CSVs)")

SCHEMA = dbutils.widgets.get("schema").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
VOLUME = dbutils.widgets.get("volume").strip()
assert SCHEMA, "Set your schema in the widget above before running anything else."

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Bronze tables will land in {CATALOG}.{SCHEMA}, reading CSVs from {RAW_PATH}")

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

def ingest_csv_to_bronze(csv_filename: str, table_name: str) -> None:
    """Read {RAW_PATH}/{csv_filename} and write it to {CATALOG}.{SCHEMA}.bronze_{table_name}."""
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/{csv_filename}")
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{table_name}")
    )


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

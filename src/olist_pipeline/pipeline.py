"""Orchestration entrypoint — the ONLY place in this package that talks to
Unity Catalog. Everything else (staging.py, transforms.py) is pure
DataFrame-in-DataFrame-out functions, tested in isolation in tests/.

This is what resources/reference_pipeline_job.yml runs as a job task. It is
the "normal engineering" counterpart to notebooks/01-03: same three
business questions, but as a tested, importable, parameterized module
instead of notebook cells with TODOs.

Run standalone (e.g. from a job, or `python -m olist_pipeline.pipeline ...`):
    python pipeline.py --catalog dtr_sandbox --schema ra26_elt_ex_han --volume raw_data
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame

from olist_pipeline import staging, transforms
from olist_pipeline.session import get_spark

logger = logging.getLogger("olist_pipeline")
logging.basicConfig(level=logging.INFO)

# (csv filename in the volume, bronze table name) — same 8 tables the
# notebook exercise lands (the volume has a 9th CSV, geolocation, from the
# raw Kaggle export, but nothing in this exercise needs it, so it's left
# out here too), so bronze stays a shared landing zone regardless
# of which path (notebook or this module) populated it.
CSV_TO_BRONZE_TABLE = [
    ("olist_customers_dataset.csv", "bronze_customers"),
    ("olist_orders_dataset.csv", "bronze_orders"),
    ("olist_order_items_dataset.csv", "bronze_order_items"),
    ("olist_order_payments_dataset.csv", "bronze_order_payments"),
    ("olist_order_reviews_dataset.csv", "bronze_order_reviews"),
    ("olist_products_dataset.csv", "bronze_products"),
    ("olist_sellers_dataset.csv", "bronze_sellers"),
    ("product_category_name_translation.csv", "bronze_category_translation"),
]


def ingest_bronze(spark, catalog: str, schema: str, volume: str) -> None:
    """CSV (Volume) -> managed Delta table. Overwrite each run — Bronze here
    is a re-runnable landing zone, not an append-only history."""
    raw_path = f"/Volumes/{catalog}/{schema}/{volume}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    for csv_filename, table_name in CSV_TO_BRONZE_TABLE:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{raw_path}/{csv_filename}")
        )
        df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")
        logger.info("bronze: landed %s.%s.%s", catalog, schema, table_name)


def _read(spark, catalog: str, schema: str, table: str) -> DataFrame:
    return spark.table(f"{catalog}.{schema}.{table}")


def build_gold(spark, catalog: str, schema: str) -> None:
    """Bronze -> staging -> gold, in memory, one write per gold table."""
    stg_customers = staging.stg_customers(_read(spark, catalog, schema, "bronze_customers"))
    stg_orders = staging.stg_orders(_read(spark, catalog, schema, "bronze_orders"))
    stg_order_items = staging.stg_order_items(_read(spark, catalog, schema, "bronze_order_items"))
    stg_products = staging.stg_products(_read(spark, catalog, schema, "bronze_products"))
    stg_order_reviews = staging.stg_order_reviews(
        _read(spark, catalog, schema, "bronze_order_reviews")
    )
    stg_category_translation = staging.stg_category_translation(
        _read(spark, catalog, schema, "bronze_category_translation")
    )

    gold_tables = {
        "gold_delivery_performance_by_state_prod": transforms.delivery_performance_by_state(
            stg_orders, stg_customers
        ),
        "gold_monthly_revenue_by_category_prod": transforms.monthly_revenue_by_category(
            stg_order_items, stg_products, stg_orders, stg_category_translation
        ),
        "gold_review_score_vs_delivery_delay_prod": transforms.review_score_vs_delivery_delay(
            stg_orders, stg_order_reviews
        ),
    }
    for table_name, df in gold_tables.items():
        df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")
        logger.info("gold: wrote %s.%s.%s", catalog, schema, table_name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume", default="raw_data")
    args = parser.parse_args()

    spark = get_spark()
    ingest_bronze(spark, args.catalog, args.schema, args.volume)
    build_gold(spark, args.catalog, args.schema)
    logger.info("done: %s.%s is fully built", args.catalog, args.schema)


if __name__ == "__main__":
    main()

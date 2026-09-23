"""Bronze -> staging: light cleanup only (rename, cast, dedupe-ready types).

No business logic here — mirrors dbt_project/models/staging/*.sql exactly,
column for column, so a PySpark run and a dbt run of the same question
produce identical numbers. If you change one, change the other.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def stg_customers(bronze_customers: DataFrame) -> DataFrame:
    return bronze_customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        F.upper(F.col("customer_state")).alias("customer_state"),
    )


def stg_orders(bronze_orders: DataFrame) -> DataFrame:
    return bronze_orders.select(
        "order_id",
        "customer_id",
        "order_status",
        F.col("order_purchase_timestamp").cast("timestamp").alias("order_purchase_ts"),
        F.col("order_approved_at").cast("timestamp").alias("order_approved_ts"),
        F.col("order_delivered_carrier_date").cast("timestamp").alias("order_delivered_carrier_ts"),
        F.col("order_delivered_customer_date").cast("timestamp").alias("order_delivered_customer_ts"),
        F.col("order_estimated_delivery_date").cast("timestamp").alias("order_estimated_delivery_ts"),
    )


def stg_order_items(bronze_order_items: DataFrame) -> DataFrame:
    return bronze_order_items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_ts"),
        F.col("price").cast("double").alias("price"),
        F.col("freight_value").cast("double").alias("freight_value"),
    )


def stg_products(bronze_products: DataFrame) -> DataFrame:
    return bronze_products.select("product_id", "product_category_name")


def stg_order_reviews(bronze_order_reviews: DataFrame) -> DataFrame:
    return bronze_order_reviews.select(
        "review_id",
        "order_id",
        F.col("review_score").cast("int").alias("review_score"),
        F.col("review_creation_date").cast("timestamp").alias("review_creation_ts"),
        F.col("review_answer_timestamp").cast("timestamp").alias("review_answer_ts"),
    )


def stg_category_translation(bronze_category_translation: DataFrame) -> DataFrame:
    return bronze_category_translation.select(
        "product_category_name", "product_category_name_english"
    )

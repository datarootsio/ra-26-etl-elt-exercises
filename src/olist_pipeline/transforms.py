"""Staging -> gold: the three business questions, as pure functions.

Each function mirrors its dbt equivalent 1:1
(dbt_project/models/marts/*.sql and dbt_project/solutions/*_SOLUTION.sql) —
same filters, same joins, same rounding — so PySpark and dbt runs of the
same question are directly comparable. That comparison is the whole point
of the exercise (see notebooks/02_etl_style_transform.py); if these drift
from the SQL, the comparison stops meaning anything.

All inputs are already-staged (typed, cleaned) DataFrames — see staging.py.
Nothing here touches Unity Catalog; that's pipeline.py's job.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def delivery_performance_by_state(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Question 1 — delivery delay and lateness rate by customer state.

    Output columns: customer_state, order_count, avg_delay_days, pct_late.
    Only delivered orders with a non-null delivery date count.
    """
    delivered = orders.where(
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_customer_ts").isNotNull()
    )
    joined = delivered.join(customers, on="customer_id", how="inner").withColumn(
        "delay_days",
        F.datediff(F.col("order_delivered_customer_ts"), F.col("order_estimated_delivery_ts")),
    )
    return (
        joined.groupBy("customer_state")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.avg("delay_days"), 1).alias("avg_delay_days"),
            F.round(
                100.0 * F.sum(F.when(F.col("delay_days") > 0, 1).otherwise(0)) / F.count("*"),
                1,
            ).alias("pct_late"),
        )
        .orderBy(F.desc("pct_late"))
    )


def monthly_revenue_by_category(
    order_items: DataFrame,
    products: DataFrame,
    orders: DataFrame,
    category_translation: DataFrame,
) -> DataFrame:
    """Question 2 — revenue by month and English category name.

    Output columns: order_month, category_english, revenue, order_count.
    Canceled orders are excluded. Unmapped categories fall back to 'unknown'.
    """
    items = (
        order_items.join(products, on="product_id", how="inner")
        .join(orders.where(F.col("order_status") != "canceled"), on="order_id", how="inner")
    )
    translated = items.join(
        category_translation, on="product_category_name", how="left"
    ).withColumn(
        "category_english",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
    )
    return (
        translated.withColumn("order_month", F.date_trunc("month", F.col("order_purchase_ts")))
        .groupBy("order_month", "category_english")
        .agg(
            F.round(F.sum("price"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("order_month", "category_english")
    )


def review_score_vs_delivery_delay(orders: DataFrame, order_reviews: DataFrame) -> DataFrame:
    """Question 3 — does shipping speed drive satisfaction?

    Output columns: delay_bucket, order_count, avg_review_score.
    Buckets: early_or_on_time, 1_3_days_late, 4_7_days_late, 8_plus_days_late.
    """
    delivered = orders.where(
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_customer_ts").isNotNull()
    ).withColumn(
        "delay_days",
        F.datediff(F.col("order_delivered_customer_ts"), F.col("order_estimated_delivery_ts")),
    )
    bucketed = delivered.join(order_reviews, on="order_id", how="inner").withColumn(
        "delay_bucket",
        F.when(F.col("delay_days") <= 0, "early_or_on_time")
        .when(F.col("delay_days").between(1, 3), "1_3_days_late")
        .when(F.col("delay_days").between(4, 7), "4_7_days_late")
        .otherwise("8_plus_days_late"),
    )
    return bucketed.groupBy("delay_bucket").agg(
        F.count("*").alias("order_count"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
    )

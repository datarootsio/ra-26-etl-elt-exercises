"""Unit tests for bronze -> staging casts. One test per "gotcha" cast, not
an exhaustive one per column — the rest of staging.py follows the same
`select` + `cast`/`alias` pattern.
"""
from __future__ import annotations

from olist_pipeline import staging


def test_stg_customers_upper_cases_state(spark):
    bronze = spark.createDataFrame(
        [("c1", "u1", "01310", "sao paulo", "sp")],
        [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ],
    )
    row = staging.stg_customers(bronze).collect()[0]
    assert row["customer_state"] == "SP"


def test_stg_orders_casts_string_timestamps(spark):
    bronze = spark.createDataFrame(
        [
            (
                "o1",
                "c1",
                "delivered",
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-02 09:00:00",
                "2024-01-05 14:00:00",
                "2024-01-06 00:00:00",
            )
        ],
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    row = staging.stg_orders(bronze).collect()[0]
    assert row["order_purchase_ts"].isoformat() == "2024-01-01T10:00:00"
    assert row["order_delivered_customer_ts"].isoformat() == "2024-01-05T14:00:00"


def test_stg_order_items_casts_price_to_double(spark):
    bronze = spark.createDataFrame(
        [("o1", 1, "p1", "s1", "2024-01-01 00:00:00", "19.90", "7.50")],
        [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
    )
    row = staging.stg_order_items(bronze).collect()[0]
    assert row["price"] == 19.90
    assert row["freight_value"] == 7.50
    assert isinstance(row["price"], float)

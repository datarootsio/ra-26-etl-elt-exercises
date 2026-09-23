"""Unit tests for the three gold transforms — small, hand-built inputs with
expected outputs computed by hand, not just "did it run". This is what the
notebook exercise (notebooks/02-03) deliberately skips, and exactly what
you'd want before trusting either the PySpark or the dbt version.
"""
from __future__ import annotations

from datetime import datetime

from olist_pipeline import transforms


def test_delivery_performance_by_state(spark):
    orders = spark.createDataFrame(
        [
            # order_id, customer_id, order_status, delivered_ts, estimated_ts
            ("o1", "c1", "delivered", datetime(2024, 1, 10), datetime(2024, 1, 5)),  # +5 late
            ("o2", "c2", "delivered", datetime(2024, 1, 3), datetime(2024, 1, 5)),  # -2 early
            ("o3", "c3", "delivered", datetime(2024, 1, 5), datetime(2024, 1, 5)),  # 0 on time
            ("o4", "c1", "shipped", datetime(2024, 1, 10), datetime(2024, 1, 5)),  # not delivered
            ("o5", "c2", "delivered", None, datetime(2024, 1, 5)),  # null delivery date
        ],
        [
            "order_id", "customer_id", "order_status",
            "order_delivered_customer_ts", "order_estimated_delivery_ts",
        ],
    )
    customers = spark.createDataFrame(
        [("c1", "sp"), ("c2", "rj"), ("c3", "sp")],
        ["customer_id", "customer_state"],
    )

    result = {
        r["customer_state"]: r
        for r in transforms.delivery_performance_by_state(orders, customers).collect()
    }

    assert set(result) == {"sp", "rj"}
    assert result["sp"]["order_count"] == 2
    assert result["sp"]["avg_delay_days"] == 2.5  # (5 + 0) / 2
    assert result["sp"]["pct_late"] == 50.0  # 1 of 2 has delay > 0
    assert result["rj"]["order_count"] == 1
    assert result["rj"]["avg_delay_days"] == -2.0
    assert result["rj"]["pct_late"] == 0.0


def test_delivery_performance_orders_ranked_by_pct_late_desc(spark):
    orders = spark.createDataFrame(
        [
            ("o1", "c1", "delivered", datetime(2024, 1, 10), datetime(2024, 1, 5)),
            ("o2", "c2", "delivered", datetime(2024, 1, 3), datetime(2024, 1, 5)),
        ],
        [
            "order_id", "customer_id", "order_status",
            "order_delivered_customer_ts", "order_estimated_delivery_ts",
        ],
    )
    customers = spark.createDataFrame(
        [("c1", "sp"), ("c2", "rj")], ["customer_id", "customer_state"]
    )

    ranked = [
        r["customer_state"]
        for r in transforms.delivery_performance_by_state(orders, customers).collect()
    ]
    assert ranked == ["sp", "rj"]  # sp is 100% late, rj is 0% late


def test_monthly_revenue_by_category(spark):
    order_items = spark.createDataFrame(
        [
            ("order1", "p1", 100.0),
            ("order1", "p1", 50.0),  # same order + product -> tests distinct order_count
            ("order2", "p2", 30.0),
            ("order3", "p1", 999.0),  # order3 is canceled -> must be excluded
            ("order4", "p3", 20.0),  # p3 has no category translation -> 'unknown'
        ],
        ["order_id", "product_id", "price"],
    )
    products = spark.createDataFrame(
        [("p1", "moveis_decoracao"), ("p2", "beleza_saude"), ("p3", "sem_categoria")],
        ["product_id", "product_category_name"],
    )
    orders = spark.createDataFrame(
        [
            ("order1", "delivered", datetime(2024, 1, 15)),
            ("order2", "delivered", datetime(2024, 1, 20)),
            ("order3", "canceled", datetime(2024, 1, 10)),
            ("order4", "delivered", datetime(2024, 2, 1)),
        ],
        ["order_id", "order_status", "order_purchase_ts"],
    )
    category_translation = spark.createDataFrame(
        [("moveis_decoracao", "furniture_decor"), ("beleza_saude", "health_beauty")],
        ["product_category_name", "product_category_name_english"],
    )

    rows = {
        (r["order_month"].strftime("%Y-%m"), r["category_english"]): r
        for r in transforms.monthly_revenue_by_category(
            order_items, products, orders, category_translation
        ).collect()
    }

    assert set(rows) == {
        ("2024-01", "furniture_decor"), ("2024-01", "health_beauty"), ("2024-02", "unknown"),
    }
    assert rows[("2024-01", "furniture_decor")]["revenue"] == 150.0
    assert rows[("2024-01", "furniture_decor")]["order_count"] == 1  # oi1+oi2 are the same order
    assert rows[("2024-01", "health_beauty")]["revenue"] == 30.0
    assert rows[("2024-02", "unknown")]["revenue"] == 20.0
    # order3 (canceled) must not appear anywhere, at any price
    assert 999.0 not in [r["revenue"] for r in rows.values()]


def test_review_score_vs_delivery_delay(spark):
    orders = spark.createDataFrame(
        [
            ("o1", "delivered", datetime(2024, 1, 4), datetime(2024, 1, 5)),  # -1 -> on_time
            ("o2", "delivered", datetime(2024, 1, 7), datetime(2024, 1, 5)),  # +2 -> 1_3_days
            ("o3", "delivered", datetime(2024, 1, 10), datetime(2024, 1, 5)),  # +5 -> 4_7_days
            ("o4", "delivered", datetime(2024, 1, 15), datetime(2024, 1, 5)),  # +10 -> 8_plus
            ("o5", "delivered", None, datetime(2024, 1, 5)),  # excluded: no delivery date
            ("o6", "shipped", datetime(2024, 1, 15), datetime(2024, 1, 5)),  # excluded: not deliv.
        ],
        [
            "order_id", "order_status",
            "order_delivered_customer_ts", "order_estimated_delivery_ts",
        ],
    )
    order_reviews = spark.createDataFrame(
        [("o1", 5), ("o2", 4), ("o3", 3), ("o4", 1), ("o5", 2), ("o6", 5)],
        ["order_id", "review_score"],
    )

    result = {
        r["delay_bucket"]: r
        for r in transforms.review_score_vs_delivery_delay(orders, order_reviews).collect()
    }

    assert set(result) == {"early_or_on_time", "1_3_days_late", "4_7_days_late", "8_plus_days_late"}
    assert result["early_or_on_time"]["order_count"] == 1
    assert result["early_or_on_time"]["avg_review_score"] == 5.0
    assert result["8_plus_days_late"]["avg_review_score"] == 1.0
    # o5/o6 must not leak into any bucket
    assert sum(r["order_count"] for r in result.values()) == 4

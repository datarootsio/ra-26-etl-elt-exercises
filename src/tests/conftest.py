"""Local Spark session for tests — no Databricks connection needed.

This is the whole point of writing staging.py / transforms.py as pure
DataFrame functions: `pytest` runs them against a throwaway local
SparkSession, same as any other Python unit test. No cluster, no UC, no
network.
"""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("olist_pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

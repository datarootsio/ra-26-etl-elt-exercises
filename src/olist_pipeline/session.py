"""Spark session helper for local and Databricks runs.

Same pattern as the sdp-tech-talk bundle: prefer Databricks Connect (local
IDE / CI runs against real serverless compute), fall back to a plain local
SparkSession — which is also what lets tests/ run without any Databricks
connection at all.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger("olist_pipeline")


def get_spark(app_name: str = "olist_pipeline") -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        logger.info("Creating a Databricks Connect session...")
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        logger.info("Databricks Connect not available. Creating a local Spark session...")
        return SparkSession.builder.appName(app_name).getOrCreate()

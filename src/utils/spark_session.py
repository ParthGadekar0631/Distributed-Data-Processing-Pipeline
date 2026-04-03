from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession

from .logger import get_logger

LOGGER = get_logger(__name__)


def create_spark_session(app_name: str, config: Dict[str, Any]) -> SparkSession:
    spark_conf = config.get("spark", {})
    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_conf.get("master", "local[*]"))
    )

    for key, value in spark_conf.get("configs", {}).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    LOGGER.info("Spark session created for %s", app_name)
    return session

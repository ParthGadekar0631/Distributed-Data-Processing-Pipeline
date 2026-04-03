from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import DataFrame, functions as F, Window

from src.utils.io_utils import ensure_dir, project_path
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def enrich_trips(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("trip_id").orderBy(F.col("tpep_dropoff_datetime").desc())
    deduped = df.withColumn("row_number", F.row_number().over(window_spec)).filter(
        F.col("row_number") == 1
    ).drop("row_number")

    enriched = (
        deduped
        .withColumn(
            "trip_duration_minutes",
            (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0,
        )
        .withColumn("route_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn(
            "trip_speed_estimate",
            F.when(F.col("trip_distance") > 0, F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)).otherwise(None),
        )
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")).otherwise(None),
        )
        .withColumn("payment_type", F.upper(F.col("payment_type")))
        .withColumn("trip_type", F.upper(F.col("trip_type")))
    )
    return enriched


def write_silver(df: DataFrame, config: Dict[str, object], ingestion_date: str) -> Tuple[str, int]:
    output_dir = project_path(config["paths"]["silver_root"]) / f"ingestion_date={ingestion_date}"
    ensure_dir(output_dir)
    partition_cols = ["route_date", "vendor_id"]
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(str(output_dir))
    row_count = df.count()
    LOGGER.info("Silver data written to %s (%s rows)", output_dir, row_count)
    return str(output_dir), row_count

from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import DataFrame, functions as F

from src.utils.io_utils import ensure_dir, project_path
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def _write_table(
    df: DataFrame,
    table_name: str,
    config: Dict[str, object],
    ingestion_date: str,
) -> Tuple[str, int]:
    gold_root = project_path(config["paths"]["gold_root"]) / table_name
    output_dir = gold_root / f"ingestion_date={ingestion_date}"
    ensure_dir(output_dir)
    partitions = config["aggregation"]["gold_tables"].get(table_name, {}).get(
        "partition_columns", []
    )
    writer = df.write.mode("overwrite")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(str(output_dir))
    count = df.count()
    LOGGER.info("Gold table %s written (%s rows)", table_name, count)
    return str(output_dir), count


def build_gold_tables(
    silver_df: DataFrame, config: Dict[str, object], ingestion_date: str
) -> Dict[str, Dict[str, object]]:
    outputs = {}

    daily_vendor = silver_df.groupBy("route_date", "vendor_id").agg(
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.sum("tip_amount"), 2).alias("total_tip"),
        F.count("trip_id").alias("trip_count"),
    )
    path, rows = _write_table(daily_vendor, "daily_vendor_revenue", config, ingestion_date)
    outputs["daily_vendor_revenue"] = {"path": path, "rows": rows}

    zone_summary = silver_df.groupBy("route_date", "pickup_zone").agg(
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.count("trip_id").alias("trip_count"),
    )
    path, rows = _write_table(zone_summary, "zone_distance_summary", config, ingestion_date)
    outputs["zone_distance_summary"] = {"path": path, "rows": rows}

    pickup_hour = silver_df.groupBy("route_date", "pickup_hour").agg(
        F.count("trip_id").alias("trip_count"),
    )
    path, rows = _write_table(pickup_hour, "pickup_hour_trends", config, ingestion_date)
    outputs["pickup_hour_trends"] = {"path": path, "rows": rows}

    payment_type = silver_df.groupBy("route_date", "payment_type").agg(
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.count("trip_id").alias("trip_count"),
    )
    path, rows = _write_table(payment_type, "payment_type_summary", config, ingestion_date)
    outputs["payment_type_summary"] = {"path": path, "rows": rows}

    return outputs

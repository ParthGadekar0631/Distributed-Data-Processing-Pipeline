from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import DataFrame, functions as F

from src.utils.logger import get_logger

LOGGER = get_logger(__name__)

REQUIRED_COLUMNS = [
    "trip_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "trip_distance",
]


def run_validations(
    df: DataFrame, config: Dict[str, object]
) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    val_cfg = config["validation"]

    rules = {
        "fare_non_negative": F.col("fare_amount") >= val_cfg["min_fare_amount"],
        "positive_distance": F.col("trip_distance") >= val_cfg["min_trip_distance"],
        "passenger_count_range": (
            F.col("passenger_count")
            .between(val_cfg["passenger_count_min"], val_cfg["passenger_count_max"])
        ),
        "pickup_before_dropoff": F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"),
    }

    for column in REQUIRED_COLUMNS:
        rules[f"required_{column}"] = F.col(column).isNotNull()

    flag_columns = []
    for rule_name, condition in rules.items():
        flag_columns.append(F.when(~condition, F.lit(rule_name)))

    df_with_errors = df.withColumn(
        "validation_errors",
        F.array_remove(F.array(*flag_columns), F.lit(None))
    )

    # schema corruption indicator
    df_with_errors = df_with_errors.withColumn(
        "validation_errors",
        F.when(
            F.col("_corrupt_record").isNotNull(),
            F.array_union(F.col("validation_errors"), F.array(F.lit("schema_corruption"))),
        ).otherwise(F.col("validation_errors")),
    )

    invalid_df = df_with_errors.filter(F.size("validation_errors") > 0).withColumn(
        "validation_reason", F.array_join("validation_errors", ",")
    )
    valid_df = df_with_errors.filter(F.size("validation_errors") == 0).drop(
        "validation_errors", "_corrupt_record"
    )

    rule_counts = {
        "valid_rows": valid_df.count(),
        "invalid_rows": invalid_df.count(),
    }

    for rule_name in rules:
        rule_counts[rule_name] = df_with_errors.filter(
            F.array_contains(F.col("validation_errors"), rule_name)
        ).count()

    rule_counts["schema_corruption"] = df_with_errors.filter(
        F.array_contains(F.col("validation_errors"), "schema_corruption")
    ).count()

    LOGGER.info(
        "Validation completed: %s valid / %s invalid",
        rule_counts["valid_rows"],
        rule_counts["invalid_rows"],
    )
    return valid_df, invalid_df, rule_counts

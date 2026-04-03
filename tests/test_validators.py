from datetime import datetime

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)

from src.validation.validators import run_validations
from src.utils.io_utils import load_config


def _schema():
    return StructType(
        [
            StructField("trip_id", StringType()),
            StructField("vendor_id", StringType()),
            StructField("tpep_pickup_datetime", TimestampType()),
            StructField("tpep_dropoff_datetime", TimestampType()),
            StructField("passenger_count", IntegerType()),
            StructField("trip_distance", DoubleType()),
            StructField("fare_amount", DoubleType()),
            StructField("_corrupt_record", StringType()),
        ]
    )


def test_validation_separates_invalid_rows(spark):
    rows = [
        (
            "1",
            "CMT",
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 20, 0),
            2,
            3.4,
            12.5,
        ) + (None,),
        (
            "2",
            "CMT",
            datetime(2024, 1, 1, 11, 0, 0),
            datetime(2024, 1, 1, 10, 0, 0),
            0,
            -4.0,
            -10.0,
        ) + (None,),
    ]
    df = spark.createDataFrame(rows, schema=_schema())
    config = load_config()
    valid_df, invalid_df, stats = run_validations(df, config)

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert stats["invalid_rows"] == 1

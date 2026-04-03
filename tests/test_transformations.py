from datetime import datetime

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)

from src.transformation.transform import enrich_trips


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
            StructField("tip_amount", DoubleType()),
            StructField("payment_type", StringType()),
            StructField("trip_type", StringType()),
        ]
    )


def test_enrich_trips_adds_features(spark):
    rows = [
        (
            "1",
            "CMT",
            datetime(2024, 1, 1, 8, 0, 0),
            datetime(2024, 1, 1, 8, 30, 0),
            1,
            5.0,
            20.0,
            4.0,
            "card",
            "street",
        )
    ]
    df = spark.createDataFrame(rows, schema=_schema())
    enriched = enrich_trips(df)
    row = enriched.collect()[0]
    assert row.trip_duration_minutes == 30.0
    assert row.pickup_hour == 8
    assert row.fare_per_mile == 4.0

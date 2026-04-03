from datetime import date

from pyspark.sql import Row

from src.aggregation.build_gold import build_gold_tables
from src.utils.io_utils import load_config


def test_build_gold_outputs_written(spark):
    config = load_config()
    rows = [
        Row(
            trip_id="1",
            route_date=date(2024, 1, 1),
            vendor_id="CMT",
            fare_amount=20.0,
            tip_amount=4.0,
            pickup_zone="Midtown",
            pickup_hour=8,
            payment_type="CARD",
            trip_distance=5.0,
        ),
        Row(
            trip_id="2",
            route_date=date(2024, 1, 1),
            vendor_id="VTS",
            fare_amount=15.0,
            tip_amount=2.0,
            pickup_zone="Downtown",
            pickup_hour=9,
            payment_type="CASH",
            trip_distance=7.0,
        ),
    ]
    df = spark.createDataFrame(rows)
    outputs = build_gold_tables(df, config, "2024-01-01")

    assert set(outputs.keys()) == {
        "daily_vendor_revenue",
        "zone_distance_summary",
        "pickup_hour_trends",
        "payment_type_summary",
    }
    for meta in outputs.values():
        assert meta["rows"] > 0

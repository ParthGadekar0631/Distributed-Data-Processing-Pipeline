from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd
from faker import Faker

from src.utils.io_utils import ensure_dir, load_config, project_path
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)
FAKER = Faker()


DIRTY_RATE = 0.05
CORRUPT_RATE = 0.01


def _build_dirty_record(base_record: Dict[str, object]) -> Dict[str, object]:
    record = base_record.copy()
    issue = random.choice(
        [
            "null_passenger",
            "negative_fare",
            "invalid_timestamp",
            "negative_distance",
            "impossible_passenger",
        ]
    )
    if issue == "null_passenger":
        record["passenger_count"] = None
    elif issue == "negative_fare":
        record["fare_amount"] = -abs(record["fare_amount"])
    elif issue == "invalid_timestamp":
        record["tpep_dropoff_datetime"] = record["tpep_pickup_datetime"]
    elif issue == "negative_distance":
        record["trip_distance"] = -abs(record["trip_distance"])
    elif issue == "impossible_passenger":
        record["passenger_count"] = 12
    return record


def _base_record(
    ingestion_date: datetime,
    config: Dict[str, object],
) -> Dict[str, object]:
    pickup_time = ingestion_date + timedelta(minutes=random.randint(0, 23 * 60))
    trip_minutes = random.randint(5, 90)
    dropoff_time = pickup_time + timedelta(minutes=trip_minutes)
    distance = round(random.uniform(0.3, 25.0), 2)
    fare = round(distance * random.uniform(2.5, 4.5), 2)

    return {
        "trip_id": str(uuid.uuid4()),
        "vendor_id": random.choice(config["data_generation"]["vendors"]),
        "tpep_pickup_datetime": pickup_time.isoformat(sep=" "),
        "tpep_dropoff_datetime": dropoff_time.isoformat(sep=" "),
        "passenger_count": random.randint(1, 4),
        "trip_distance": distance,
        "pickup_longitude": round(random.uniform(-74.05, -73.75), 6),
        "pickup_latitude": round(random.uniform(40.63, 40.85), 6),
        "dropoff_longitude": round(random.uniform(-74.05, -73.75), 6),
        "dropoff_latitude": round(random.uniform(40.63, 40.85), 6),
        "pickup_zone": random.choice(config["data_generation"]["pickup_zones"]),
        "dropoff_zone": random.choice(config["data_generation"]["dropoff_zones"]),
        "fare_amount": fare,
        "extra": round(random.uniform(0, 5), 2),
        "mta_tax": 0.5,
        "tip_amount": round(fare * random.uniform(0, 0.25), 2),
        "tolls_amount": round(random.uniform(0, 10), 2),
        "payment_type": random.choice(config["data_generation"]["payment_types"]),
        "trip_type": random.choice(["street", "dispatch"]),
        "ingestion_date": ingestion_date.date().isoformat(),
    }


def generate_dataset(size: str, ingestion_date: str) -> Path:
    config = load_config()
    record_map = config["data_generation"]["record_size"]
    if size not in record_map:
        raise KeyError(f"Unknown data size '{size}'")

    record_count = record_map[size]
    LOGGER.info("Generating %s records for %s", record_count, ingestion_date)
    ingestion_dt = datetime.fromisoformat(ingestion_date)

    rows = []
    for _ in range(record_count):
        record = _base_record(ingestion_dt, config)
        if random.random() < DIRTY_RATE:
            record = _build_dirty_record(record)
        rows.append(record)

    # inject duplicates
    rows.extend(random.sample(rows, k=max(1, record_count // 50)))

    df = pd.DataFrame(rows)

    output_dir = (
        project_path(config["paths"]["bronze_root"])
        / f"ingestion_date={ingestion_date}"
    )
    ensure_dir(output_dir)
    output_file = output_dir / "part-00000.csv"
    df.to_csv(output_file, index=False)

    # Inject corrupted lines
    with output_file.open("a", encoding="utf-8") as fp:
        for _ in range(max(1, record_count // 2000)):
            fp.write("corrupted,line,that,breaks,schema\n")

    LOGGER.info("Raw dataset written to %s", output_file)
    return output_file

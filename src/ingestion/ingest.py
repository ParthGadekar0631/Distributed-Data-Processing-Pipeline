from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.models.schema_definitions import build_struct_type
from src.utils.io_utils import load_config, load_schema_config, project_path
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def ingest_raw_data(
    spark: SparkSession, config: Dict[str, object], ingestion_date: str
) -> Tuple[DataFrame, Dict[str, int]]:
    schema_def = load_schema_config()
    schema = build_struct_type("trip_schema", schema_def)

    bronze_root = project_path(config["paths"]["bronze_root"])
    raw_path = bronze_root / f"ingestion_date={ingestion_date}"
    if not raw_path.exists():
        raise FileNotFoundError(f"No raw data found at {raw_path}")

    bad_records_dir = project_path(config["paths"]["quarantine_root"]) / "bad_records"
    bad_records_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Reading raw files from %s", raw_path)
    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("badRecordsPath", str(bad_records_dir))
        .schema(schema)
        .csv(str(raw_path))
    )

    total = df.count()
    corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
    LOGGER.info("Ingested %s rows (%s corrupt)", total, corrupt)

    stats = {
        "records_read": total,
        "corrupt_records": corrupt,
    }

    return df, stats

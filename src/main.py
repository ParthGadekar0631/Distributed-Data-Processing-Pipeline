from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from src.aggregation.build_gold import build_gold_tables
from src.generator.generate_data import generate_dataset
from src.ingestion.ingest import ingest_raw_data
from src.monitoring.metrics import MetricsTracker, summarize_reports
from src.transformation.transform import enrich_trips, write_silver
from src.utils.io_utils import ensure_dir, load_config, project_path, write_json
from src.utils.logger import get_logger
from src.utils.retry import retry
from src.utils.spark_session import create_spark_session
from src.validation.validators import run_validations

app = typer.Typer(add_completion=False, help="Distributed PySpark data pipeline CLI")
console = Console()
LOGGER = get_logger(__name__)


def _write_quarantine(df, config, ingestion_date: str, report: dict) -> Optional[str]:
    if df is None:
        return None
    quarantine_dir = project_path(config["paths"]["quarantine_root"]) / f"ingestion_date={ingestion_date}"
    ensure_dir(quarantine_dir)
    df.write.mode("overwrite").parquet(str(quarantine_dir / "invalid_rows"))
    report_path = quarantine_dir / "validation_report.json"
    write_json(report, report_path)
    return str(quarantine_dir)


def _load_silver(spark, config, ingestion_date: str):
    path = project_path(config["paths"]["silver_root"]) / f"ingestion_date={ingestion_date}"
    if not path.exists():
        raise FileNotFoundError(f"Silver dataset not found at {path}")
    return spark.read.parquet(str(path))


@retry(retries=3, backoff_factor=0.75)
def _safe_ingest(spark, config, ingestion_date: str):
    return ingest_raw_data(spark, config, ingestion_date)


@retry(retries=2, backoff_factor=1.0)
def _safe_build_gold(silver_df, config, ingestion_date: str):
    return build_gold_tables(silver_df, config, ingestion_date)


@app.command("generate-data")
def generate_data(
    size: str = typer.Option("small", "--size", help="Record size preset"),
    ingestion_date: str = typer.Option(..., "--ingestion-date", help="YYYY-MM-DD"),
) -> None:
    path = generate_dataset(size=size, ingestion_date=ingestion_date)
    console.print(f"Raw dataset generated at [green]{path}[/green]")


@app.command("run-pipeline")
def run_pipeline(
    ingestion_date: str = typer.Option(..., "--ingestion-date", help="Process date"),
    size: Optional[str] = typer.Option(None, "--generate-size", help="Optional raw generation size"),
) -> None:
    config = load_config()
    tracker = MetricsTracker(ingestion_date, config)

    if size:
        with tracker.track_step("generate_data"):
            generate_dataset(size=size, ingestion_date=ingestion_date)

    spark = create_spark_session("DistributedPipeline", config)
    try:
        with tracker.track_step("ingestion"):
            raw_df, ingest_stats = _safe_ingest(spark, config, ingestion_date)
        tracker.set_stat("ingestion", ingest_stats)

        with tracker.track_step("validation"):
            valid_df, invalid_df, validation_stats = run_validations(raw_df, config)
        tracker.set_stat("validation", validation_stats)
        quarantine_path = _write_quarantine(invalid_df, config, ingestion_date, validation_stats)
        tracker.set_stat("quarantine_path", quarantine_path)

        with tracker.track_step("transformation"):
            silver_df = enrich_trips(valid_df)
            silver_path, silver_rows = write_silver(silver_df, config, ingestion_date)
        tracker.set_stat("silver", {"path": silver_path, "rows": silver_rows})

        with tracker.track_step("gold"):
            outputs = _safe_build_gold(silver_df, config, ingestion_date)
        tracker.set_stat("gold", outputs)

    finally:
        spark.stop()
        metrics_path = tracker.finalize()
        console.print(f"Metrics written to {metrics_path}")


@app.command("validate-only")
def validate_only(
    ingestion_date: str = typer.Option(..., "--ingestion-date"),
) -> None:
    config = load_config()
    spark = create_spark_session("ValidationJob", config)
    tracker = MetricsTracker(ingestion_date, config)
    quarantine_path: Optional[str] = None
    try:
        with tracker.track_step("ingestion"):
            raw_df, ingest_stats = _safe_ingest(spark, config, ingestion_date)
        with tracker.track_step("validation"):
            _, invalid_df, validation_stats = run_validations(raw_df, config)
        quarantine_path = _write_quarantine(invalid_df, config, ingestion_date, validation_stats)
        tracker.set_stat("validation", validation_stats)
    finally:
        spark.stop()
        tracker.finalize()
    console.print(f"Validation report stored at {quarantine_path}")


@app.command("build-gold")
def build_gold(
    ingestion_date: str = typer.Option(..., "--ingestion-date"),
) -> None:
    config = load_config()
    spark = create_spark_session("GoldBuilder", config)
    try:
        silver_df = _load_silver(spark, config, ingestion_date)
        outputs = _safe_build_gold(silver_df, config, ingestion_date)
        console.print(outputs)
    finally:
        spark.stop()


@app.command("monitoring-summary")
def monitoring_summary(limit: int = typer.Option(5, "--limit")) -> None:
    summary = summarize_reports(limit)
    console.print(summary)


if __name__ == "__main__":
    app()

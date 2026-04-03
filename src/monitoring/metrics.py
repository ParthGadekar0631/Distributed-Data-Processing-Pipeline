from __future__ import annotations

import time
from contextlib import contextmanager
import json
from typing import Any, Dict, Iterator

from src.utils.io_utils import (
    ensure_dir,
    load_config,
    project_path,
    timestamped_filename,
    write_json,
)
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


class MetricsTracker:
    def __init__(self, ingestion_date: str, config: Dict[str, Any]):
        self.ingestion_date = ingestion_date
        self.config = config
        self.payload: Dict[str, Any] = {
            "ingestion_date": ingestion_date,
            "start_time": time.time(),
            "steps": [],
            "stats": {},
        }

    @contextmanager
    def track_step(self, name: str) -> Iterator[None]:
        start = time.perf_counter()
        LOGGER.info("Starting step: %s", name)
        try:
            yield
        finally:
            duration = time.perf_counter() - start
            LOGGER.info("Finished %s in %.2fs", name, duration)
            self.payload["steps"].append({"name": name, "duration_seconds": round(duration, 2)})

    def set_stat(self, key: str, value: Any) -> None:
        self.payload["stats"][key] = value

    def finalize(self) -> str:
        self.payload["end_time"] = time.time()
        reports_dir = project_path(self.config["paths"]["reports_root"])
        ensure_dir(reports_dir)
        file_name = timestamped_filename(self.config["monitoring"]["metrics_file_prefix"])
        output_path = reports_dir / file_name
        write_json(self.payload, output_path)
        LOGGER.info("Metrics written to %s", output_path)
        return str(output_path)


def summarize_reports(limit: int = 5) -> Dict[str, Any]:
    config = load_config()
    reports_dir = project_path(config["paths"]["reports_root"])
    files = sorted(reports_dir.glob("pipeline_metrics_*.json"), reverse=True)[:limit]
    summaries = []
    for path in files:
        try:
            with path.open() as fp:
                data = fp.read()
        except FileNotFoundError:
            continue
        try:
            import json

            parsed = json.loads(data)
        except json.JSONDecodeError:
            continue
        summaries.append(
            {
                "path": str(path),
                "ingestion_date": parsed.get("ingestion_date"),
                "stats": parsed.get("stats", {}),
                "step_count": len(parsed.get("steps", [])),
            }
        )
    return {"reports": summaries, "count": len(summaries)}

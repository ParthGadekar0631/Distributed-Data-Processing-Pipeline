from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schemas.json"


def project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def load_config(path: Path | None = None) -> Dict[str, Any]:
    target = path or CONFIG_PATH
    with target.open() as stream:
        return yaml.safe_load(stream)


def load_schema_config(path: Path | None = None) -> Dict[str, Any]:
    target = path or SCHEMA_PATH
    with target.open() as stream:
        return json.load(stream)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(payload: Dict[str, Any], output_path: Path) -> None:
    ensure_dir(output_path.parent)
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2, default=str)


def timestamped_filename(prefix: str, suffix: str = "json") -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}.{suffix}"

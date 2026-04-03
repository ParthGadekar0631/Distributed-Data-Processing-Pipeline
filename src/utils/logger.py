from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"


def _build_handler(handler: logging.Handler) -> logging.Handler:
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    return handler


def get_logger(name: str, log_dir: Optional[Path] = None) -> logging.Logger:
    """Return a logger that logs to console and rotating file."""

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    console_handler = _build_handler(logging.StreamHandler())
    logger.addHandler(console_handler)

    directory = (log_dir or DEFAULT_LOG_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    file_handler = _build_handler(
        RotatingFileHandler(
            directory / "pipeline.log", maxBytes=5_000_000, backupCount=5
        )
    )
    logger.addHandler(file_handler)
    logger.propagate = False

    logger.debug("Logger %s initialized with directory %s", name, directory)
    return logger

"""Central logging configuration."""
from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(log_dir: Path | None = None) -> None:
    """Set up a basic rotating log handler."""

    target_dir = log_dir or Path.cwd() / "logs"
    target_dir.mkdir(parents=True, exist_ok=True)
    log_file = target_dir / "t212_ingestion.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )

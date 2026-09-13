"""Structured logging configuration (stdlib ``logging`` only).

Supports two output formats controlled by ``Settings.log_format``:

- **plain**: Human-readable ``%(asctime)s %(levelname)s %(name)s %(message)s``
- **json**: One JSON object per line via ``json.dumps`` — suitable for log
  aggregation systems (ELK, CloudWatch, etc.).

An optional rotating file handler is added when ``Settings.log_dir`` is set.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
from datetime import UTC, datetime
from pathlib import Path


class _JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter using ``json.dumps``."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)


_PLAIN_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"


def setup_logging(
    level: str = "INFO",
    fmt: str = "plain",
    log_dir: Path | None = None,
) -> None:
    """Configure the root logger.

    Args:
        level: Logging level name (e.g. ``"INFO"``, ``"DEBUG"``).
        fmt: ``"plain"`` for human-readable or ``"json"`` for structured.
        log_dir: If provided, a ``RotatingFileHandler`` is added writing
            to ``<log_dir>/collector.log`` (10 MB × 5 backups).
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Clear existing handlers to allow re-configuration
    root.handlers.clear()

    # Console handler
    console = logging.StreamHandler()
    if fmt == "json":
        console.setFormatter(_JsonFormatter())
    else:
        console.setFormatter(logging.Formatter(_PLAIN_FORMAT))
    root.addHandler(console)

    # Optional file handler
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_dir / "collector.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        if fmt == "json":
            file_handler.setFormatter(_JsonFormatter())
        else:
            file_handler.setFormatter(logging.Formatter(_PLAIN_FORMAT))
        root.addHandler(file_handler)

    root.debug("Logging configured: level=%s, format=%s, log_dir=%s", level, fmt, log_dir)

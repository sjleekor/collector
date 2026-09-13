"""High-level orchestration entry point.

This module provides a programmatic API for running pipeline operations
without going through the CLI.  Useful for embedding in notebooks,
schedulers, or test harnesses.

For CLI usage, see :mod:`collector.kr.cli.app`.
"""

from __future__ import annotations

import logging

from collector.kr.infra.config.settings import Settings, get_settings
from collector.kr.infra.logging.setup import setup_logging

logger = logging.getLogger(__name__)


def run(settings: Settings | None = None) -> None:
    """Run the default pipeline workflow.

    Args:
        settings: Application settings.  If ``None``, loaded from
            environment / ``.env``.

    Raises:
        NotImplementedError: Stub — not yet implemented.
    """
    if settings is None:
        settings = get_settings()

    setup_logging(
        level=settings.log_level,
        fmt=settings.log_format.value,
        log_dir=settings.log_dir,
    )

    logger.info("Starting KRX data pipeline (run_mode=%s)…", settings.run_mode.value)

    # TODO: Implement default orchestration
    #   1. sync_universe (default source from config)
    #   2. backfill_daily_prices (all active tickers, since listing)
    #   3. validate
    raise NotImplementedError("Pipeline orchestration is not implemented yet.")

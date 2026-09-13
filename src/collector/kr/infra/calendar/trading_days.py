"""Trading-day calendar strategy for KRX (stdlib-only baseline).

Baseline strategy:
    • Weekends (Saturday, Sunday) are non-trading days.
    • Public holidays are loaded from an optional CSV file packaged at
      ``collector/kr/infra/calendar/data/holidays_krx.csv``.  If the file
      is missing, only weekends are excluded.

Optional plug-in upgrade path:
    If ``exchange_calendars`` or ``pandas_market_calendars`` is installed,
    you can replace :func:`get_trading_days` with a wrapper around::

        import exchange_calendars as xcals
        cal = xcals.get_calendar("XKRX")
        sessions = cal.sessions_in_range(start, end)

    The function signature stays the same — callers are unaffected.

Holiday CSV format (``data/holidays_krx.csv``)::

    date,name
    2024-01-01,New Year
    2024-02-09,Lunar New Year
    ...
"""

from __future__ import annotations

import csv
import logging
from datetime import date, timedelta
from importlib.resources import files
from pathlib import Path

logger = logging.getLogger(__name__)


def _default_holidays_path() -> Path:
    return Path(str(files("collector.kr.infra.calendar") / "data" / "holidays_krx.csv"))


def load_holidays(path: Path | None = None) -> set[date]:
    """Load KRX holidays from a CSV file.

    Args:
        path: Path to the holidays CSV.  Defaults to the packaged
            ``data/holidays_krx.csv``.

    Returns:
        Set of holiday dates.  Empty set if the file does not exist.
    """
    path = path or _default_holidays_path()
    if not path.exists():
        logger.warning("Holiday file not found: %s — using weekends only.", path)
        return set()

    holidays: set[date] = set()
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                holidays.add(date.fromisoformat(row["date"]))
            except (KeyError, ValueError) as exc:
                logger.warning("Skipping invalid holiday row %r: %s", row, exc)
    logger.info("Loaded %d holidays from %s.", len(holidays), path)
    return holidays


def get_trading_days(
    start: date,
    end: date,
    holidays: set[date] | None = None,
) -> list[date]:
    """Return sorted list of KRX trading days in ``[start, end]``.

    Args:
        start: Range start (inclusive).
        end: Range end (inclusive).
        holidays: Pre-loaded holiday set.  If ``None``, holidays are
            loaded from the default CSV path.

    Returns:
        Sorted list of dates that are expected trading days.
    """
    if holidays is None:
        holidays = load_holidays()

    trading_days: list[date] = []
    current = start
    while current <= end:
        # Monday=0 … Sunday=6; weekdays are 0–4
        if current.weekday() < 5 and current not in holidays:
            trading_days.append(current)
        current += timedelta(days=1)
    return trading_days

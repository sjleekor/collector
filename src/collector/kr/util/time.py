"""Timezone helpers — Asia/Seoul throughout.

All date/time operations in the pipeline use Asia/Seoul (KST, UTC+9).
The timezone is fixed in code and not exposed as a configuration option.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
"""Asia/Seoul timezone instance (KST, UTC+9)."""


def now_kst() -> datetime:
    """Return the current datetime in Asia/Seoul."""
    return datetime.now(tz=KST)


def today_kst() -> date:
    """Return today's date in Asia/Seoul."""
    return now_kst().date()


def to_kst(dt: datetime) -> datetime:
    """Convert a datetime to Asia/Seoul.

    Args:
        dt: A timezone-aware or naive datetime.  Naive datetimes are
            assumed to be in UTC before conversion.

    Returns:
        Timezone-aware datetime in Asia/Seoul.
    """
    if dt.tzinfo is None:

        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(KST)

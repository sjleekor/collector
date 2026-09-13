"""Point-in-time availability rules for common market and macro features."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, timedelta

AvailabilityPolicy = str
KrxTradingDayProvider = Callable[[date, date], Sequence[date]]

NEXT_KRX_SESSION = "next_krx_session"
SAME_KRX_SESSION_MORNING = "same_krx_session_morning"
RELEASE_DATE = "release_date"
EVENT_DATE = "event_date"
MANUAL_LAG_DAYS = "manual_lag_days"

VALID_AVAILABILITY_POLICIES = {
    NEXT_KRX_SESSION,
    SAME_KRX_SESSION_MORNING,
    RELEASE_DATE,
    EVENT_DATE,
    MANUAL_LAG_DAYS,
}


def compute_available_from(
    *,
    policy: AvailabilityPolicy,
    observation_date: date,
    period_end_date: date | None = None,
    release_date: date | None = None,
    source_timezone: str = "Asia/Seoul",
    manual_lag_days: int = 0,
    krx_trading_days: KrxTradingDayProvider | None = None,
) -> date:
    """Return the first KRX feature date where an observation may be used.

    Args:
        policy: Catalog availability policy.
        observation_date: Source-market observation date.
        period_end_date: Canonical period end for macro observations.
        release_date: Official release/event date when known.
        source_timezone: Source market timezone name from the series catalog.
        manual_lag_days: Conservative calendar-day lag applied by catalog.
        krx_trading_days: Calendar provider returning KRX trading days in a
            closed date range. Defaults to a weekends-only calendar.
    """
    if policy not in VALID_AVAILABILITY_POLICIES:
        raise ValueError(f"Unsupported availability policy: {policy}")
    if manual_lag_days < 0:
        raise ValueError("manual_lag_days must be non-negative")

    calendar = krx_trading_days or _weekdays_only

    if policy == NEXT_KRX_SESSION:
        return _next_krx_trading_day_on_or_after(
            observation_date + timedelta(days=1 + manual_lag_days),
            calendar,
        )

    if policy == SAME_KRX_SESSION_MORNING:
        base_date = _krx_morning_base_date(observation_date, source_timezone)
        return _next_krx_trading_day_on_or_after(
            base_date + timedelta(days=manual_lag_days),
            calendar,
        )

    if policy == RELEASE_DATE:
        if release_date is None:
            raise ValueError("release_date policy requires release_date")
        return _next_krx_trading_day_on_or_after(
            release_date + timedelta(days=manual_lag_days),
            calendar,
        )

    if policy == EVENT_DATE:
        event_date = release_date or observation_date
        return _next_krx_trading_day_on_or_after(
            event_date + timedelta(days=manual_lag_days),
            calendar,
        )

    lag_anchor = period_end_date or observation_date
    return _next_krx_trading_day_on_or_after(
        lag_anchor + timedelta(days=manual_lag_days),
        calendar,
    )


def _krx_morning_base_date(observation_date: date, source_timezone: str) -> date:
    """Map a source-market observation date to the KRX morning it can feed."""
    if source_timezone == "Asia/Seoul":
        return observation_date
    return observation_date + timedelta(days=1)


def _next_krx_trading_day_on_or_after(
    candidate: date,
    krx_trading_days: KrxTradingDayProvider,
) -> date:
    search_end = candidate + timedelta(days=366)
    trading_days = sorted(
        day for day in krx_trading_days(candidate, search_end) if day >= candidate
    )
    if not trading_days:
        raise ValueError(f"No KRX trading day found on or after {candidate}")
    return trading_days[0]


def _weekdays_only(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days

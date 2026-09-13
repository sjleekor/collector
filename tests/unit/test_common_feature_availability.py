from datetime import date, timedelta

import pytest

from collector.kr.domain.availability import (
    EVENT_DATE,
    MANUAL_LAG_DAYS,
    NEXT_KRX_SESSION,
    RELEASE_DATE,
    SAME_KRX_SESSION_MORNING,
    compute_available_from,
)


def _krx_days(start: date, end: date) -> list[date]:
    holidays = {date(2026, 6, 3)}
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            days.append(current)
        current += timedelta(days=1)
    return days


def test_next_krx_session_uses_next_trading_day() -> None:
    available_from = compute_available_from(
        policy=NEXT_KRX_SESSION,
        observation_date=date(2026, 6, 8),
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 9)


def test_next_krx_session_skips_weekend() -> None:
    available_from = compute_available_from(
        policy=NEXT_KRX_SESSION,
        observation_date=date(2026, 6, 5),
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 8)


def test_same_krx_session_morning_allows_previous_us_close_on_korean_session() -> None:
    available_from = compute_available_from(
        policy=SAME_KRX_SESSION_MORNING,
        observation_date=date(2026, 6, 5),
        source_timezone="America/New_York",
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 8)


def test_same_krx_session_morning_keeps_korean_morning_data_on_same_session() -> None:
    available_from = compute_available_from(
        policy=SAME_KRX_SESSION_MORNING,
        observation_date=date(2026, 6, 8),
        source_timezone="Asia/Seoul",
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 8)


def test_release_date_policy_blocks_monthly_value_until_release_date() -> None:
    available_from = compute_available_from(
        policy=RELEASE_DATE,
        observation_date=date(2026, 5, 31),
        period_end_date=date(2026, 5, 31),
        release_date=date(2026, 6, 12),
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 12)


def test_release_date_policy_moves_weekend_release_to_next_trading_day() -> None:
    available_from = compute_available_from(
        policy=RELEASE_DATE,
        observation_date=date(2026, 5, 31),
        period_end_date=date(2026, 5, 31),
        release_date=date(2026, 6, 13),
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 15)


def test_event_date_policy_uses_release_date_when_present() -> None:
    available_from = compute_available_from(
        policy=EVENT_DATE,
        observation_date=date(2026, 6, 10),
        release_date=date(2026, 6, 11),
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 11)


def test_manual_lag_days_policy_anchors_to_period_end() -> None:
    available_from = compute_available_from(
        policy=MANUAL_LAG_DAYS,
        observation_date=date(2026, 5, 31),
        period_end_date=date(2026, 5, 31),
        manual_lag_days=12,
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 12)


def test_manual_lag_days_skips_krx_holiday() -> None:
    available_from = compute_available_from(
        policy=MANUAL_LAG_DAYS,
        observation_date=date(2026, 6, 1),
        manual_lag_days=2,
        krx_trading_days=_krx_days,
    )

    assert available_from == date(2026, 6, 4)


def test_release_date_policy_requires_release_date() -> None:
    with pytest.raises(ValueError, match="requires release_date"):
        compute_available_from(
            policy=RELEASE_DATE,
            observation_date=date(2026, 5, 31),
            krx_trading_days=_krx_days,
        )


def test_unknown_policy_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported availability policy"):
        compute_available_from(
            policy="unknown_policy",
            observation_date=date(2026, 6, 8),
            krx_trading_days=_krx_days,
        )

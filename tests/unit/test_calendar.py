"""Unit tests for collector.kr.infra.calendar.trading_days."""

from datetime import date
from pathlib import Path

from collector.kr.infra.calendar.trading_days import get_trading_days, load_holidays


class TestLoadHolidays:
    """Tests for load_holidays()."""

    def test_load_holidays_from_existing_file(self) -> None:
        """Loading the project's holidays CSV should return a non-empty set."""
        holidays = load_holidays()
        assert len(holidays) > 0
        # Spot-check: 2024-01-01 (New Year) should be present
        assert date(2024, 1, 1) in holidays

    def test_load_holidays_missing_file_returns_empty(self, tmp_path: Path) -> None:
        """When the CSV does not exist, return an empty set (no crash)."""
        holidays = load_holidays(tmp_path / "nonexistent.csv")
        assert holidays == set()


class TestGetTradingDays:
    """Tests for get_trading_days()."""

    def test_new_year_excluded(self) -> None:
        """2024-01-01 (New Year, Monday) must be excluded from trading days."""
        holidays = load_holidays()
        result = get_trading_days(date(2024, 1, 1), date(2024, 1, 5), holidays=holidays)
        # 2024-01-01 is Mon (holiday), 01-02 Tue, 01-03 Wed, 01-04 Thu, 01-05 Fri
        assert date(2024, 1, 1) not in result
        assert date(2024, 1, 2) in result
        assert date(2024, 1, 3) in result
        assert date(2024, 1, 4) in result
        assert date(2024, 1, 5) in result
        assert len(result) == 4

    def test_weekends_excluded(self) -> None:
        """Saturday and Sunday should never appear in trading days."""
        # 2024-01-06 is Saturday, 2024-01-07 is Sunday
        holidays: set[date] = set()
        result = get_trading_days(date(2024, 1, 6), date(2024, 1, 7), holidays=holidays)
        assert result == []

    def test_full_week_no_holidays(self) -> None:
        """A full Mon-Fri week with no holidays should return 5 days."""
        # 2024-01-08 (Mon) to 2024-01-12 (Fri) — no holidays this week
        holidays: set[date] = set()
        result = get_trading_days(date(2024, 1, 8), date(2024, 1, 12), holidays=holidays)
        assert len(result) == 5

    def test_holidays_none_loads_default(self) -> None:
        """When holidays=None, the function loads from default CSV path."""
        # This test just verifies it runs without error.
        # It may use the real CSV if present, or weekends-only if not.
        result = get_trading_days(date(2024, 1, 2), date(2024, 1, 3), holidays=None)
        assert isinstance(result, list)

    def test_2026_observed_krx_holidays_are_excluded(self) -> None:
        """Observed 2026 KRX holidays must be excluded from trading days."""
        holidays = load_holidays()
        result = get_trading_days(date(2026, 1, 1), date(2026, 6, 10), holidays=holidays)

        for holiday in (
            date(2026, 1, 1),
            date(2026, 2, 16),
            date(2026, 2, 17),
            date(2026, 2, 18),
            date(2026, 3, 2),
            date(2026, 5, 1),
            date(2026, 5, 5),
            date(2026, 5, 25),
            date(2026, 6, 3),
        ):
            assert holiday not in result

        assert len(result) == 106

    def test_empty_range(self) -> None:
        """When start > end, return an empty list."""
        holidays: set[date] = set()
        result = get_trading_days(date(2024, 1, 5), date(2024, 1, 1), holidays=holidays)
        assert result == []


class TestHolidays2027:
    """2027 rows of the packaged CSV (provisional until KRX publishes the 2027 notice)."""

    def test_2027_weekday_closures(self) -> None:
        holidays = {d for d in load_holidays() if d.year == 2027 and d.weekday() < 5}
        assert holidays == {
            date(2027, 1, 1), date(2027, 2, 8), date(2027, 2, 9), date(2027, 3, 1),
            date(2027, 5, 5), date(2027, 5, 13), date(2027, 8, 16), date(2027, 9, 14),
            date(2027, 9, 15), date(2027, 9, 16), date(2027, 10, 4), date(2027, 10, 11),
            date(2027, 12, 27), date(2027, 12, 31),
        }

    def test_2027_matches_xkrx(self) -> None:
        import pytest

        xcals = pytest.importorskip("exchange_calendars")
        cal = xcals.get_calendar("XKRX", start="2027-01-01", end="2027-12-31")
        holidays = load_holidays()
        ours = get_trading_days(date(2027, 1, 4), date(2027, 12, 30), holidays=holidays)
        theirs = [s.date() for s in cal.sessions_in_range("2027-01-04", "2027-12-30")]
        assert ours == theirs

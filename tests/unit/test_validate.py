"""Unit tests for the validate use-case (O-3, O-4).

The point of these is one behaviour: validating a PAST date must compare
against the universe listed on that date, not against today's active set.

The old code did the latter, and it is structurally incapable of finding a gap
— the tickers that are missing from a past cross-section are exactly the ones
that have since delisted, so they are absent from the expected set too and
cancel out. That is how 13.9% of the 2016 cross-section stayed uncollected with
a daily validator running in prod (poc/survivorship_gap.md).
"""

from __future__ import annotations

from datetime import date

from collector.kr.domain.enums import ListingStatus, Market, RunStatus, Source
from collector.kr.domain.models import DailyBar, Stock
from collector.kr.service.validate import validate
from collector.kr.util.time import now_kst

TRADING_DAY = date(2016, 6, 30)
LIVE, DELISTED = "005930", "058530"


def _bar(ticker: str, *, low=900, open_=1000, close=1050, high=1100, volume=1) -> DailyBar:
    return DailyBar(
        ticker=ticker,
        market=Market.KOSPI,
        trade_date=TRADING_DAY,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        source=Source.PYKRX,
        fetched_at=now_kst(),
    )


def _stock(ticker: str) -> Stock:
    return Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name=ticker,
        status=ListingStatus.ACTIVE,
        last_seen_date=TRADING_DAY,
        source=Source.PYKRX,
    )


class FakeStorage:
    def __init__(
        self,
        bars: list[DailyBar],
        *,
        active: list[str] | None = None,
        snapshot: tuple[date, set[str]] | None = None,
        snapshot_counts: list[tuple[date, Source, int]] | None = None,
    ) -> None:
        self._bars = bars
        self._active = active or []
        self._snapshot = snapshot
        self._snapshot_counts = snapshot_counts or []
        self.recorded_runs: list = []
        self.active_calls = 0

    def get_daily_bars(self, target_date, market=None):  # noqa: ANN001
        return list(self._bars)

    def get_active_stocks(self, market=None):  # noqa: ANN001
        self.active_calls += 1
        return [_stock(t) for t in self._active]

    def get_universe_as_of(self, as_of, market=None):  # noqa: ANN001
        return self._snapshot

    def get_snapshot_record_counts(self, limit=24):  # noqa: ANN001
        return list(self._snapshot_counts)

    def record_run(self, run) -> None:  # noqa: ANN001 - test stub
        self.recorded_runs.append(run)


def _counts(storage: FakeStorage) -> dict:
    return storage.recorded_runs[-1].counts


def test_past_date_uses_the_snapshot_universe_and_finds_the_delisted_gap() -> None:
    # Only the surviving ticker has a bar. The delisted one was listed that day.
    storage = FakeStorage(
        [_bar(LIVE)],
        active=[LIVE],  # today's active set no longer contains DELISTED
        snapshot=(TRADING_DAY, {LIVE, DELISTED}),
    )

    validate(storage=storage, target_date=TRADING_DAY)

    counts = _counts(storage)
    assert counts["missing_tickers"] == 1
    assert counts["universe_size"] == 2
    # The active set must not be consulted when a snapshot covers the date.
    assert storage.active_calls == 0
    assert storage.recorded_runs[-1].params["universe_source"] == f"snapshot:{TRADING_DAY}"


def test_the_active_set_alone_cannot_see_that_gap() -> None:
    # Same data, no snapshot: the delisted ticker is absent from both sides,
    # so the old comparison reports a clean run. This is the blind spot.
    storage = FakeStorage([_bar(LIVE)], active=[LIVE], snapshot=None)

    validate(storage=storage, target_date=TRADING_DAY)

    assert _counts(storage)["missing_tickers"] == 0
    assert storage.recorded_runs[-1].params["universe_source"] == "active"


def test_universe_size_is_recorded_so_a_zero_is_interpretable() -> None:
    # missing=0 against an empty universe reads the same as missing=0 against a
    # complete one unless the denominator is reported.
    storage = FakeStorage([], active=[], snapshot=None)

    validate(storage=storage, target_date=TRADING_DAY)

    counts = _counts(storage)
    assert counts["missing_tickers"] == 0
    assert counts["universe_size"] == 0


def test_non_trading_day_skips_the_universe_comparison() -> None:
    storage = FakeStorage([], active=[LIVE], snapshot=(TRADING_DAY, {LIVE, DELISTED}))

    validate(storage=storage, target_date=date(2016, 7, 2))  # Saturday

    counts = _counts(storage)
    assert counts["missing_tickers"] == 0
    assert counts["universe_size"] == 0
    assert storage.recorded_runs[-1].params["universe_source"] == "none"


def test_ohlc_violations_are_still_reported() -> None:
    storage = FakeStorage([_bar(LIVE, close=9999)], active=[LIVE], snapshot=None)

    validate(storage=storage, target_date=TRADING_DAY)

    assert _counts(storage)["ohlc_violations"] == 1


def test_universe_drift_alerts_on_a_large_step_within_one_source() -> None:
    storage = FakeStorage(
        [_bar(LIVE)],
        active=[LIVE],
        snapshot=(TRADING_DAY, {LIVE}),
        snapshot_counts=[
            (date(2016, 6, 30), Source.PYKRX, 1000),
            (date(2016, 5, 31), Source.PYKRX, 2000),
        ],
    )

    validate(storage=storage, target_date=TRADING_DAY)

    counts = _counts(storage)
    assert counts["universe_drift_alerts"] == 1
    assert "Drift: 1" in storage.recorded_runs[-1].error_summary


def test_drift_is_compared_within_a_source_not_across_them() -> None:
    # A backfilled snapshot and a live one come from different collection
    # paths; a step between them says nothing about the market.
    storage = FakeStorage(
        [_bar(LIVE)],
        active=[LIVE],
        snapshot=(TRADING_DAY, {LIVE}),
        snapshot_counts=[
            (date(2016, 6, 30), Source.PYKRX, 2000),
            (date(2016, 5, 31), Source.PYKRX_BACKFILL, 1000),
        ],
    )

    validate(storage=storage, target_date=TRADING_DAY)

    assert _counts(storage)["universe_drift_alerts"] == 0


def test_small_universe_changes_do_not_alert() -> None:
    storage = FakeStorage(
        [_bar(LIVE)],
        active=[LIVE],
        snapshot=(TRADING_DAY, {LIVE}),
        snapshot_counts=[
            (date(2016, 6, 30), Source.PYKRX, 2010),
            (date(2016, 5, 31), Source.PYKRX, 2000),
        ],
    )

    validate(storage=storage, target_date=TRADING_DAY)

    assert _counts(storage)["universe_drift_alerts"] == 0
    assert storage.recorded_runs[-1].status is RunStatus.SUCCESS


def test_drift_check_can_be_disabled() -> None:
    storage = FakeStorage(
        [_bar(LIVE)],
        active=[LIVE],
        snapshot=(TRADING_DAY, {LIVE}),
        snapshot_counts=[
            (date(2016, 6, 30), Source.PYKRX, 1),
            (date(2016, 5, 31), Source.PYKRX, 1000),
        ],
    )

    validate(storage=storage, target_date=TRADING_DAY, universe_drift_pct=0)

    assert _counts(storage)["universe_drift_alerts"] == 0

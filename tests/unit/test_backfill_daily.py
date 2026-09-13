"""Unit tests for bounded new-ticker start resolution in daily backfill.

Covers the incremental baseline-missing resolution (listing_date /
first_seen_date / --new-ticker-start with clamping) and the non-incremental
explicit-start repair path. See docs/dev/20260630_start_date/03_implementation_steps.md
(§5, tests T1-T8b).
"""

from __future__ import annotations

from datetime import date

from collector.kr.domain.enums import ListingStatus, Market, Source, UniverseScope
from collector.kr.domain.models import DailyBar, DailyPriceResult, Stock, UpsertResult
from collector.kr.service.backfill_daily import backfill_daily_prices
from collector.kr.util.time import now_kst

END = date(2026, 7, 3)
MAX_AUTO_RANGE_DAYS = 10
GUARD_START = date(2026, 6, 24)  # END - (MAX_AUTO_RANGE_DAYS - 1)


def _stock(
    ticker: str = "475040",
    *,
    listing_date: date | None = None,
    first_seen_date: date | None = None,
) -> Stock:
    return Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name="New Corp",
        status=ListingStatus.ACTIVE,
        last_seen_date=END,
        source=Source.FDR,
        listing_date=listing_date,
        first_seen_date=first_seen_date,
    )


class FakeProvider:
    """Fake price provider returning one bar and recording fetch ranges."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Market, date, date]] = []

    def fetch_daily_ohlcv(
        self, ticker: str, market: Market, start: date, end: date
    ) -> DailyPriceResult:
        self.calls.append((ticker, market, start, end))
        bar = DailyBar(
            ticker=ticker,
            market=market,
            trade_date=start,
            open=1000,
            high=1100,
            low=900,
            close=1050,
            volume=1,
            source=Source.PYKRX,
            fetched_at=now_kst(),
        )
        return DailyPriceResult(ticker=ticker, bars=[bar])


class FakeStorage:
    """Minimal Storage stub for the daily-backfill service."""

    def __init__(
        self,
        stocks: list[Stock],
        *,
        max_trade_dates: dict[str, date] | None = None,
        min_trade_dates: dict[str, date] | None = None,
        missing_days: dict[str, list[date]] | None = None,
    ) -> None:
        self._stocks = stocks
        self._max_trade_dates = max_trade_dates or {}
        self._min_trade_dates = min_trade_dates or {}
        self._missing_days = missing_days or {}
        self.missing_day_queries: list[tuple[str, date, date]] = []
        self.recorded_runs: list = []

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        return list(self._stocks)

    def get_max_trade_date(self, ticker: str) -> date | None:
        return self._max_trade_dates.get(ticker)

    def get_min_trade_date(self, ticker: str) -> date | None:
        return self._min_trade_dates.get(ticker)

    def query_missing_days(self, ticker: str, start: date, end: date) -> list[date]:
        self.missing_day_queries.append((ticker, start, end))
        return list(self._missing_days.get(ticker, []))

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        return UpsertResult(updated=len(bars))

    def record_run(self, run) -> None:  # noqa: ANN001 - test stub
        self.recorded_runs.append(run)


def _run(storage: FakeStorage, provider: FakeProvider, **kwargs):
    """Invoke the service with sleep-free defaults."""
    defaults = dict(
        market=Market.KOSPI,
        end=END,
        rate_limit_seconds=0.0,
        long_rest_interval=0,
        incremental=True,
        max_auto_range_days=MAX_AUTO_RANGE_DAYS,
    )
    defaults.update(kwargs)
    return backfill_daily_prices(provider=provider, storage=storage, **defaults)


def test_t1_listing_date_within_window_not_clamped() -> None:
    ticker = "475040"
    storage = FakeStorage([_stock(ticker, listing_date=date(2026, 6, 30))])
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.errors == {}
    assert result.baseline_clamped_tickers == 0
    assert result.auto_new_ticker_start_tickers == 1
    # First (only) fetch starts exactly at the listing date.
    assert provider.calls[0][2] == date(2026, 6, 30)


def test_t2_listing_date_older_than_window_is_clamped() -> None:
    ticker = "475040"
    storage = FakeStorage([_stock(ticker, listing_date=date(2026, 6, 1))])
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.errors == {}
    assert result.baseline_clamped_tickers == 1
    assert result.auto_new_ticker_start_tickers == 1
    # Clamped up to the guard window start (end - (N-1)).
    assert provider.calls[0][2] == GUARD_START


def test_t3_first_seen_date_used_when_listing_date_missing() -> None:
    ticker = "153890"
    storage = FakeStorage([_stock(ticker, first_seen_date=date(2026, 6, 30))])
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.errors == {}
    assert result.baseline_clamped_tickers == 0
    assert result.auto_new_ticker_start_tickers == 1
    assert provider.calls[0][2] == date(2026, 6, 30)


def test_t4_no_dates_and_no_new_ticker_start_records_baseline_missing() -> None:
    ticker = "0164H0"
    storage = FakeStorage([_stock(ticker)])
    provider = FakeProvider()

    result = _run(storage, provider)

    assert ticker in result.errors
    assert "No stored daily_ohlcv baseline" in result.errors[ticker]
    assert result.auto_new_ticker_start_tickers == 0
    assert result.baseline_clamped_tickers == 0
    assert provider.calls == []


def test_t5_explicit_new_ticker_start_not_clamped_but_guarded() -> None:
    ticker = "475040"
    old_start = date(2020, 1, 1)

    # Without allow_large_range the resolved range trips the size guard.
    storage = FakeStorage([_stock(ticker, listing_date=date(2026, 6, 1))])
    provider = FakeProvider()
    result = _run(storage, provider, new_ticker_start=old_start)

    assert ticker in result.errors
    assert "too large" in result.errors[ticker]
    assert result.auto_new_ticker_start_tickers == 0  # explicit start does not count
    assert result.baseline_clamped_tickers == 0
    assert provider.calls == []

    # With allow_large_range the explicit start is honored verbatim (not clamped).
    storage2 = FakeStorage([_stock(ticker, listing_date=date(2026, 6, 1))])
    provider2 = FakeProvider()
    result2 = _run(storage2, provider2, new_ticker_start=old_start, allow_large_range=True)

    assert result2.errors == {}
    assert provider2.calls[0][2] == old_start


def test_t6_no_max_auto_range_days_uses_auto_start_unclamped() -> None:
    ticker = "475040"
    storage = FakeStorage([_stock(ticker, listing_date=date(2020, 1, 1))])
    provider = FakeProvider()

    result = _run(storage, provider, max_auto_range_days=None)

    assert result.errors == {}
    assert result.baseline_clamped_tickers == 0
    assert result.auto_new_ticker_start_tickers == 1
    assert provider.calls[0][2] == date(2020, 1, 1)


def test_t7_future_listing_date_is_no_work_skip() -> None:
    ticker = "475040"
    storage = FakeStorage([_stock(ticker, listing_date=date(2026, 8, 1))])
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.errors == {}
    assert provider.calls == []  # start > end -> no_work skip


def test_t8_stale_existing_baseline_fails_range_guard() -> None:
    ticker = "475040"
    storage = FakeStorage(
        [_stock(ticker, listing_date=date(2026, 6, 30))],
        max_trade_dates={ticker: date(2026, 1, 1)},
    )
    provider = FakeProvider()

    result = _run(storage, provider)

    assert ticker in result.errors
    assert "too large" in result.errors[ticker]
    # Baseline present -> new-ticker resolution never runs.
    assert result.auto_new_ticker_start_tickers == 0
    assert result.baseline_clamped_tickers == 0


def test_t8_stale_existing_baseline_passes_with_allow_large_range() -> None:
    ticker = "475040"
    storage = FakeStorage(
        [_stock(ticker, listing_date=date(2026, 6, 30))],
        max_trade_dates={ticker: date(2026, 1, 1)},
    )
    provider = FakeProvider()

    result = _run(storage, provider, allow_large_range=True)

    assert result.errors == {}
    assert provider.calls[0][2] == date(2026, 1, 2)  # MAX(trade_date) + 1


def test_t8a_non_incremental_explicit_start_before_min_is_honored() -> None:
    ticker = "475040"
    explicit_start = date(2020, 1, 1)
    storage = FakeStorage(
        [_stock(ticker)],
        min_trade_dates={ticker: date(2026, 6, 24)},
    )
    provider = FakeProvider()

    _run(storage, provider, incremental=False, start=explicit_start)

    # Explicit --start is not clamped forward to MIN(trade_date).
    assert storage.missing_day_queries == [(ticker, explicit_start, END)]


def test_t8b_non_incremental_default_start_clamps_to_min() -> None:
    ticker = "475040"
    stored_min = date(2026, 1, 1)
    storage = FakeStorage(
        [_stock(ticker)],
        min_trade_dates={ticker: stored_min},
    )
    provider = FakeProvider()

    _run(storage, provider, incremental=False, start=None)

    # start is None -> default early date is clamped up to MIN(trade_date).
    assert storage.missing_day_queries == [(ticker, stored_min, END)]


# ---------------------------------------------------------------------------
# --refetch / --include-delisted (poc/n1_adjusted_price_vintage.md,
# poc/survivorship_gap.md)
# ---------------------------------------------------------------------------


class _RepairStorage(FakeStorage):
    """FakeStorage plus the full-master accessor and a delisted row."""

    def __init__(self, stocks: list[Stock], delisted: list[Stock] | None = None, **kwargs) -> None:
        super().__init__(stocks, **kwargs)
        self._delisted = delisted or []
        self.get_stocks_calls: list[tuple] = []

    def get_stocks(self, market=None, statuses=None, tickers=None):  # noqa: ANN001
        self.get_stocks_calls.append((market, statuses, tuple(tickers or ())))
        rows = list(self._stocks) + list(self._delisted)
        if tickers:
            wanted = set(tickers)
            rows = [s for s in rows if s.ticker in wanted]
        return rows


def _delisted_stock(ticker: str) -> Stock:
    return Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name="Gone Corp",
        status=ListingStatus.DELISTED,
        last_seen_date=date(2024, 6, 24),
        source=Source.PYKRX,
    )


def test_refetch_ignores_gap_detection_and_covers_the_whole_range() -> None:
    # Gap detection reports no missing days, so a plain run fetches nothing.
    # That is exactly why a stale adjusted row can never be corrected.
    ticker = "005930"
    storage = _RepairStorage([_stock(ticker)], missing_days={ticker: []})
    provider = FakeProvider()

    plain = _run(storage, provider, incremental=False, start=date(2026, 6, 1), tickers=[ticker])
    assert provider.calls == []
    assert plain.bars_upserted == 0

    provider2 = FakeProvider()
    repaired = _run(
        storage,
        _p := provider2,
        incremental=False,
        refetch=True,
        start=date(2026, 6, 1),
        tickers=[ticker],
    )
    assert len(provider2.calls) == 1
    assert provider2.calls[0][2] == date(2026, 6, 1)
    assert repaired.bars_upserted == 1
    # Gap detection must not even be consulted in refetch mode.
    assert storage.missing_day_queries == [(ticker, date(2026, 6, 1), END)]


def test_refetch_with_incremental_is_rejected() -> None:
    # incremental starts after MAX(trade_date), so combining the two would
    # re-fetch nothing while looking like a successful repair.
    storage = _RepairStorage([_stock()])
    provider = FakeProvider()

    result = _run(storage, provider, incremental=True, refetch=True)

    assert "pipeline" in result.errors
    assert "cannot be combined" in result.errors["pipeline"]
    assert provider.calls == []


def test_historical_scope_reaches_a_delisted_ticker() -> None:
    # Under CURRENT a delisted ticker is unreachable even by name, because the
    # allowlist filters the active-only result.
    active, gone = "005930", "058530"
    storage = _RepairStorage([_stock(active)], delisted=[_delisted_stock(gone)])

    without = FakeProvider()
    _run(
        storage,
        without,
        incremental=False,
        refetch=True,
        start=date(2024, 1, 1),
        tickers=[gone],
    )
    assert without.calls == []

    with_flag = FakeProvider()
    _run(
        storage,
        with_flag,
        incremental=False,
        refetch=True,
        start=date(2024, 1, 1),
        tickers=[gone],
        scope=UniverseScope.HISTORICAL,
    )
    # The range is chunked yearly, so assert on which ticker was reached.
    assert {c[0] for c in with_flag.calls} == {gone}


def test_historical_scope_passes_the_ticker_filter_to_storage() -> None:
    # The allowlist must be applied in the query, not by filtering an
    # active-only result afterwards.
    storage = _RepairStorage([_stock("005930")], delisted=[_delisted_stock("058530")])

    _run(
        storage,
        FakeProvider(),
        incremental=False,
        refetch=True,
        start=date(2024, 1, 1),
        tickers=["058530"],
        scope=UniverseScope.HISTORICAL,
    )

    assert storage.get_stocks_calls == [(Market.KOSPI, None, ("058530",))]

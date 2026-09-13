"""Unit tests for the daily market-cap backfill service (N1).

Covers the three properties the plan calls out as load-bearing
(`docs/dev/20260731_raw_features/02_data_expansion_plan/02_w1_daily_market_cap.md`):

  * only trading days are requested, and one request per market;
  * a slice is complete only when the stored row count matches the response —
    "rows exist" is not enough, or an interrupted slice is stranded forever;
  * a failing slice is collected, not raised, so the run finishes ``partial``.
"""

from __future__ import annotations

from datetime import date

from collector.kr.domain.enums import Market, RunStatus, RunType, Source
from collector.kr.domain.models import (
    DailyMarketCapResult,
    DailyMarketCapRow,
    UpsertResult,
)
from collector.kr.service.backfill_market_cap import backfill_market_cap
from collector.kr.util.time import now_kst

# 2024-01-01 is a holiday, 01-02..01-05 are sessions, 01-06/07 is a weekend.
START = date(2024, 1, 1)
END = date(2024, 1, 7)
SESSIONS = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]


def _row(ticker: str, trade_date: date, market: Market) -> DailyMarketCapRow:
    return DailyMarketCapRow(
        ticker=ticker,
        market=market,
        trade_date=trade_date,
        source_close=1000,
        market_cap=1_000_000,
        trading_value=5_000,
        listed_shares=1_000,
        volume=5,
        source=Source.PYKRX,
        fetched_at=now_kst(),
    )


class FakeProvider:
    """Returns ``rows_per_slice`` rows and records every call."""

    def __init__(self, rows_per_slice: int = 3, fail_on: set[tuple[date, Market]] | None = None):
        self.rows_per_slice = rows_per_slice
        self.fail_on = fail_on or set()
        self.calls: list[tuple[date, Market]] = []

    def fetch_by_date(self, trade_date: date, market: Market) -> DailyMarketCapResult:
        self.calls.append((trade_date, market))
        if (trade_date, market) in self.fail_on:
            return DailyMarketCapResult(trade_date=trade_date, market=market, error="upstream boom")
        rows = [_row(f"{i:06d}", trade_date, market) for i in range(self.rows_per_slice)]
        return DailyMarketCapResult(
            trade_date=trade_date,
            market=market,
            rows=rows,
            response_rows=len(rows),
        )


class FakeStorage:
    """Minimal Storage stub for the market-cap backfill service."""

    def __init__(
        self,
        stored: dict[tuple[date, Market], int] | None = None,
        *,
        upsert_short_by: int = 0,
    ) -> None:
        self._stored = dict(stored or {})
        self._upsert_short_by = upsert_short_by
        self.upserted: list[list[DailyMarketCapRow]] = []
        self.recorded_runs: list = []

    def get_market_cap_slice_row_counts(
        self, start: date, end: date, market: Market | None = None
    ) -> dict[tuple[date, Market], int]:
        return dict(self._stored)

    def upsert_daily_market_cap(self, rows: list[DailyMarketCapRow]) -> UpsertResult:
        self.upserted.append(list(rows))
        return UpsertResult(updated=max(0, len(rows) - self._upsert_short_by))

    def record_run(self, run) -> None:  # noqa: ANN001 - test stub
        self.recorded_runs.append(run)


def _run(storage: FakeStorage, provider: FakeProvider, **kwargs):
    # No throttle: a disabled HumanThrottlePolicy sleeps nothing, and these
    # tests are about the loop, not the pacing.
    defaults = dict(start=START, end=END)
    defaults.update(kwargs)
    return backfill_market_cap(provider=provider, storage=storage, **defaults)


def test_only_trading_days_are_requested_once_per_market() -> None:
    storage = FakeStorage()
    provider = FakeProvider()

    result = _run(storage, provider)

    # 2024-01-01 (holiday) and the weekend are never requested.
    requested_dates = {d for d, _ in provider.calls}
    assert requested_dates == set(SESSIONS)
    # One request per (date, market), never market='ALL'.
    assert len(provider.calls) == len(SESSIONS) * 2
    assert {m for _, m in provider.calls} == {Market.KOSPI, Market.KOSDAQ}
    assert result.slices_completed == len(SESSIONS) * 2
    assert result.errors == {}


def test_market_filter_limits_requests_to_one_market() -> None:
    storage = FakeStorage()
    provider = FakeProvider()

    _run(storage, provider, markets=[Market.KOSPI])

    assert {m for _, m in provider.calls} == {Market.KOSPI}


def test_complete_slices_are_skipped_and_force_overrides() -> None:
    stored = {(d, m): 3 for d in SESSIONS for m in (Market.KOSPI, Market.KOSDAQ)}
    storage = FakeStorage(stored)
    provider = FakeProvider()

    result = _run(storage, provider)

    assert provider.calls == []
    assert result.slices_skipped == len(SESSIONS) * 2
    assert result.slices_completed == 0

    forced_storage = FakeStorage(stored)
    forced_provider = FakeProvider()
    forced = _run(forced_storage, forced_provider, force=True)

    assert len(forced_provider.calls) == len(SESSIONS) * 2
    assert forced.slices_skipped == 0


def test_short_stored_slice_is_refetched_not_treated_as_done() -> None:
    # The interrupted-write case: rows exist, so an existence check would skip
    # this slice forever.  It holds far fewer rows than its siblings.
    stored = {(d, m): 100 for d in SESSIONS for m in (Market.KOSPI, Market.KOSDAQ)}
    short = (SESSIONS[1], Market.KOSPI)
    stored[short] = 4
    storage = FakeStorage(stored)
    provider = FakeProvider()

    result = _run(storage, provider)

    assert provider.calls == [short]
    assert result.slices_skipped == len(SESSIONS) * 2 - 1
    assert result.slices_completed == 1


def test_row_count_mismatch_marks_slice_incomplete() -> None:
    # Storage wrote fewer rows than the provider returned — the slice must not
    # be counted as completed, and the run must end partial.
    storage = FakeStorage(upsert_short_by=1)
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.slices_completed == 0
    assert result.rows_upserted == 0
    assert len(result.errors) == len(SESSIONS) * 2
    assert all("row count mismatch" in msg for msg in result.errors.values())
    assert storage.recorded_runs[-1].status is RunStatus.PARTIAL


def test_single_slice_failure_is_collected_and_run_is_partial() -> None:
    failing = (SESSIONS[0], Market.KOSDAQ)
    storage = FakeStorage()
    provider = FakeProvider(fail_on={failing})

    result = _run(storage, provider)

    assert list(result.errors) == ["2024-01-02/KOSDAQ"]
    assert result.slices_completed == len(SESSIONS) * 2 - 1
    run = storage.recorded_runs[-1]
    assert run.status is RunStatus.PARTIAL
    assert run.run_type is RunType.MARKET_CAP_BACKFILL


def test_one_upsert_call_per_slice() -> None:
    # Slice atomicity: the service must not split a slice across calls, or a
    # crash mid-slice leaves a permanently partial slice behind.
    storage = FakeStorage()
    provider = FakeProvider(rows_per_slice=250)

    _run(storage, provider)

    assert len(storage.upserted) == len(SESSIONS) * 2
    assert all(len(batch) == 250 for batch in storage.upserted)


def test_empty_response_leaves_slice_retryable() -> None:
    # Calendar says session, exchange returned only zero-filled rows.  Not an
    # error, but not completed either.
    storage = FakeStorage()
    provider = FakeProvider(rows_per_slice=0)

    result = _run(storage, provider)

    assert result.slices_completed == 0
    assert result.rows_upserted == 0
    assert result.errors == {}
    assert storage.upserted == []
    assert storage.recorded_runs[-1].status is RunStatus.SUCCESS


def test_start_after_end_fails_the_run() -> None:
    storage = FakeStorage()
    provider = FakeProvider()

    result = _run(storage, provider, start=END, end=START)

    assert "pipeline" in result.errors
    assert storage.recorded_runs[-1].status is RunStatus.FAILED


# ---------------------------------------------------------------------------
# Source-blocked circuit breaker
# ---------------------------------------------------------------------------


def test_a_blocked_source_stops_the_run_instead_of_grinding_through_it(monkeypatch) -> None:
    # 6,000 slices x 4 retries against a server already refusing is how a
    # temporary throttle becomes a lasting block.
    monkeypatch.setattr("collector.kr.util.retry.time.sleep", lambda _s: None)
    storage = FakeStorage()
    all_slices = {(d, m) for d in SESSIONS for m in (Market.KOSPI, Market.KOSDAQ)}
    provider = FakeProvider(fail_on=all_slices)

    result = _run(storage, provider, max_consecutive_failures=3)

    # Three slices, six requests. It used to be twelve: @retry made four
    # attempts half a second apart per slice, which is the shape KRX restricted
    # the collector's IP for on 2026-08-16. The retry is now single and waits
    # out the throttle's error backoff first, so a blocked source costs two
    # requests per slice instead of four.
    assert len({call for call in provider.calls}) == 3
    assert result.slices_attempted == 3
    assert len(provider.calls) == 6
    assert "source_blocked" in result.errors
    assert storage.recorded_runs[-1].status is RunStatus.FAILED


def test_scattered_failures_do_not_trip_the_guard(monkeypatch) -> None:
    # One bad slice between good ones is an item error, not a blocked source.
    monkeypatch.setattr("collector.kr.util.retry.time.sleep", lambda _s: None)
    storage = FakeStorage()
    provider = FakeProvider(fail_on={(SESSIONS[0], Market.KOSDAQ), (SESSIONS[2], Market.KOSPI)})

    result = _run(storage, provider, max_consecutive_failures=3)

    assert len(result.errors) == 2
    assert "source_blocked" not in result.errors
    assert result.slices_completed == len(SESSIONS) * 2 - 2
    assert storage.recorded_runs[-1].status is RunStatus.PARTIAL


def test_guard_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setattr("collector.kr.util.retry.time.sleep", lambda _s: None)
    storage = FakeStorage()
    all_slices = {(d, m) for d in SESSIONS for m in (Market.KOSPI, Market.KOSDAQ)}
    provider = FakeProvider(fail_on=all_slices)

    result = _run(storage, provider, max_consecutive_failures=0)

    assert result.slices_attempted == len(SESSIONS) * 2
    assert "source_blocked" not in result.errors


def test_the_throttle_paces_and_backs_off_around_every_request() -> None:
    """KRX gets KRX pacing, whichever library makes the call.

    This service used to sleep a flat ``rate_limit_seconds`` between slices with
    no error backoff at all -- 0.4s in the deployed wrapper, against the MDC
    path's 1.5-4.0s spacing and 45-180s post-error backoff, on the same
    ``data.krx.co.kr`` host. On 2026-08-16 KRX restricted the collector's IP for
    "자동화 수단을 통한 비정상 대량 조회". Two paces for one portal was the defect,
    so the pykrx path now takes the same HumanThrottle the MDC path does.
    """
    from collector.kr.util.pipeline import HumanThrottle, HumanThrottlePolicy

    slept: list[float] = []
    throttle = HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=1.5,
            max_delay_seconds=4.0,
            error_backoff_min_seconds=45.0,
            error_backoff_max_seconds=180.0,
        ),
        sleep_fn=slept.append,
        monotonic_fn=lambda: 0.0,
    )

    storage = FakeStorage()
    provider = FakeProvider(fail_on={(SESSIONS[0], Market.KOSPI)})

    _run(storage, provider, throttle=throttle, max_consecutive_failures=0)

    assert throttle.completed_requests == len(provider.calls)
    # Every gap is inside the configured spacing, and the one failed slice
    # bought a real backoff rather than another immediate request.
    spacings = [s for s in slept if s <= 4.0]
    backoffs = [s for s in slept if 45.0 <= s <= 180.0]
    assert spacings, "requests must be spaced"
    assert all(1.5 <= s <= 4.0 for s in spacings)
    assert len(backoffs) == 1

"""Unit tests for the historical universe-snapshot backfill (N3).

The dangerous part of this work package is not the fetch, it is the write.
``upsert_stock_master`` persists the snapshot AND upserts ``stock_master`` in
one call, so reusing it for a backfill would let a 2016 ticker list overwrite
the current universe.  Several tests below exist only to pin that down
(`04_w1_pit_universe.md` 3.1, 7).
"""

from __future__ import annotations

import uuid
from datetime import date

from collector.kr.domain.enums import ListingStatus, Market, RunStatus, RunType, Source
from collector.kr.domain.models import (
    Stock,
    StockUniverseSnapshot,
    UniverseResult,
    UpsertResult,
)
from collector.kr.service.backfill_universe_snapshots import (
    BACKFILL_SNAPSHOT_SOURCES,
    backfill_universe_snapshots,
    month_end_trading_days,
)
from collector.kr.util.time import now_kst

START = date(2024, 1, 1)
END = date(2024, 3, 31)
# 2024-01-31 Wed, 2024-02-29 Thu, 2024-03-29 Fri (03-30/31 is a weekend).
EXPECTED_MONTH_ENDS = [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 29)]


def _snapshot(as_of: date, tickers: list[str], source: Source) -> StockUniverseSnapshot:
    return StockUniverseSnapshot(
        snapshot_id=str(uuid.uuid4()),
        as_of_date=as_of,
        source=source,
        fetched_at=now_kst(),
        records=[
            Stock(
                ticker=t,
                market=Market.KOSPI,
                name=t,
                status=ListingStatus.ACTIVE,
                last_seen_date=as_of,
                source=source,
            )
            for t in tickers
        ],
    )


class FakeProvider:
    """Returns a snapshot per ``as_of`` and records the calls."""

    def __init__(self, fail_on: set[date] | None = None, empty_on: set[date] | None = None):
        self.fail_on = fail_on or set()
        self.empty_on = empty_on or set()
        self.calls: list[tuple[tuple[Market, ...], date | None]] = []

    def fetch_universe(self, markets: list[Market], as_of: date | None = None) -> UniverseResult:
        self.calls.append((tuple(markets), as_of))
        if as_of in self.fail_on:
            return UniverseResult(error="upstream boom")
        tickers = [] if as_of in self.empty_on else ["005930", "000660"]
        return UniverseResult(
            snapshot=_snapshot(as_of or date.today(), tickers, Source.PYKRX_BACKFILL)
        )


class FakeStorage:
    """Storage stub that fails loudly if stock_master is touched."""

    def __init__(self, existing: set[date] | None = None) -> None:
        self._existing = existing or set()
        self.snapshot_writes: list[StockUniverseSnapshot] = []
        self.recorded_runs: list = []
        self.snapshot_source_queries: list[Source] = []

    def get_existing_snapshot_dates(self, source: Source) -> set[date]:
        # The skip check spans every backfill provenance, not just this run's:
        # the pykrx series stopped at 60/152 when KRX blocked the host and the
        # Open API series continues it, so a month-end already collected under
        # either source must not be collected again under the other.
        assert source in BACKFILL_SNAPSHOT_SOURCES
        self.snapshot_source_queries.append(source)
        return set(self._existing)

    def insert_stock_master_snapshot_only(self, snapshot: StockUniverseSnapshot) -> UpsertResult:
        self.snapshot_writes.append(snapshot)
        return UpsertResult(inserted=snapshot.record_count)

    def upsert_stock_master(self, stocks, snapshot) -> UpsertResult:  # noqa: ANN001
        raise AssertionError(
            "backfill must not call upsert_stock_master — it would rewrite the "
            "current universe from a historical ticker list"
        )

    def record_run(self, run) -> None:  # noqa: ANN001 - test stub
        self.recorded_runs.append(run)


def _run(storage: FakeStorage, provider: FakeProvider, **kwargs):
    defaults = dict(start=START, end=END)
    defaults.update(kwargs)
    return backfill_universe_snapshots(provider=provider, storage=storage, **defaults)


def test_month_end_trading_days_picks_the_last_session_of_each_month() -> None:
    assert month_end_trading_days(START, END) == EXPECTED_MONTH_ENDS


def test_month_end_skips_a_month_with_no_session_in_range() -> None:
    # A window that only covers a weekend contributes nothing.
    assert month_end_trading_days(date(2024, 3, 30), date(2024, 3, 31)) == []


def test_month_end_handles_a_partial_final_month() -> None:
    # End mid-month: the last session on or before `end`, not the month's.
    days = month_end_trading_days(date(2024, 1, 1), date(2024, 2, 15))
    assert days == [date(2024, 1, 31), date(2024, 2, 15)]


def test_backfill_writes_one_snapshot_per_month_end() -> None:
    storage = FakeStorage()
    provider = FakeProvider()

    result = _run(storage, provider)

    assert [as_of for _, as_of in provider.calls] == EXPECTED_MONTH_ENDS
    assert result.snapshots_written == 3
    assert result.items_written == 6
    assert result.errors == {}
    assert storage.recorded_runs[-1].run_type is RunType.UNIVERSE_SNAPSHOT_BACKFILL
    assert storage.recorded_runs[-1].status is RunStatus.SUCCESS


def test_backfill_never_touches_stock_master() -> None:
    # FakeStorage.upsert_stock_master raises; reaching it fails the test.
    # This is the trap 04_w1_pit_universe.md 3.1 calls out.
    storage = FakeStorage()
    provider = FakeProvider()

    _run(storage, provider)

    assert len(storage.snapshot_writes) == 3


def test_snapshots_are_tagged_pykrx_backfill_not_pykrx() -> None:
    # sync_universe infers delistings by diffing consecutive snapshots. A
    # reconstructed snapshot carrying Source.PYKRX would enter that diff and
    # fabricate listings/delistings.
    storage = FakeStorage()
    provider = FakeProvider()

    _run(storage, provider)

    assert all(s.source is Source.PYKRX_BACKFILL for s in storage.snapshot_writes)
    assert all(
        r.source is Source.PYKRX_BACKFILL for s in storage.snapshot_writes for r in s.records
    )


def test_skip_check_spans_every_backfill_provenance() -> None:
    # The pykrx series stopped at 60/152 when KRX blocked this host; the Open
    # API series continues it. Asking only about this run's source would
    # re-collect those month-ends under a second provenance and leave two
    # snapshots per date for backfill-master to reconcile.
    storage = FakeStorage()

    _run(storage, FakeProvider())

    assert set(storage.snapshot_source_queries) == set(BACKFILL_SNAPSHOT_SOURCES)


def test_existing_dates_are_skipped_and_force_overrides() -> None:
    storage = FakeStorage(existing={date(2024, 1, 31), date(2024, 2, 29)})
    provider = FakeProvider()

    result = _run(storage, provider)

    assert [as_of for _, as_of in provider.calls] == [date(2024, 3, 29)]
    assert result.snapshots_skipped == 2
    assert result.snapshots_written == 1

    forced_storage = FakeStorage(existing={date(2024, 1, 31), date(2024, 2, 29)})
    forced_provider = FakeProvider()
    forced = _run(forced_storage, forced_provider, force=True)

    assert forced.snapshots_skipped == 0
    assert forced.snapshots_written == 3


def test_provider_failure_is_collected_and_run_is_partial() -> None:
    storage = FakeStorage()
    provider = FakeProvider(fail_on={date(2024, 2, 29)})

    result = _run(storage, provider)

    assert list(result.errors) == ["2024-02-29"]
    assert result.snapshots_written == 2
    assert storage.recorded_runs[-1].status is RunStatus.PARTIAL


def test_empty_universe_is_an_error_not_a_written_snapshot() -> None:
    # An empty ticker list is a collection failure, not a month where nothing
    # was listed. Writing it would look like a market-wide delisting.
    storage = FakeStorage()
    provider = FakeProvider(empty_on={date(2024, 1, 31)})

    result = _run(storage, provider)

    assert list(result.errors) == ["2024-01-31"]
    assert result.snapshots_written == 2
    assert all(s.as_of_date != date(2024, 1, 31) for s in storage.snapshot_writes)


def test_start_after_end_fails_the_run() -> None:
    storage = FakeStorage()
    provider = FakeProvider()

    result = _run(storage, provider, start=END, end=START)

    assert "pipeline" in result.errors
    assert storage.recorded_runs[-1].status is RunStatus.FAILED


def test_an_empty_universe_backs_off_like_any_other_error() -> None:
    """An empty ticker list on a trading day is a refusal wearing an ordinary face.

    pykrx's ``dataframe_empty_handler`` swallows the JSON decode error a blocked
    session produces and returns an empty result, so "no records" and "the
    source is refusing" arrive identically. Treating the empty case as a quiet
    skip meant the run kept its normal pace straight through a block -- which is
    what KRX restricted the collector's IP for on 2026-08-16.
    """
    from collector.kr.util.pipeline import HumanThrottle, HumanThrottlePolicy

    slept: list[float] = []
    throttle = HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=1.0,
            max_delay_seconds=1.0,
            error_backoff_min_seconds=45.0,
            error_backoff_max_seconds=180.0,
        ),
        sleep_fn=slept.append,
        monotonic_fn=lambda: 0.0,
    )

    storage = FakeStorage()
    provider = FakeProvider(empty_on={EXPECTED_MONTH_ENDS[0]})

    _run(storage, provider, throttle=throttle, max_consecutive_failures=0)

    assert [s for s in slept if 45.0 <= s <= 180.0], "an empty universe must back off"

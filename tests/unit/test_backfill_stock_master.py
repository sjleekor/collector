"""Recovering historical securities into ``stock_master`` from the PIT snapshots.

``UniverseScope.HISTORICAL`` means "every row in the stock master", and it works
— but the master only knows what the collector has watched since 2026-04, so
everything delisted before then is absent. Measured 2026-08-16: 1,299 of the
3,959 corps that ever carried a ticker have no row at all, and on the first 24
reconstructed snapshots, 372 tickers appear that the master has never seen — and
exactly those 372 have zero rows in ``daily_ohlcv``.

Absence from the master is not "not fetched yet", it is unreachable: price
collection resolves targets from that table, and ``--tickers`` filters against
the same table, so naming a missing ticker returns nothing. No flag reaches past
it, which is why the fix is to make the master contain what it claims to.
"""

from __future__ import annotations

from datetime import date

from collector.kr.domain.enums import ListingStatus, Market, RunStatus, Source
from collector.kr.domain.models import Stock
from collector.kr.service.backfill_stock_master import (
    backfill_stock_master_from_snapshots,
)


def _snapshot_only(ticker: str, market: Market = Market.KOSPI) -> Stock:
    """A candidate as the storage layer returns it: status not yet decided."""
    return Stock(
        ticker=ticker,
        market=market,
        name=f"{ticker}-name",
        status=ListingStatus.UNKNOWN,
        last_seen_date=date(2018, 3, 30),
        source=Source.PYKRX_BACKFILL,
        first_seen_date=date(2014, 6, 30),
    )


class _FakeStorage:
    def __init__(self, candidates: list[Stock] | None = None, fail: bool = False) -> None:
        self._candidates = candidates or []
        self._fail = fail
        self.upserted: list[Stock] = []
        self.runs: list[object] = []
        self.requested_sources: list[list[Source]] = []

    def get_stocks_seen_only_in_snapshots(self, sources: list[Source] | None = None) -> list[Stock]:
        if self._fail:
            raise RuntimeError("snapshot read failed")
        self.requested_sources.append(list(sources or []))
        return list(self._candidates)

    def upsert_stock_master_rows(self, stocks: list[Stock]) -> int:
        self.upserted.extend(stocks)
        return len(stocks)

    def record_run(self, run) -> None:  # noqa: ANN001
        self.runs.append(run)


def test_recovered_securities_are_written_as_delisted() -> None:
    # Absence from the master is positive evidence that no live universe sync
    # ever saw the ticker, so it is not listed today.
    storage = _FakeStorage([_snapshot_only("000660"), _snapshot_only("035720", Market.KOSDAQ)])

    result = backfill_stock_master_from_snapshots(storage)

    assert result.candidates == 2
    assert result.rows_upserted == 2
    assert [s.status for s in storage.upserted] == [
        ListingStatus.DELISTED,
        ListingStatus.DELISTED,
    ]


def test_the_snapshot_fields_survive_the_status_change() -> None:
    storage = _FakeStorage([_snapshot_only("000660")])

    backfill_stock_master_from_snapshots(storage)

    recovered = storage.upserted[0]
    assert recovered.ticker == "000660"
    assert recovered.market is Market.KOSPI
    assert recovered.name == "000660-name"
    assert recovered.first_seen_date == date(2014, 6, 30)
    assert recovered.last_seen_date == date(2018, 3, 30)
    assert recovered.source is Source.PYKRX_BACKFILL


def test_a_dry_run_counts_without_writing() -> None:
    storage = _FakeStorage([_snapshot_only("000660")])

    result = backfill_stock_master_from_snapshots(storage, dry_run=True)

    assert result.candidates == 1
    assert result.rows_upserted == 0
    assert storage.upserted == []
    assert result.dry_run is True


def test_nothing_to_recover_is_a_success_not_an_error() -> None:
    storage = _FakeStorage([])

    result = backfill_stock_master_from_snapshots(storage)

    assert result.candidates == 0
    assert result.rows_upserted == 0
    assert result.errors == {}
    assert storage.runs[-1].status is RunStatus.SUCCESS


def test_it_reads_only_the_historical_backfill_snapshots_by_default() -> None:
    # The live FDR snapshots cannot contribute: anything in them is in the
    # master by construction, so reading them is wasted work at best.  Both
    # backfill provenances do contribute — the pykrx series stopped at 60/152
    # when KRX blocked this host and the Open API series continues it, so
    # reading only one would leave half the recovered tickers behind.
    storage = _FakeStorage([])

    backfill_stock_master_from_snapshots(storage)

    assert storage.requested_sources == [[Source.PYKRX_BACKFILL, Source.KRX_OPENAPI_BACKFILL]]


def test_an_explicit_source_list_is_honoured() -> None:
    storage = _FakeStorage([])

    backfill_stock_master_from_snapshots(storage, sources=[Source.PYKRX_BACKFILL, Source.FDR])

    assert storage.requested_sources == [[Source.PYKRX_BACKFILL, Source.FDR]]


def test_a_failure_is_recorded_as_a_failed_run_and_reported() -> None:
    storage = _FakeStorage(fail=True)

    result = backfill_stock_master_from_snapshots(storage)

    assert "pipeline" in result.errors
    assert storage.runs[-1].status is RunStatus.FAILED
    assert storage.upserted == []


def test_the_run_records_what_it_did() -> None:
    storage = _FakeStorage([_snapshot_only("000660")])

    backfill_stock_master_from_snapshots(storage)

    run = storage.runs[-1]
    assert run.counts == {"candidates": 1, "rows_upserted": 1}
    assert run.params["sources"] == ["PYKRX_BACKFILL", "KRX_OPENAPI_BACKFILL"]

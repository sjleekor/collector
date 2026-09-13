"""Unit tests for the historical pykrx universe provider (N3).

The reason this provider exists at all is cost.  ``PykrxUniverseProvider``
resolves names with one ``get_market_ticker_name`` call per ticker — measured
at 0.354s, which is ~38 hours for 145 month-ends x 2 markets x ~2,700 tickers.
``get_market_price_change_by_ticker`` returns every name in one call, and its
ticker set matched ``get_market_ticker_list`` exactly at four sampled dates
spanning 2014-2024.

So the tests below pin two things: names come from the bulk call, and a failure
of that call degrades to the ticker rather than failing the snapshot.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from collector.kr.adapters.universe_pykrx import provider as provider_module
from collector.kr.adapters.universe_pykrx.provider import (
    PykrxHistoricalUniverseProvider,
)
from collector.kr.domain.enums import ListingStatus, Market, Source

AS_OF = date(2016, 6, 30)


class _FakeStock:
    def __init__(
        self,
        tickers: dict[str, list[str]],
        names: pd.DataFrame | None = None,
        name_error: Exception | None = None,
    ) -> None:
        self._tickers = tickers
        self._names = names
        self._name_error = name_error
        self.name_calls: list[tuple[str, str, str]] = []
        self.ticker_name_calls: list[str] = []

    def get_market_ticker_list(self, date_str, market):  # noqa: ANN001
        return list(self._tickers.get(market, []))

    def get_market_price_change_by_ticker(self, fromdate, todate, market):  # noqa: ANN001
        self.name_calls.append((fromdate, todate, market))
        if self._name_error is not None:
            raise self._name_error
        return self._names

    def get_market_ticker_name(self, ticker):  # noqa: ANN001
        # Never acceptable here — one request per ticker is the whole problem.
        self.ticker_name_calls.append(ticker)
        return "SHOULD NOT BE CALLED"


def _name_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {"종목명": ["삼성전자", "SK하이닉스"], "종가": [1, 2]},
        index=pd.Index(["005930", "000660"], name="티커"),
    )


@pytest.fixture(autouse=True)
def _clear_module_cache():
    yield
    provider_module.get_pykrx_stock_module.cache_clear()


def _patched(monkeypatch, fake: _FakeStock) -> PykrxHistoricalUniverseProvider:
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake)
    return PykrxHistoricalUniverseProvider()


def test_names_come_from_one_bulk_call_per_market(monkeypatch) -> None:
    fake = _FakeStock({"KOSPI": ["005930", "000660"]}, names=_name_frame())

    result = _patched(monkeypatch, fake).fetch_universe([Market.KOSPI], as_of=AS_OF)

    assert result.error is None
    assert fake.name_calls == [("20160630", "20160630", "KOSPI")]
    # The per-ticker path must never be reached.
    assert fake.ticker_name_calls == []

    names = {s.ticker: s.name for s in result.snapshot.records}
    assert names == {"005930": "삼성전자", "000660": "SK하이닉스"}


def test_snapshot_is_tagged_pykrx_backfill(monkeypatch) -> None:
    fake = _FakeStock({"KOSPI": ["005930", "000660"]}, names=_name_frame())

    result = _patched(monkeypatch, fake).fetch_universe([Market.KOSPI], as_of=AS_OF)

    assert result.snapshot.source is Source.PYKRX_BACKFILL
    assert result.snapshot.as_of_date == AS_OF
    assert all(s.source is Source.PYKRX_BACKFILL for s in result.snapshot.records)
    assert all(s.status is ListingStatus.ACTIVE for s in result.snapshot.records)


def test_name_lookup_failure_degrades_to_ticker(monkeypatch) -> None:
    # The point of this backfill is "was it listed that day", so a missing name
    # must not lose the snapshot.
    fake = _FakeStock({"KOSPI": ["005930"]}, name_error=RuntimeError("names down"))

    result = _patched(monkeypatch, fake).fetch_universe([Market.KOSPI], as_of=AS_OF)

    assert result.error is None
    assert [s.name for s in result.snapshot.records] == ["005930"]


def test_ticker_missing_from_the_name_frame_degrades_to_ticker(monkeypatch) -> None:
    fake = _FakeStock({"KOSPI": ["005930", "000660", "035720"]}, names=_name_frame())

    result = _patched(monkeypatch, fake).fetch_universe([Market.KOSPI], as_of=AS_OF)

    names = {s.ticker: s.name for s in result.snapshot.records}
    assert names["035720"] == "035720"


def test_markets_are_requested_separately(monkeypatch) -> None:
    fake = _FakeStock(
        {"KOSPI": ["005930"], "KOSDAQ": ["000660"]},
        names=_name_frame(),
    )

    result = _patched(monkeypatch, fake).fetch_universe([Market.KOSPI, Market.KOSDAQ], as_of=AS_OF)

    assert [c[2] for c in fake.name_calls] == ["KOSPI", "KOSDAQ"]
    by_ticker = {s.ticker: s.market for s in result.snapshot.records}
    assert by_ticker == {"005930": Market.KOSPI, "000660": Market.KOSDAQ}


def test_empty_market_is_skipped_not_fatal(monkeypatch) -> None:
    fake = _FakeStock({"KOSPI": ["005930"], "KOSDAQ": []}, names=_name_frame())

    result = _patched(monkeypatch, fake).fetch_universe([Market.KOSPI, Market.KOSDAQ], as_of=AS_OF)

    assert result.error is None
    assert [s.ticker for s in result.snapshot.records] == ["005930"]


def test_upstream_exception_becomes_result_error(monkeypatch) -> None:
    class _Boom(_FakeStock):
        def get_market_ticker_list(self, date_str, market):  # noqa: ANN001
            raise ValueError("Expecting value: line 1 column 1")

    result = _patched(monkeypatch, _Boom({})).fetch_universe([Market.KOSPI], as_of=AS_OF)

    assert result.snapshot is None
    assert "Expecting value" in result.error

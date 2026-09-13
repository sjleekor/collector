"""Unit tests for the pykrx market-cap adapter (N1).

The fixtures mirror shapes observed against live pykrx 1.2.8 on 2026-08-15
(`docs/dev/20260731_raw_features/02_data_expansion_plan/poc/n1_pykrx_market_cap.md`),
not a simplified stand-in — `00_status.md` 4c traces three missed B-2 defects to
fixtures that were tidier than the real responses.

The two shapes that matter:

  * a holiday returns EVERY ticker with the price columns zeroed and
    상장주식수 populated — not an empty frame;
  * on a real session a halted name has a live close with 거래량/거래대금 at
    zero, and those zeros are real.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from collector.kr.adapters.market_cap_pykrx import provider as provider_module
from collector.kr.adapters.market_cap_pykrx.provider import PykrxMarketCapProvider
from collector.kr.domain.enums import Market, Source

TRADE_DATE = date(2024, 1, 2)


def _session_frame() -> pd.DataFrame:
    """Two normal names plus one halted name (real zeros)."""
    return pd.DataFrame(
        {
            "종가": [79600, 142400, 5230],
            "시가총액": [475194690980000, 103667536776000, 5230000],
            "거래량": [17142847, 2147458, 0],
            "거래대금": [1356958225913, 304025996500, 0],
            "상장주식수": [5969782550, 728002365, 1000],
        },
        index=pd.Index(["005930", "000660", "900110"], name="티커"),
    )


def _holiday_frame() -> pd.DataFrame:
    """Holiday: every ticker present, prices zeroed, share count populated."""
    return pd.DataFrame(
        {
            "종가": [0, 0],
            "시가총액": [0, 0],
            "거래량": [0, 0],
            "거래대금": [0, 0],
            "상장주식수": [45252759, 56702415],
        },
        index=pd.Index(["095570", "003460"], name="티커"),
    )


class _FakeStock:
    def __init__(self, frame: pd.DataFrame | None, raises: Exception | None = None) -> None:
        self._frame = frame
        self._raises = raises
        self.calls: list[tuple[str, str, bool]] = []

    def get_market_cap_by_ticker(self, date_str, market, alternative):  # noqa: ANN001
        self.calls.append((date_str, market, alternative))
        if self._raises is not None:
            raise self._raises
        return self._frame


@pytest.fixture(autouse=True)
def _clear_module_cache():
    yield
    provider_module.get_pykrx_stock_module.cache_clear()


def _patched(monkeypatch, fake: _FakeStock) -> PykrxMarketCapProvider:
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake)
    return PykrxMarketCapProvider()


def test_session_rows_are_parsed_and_market_comes_from_the_argument(monkeypatch) -> None:
    fake = _FakeStock(_session_frame())
    result = _patched(monkeypatch, fake).fetch_by_date(TRADE_DATE, Market.KOSDAQ)

    assert result.error is None
    assert result.response_rows == 3
    assert len(result.rows) == 3

    first = result.rows[0]
    assert first.ticker == "005930"
    assert first.trade_date == TRADE_DATE
    assert first.source_close == 79600
    assert first.market_cap == 475194690980000
    assert first.trading_value == 1356958225913
    assert first.listed_shares == 5969782550
    assert first.volume == 17142847
    assert first.source is Source.PYKRX
    # The response carries no market column; the argument fills it. A
    # stock_master join here would leak a post-transfer market backwards.
    assert all(r.market is Market.KOSDAQ for r in result.rows)


def test_halted_name_keeps_its_real_zeros(monkeypatch) -> None:
    fake = _FakeStock(_session_frame())
    result = _patched(monkeypatch, fake).fetch_by_date(TRADE_DATE, Market.KOSPI)

    halted = next(r for r in result.rows if r.ticker == "900110")
    assert halted.source_close == 5230
    # Zero volume on a live close is a halt, not a missing value.
    assert halted.volume == 0
    assert halted.trading_value == 0


def test_holiday_zero_fill_is_dropped_entirely(monkeypatch) -> None:
    fake = _FakeStock(_holiday_frame())
    result = _patched(monkeypatch, fake).fetch_by_date(date(2024, 1, 1), Market.KOSPI)

    assert result.error is None
    # response_rows keeps the raw count so the service can report what it saw.
    assert result.response_rows == 2
    assert result.rows == []


def test_alternative_is_pinned_false(monkeypatch) -> None:
    # alternative=True substitutes the previous session under the requested
    # date — a wrong row that looks right. Pin it explicitly.
    fake = _FakeStock(_session_frame())
    _patched(monkeypatch, fake).fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert fake.calls == [("20240102", "KOSPI", False)]


def test_unexpected_columns_are_reported_not_silently_parsed(monkeypatch) -> None:
    renamed = _session_frame().rename(columns={"거래대금": "거래대금액"})
    fake = _FakeStock(renamed)
    result = _patched(monkeypatch, fake).fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert result.rows == []
    assert result.error is not None
    assert "거래대금" in result.error


def test_empty_frame_is_not_an_error(monkeypatch) -> None:
    fake = _FakeStock(pd.DataFrame())
    result = _patched(monkeypatch, fake).fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert result.error is None
    assert result.rows == []


def test_upstream_exception_becomes_result_error(monkeypatch) -> None:
    fake = _FakeStock(None, raises=ValueError("Expecting value: line 1 column 1"))
    result = _patched(monkeypatch, fake).fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert result.rows == []
    assert result.error is not None
    assert "Expecting value" in result.error

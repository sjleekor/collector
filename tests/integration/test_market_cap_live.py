"""Live pykrx checks for the market-cap adapter (N1).

Gated on ``RUN_LIVE_PYKRX_TEST=1``.  These pin the response properties the
adapter and the mart both rely on, so an upstream change surfaces here instead
of as silently wrong data:

  * the response has no market column, and ``ALL`` folds in KONEX;
  * ``市価総額 == 종가 x 상장주식수`` holds exactly, which is why the mart treats
    ``market_cap`` as derived and ``listed_shares`` as the new information;
  * a holiday returns zero-filled rows rather than an empty frame;
  * the ticker set matches ``get_market_ticker_list`` — the basis for using N1
    rows as the daily PIT universe (04_w1_pit_universe.md 3.5).
"""

from __future__ import annotations

import os
from datetime import date

import pytest

from collector.kr.adapters.market_cap_pykrx.provider import PykrxMarketCapProvider
from collector.kr.adapters.pykrx_auth import get_pykrx_stock_module
from collector.kr.domain.enums import Market

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_PYKRX_TEST") != "1",
    reason="Set RUN_LIVE_PYKRX_TEST=1 to run live pykrx checks.",
)

SESSION = date(2024, 1, 2)
HOLIDAY = date(2024, 1, 1)


@pytest.mark.parametrize("market", [Market.KOSPI, Market.KOSDAQ])
def test_fetch_by_date_returns_usable_rows(market: Market) -> None:
    result = PykrxMarketCapProvider().fetch_by_date(SESSION, market)

    assert result.error is None
    assert result.rows
    assert result.response_rows == len(result.rows)  # no zero-fill on a session
    assert all(r.market is market for r in result.rows)
    assert all(r.trade_date == SESSION for r in result.rows)
    assert all(r.source_close and r.source_close > 0 for r in result.rows)


def test_market_cap_is_close_times_listed_shares() -> None:
    # Exact on every sampled row as of 2026-08-15. If this ever fails, the
    # market-cap definition changed (preferred shares, foreign DRs) and
    # fin_log_mcap's replacement needs rethinking.
    result = PykrxMarketCapProvider().fetch_by_date(SESSION, Market.KOSPI)

    mismatches = [
        r.ticker
        for r in result.rows
        if r.market_cap != (r.source_close or 0) * (r.listed_shares or 0)
    ]
    assert mismatches == []


def test_holiday_returns_zero_filled_rows_and_adapter_drops_them() -> None:
    stock = get_pykrx_stock_module()
    raw = stock.get_market_cap_by_ticker(
        HOLIDAY.strftime("%Y%m%d"), market="KOSPI", alternative=False
    )
    # Not an empty frame — this is the property the trading calendar guards.
    assert not raw.empty
    assert (raw["종가"] == 0).all()

    result = PykrxMarketCapProvider().fetch_by_date(HOLIDAY, Market.KOSPI)
    assert result.error is None
    assert result.rows == []
    assert result.response_rows > 0


def test_all_market_would_fold_in_konex() -> None:
    stock = get_pykrx_stock_module()
    date_str = SESSION.strftime("%Y%m%d")

    all_tickers = set(stock.get_market_cap_by_ticker(date_str, market="ALL").index)
    kospi = set(stock.get_market_cap_by_ticker(date_str, market="KOSPI").index)
    kosdaq = set(stock.get_market_cap_by_ticker(date_str, market="KOSDAQ").index)
    konex = set(stock.get_market_ticker_list(date_str, market="KONEX"))

    assert not kospi & kosdaq
    assert all_tickers - (kospi | kosdaq) == konex


@pytest.mark.parametrize("market", [Market.KOSPI, Market.KOSDAQ])
def test_row_set_matches_the_listed_universe(market: Market) -> None:
    # Basis for using daily_market_cap rows as the daily PIT universe.
    stock = get_pykrx_stock_module()
    listed = set(stock.get_market_ticker_list(SESSION.strftime("%Y%m%d"), market=market.value))

    result = PykrxMarketCapProvider().fetch_by_date(SESSION, market)

    assert {r.ticker for r in result.rows} == listed

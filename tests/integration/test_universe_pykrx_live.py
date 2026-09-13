"""Live integration tests for pykrx-backed universe fetching.

These tests hit the real upstream data source and are intentionally opt-in.
Enable them only when you explicitly want to verify that pykrx's KRX listing
endpoints are currently working.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

from collector.kr.adapters.pykrx_auth import get_pykrx_stock_module
from collector.kr.adapters.universe_pykrx.provider import PykrxUniverseProvider
from collector.kr.domain.enums import Market, Source

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_PYKRX_TEST") != "1",
    reason="Set RUN_LIVE_PYKRX_TEST=1 to run live pykrx checks.",
)


@pytest.mark.parametrize("market", [Market.KOSPI, Market.KOSDAQ])
def test_pykrx_stock_listing_live(market: Market) -> None:
    """Verify that the pykrx listing call used by the project still works."""
    stock = get_pykrx_stock_module()
    target_date = date.today().strftime("%Y%m%d")

    tickers = stock.get_market_ticker_list(target_date, market=market.value)

    assert tickers
    assert all(isinstance(ticker, str) and ticker.strip() for ticker in tickers[:20])

    sample_names = [stock.get_market_ticker_name(ticker) for ticker in tickers[:10]]
    assert all(isinstance(name, str) and name.strip() for name in sample_names)


def test_pykrx_universe_provider_live() -> None:
    """Verify that the project's pykrx adapter can map live listing data."""
    result = PykrxUniverseProvider().fetch_universe([Market.KOSPI, Market.KOSDAQ])

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.source == Source.PYKRX
    assert result.snapshot.record_count > 1000

    sample = result.snapshot.records[:10]
    assert sample
    assert all(stock.ticker for stock in sample)
    assert all(stock.name for stock in sample)

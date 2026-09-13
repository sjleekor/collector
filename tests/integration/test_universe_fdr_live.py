"""Live integration tests for FinanceDataReader-backed universe fetching.

These tests hit the real upstream data source and are intentionally opt-in.
Enable them only when you explicitly want to verify that FinanceDataReader's
KRX listing endpoints are currently working.
"""

from __future__ import annotations

import os

import FinanceDataReader as fdr
import pytest

from collector.kr.adapters.universe_fdr.provider import FdrUniverseProvider
from collector.kr.domain.enums import Market, Source

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_FDR_TEST") != "1",
    reason="Set RUN_LIVE_FDR_TEST=1 to run live FinanceDataReader checks.",
)


@pytest.mark.parametrize("market", ["KOSPI", "KOSDAQ"])
def test_finance_datareader_stock_listing_live(market: str) -> None:
    """Verify that the exact FDR listing call used by the project still works."""
    df = fdr.StockListing(market)

    assert not df.empty
    assert "Name" in df.columns
    assert "Code" in df.columns or "Symbol" in df.columns

    ticker_column = "Code" if "Code" in df.columns else "Symbol"
    tickers = df[ticker_column].astype(str).str.strip()
    names = df["Name"].astype(str).str.strip()

    assert tickers.ne("").any()
    assert names.ne("").any()


def test_fdr_universe_provider_live() -> None:
    """Verify that the project's FDR adapter can map live listing data."""
    result = FdrUniverseProvider().fetch_universe([Market.KOSPI, Market.KOSDAQ])

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.source == Source.FDR
    assert result.snapshot.record_count > 1000

    sample = result.snapshot.records[:10]
    assert sample
    assert all(stock.ticker for stock in sample)
    assert all(stock.name for stock in sample)

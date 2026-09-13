"""Port: Daily market-cap provider interface.

Any adapter that can supply KRX market cap / trading value / listed shares for
a whole market on one date must conform to ``MarketCapProvider``.

The unit of work is a ``(trade_date, market)`` SLICE, not a ticker.  Fetching
by date is roughly a thousand times cheaper than looping tickers, and the
per-market split is not an optimisation choice — the response carries no market
column, so one request per market is the only way to fill it without joining
``stock_master`` (which would leak a stock's present-day market backwards).
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import Market
from collector.kr.domain.models import DailyMarketCapResult


@runtime_checkable
class MarketCapProvider(Protocol):
    """Fetches one ``(trade_date, market)`` slice of market-cap rows.

    Implementations:
        - ``PykrxMarketCapProvider`` (pykrx / KRX)
    """

    def fetch_by_date(self, trade_date: date, market: Market) -> DailyMarketCapResult:
        """Retrieve every listed stock's market-cap row for one date and market.

        Implementations never raise for an upstream failure — the error goes in
        ``DailyMarketCapResult.error`` so the caller can record a partial run.

        Args:
            trade_date: Trading date to fetch.
            market: Market segment to fetch.

        Returns:
            ``DailyMarketCapResult`` containing rows or an error.
        """
        ...

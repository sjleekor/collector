"""Port: Price provider interface.

Any adapter that can supply daily (or future intraday) OHLCV bars must
conform to the ``PriceProvider`` protocol.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import Market
from collector.kr.domain.models import DailyPriceResult


@runtime_checkable
class PriceProvider(Protocol):
    """Fetches daily OHLCV bars for a single ticker.

    Implementations:
        - ``PykrxDailyPriceProvider`` (pykrx)
    """

    def fetch_daily_ohlcv(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> DailyPriceResult:
        """Retrieve daily OHLCV bars for the given ticker and date range.

        Args:
            ticker: 6-digit KRX ticker code.
            market: Market segment.
            start: First trade date (inclusive).
            end: Last trade date (inclusive).

        Returns:
            ``DailyPriceResult`` containing bars or an error.
        """
        ...


# ---------------------------------------------------------------------------
# Future extension point: Intraday
# ---------------------------------------------------------------------------
# class IntradayPriceProvider(Protocol):
#     """Fetches intraday (minute/hour) OHLCV bars for a single ticker.
#
#     NOT IMPLEMENTED — reserved for future use.
#
#     def fetch_intraday_bars(
#         self,
#         ticker: str,
#         date: date,
#         interval: str,  # e.g. "1m", "5m", "1h"
#     ) -> IntradayPriceResult:
#         ...
#     """
#     ...

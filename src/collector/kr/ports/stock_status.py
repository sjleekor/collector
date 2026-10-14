"""Port: per-ticker KIS status-flag provider.

Separate from :mod:`collector.kr.ports.flows` because the unit of work is
different: one live snapshot per ticker, no date range, nothing to page.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import FlowRequestStats, StockStatusFetchResult


@runtime_checkable
class StockStatusProvider(Protocol):
    """Fetch the current trading-status flags of one ticker."""

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        ...

    def request_stats(self) -> FlowRequestStats:
        """Return real HTTP counters accumulated so far."""
        ...

    def fetch_stock_status(self, ticker: str, market: Market) -> StockStatusFetchResult:
        """Fetch the flags for one ticker; failures come back as ``error``."""
        ...

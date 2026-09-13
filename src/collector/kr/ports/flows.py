"""Ports: security flow provider interfaces.

There are two, because the two upstreams have genuinely different work units:

* :class:`FlowProvider` — ``(trade_date, market) -> every ticker``. KRX MDC.
* :class:`TickerFlowProvider` — ``(ticker) -> a date range``. KIS Developers.

Collapsing them into one protocol would mean the KIS side answering a
whole-market question one ticker at a time, throwing away the 30-100 sessions
per call that make it cheap, and leaving the failure unit ("which market-day
is missing") a lie — under KIS a failure is a *per-ticker hole*.  Keeping them
apart lets each service checkpoint the unit it actually retries.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import FlowRequestStats, SecurityFlowFetchResult


@runtime_checkable
class FlowProvider(Protocol):
    """Fetch daily investor/shorting/ownership flow metrics."""

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        ...

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Fetch investor net-buy volume metrics for one ticker/date range."""
        ...

    def fetch_investor_net_volume_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        """Fetch all-ticker investor net-buy volume metrics for one market/date."""
        ...

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Fetch short-selling metrics for one ticker/date range."""
        ...

    def fetch_shorting_trading_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        """Fetch all-ticker short-selling trading metrics for one market/date."""
        ...

    def fetch_shorting_balance_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        """Fetch all-ticker short-selling balance metrics for one market/date."""
        ...

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        """Fetch foreign ownership shares for one market/date."""
        ...

    def unsupported_metric_codes(self) -> list[str]:
        """Return metric codes intentionally left unsupported by this provider."""
        ...


@runtime_checkable
class TickerFlowProvider(Protocol):
    """Fetch flow metrics one ticker at a time over a date range."""

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        ...

    def supported_flow_groups(self) -> tuple[str, ...]:
        """Return the flow metric groups this provider can fill."""
        ...

    def unsupported_metric_codes(self) -> list[str]:
        """Return metric codes intentionally left unsupported by this provider."""
        ...

    def fetch_foreign_holding(
        self,
        ticker: str,
        market: Market,
        trade_date: date,
    ) -> SecurityFlowFetchResult:
        """Fetch foreign ownership shares for one ticker.

        The upstream value is a live snapshot with no business date attached,
        so ``trade_date`` is the caller's assertion about which session it
        belongs to.  Callers must only pass the most recent session.
        """
        ...

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Fetch investor net-buy volume metrics for one ticker/date range."""
        ...

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Fetch short-selling metrics for one ticker/date range."""
        ...

    def request_stats(self) -> FlowRequestStats:
        """Return real HTTP counters accumulated so far.

        Logical request counts cannot audit a published quota or explain a
        block; only the transport layer knows how many calls actually left.
        """
        ...

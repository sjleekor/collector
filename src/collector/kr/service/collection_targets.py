"""The single place that decides what a collection targets.

Before this module every service resolved its own targets, and every one of
them independently reached for the currently-listed set::

    storage.get_dart_corp_master(active_only=True, tickers=tickers)   # x5
    storage.get_active_stocks(market)                                 # prices

Six independent decisions, all the same, all invisible — and all wrong for a
historical backfill.  The measured result was 2.0-2.3% coverage of the 1,330
delisted names in every DART raw table and in ``daily_ohlcv``, and 13.9% of the
2016 cross-section missing (``poc/survivorship_gap.md``).

Adding an opt-in flag per service does not fix that: the next collector will
default the same way.  Routing every service through these two functions gives
one place that knows about ``active_only`` / ``get_stocks``, and
``tests/unit/test_collection_targets.py`` fails if a service goes around them.
"""

from __future__ import annotations

from collector.kr.domain.enums import Market, UniverseScope
from collector.kr.domain.models import DartCorp, Stock
from collector.kr.ports.storage import Storage


def resolve_dart_targets(
    storage: Storage,
    scope: UniverseScope,
    tickers: list[str] | None = None,
) -> list[DartCorp]:
    """Return the OpenDART corporations a collection should target.

    Args:
        storage: Storage to read the corp master from.
        scope: ``CURRENT`` for currently-listed corps (2,657), ``HISTORICAL``
            for every corp that ever carried a ticker (3,959).
        tickers: Optional ticker allowlist.

    Returns:
        Corp-master rows, always restricted to ticker-mapped corps — the
        ~112k entities that never had a ticker are not collection targets
        under either scope.
    """
    historical = scope is UniverseScope.HISTORICAL
    corps = storage.get_dart_corp_master(
        active_only=not historical,
        tickers=tickers,
        include_delisted=historical,
    )
    return [corp for corp in corps if corp.ticker]


def resolve_price_targets(
    storage: Storage,
    scope: UniverseScope,
    market: Market | None = None,
    tickers: list[str] | None = None,
) -> list[Stock]:
    """Return the stocks a price collection should target.

    Args:
        storage: Storage to read the stock master from.
        scope: ``CURRENT`` for the active universe, ``HISTORICAL`` for every
            row in the stock master regardless of listing status.
        market: Optional market filter.
        tickers: Optional ticker allowlist.  Under ``HISTORICAL`` this is
            applied in the query; under ``CURRENT`` it filters the active
            result, so naming a delisted ticker there returns nothing — which
            is the behaviour that made delisted names unreachable.

    Returns:
        Stock-master rows.
    """
    if scope is UniverseScope.HISTORICAL:
        return storage.get_stocks(market=market, tickers=tickers)

    if tickers:
        wanted = set(tickers)
        return [s for s in storage.get_active_stocks(market) if s.ticker in wanted]
    return storage.get_active_stocks(market)

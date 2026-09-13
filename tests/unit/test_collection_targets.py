"""Unit tests for collection-target resolution (T group).

The last test in this file is the point of the module. Six services each
resolved their own targets and each independently reached for the
currently-listed set — six identical, invisible decisions, all wrong for a
historical backfill. Adding an opt-in flag per service does not fix that; the
next collector defaults the same way.

So the enforcement is a test that fails when a service goes around the shared
resolver, in the same spirit as ``test_catalog_covers_all_pipeline_tables``.
"""

from __future__ import annotations

import pathlib

from collector.kr.domain.enums import ListingStatus, Market, Source, UniverseScope
from collector.kr.domain.models import DartCorp, Stock
from collector.kr.service.collection_targets import (
    resolve_dart_targets,
    resolve_price_targets,
)
from collector.kr.util.time import now_kst

LIVE, GONE = "005930", "058530"


def _corp(ticker: str | None, corp_code: str) -> DartCorp:
    return DartCorp(
        corp_code=corp_code,
        corp_name=f"corp-{corp_code}",
        ticker=ticker,
        market=Market.KOSPI if ticker else None,
        stock_name=None,
        modify_date=None,
        is_active=ticker == LIVE,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def _stock(ticker: str, status: ListingStatus) -> Stock:
    return Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name=ticker,
        status=status,
        last_seen_date=None,
        source=Source.PYKRX,
    )


class FakeStorage:
    def __init__(self) -> None:
        self.dart_calls: list[dict] = []
        self.stock_calls: list[dict] = []
        self.active_calls: list[Market | None] = []

    def get_dart_corp_master(
        self, active_only=True, tickers=None, include_delisted=False
    ):  # noqa: ANN001
        self.dart_calls.append(
            {
                "active_only": active_only,
                "tickers": tickers,
                "include_delisted": include_delisted,
            }
        )
        rows = [_corp(LIVE, "1"), _corp(GONE, "2"), _corp(None, "3")]
        if not include_delisted and active_only:
            rows = [c for c in rows if c.is_active]
        if tickers:
            rows = [c for c in rows if c.ticker in set(tickers)]
        return rows

    def get_stocks(self, market=None, statuses=None, tickers=None):  # noqa: ANN001
        self.stock_calls.append({"market": market, "statuses": statuses, "tickers": tickers})
        rows = [_stock(LIVE, ListingStatus.ACTIVE), _stock(GONE, ListingStatus.DELISTED)]
        if tickers:
            rows = [s for s in rows if s.ticker in set(tickers)]
        return rows

    def get_active_stocks(self, market=None):  # noqa: ANN001
        self.active_calls.append(market)
        return [_stock(LIVE, ListingStatus.ACTIVE)]


def test_dart_current_scope_asks_for_active_only() -> None:
    storage = FakeStorage()

    corps = resolve_dart_targets(storage, UniverseScope.CURRENT)

    assert storage.dart_calls == [{"active_only": True, "tickers": None, "include_delisted": False}]
    assert [c.ticker for c in corps] == [LIVE]


def test_dart_historical_scope_reaches_delisted_corps() -> None:
    storage = FakeStorage()

    corps = resolve_dart_targets(storage, UniverseScope.HISTORICAL)

    assert storage.dart_calls == [{"active_only": False, "tickers": None, "include_delisted": True}]
    assert {c.ticker for c in corps} == {LIVE, GONE}


def test_dart_targets_never_include_corps_without_a_ticker() -> None:
    # ~112k entities in the corp master never had a ticker. They are not
    # collection targets under either scope; including them would multiply the
    # request count for rows no feature reads.
    storage = FakeStorage()

    for scope in (UniverseScope.CURRENT, UniverseScope.HISTORICAL):
        corps = resolve_dart_targets(storage, scope)
        assert all(c.ticker for c in corps), scope


def test_price_current_scope_uses_the_active_universe() -> None:
    storage = FakeStorage()

    stocks = resolve_price_targets(storage, UniverseScope.CURRENT, Market.KOSPI)

    assert storage.active_calls == [Market.KOSPI]
    assert storage.stock_calls == []
    assert [s.ticker for s in stocks] == [LIVE]


def test_price_current_scope_cannot_reach_a_delisted_ticker_by_name() -> None:
    # This is the behaviour that made delisted names unreachable: the allowlist
    # filters an active-only result, so naming one returns nothing.
    storage = FakeStorage()

    stocks = resolve_price_targets(storage, UniverseScope.CURRENT, tickers=[GONE])

    assert stocks == []


def test_price_historical_scope_applies_the_allowlist_in_the_query() -> None:
    storage = FakeStorage()

    stocks = resolve_price_targets(storage, UniverseScope.HISTORICAL, tickers=[GONE])

    assert storage.active_calls == []
    assert storage.stock_calls == [{"market": None, "statuses": None, "tickers": [GONE]}]
    assert [s.ticker for s in stocks] == [GONE]


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------

_SERVICE_DIR = pathlib.Path(__file__).resolve().parents[2] / "src" / "collector" / "kr" / "service"

# Three services legitimately touch the universe accessors:
#   collection_targets — the resolver itself
#   sync_universe      — computes the universe; its snapshot diff is how
#                        delistings are detected at all
#   sync_dart_corp     — sets is_active on the corp master by matching against
#                        the active universe, i.e. it DEFINES that flag
_ALLOWED = {"collection_targets.py", "sync_universe.py", "sync_dart_corp.py"}

# The two universe accessors. Narrow on purpose: `active_only=` also appears on
# get_common_feature_series, which is about series being enabled and has
# nothing to do with the stock universe.
_BYPASS_MARKERS = ("get_active_stocks(", "get_dart_corp_master(")


def test_no_service_resolves_collection_targets_on_its_own() -> None:
    """Fail when a service reaches for a universe accessor directly.

    Six services once did, identically, and the resulting 2% coverage of
    delisted names went unnoticed for the life of the pipeline. A new collector
    must go through ``collection_targets`` so the choice of universe is made in
    one place and is visible at the call site.
    """
    offenders: list[str] = []
    for path in sorted(_SERVICE_DIR.rglob("*.py")):
        if path.name in _ALLOWED:
            continue
        source = path.read_text(encoding="utf-8")
        for marker in _BYPASS_MARKERS:
            if marker in source:
                offenders.append(f"{path.name}: {marker}")

    assert offenders == [], (
        "these services resolve targets directly; use "
        "service.collection_targets.resolve_dart_targets / resolve_price_targets "
        f"instead: {offenders}"
    )

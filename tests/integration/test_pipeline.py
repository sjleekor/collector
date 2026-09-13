"""End-to-End Integration tests for the KRX pipeline.

These tests use the real PostgreSQL database but mock the external
data providers (FDR, pykrx) to avoid network flakiness and rate limits.
"""

import uuid
from datetime import date

import pytest

from collector.kr.domain.enums import ListingStatus, Market, Source
from collector.kr.domain.models import (
    DailyBar,
    DailyPriceResult,
    Stock,
    StockUniverseSnapshot,
    UniverseResult,
)
from collector.kr.infra.config.settings import get_settings
from collector.kr.infra.db_postgres.repositories import PostgresStorage
from collector.kr.service.backfill_daily import backfill_daily_prices
from collector.kr.service.sync_universe import sync_universe
from collector.kr.service.validate import validate
from collector.kr.util.time import now_kst


class MockUniverseProvider:
    def fetch_universe(self, markets: list[Market], as_of: date | None = None) -> UniverseResult:
        records = [
            Stock(
                ticker="000001",
                market=Market.KOSPI,
                name="Mock Corp A",
                status=ListingStatus.ACTIVE,
                last_seen_date=as_of or date.today(),
                source=Source.FDR,
            ),
            Stock(
                ticker="000002",
                market=Market.KOSDAQ,
                name="Mock Corp B",
                status=ListingStatus.ACTIVE,
                last_seen_date=as_of or date.today(),
                source=Source.FDR,
            ),
        ]
        snapshot = StockUniverseSnapshot(
            snapshot_id=str(uuid.uuid4()),
            as_of_date=as_of or date.today(),
            source=Source.FDR,
            fetched_at=now_kst(),
            records=records,
        )
        return UniverseResult(snapshot=snapshot)


class MockPriceProvider:
    def fetch_daily_ohlcv(
        self, ticker: str, market: Market, start: date, end: date
    ) -> DailyPriceResult:
        bars = [
            DailyBar(
                ticker=ticker,
                market=market,
                trade_date=start,
                open=1000,
                high=1100,
                low=900,
                close=1050,
                volume=5000,
                source=Source.PYKRX,
                fetched_at=now_kst(),
            )
        ]
        return DailyPriceResult(ticker=ticker, bars=bars)


@pytest.fixture(scope="session")
def storage() -> PostgresStorage:
    settings = get_settings()
    store = PostgresStorage(settings.db_dsn)
    try:
        store.init_schema()
    except Exception as e:
        pytest.skip(f"Could not initialize DB: {e}")
    return store


@pytest.fixture(autouse=True)
def clean_db(storage: PostgresStorage) -> None:
    """Clean the tables before and after the tests."""
    from collector.kr.infra.db_postgres.connection import get_connection

    def _truncate() -> None:
        with get_connection(storage._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE TABLE daily_ohlcv, stock_master, "
                    "stock_master_snapshot_items, stock_master_snapshot, "
                    "ingestion_runs CASCADE;"
                )

    _truncate()
    yield
    _truncate()


def test_end_to_end_pipeline(storage: PostgresStorage) -> None:
    """Test the full pipeline: sync universe -> backfill prices -> validate."""
    test_date = date(2024, 1, 10)

    # 1. Sync Universe
    universe_provider = MockUniverseProvider()
    sync_result = sync_universe(
        provider=universe_provider,
        storage=storage,
        markets=[Market.KOSPI, Market.KOSDAQ],
        as_of=test_date,
    )
    assert sync_result.error is None
    assert sync_result.upsert.updated > 0

    # Verify stock is in DB
    active_stocks = storage.get_active_stocks()
    assert len(active_stocks) == 2
    assert active_stocks[0].ticker in ("000001", "000002")

    # 2. Backfill Daily Prices
    price_provider = MockPriceProvider()
    backfill_result = backfill_daily_prices(
        provider=price_provider,
        storage=storage,
        market=Market.KOSPI,  # Only backfill KOSPI
        start=test_date,
        end=test_date,
        rate_limit_seconds=0.0,
    )

    assert len(backfill_result.errors) == 0
    assert backfill_result.tickers_processed == 1  # Only 1 KOSPI stock mock
    assert backfill_result.bars_upserted == 1

    # Verify price is in DB
    bars = storage.get_daily_bars(target_date=test_date, market=Market.KOSPI)
    assert len(bars) == 1
    assert bars[0].ticker == "000001"
    assert bars[0].close == 1050

    # 3. Validate
    # Running validation should log missing days for the KOSDAQ stock (since we didn't backfill it)
    # but the KOSPI stock should pass.
    # Validation service doesn't return anything, but it shouldn't crash.
    validate(storage=storage, market=Market.KOSPI, target_date=test_date)


def _snapshot(records: list[Stock], as_of: date) -> StockUniverseSnapshot:
    return StockUniverseSnapshot(
        snapshot_id=str(uuid.uuid4()),
        as_of_date=as_of,
        source=Source.FDR,
        fetched_at=now_kst(),
        records=records,
    )


def test_first_seen_and_listing_dates_persist_and_are_preserved(
    storage: PostgresStorage,
) -> None:
    """T14: first_seen_date is set once and preserved; a NULL-listing sync does
    not erase an existing listing_date."""
    first_as_of = date(2026, 6, 1)
    ticker = "111111"

    # First sync (FDR) with an explicit listing date.
    fdr_stock = Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name="First Corp",
        status=ListingStatus.ACTIVE,
        last_seen_date=first_as_of,
        source=Source.FDR,
        listing_date=date(2026, 5, 20),
    )
    storage.upsert_stock_master([fdr_stock], _snapshot([fdr_stock], first_as_of))

    stored = {s.ticker: s for s in storage.get_active_stocks()}[ticker]
    assert stored.listing_date == date(2026, 5, 20)
    assert stored.first_seen_date == first_as_of

    # Second sync (pykrx) later, with NO listing date -> must not erase it, and
    # first_seen_date must stay at the original value.
    second_as_of = date(2026, 6, 10)
    pykrx_stock = Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name="First Corp",
        status=ListingStatus.ACTIVE,
        last_seen_date=second_as_of,
        source=Source.PYKRX,
        listing_date=None,
    )
    storage.upsert_stock_master([pykrx_stock], _snapshot([pykrx_stock], second_as_of))

    stored = {s.ticker: s for s in storage.get_active_stocks()}[ticker]
    assert stored.listing_date == date(2026, 5, 20)  # preserved
    assert stored.first_seen_date == first_as_of  # preserved
    assert stored.last_seen_date == second_as_of  # updated


def test_get_active_stocks_round_trips_dates_and_snapshot_items(
    storage: PostgresStorage,
) -> None:
    """T15: get_active_stocks returns both dates; snapshot item rows persist
    listing_date."""
    as_of = date(2026, 6, 15)
    ticker = "222222"
    stock = Stock(
        ticker=ticker,
        market=Market.KOSDAQ,
        name="Round Trip Corp",
        status=ListingStatus.ACTIVE,
        last_seen_date=as_of,
        source=Source.FDR,
        listing_date=date(2026, 6, 12),
    )
    snapshot = _snapshot([stock], as_of)
    storage.upsert_stock_master([stock], snapshot)

    stored = {s.ticker: s for s in storage.get_active_stocks()}[ticker]
    assert stored.listing_date == date(2026, 6, 12)
    assert stored.first_seen_date == as_of

    from collector.kr.infra.db_postgres.connection import get_connection

    with get_connection(storage._dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT listing_date FROM stock_master_snapshot_items "
                "WHERE snapshot_id = %s AND ticker = %s AND market = %s",
                (snapshot.snapshot_id, ticker, Market.KOSDAQ.value),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == date(2026, 6, 12)

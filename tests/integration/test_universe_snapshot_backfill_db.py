"""Integration checks for the universe-snapshot backfill writer (N3).

Skipped automatically when PostgreSQL is unreachable.

``insert_stock_master_snapshot_only`` exists because ``upsert_stock_master``
also writes ``stock_master``, and a reconstructed historical snapshot must not
rewrite the current universe.  That guarantee lives in SQL, so it is verified
against a real database rather than a stub: the test seeds a ``stock_master``
row, writes a past-dated snapshot whose contents contradict it, and asserts the
master row is untouched.
"""

from __future__ import annotations

import uuid
from datetime import date

import pytest

from collector.kr.domain.enums import ListingStatus, Market, Source
from collector.kr.domain.models import Stock, StockUniverseSnapshot
from collector.kr.infra.config.settings import get_settings
from collector.kr.infra.db_postgres.connection import get_connection
from collector.kr.infra.db_postgres.repositories import PostgresStorage
from collector.kr.util.time import now_kst

AS_OF = date(2016, 6, 30)
LIVE_TICKER = "TST001"
BACKFILL_TICKER = "TST002"


@pytest.fixture()
def storage() -> PostgresStorage:
    dsn = get_settings().db_dsn
    try:
        with get_connection(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.stock_master_snapshot')")
                if cur.fetchone()[0] is None:
                    pytest.skip("Schema not initialised; run `db init` first.")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Database not reachable: {exc}")
    return PostgresStorage(dsn)


@pytest.fixture(autouse=True)
def _cleanup(storage: PostgresStorage):
    def purge() -> None:
        with get_connection(get_settings().db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM stock_master_snapshot_items
                    WHERE snapshot_id IN (
                        SELECT snapshot_id FROM stock_master_snapshot
                        WHERE source = %s AND as_of_date = %s
                    )
                    """,
                    (Source.PYKRX_BACKFILL.value, AS_OF),
                )
                cur.execute(
                    "DELETE FROM stock_master_snapshot WHERE source = %s AND as_of_date = %s",
                    (Source.PYKRX_BACKFILL.value, AS_OF),
                )
                cur.execute(
                    "DELETE FROM stock_master WHERE ticker IN (%s, %s)",
                    (LIVE_TICKER, BACKFILL_TICKER),
                )

    purge()
    yield
    purge()


def _snapshot(tickers: list[str]) -> StockUniverseSnapshot:
    return StockUniverseSnapshot(
        snapshot_id=str(uuid.uuid4()),
        as_of_date=AS_OF,
        source=Source.PYKRX_BACKFILL,
        fetched_at=now_kst(),
        records=[
            Stock(
                ticker=t,
                market=Market.KOSPI,
                name=f"historical-{t}",
                status=ListingStatus.ACTIVE,
                last_seen_date=AS_OF,
                source=Source.PYKRX_BACKFILL,
            )
            for t in tickers
        ],
    )


def _master_rows() -> dict[str, tuple]:
    with get_connection(get_settings().db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, name, status, last_seen_date
                FROM stock_master WHERE ticker IN (%s, %s)
                """,
                (LIVE_TICKER, BACKFILL_TICKER),
            )
            return {row[0]: row for row in cur.fetchall()}


def _seed_master() -> None:
    with get_connection(get_settings().db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_master
                    (ticker, market, name, status, last_seen_date, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    LIVE_TICKER,
                    Market.KOSPI.value,
                    "current-name",
                    ListingStatus.ACTIVE.value,
                    date(2026, 8, 15),
                    Source.PYKRX.value,
                ),
            )


def test_snapshot_write_leaves_stock_master_untouched(storage: PostgresStorage) -> None:
    _seed_master()
    before = _master_rows()

    # The snapshot contradicts stock_master on every field that
    # upsert_stock_master would have overwritten: a different name for the live
    # ticker, an older last_seen_date, and a ticker that no longer exists.
    result = storage.insert_stock_master_snapshot_only(_snapshot([LIVE_TICKER, BACKFILL_TICKER]))

    assert result.inserted == 2
    after = _master_rows()
    assert after == before
    # A historical-only ticker must not be created in the current universe.
    assert BACKFILL_TICKER not in after


def test_snapshot_write_is_idempotent_on_as_of_and_source(storage: PostgresStorage) -> None:
    first = storage.insert_stock_master_snapshot_only(_snapshot([LIVE_TICKER]))
    second = storage.insert_stock_master_snapshot_only(_snapshot([LIVE_TICKER]))

    assert first.inserted == 1
    assert second.inserted == 0

    with get_connection(get_settings().db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM stock_master_snapshot WHERE source = %s AND as_of_date = %s",
                (Source.PYKRX_BACKFILL.value, AS_OF),
            )
            assert cur.fetchone()[0] == 1


def test_existing_snapshot_dates_is_scoped_by_source(storage: PostgresStorage) -> None:
    storage.insert_stock_master_snapshot_only(_snapshot([LIVE_TICKER]))

    backfilled = storage.get_existing_snapshot_dates(Source.PYKRX_BACKFILL)
    live = storage.get_existing_snapshot_dates(Source.PYKRX)

    assert AS_OF in backfilled
    assert AS_OF not in live

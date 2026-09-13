"""Use-case: Sync the stock universe (stock master).

Responsibilities:
    1. Fetch the current universe from a ``UniverseProvider``.
    2. Compare with existing stock_master to compute diffs:
       - New tickers (not previously seen).
       - Delisted tickers (previously active, now absent).
       - Name changes (same ticker, different name).
    3. Persist the snapshot and upsert stock_master via ``Storage``.
    4. Record the ingestion run for auditability.
"""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.domain.enums import ListingStatus, Market, RunStatus, RunType
from collector.kr.domain.models import IngestionRun, Stock, SyncResult
from collector.kr.ports.storage import Storage
from collector.kr.ports.universe import UniverseProvider
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


def sync_universe(
    provider: UniverseProvider,
    storage: Storage,
    markets: list[Market],
    as_of: date | None = None,
    full_refresh: bool = False,
) -> SyncResult:
    """Synchronise the stock universe from *provider* into *storage*.

    Args:
        provider: Universe data source (FDR or pykrx).
        storage: Persistence backend.
        markets: Market segments to sync.
        as_of: Reference date for the snapshot.  ``None`` → today (KST).
        full_refresh: If ``True``, replace all stock_master rows instead
            of computing an incremental diff.

    Returns:
        ``SyncResult`` with upsert counters and diff lists.
    """
    run = IngestionRun(
        run_type=RunType.UNIVERSE_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "markets": [m.value for m in markets],
            "as_of": str(as_of) if as_of else str(today_kst()),
            "full_refresh": full_refresh,
        },
    )
    storage.record_run(run)

    try:
        # 1. Fetch universe from provider
        result = provider.fetch_universe(markets, as_of)
        if result.error:
            raise RuntimeError(f"Provider failed: {result.error}")

        snapshot = result.snapshot
        if not snapshot:
            raise RuntimeError("Provider returned no snapshot.")

        logger.info("Fetched %d records from provider.", snapshot.record_count)

        # 2. Get existing active stocks from storage to compute diffs
        existing_stocks_map: dict[str, Stock] = {}
        if not full_refresh:
            for market in markets:
                active_stocks = storage.get_active_stocks(market)
                for s in active_stocks:
                    existing_stocks_map[s.ticker] = s

        # 3. Compute diffs
        new_tickers = []
        name_changes = []

        snapshot_tickers = set()
        for s in snapshot.records:
            snapshot_tickers.add(s.ticker)
            if s.ticker not in existing_stocks_map:
                new_tickers.append(s.ticker)
            else:
                existing_s = existing_stocks_map[s.ticker]
                if existing_s.name != s.name:
                    name_changes.append((s.ticker, existing_s.name, s.name))

        delisted_tickers = []
        if not full_refresh:
            for ticker in existing_stocks_map:
                if ticker not in snapshot_tickers:
                    delisted_tickers.append(ticker)

        logger.info(
            "Diffs - New: %d, Delisted: %d, Name Changes: %d",
            len(new_tickers),
            len(delisted_tickers),
            len(name_changes),
        )

        # 4. Prepare records to upsert
        # We need to explicitly mark delisted stocks
        upsert_records = list(snapshot.records)

        for ticker in delisted_tickers:
            old_s = existing_stocks_map[ticker]
            # Create a delisted version of the stock
            delisted_stock = Stock(
                ticker=old_s.ticker,
                market=old_s.market,
                name=old_s.name,
                status=ListingStatus.DELISTED,
                last_seen_date=old_s.last_seen_date,  # keep the last seen date
                source=old_s.source,
                listing_date=old_s.listing_date,
                first_seen_date=old_s.first_seen_date,
            )
            upsert_records.append(delisted_stock)

        # 5. Upsert via storage
        upsert_result = storage.upsert_stock_master(upsert_records, snapshot)

        # 6. Complete the run
        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "fetched": snapshot.record_count,
            "new_tickers": len(new_tickers),
            "delisted_tickers": len(delisted_tickers),
            "name_changes": len(name_changes),
            "upsert_inserted": upsert_result.inserted,
            "upsert_updated": upsert_result.updated,
            "upsert_errors": upsert_result.errors,
        }
        storage.record_run(run)

        return SyncResult(
            upsert=upsert_result,
            new_tickers=new_tickers,
            delisted_tickers=delisted_tickers,
        )

    except Exception as exc:
        logger.exception("Universe sync failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
        return SyncResult(error=str(exc))

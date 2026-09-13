"""Recover historical securities into ``stock_master`` from the PIT snapshots.

Why this exists
---------------
``UniverseScope.HISTORICAL`` promises "every row in the stock master regardless
of listing status", and it delivers exactly that — but the master itself only
knows what the collector has watched since it started running in 2026-04. Every
security delisted before then is simply absent.

Measured 2026-08-16: of the 3,959 corporations that ever carried a ticker,
**1,299 have no row in ``stock_master`` at all**, and on the first 24 reconstructed
month-end snapshots, 372 tickers appear that the master has never heard of — and
all 372 have zero rows in ``daily_ohlcv``. The two counts match exactly, which is
the whole point: **membership in ``stock_master`` is what decides price coverage.**

Absent from the master means unreachable, not merely un-fetched. Price collection
resolves its targets from that table, and naming a missing ticker through
``--tickers`` returns nothing, because the allowlist is filtered against the same
table. No flag reaches past it.

So the fix is not another scope or another flag; it is making the master contain
what it claims to contain. The N3 month-end snapshots are the point-in-time record
of what was listed, they come from the same source as the prices, and they carry
the market per ticker — so they are what the recovery reads.

What it does not do
-------------------
Nothing here is marked ``ACTIVE``. A ticker absent from ``stock_master`` was never
seen by a live universe sync, so it is not listed today, and it is written as
``DELISTED``. ``sync_universe`` computes its delisting diff from
``get_active_stocks``, so these rows are invisible to it and cannot disturb the
live universe.

Month-end snapshots also cannot see a security that listed and delisted inside a
single month. That is a known floor on recall, not a silent one.
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass, field

from collector.kr.domain.enums import ListingStatus, RunStatus, RunType, Source
from collector.kr.domain.models import IngestionRun
from collector.kr.ports.storage import Storage
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

# Snapshots written by the historical backfill. The live FDR snapshots are
# excluded because anything in them is already in the master by construction.
# Both backfill provenances count: the pykrx path stopped at 60/152 when KRX
# blocked this host, and the Open API path continues the same series.
DEFAULT_SNAPSHOT_SOURCES = (Source.PYKRX_BACKFILL, Source.KRX_OPENAPI_BACKFILL)


@dataclass
class StockMasterBackfillResult:
    """Outcome of one ``universe backfill-master`` run."""

    candidates: int = 0
    rows_upserted: int = 0
    dry_run: bool = False
    errors: dict[str, str] = field(default_factory=dict)


def backfill_stock_master_from_snapshots(
    storage: Storage,
    *,
    sources: list[Source] | None = None,
    dry_run: bool = False,
) -> StockMasterBackfillResult:
    """Insert snapshot-only securities into ``stock_master`` as ``DELISTED``.

    Args:
        storage: Target storage.
        sources: Snapshot sources to read.  Defaults to the historical backfill.
        dry_run: Report what would be written without writing it.

    Returns:
        Counts and any pipeline error.  Never raises.
    """
    resolved_sources = list(sources or DEFAULT_SNAPSHOT_SOURCES)
    result = StockMasterBackfillResult(dry_run=dry_run)

    run = IngestionRun(
        run_type=RunType.UNIVERSE_SNAPSHOT_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "sources": [source.value for source in resolved_sources],
            "dry_run": dry_run,
        },
    )

    try:
        candidates = storage.get_stocks_seen_only_in_snapshots(sources=resolved_sources)
        result.candidates = len(candidates)
        logger.info(
            "Found %d securities present in snapshots and absent from stock_master.",
            len(candidates),
        )

        if candidates and not dry_run:
            recovered = [
                # DELISTED, not UNKNOWN: absence from the master is positive
                # evidence that no live universe sync ever saw this ticker.
                dataclasses.replace(stock, status=ListingStatus.DELISTED)
                for stock in candidates
            ]
            result.rows_upserted = storage.upsert_stock_master_rows(recovered)

        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "candidates": result.candidates,
            "rows_upserted": result.rows_upserted,
        }
        storage.record_run(run)
        return result

    except Exception as exc:
        logger.exception("Stock-master backfill failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
        result.errors["pipeline"] = str(exc)
        return result

"""Use-case: Backfill historical stock-universe snapshots.

``stock_master`` holds only 28 DELISTED rows because ``sync_universe`` infers
delistings by diffing consecutive snapshots — so it can only see what happened
after snapshot collection began.  Everything delisted before that is simply
absent from the sample, which inflates any transition or distress feature
(``fin_turn_to_profit`` keeps the companies that recovered and drops the ones
that were delisted).

``get_market_ticker_list`` accepts a date, so the past listings can be
reconstructed.  Two things bound what this service claims:

**It never writes ``stock_master``.**  It calls
``insert_stock_master_snapshot_only``; a 2016 ticker list must not overwrite
who is listed today.

**It is an audit path, not the daily universe.**  Month-end resolution cannot
see a stock that listed and delisted inside one month, and misses an intra-month
listing by up to a month.  The daily PIT universe comes from ``daily_market_cap``
rows instead — the two ticker sets were verified identical at four sampled dates
(``04_w1_pit_universe.md`` 3.5).
"""

from __future__ import annotations

import calendar
import logging
from datetime import date

from collector.kr.domain.enums import Market, RunStatus, RunType, Source
from collector.kr.domain.models import IngestionRun, UniverseSnapshotBackfillResult
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.ports.storage import Storage
from collector.kr.ports.universe import UniverseProvider
from collector.kr.util.pipeline import (
    ConsecutiveFailureGuard,
    HumanThrottle,
    HumanThrottlePolicy,
    SourceBlockedError,
    build_run_counts,
    complete_run,
    fail_run,
)
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

DEFAULT_MARKETS: tuple[Market, ...] = (Market.KOSPI, Market.KOSDAQ)
DEFAULT_START = date(2014, 6, 1)

# Provenances that mean "this month-end has already been reconstructed".
# Kept separate from the live FDR/pykrx snapshot sources on purpose.
BACKFILL_SNAPSHOT_SOURCES: tuple[Source, ...] = (
    Source.PYKRX_BACKFILL,
    Source.KRX_OPENAPI_BACKFILL,
)


def month_end_trading_days(start: date, end: date) -> list[date]:
    """Return the last trading day of each month intersecting ``[start, end]``.

    A calendar month-end is often a weekend or a holiday, so the last actual
    session of the month is used.  Months with no session in range are skipped.
    """
    days: list[date] = []
    year, month = start.year, start.month

    while (year, month) <= (end.year, end.month):
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        window_start = max(start, date(year, month, 1))
        window_end = min(end, last_day)

        if window_start <= window_end:
            sessions = get_trading_days(window_start, window_end)
            if sessions:
                days.append(sessions[-1])

        month += 1
        if month > 12:
            year, month = year + 1, 1

    return days


def backfill_universe_snapshots(
    provider: UniverseProvider,
    storage: Storage,
    markets: list[Market] | None = None,
    start: date | None = None,
    end: date | None = None,
    throttle: HumanThrottle | None = None,
    force: bool = False,
    max_consecutive_failures: int = 5,
    snapshot_source: Source = Source.PYKRX_BACKFILL,
) -> UniverseSnapshotBackfillResult:
    """Backfill month-end universe snapshots from *provider* into *storage*.

    Args:
        provider: Universe provider that honours ``as_of``
            (``PykrxHistoricalUniverseProvider``).
        storage: Target storage.
        markets: Markets to include in each snapshot.  Defaults to KOSPI+KOSDAQ.
        start: First date to consider.  Defaults to ``DEFAULT_START``.
        end: Last date to consider.  Defaults to today (KST).
        throttle: KRX pacing policy.  ``None`` means no pacing, which is for
            tests; the CLI always supplies one built from the KRX settings.
        force: Re-fetch dates that already have a backfilled snapshot.

    Returns:
        ``UniverseSnapshotBackfillResult`` with per-snapshot counters.
    """
    resolved_markets = list(markets) if markets else list(DEFAULT_MARKETS)
    resolved_start = start or DEFAULT_START
    resolved_end = end or today_kst()

    run = IngestionRun(
        run_type=RunType.UNIVERSE_SNAPSHOT_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "markets": [m.value for m in resolved_markets],
            "start": str(resolved_start),
            "end": str(resolved_end),
            "force": force,
            "max_consecutive_failures": max_consecutive_failures,
            "source": snapshot_source.value,
        },
    )
    storage.record_run(run)

    result = UniverseSnapshotBackfillResult()
    pacer = throttle or HumanThrottle(HumanThrottlePolicy(), logger_instance=logger)
    guard = ConsecutiveFailureGuard(
        max_consecutive_failures,
        label="universe snapshot backfill",
        # The throttle already backs off 45-180s after each error, so the guard
        # only needs to decide when to stop, not how long to wait.
        backoff_seconds=0.0,
        logger_instance=logger,
    )

    try:
        if resolved_start > resolved_end:
            raise ValueError(f"start ({resolved_start}) must be <= end ({resolved_end})")

        targets = month_end_trading_days(resolved_start, resolved_end)
        # Skip on *any* backfill provenance, not just this run's. The pykrx
        # path stopped at 60/152 when KRX blocked this host and the Open API
        # path resumes the same series; scoping the skip to one source would
        # re-collect those 60 month-ends under a second provenance and leave
        # two snapshots per date for the master backfill to reconcile.
        existing: set[date] = set()
        if not force:
            for source in BACKFILL_SNAPSHOT_SOURCES:
                existing |= storage.get_existing_snapshot_dates(source)
        logger.info(
            "Universe snapshot backfill %s..%s: %d month-ends (%d already stored)",
            resolved_start,
            resolved_end,
            len(targets),
            len(existing),
        )

        for as_of in targets:
            result.snapshots_attempted += 1

            if as_of in existing:
                result.snapshots_skipped += 1
                continue

            label = as_of.isoformat()
            pacer.before_request(label)
            fetch = provider.fetch_universe(markets=resolved_markets, as_of=as_of)
            pacer.after_request()

            if fetch.error or fetch.snapshot is None:
                message = fetch.error or "provider returned no snapshot"
                result.errors[label] = message
                logger.warning("Snapshot %s failed: %s", as_of, message)
                pacer.backoff_after_error(label)
                guard.record_failure(f"{as_of}: {message}")
                continue

            snapshot = fetch.snapshot
            if not snapshot.records:
                result.errors[label] = "provider returned an empty universe"
                logger.warning("Snapshot %s returned no records", as_of)
                # An empty universe on a trading day is a refusal wearing an
                # ordinary face, so it backs off like any other error.
                pacer.backoff_after_error(label)
                guard.record_failure(f"{as_of}: empty universe")
                continue

            upsert = storage.insert_stock_master_snapshot_only(snapshot)

            result.snapshots_written += 1
            result.items_written += upsert.inserted
            logger.info("Snapshot %s stored: %d tickers", as_of, snapshot.record_count)
            guard.record_success()

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                snapshots_attempted=result.snapshots_attempted,
                snapshots_skipped=result.snapshots_skipped,
                snapshots_written=result.snapshots_written,
                items_written=result.items_written,
            ),
            errors=result.errors,
            partial_subject="universe snapshots",
        )
        return result

    except SourceBlockedError as exc:
        logger.error("Universe snapshot backfill stopped: %s", exc)
        fail_run(storage, run, exc)
        result.errors["source_blocked"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("Universe snapshot backfill failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

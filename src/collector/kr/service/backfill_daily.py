"""Use-case: Backfill daily OHLCV prices.

Responsibilities:
    1. Determine the set of tickers to process (all active, or a subset).
    2. For each ticker, determine the start date. Non-incremental runs default
       to an early date (2000-01-01) clamped up to the earliest stored bar when
       no explicit ``--start`` is given; incremental runs start after
       ``MAX(trade_date)``, or — for baseline-missing tickers — resolve a bounded
       start from ``--new-ticker-start`` / ``listing_date`` / ``first_seen_date``.
    3. Chunk the date range into manageable batches to avoid memory issues
       and enable resume/checkpointing.
    4. Fetch daily bars from ``PriceProvider`` with rate limiting.
    5. Idempotent upsert via ``Storage.upsert_daily_bars``.
    6. Record the ingestion run for auditability.

Resume / checkpointing design (not yet implemented):
    - Each ticker+date-range chunk writes to ``ingestion_runs`` on completion.
    - On restart, the service queries the last successful chunk and resumes
      from the next date.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import date, timedelta

from collector.kr.domain.enums import Market, RunStatus, RunType, UniverseScope
from collector.kr.domain.models import BackfillResult, DailyPriceResult, IngestionRun, Stock
from collector.kr.ports.prices import PriceProvider
from collector.kr.ports.storage import Storage
from collector.kr.service.collection_targets import resolve_price_targets
from collector.kr.util.pipeline import ConsecutiveFailureGuard, SourceBlockedError
from collector.kr.util.retry import retry
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


def _resolve_new_ticker_incremental_start(
    stock: Stock,
    resolved_end: date,
    new_ticker_start: date | None,
    max_auto_range_days: int | None,
) -> tuple[date | None, str | None, bool]:
    """Resolve an incremental start for a baseline-missing ticker.

    Returns (resolved_start, source_label, was_clamped):
      - source_label ∈ {"new_ticker_start", "listing_date", "first_seen_date"}
        or None when no start could be derived (caller records baseline_missing).
      - was_clamped is True only when an auto-derived start (listing_date /
        first_seen_date) was raised to the guard window.
      - new_ticker_start is used verbatim and never clamped.
    """
    if new_ticker_start is not None:
        return new_ticker_start, "new_ticker_start", False

    auto_start: date | None = stock.listing_date or stock.first_seen_date
    if auto_start is None:
        return None, None, False
    source = "listing_date" if stock.listing_date else "first_seen_date"

    if max_auto_range_days is None:
        return auto_start, source, False

    guard_start = resolved_end - timedelta(days=max_auto_range_days - 1)
    if auto_start < guard_start:
        return guard_start, source, True
    return auto_start, source, False


def backfill_daily_prices(
    provider: PriceProvider,
    storage: Storage,
    market: Market | None = None,
    tickers: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    rate_limit_seconds: float = 0.2,
    long_rest_interval: int = 100,
    long_rest_seconds: float = 10.0,
    incremental: bool = False,
    lookback_days: int = 0,
    max_auto_range_days: int | None = None,
    new_ticker_start: date | None = None,
    allow_new_ticker_backfill: bool = False,
    allow_large_range: bool = False,
    refetch: bool = False,
    scope: UniverseScope = UniverseScope.CURRENT,
    max_consecutive_failures: int = 5,
) -> BackfillResult:
    """Backfill daily OHLCV bars from *provider* into *storage*.

    Args:
        refetch: If ``True``, ignore what is already stored and fetch the whole
            resolved range again, overwriting it.  Gap detection can only fill
            holes, so without this a stored row is never corrected — and naver's
            adjusted series is restated retroactively on every split, which
            leaves a spurious return at the split date for any ticker whose
            history was backfilled before it
            (``poc/n1_adjusted_price_vintage.md``: 279 such jumps across 252
            tickers in four months).  Mutually exclusive with ``incremental``.
        max_consecutive_failures: Stop the run after this many tickers fail in a
            row (0 disables).  Without it a blocked source is met with the whole
            target list; see ``ConsecutiveFailureGuard``.
        scope: Which universe to target.  ``HISTORICAL`` is required to reach
            the delisted names at all — under ``CURRENT`` even naming one in
            ``tickers`` returns nothing, because the allowlist filters an
            active-only result (``poc/survivorship_gap.md``).
        incremental: If ``True``, skip per-day gap detection and instead
            fetch a single contiguous range starting from
            ``MAX(trade_date) + 1`` for each ticker. This trusts that
            historical data is already complete and is intended for
            fast daily catch-up runs. Tickers with no stored baseline
            resolve a start via ``_resolve_new_ticker_incremental_start``
            (``--new-ticker-start`` verbatim, else ``listing_date`` /
            ``first_seen_date`` clamped to the ``max_auto_range_days``
            window); a ticker with no derivable start records a
            baseline-missing error.
    """
    run = IngestionRun(
        run_type=RunType.DAILY_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "market": market.value if market else None,
            "tickers": tickers,
            "start": str(start) if start else None,
            "end": str(end) if end else None,
            "rate_limit": rate_limit_seconds,
            "long_rest_interval": long_rest_interval,
            "long_rest_seconds": long_rest_seconds,
            "incremental": incremental,
            "lookback_days": lookback_days,
            "max_auto_range_days": max_auto_range_days,
            "new_ticker_start": str(new_ticker_start) if new_ticker_start else None,
            "allow_new_ticker_backfill": allow_new_ticker_backfill,
            "allow_large_range": allow_large_range,
            "refetch": refetch,
            "universe_scope": scope.value,
            "max_consecutive_failures": max_consecutive_failures,
        },
    )
    storage.record_run(run)

    result = BackfillResult()
    guard = ConsecutiveFailureGuard(
        max_consecutive_failures,
        label="daily price backfill",
        # Scales with the configured pace; 0 when throttling is off, so
        # "no throttle" does not silently become "back off 60s".
        backoff_seconds=rate_limit_seconds * 10,
        logger_instance=logger,
    )
    api_requests_count = 0
    no_work_tickers = 0
    baseline_missing_tickers = 0
    range_too_large_tickers = 0

    @retry(max_attempts=4, base_delay=0.5, backoff_factor=2.0)
    def _fetch_with_retry(t: str, m: Market, s: date, e: date) -> DailyPriceResult:
        res = provider.fetch_daily_ohlcv(ticker=t, market=m, start=s, end=e)
        if res.error:
            # Raise an exception so that the @retry decorator can catch it and backoff.
            raise RuntimeError(res.error)
        return res

    try:
        if lookback_days < 0:
            raise ValueError("lookback_days must be >= 0")
        if refetch and incremental:
            # incremental starts after MAX(trade_date) and would re-fetch
            # nothing; silently doing that would look like a successful repair.
            raise ValueError("refetch cannot be combined with incremental")
        if max_auto_range_days is not None and max_auto_range_days <= 0:
            raise ValueError("max_auto_range_days must be positive")

        # 1. Resolve ticker list — through the shared resolver, never by
        #    reaching for an accessor directly (service/collection_targets.py).
        target_stocks: list[Stock] = resolve_price_targets(storage, scope, market, tickers)
        if not target_stocks and tickers:
            logger.warning("None of the provided tickers matched under scope=%s.", scope.value)

        if not target_stocks:
            logger.info("No active stocks found to backfill.")
            run.ended_at = now_kst()
            run.status = RunStatus.SUCCESS
            storage.record_run(run)
            return result

        # 2. Resolve end date
        resolved_end = end or today_kst()

        # 3. For each ticker
        for stock in target_stocks:
            ticker = stock.ticker
            result.tickers_processed += 1

            # Determine start date
            resolved_start = start or date(2000, 1, 1)  # arbitrary early date for pykrx

            if incremental:
                # Incremental mode: start strictly after the last stored
                # trade date. Skips gap detection entirely — trusts that
                # historical data is already complete.
                max_stored = storage.get_max_trade_date(ticker)
                if max_stored:
                    next_date = max_stored + timedelta(days=1)
                    if lookback_days > 0:
                        next_date = min(next_date, resolved_end - timedelta(days=lookback_days))
                    if next_date > resolved_start:
                        logger.debug(
                            "Incremental: %s starts at %s (after last stored %s)",
                            ticker,
                            next_date,
                            max_stored,
                        )
                        resolved_start = next_date
                elif start is None and not allow_new_ticker_backfill:
                    auto_start, source_label, was_clamped = _resolve_new_ticker_incremental_start(
                        stock,
                        resolved_end,
                        new_ticker_start,
                        max_auto_range_days,
                    )
                    if auto_start is None:
                        baseline_missing_tickers += 1
                        result.errors[ticker] = (
                            "No stored daily_ohlcv baseline for incremental backfill. "
                            "Run explicit backfill, pass --new-ticker-start, or populate "
                            "stock_master listing_date/first_seen_date."
                        )
                        logger.warning("Skipping %s: %s", ticker, result.errors[ticker])
                        continue
                    if source_label in ("listing_date", "first_seen_date"):
                        result.auto_new_ticker_start_tickers += 1
                    if was_clamped:
                        result.baseline_clamped_tickers += 1
                        logger.warning(
                            "Clamped %s: %s=%s -> %s (window=%dd); run full backfill " "separately",
                            ticker,
                            source_label,
                            stock.listing_date or stock.first_seen_date,
                            auto_start,
                            max_auto_range_days,
                        )
                    resolved_start = auto_start
            else:
                # Clamp the *default* early start up to the ticker's earliest stored
                # trade date to avoid re-requesting pre-listing ranges the provider
                # never returns. Only when start is None: an explicit --start is an
                # operator decision and must be honored so full-history repair can
                # reach before the earliest stored bar.
                if start is None:
                    min_stored = storage.get_min_trade_date(ticker)
                    if min_stored and min_stored > resolved_start:
                        logger.debug(
                            "Clamping start for %s from %s to %s (earliest stored trade date)",
                            ticker,
                            resolved_start,
                            min_stored,
                        )
                        resolved_start = min_stored

            if resolved_start > resolved_end:
                logger.info(
                    "Nothing to fetch for %s (start=%s > end=%s). Skipping.",
                    ticker,
                    resolved_start,
                    resolved_end,
                )
                no_work_tickers += 1
                continue

            auto_range_days = (resolved_end - resolved_start).days + 1
            if (
                incremental
                and max_auto_range_days is not None
                and auto_range_days > max_auto_range_days
                and not allow_large_range
            ):
                range_too_large_tickers += 1
                result.errors[ticker] = (
                    f"Resolved incremental range is too large "
                    f"({auto_range_days} days > {max_auto_range_days})."
                )
                logger.warning("Skipping %s: %s", ticker, result.errors[ticker])
                continue
            try:
                ranges: list[tuple[date, date]] = []
                if incremental or refetch:
                    # Single contiguous range from resolved_start to resolved_end.
                    # refetch takes the same shape as incremental but for the
                    # opposite reason: incremental trusts stored history and
                    # skips it, refetch distrusts it and overwrites it.
                    ranges.append((resolved_start, resolved_end))
                else:
                    # 1. Query missing days to optimize fetching
                    missing_days = storage.query_missing_days(ticker, resolved_start, resolved_end)

                    if not missing_days:
                        logger.debug("No missing days for %s. Skipping.", ticker)
                        continue

                    # 2. Group missing days into continuous date ranges
                    current_range_start = missing_days[0]
                    current_range_end = missing_days[0]

                    for d in missing_days[1:]:
                        if d == current_range_end + timedelta(days=1):
                            current_range_end = d
                        else:
                            ranges.append((current_range_start, current_range_end))
                            current_range_start = d
                            current_range_end = d
                    ranges.append((current_range_start, current_range_end))

                # 3. Fetch and upsert for each range
                for r_start, r_end in ranges:
                    current_start = r_start
                    while current_start <= r_end:
                        # Chunk by 1 year to avoid overloading the pykrx API
                        current_end = min(current_start + timedelta(days=365), r_end)

                        logger.info(
                            "Backfilling %s from %s to %s", ticker, current_start, current_end
                        )

                        try:
                            fetch_res = _fetch_with_retry(
                                ticker, stock.market, current_start, current_end
                            )
                        except Exception as e:
                            fetch_res = DailyPriceResult(ticker=ticker, error=str(e))

                        api_requests_count += 1

                        if long_rest_interval > 0 and api_requests_count % long_rest_interval == 0:
                            logger.info(
                                "Reached %d requests. Taking a long rest for %.1f seconds...",
                                api_requests_count,
                                long_rest_seconds,
                            )
                            time.sleep(long_rest_seconds)

                        if fetch_res.error:
                            result.errors[ticker] = fetch_res.error
                            guard.record_failure(f"{ticker}: {fetch_res.error}")
                            break

                        if fetch_res.bars:
                            upsert_res = storage.upsert_daily_bars(fetch_res.bars)
                            result.bars_upserted += upsert_res.updated

                        # Rate limiting with jitter (+/- 20%)
                        if rate_limit_seconds > 0:
                            jitter = random.uniform(-0.2, 0.2) * rate_limit_seconds
                            time.sleep(max(0.0, rate_limit_seconds + jitter))

                        current_start = current_end + timedelta(days=1)

                    if ticker in result.errors:
                        break

                if ticker not in result.errors:
                    guard.record_success()

            except SourceBlockedError:
                raise
            except Exception as exc:
                logger.exception("Error backfilling ticker %s", ticker)
                result.errors[ticker] = str(exc)
                guard.record_failure(f"{ticker}: {exc}")

        # 4. Record IngestionRun
        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS if not result.errors else RunStatus.FAILED
        run.counts = {
            "tickers_processed": result.tickers_processed,
            "bars_upserted": result.bars_upserted,
            "no_work_tickers": no_work_tickers,
            "baseline_missing_tickers": baseline_missing_tickers,
            "baseline_clamped_tickers": result.baseline_clamped_tickers,
            "auto_new_ticker_start_tickers": result.auto_new_ticker_start_tickers,
            "range_too_large_tickers": range_too_large_tickers,
            "error_count": len(result.errors),
        }
        if result.errors:
            run.error_summary = f"{len(result.errors)} tickers had errors."

        storage.record_run(run)
        return result

    except SourceBlockedError as exc:
        # Not a pipeline bug: the source stopped answering.
        logger.error("Backfill stopped: %s", exc)
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        result.errors["source_blocked"] = str(exc)
        storage.record_run(run)
        return result
    except Exception as exc:
        logger.exception("Backfill pipeline failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        # Surface the failure in the result too. Without this the run is FAILED
        # in ingestion_runs while the CLI prints a success line and exits 0,
        # so a scheduler sees a green run that fetched nothing.
        result.errors["pipeline"] = str(exc)
        storage.record_run(run)
        return result

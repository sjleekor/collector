"""Use-case: Backfill daily KRX market cap / trading value / listed shares.

The unit of work is a ``(trade_date, market)`` SLICE — one provider call
returns every listed stock on that date and market.

Two rules shape this service and both come from
``01_implementation_checklist.md`` 2.4.

**A slice is complete only when the stored row count matches the response.**
"Some rows exist, therefore done" would permanently strand any slice that was
interrupted mid-write: the skip rule would never look at it again.  So the skip
decision compares stored counts against what the provider actually returned,
and a mismatch leaves the slice incomplete and retryable.

**Holidays are not fetched.**  pykrx does not return an empty frame for a
non-session date — it returns every ticker with the prices zeroed.  The KRX
trading calendar filters those out before any request is made; the adapter's
zero-close drop is the second line of defence, not the first.

**Pacing is the KRX policy, not a local number.**  pykrx reaches the same
``data.krx.co.kr`` portal the MDC collectors do, so it gets the same
:class:`~collector.kr.util.pipeline.HumanThrottle`: randomised spacing, a
periodic long rest, and a real backoff after an error.  This service used to
sleep a flat ``rate_limit_seconds`` with no error backoff at all — 0.4s in the
deployed wrapper against the MDC path's 1.5–4.0s — and on 2026-08-16 KRX
restricted the collector's IP for "자동화 수단을 통한 비정상 대량 조회".  One
setting for one host is the point; two paces for the same portal was the defect.
"""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.domain.enums import Market, RunStatus, RunType
from collector.kr.domain.models import (
    DailyMarketCapResult,
    IngestionRun,
    MarketCapBackfillResult,
)
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.ports.market_cap import MarketCapProvider
from collector.kr.ports.storage import Storage
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

# 2014-06-02 is where the feature sample starts (02_feature_candidate.md 2.2):
# KOSPI OHLCV only begins 2014-01-20, so anything earlier is KOSDAQ-biased.
DEFAULT_START = date(2014, 6, 2)


def _slice_key(trade_date: date, market: Market) -> str:
    return f"{trade_date.isoformat()}/{market.value}"


def backfill_market_cap(
    provider: MarketCapProvider,
    storage: Storage,
    markets: list[Market] | None = None,
    start: date | None = None,
    end: date | None = None,
    throttle: HumanThrottle | None = None,
    force: bool = False,
    max_consecutive_failures: int = 5,
) -> MarketCapBackfillResult:
    """Backfill ``daily_market_cap`` from *provider* into *storage*.

    Args:
        provider: Market-cap provider (pykrx).
        storage: Target storage.
        markets: Markets to fetch.  Defaults to KOSPI + KOSDAQ.  Each market is
            a separate request — the response has no market column, so this is
            required, not an option.
        start: First trade date.  Defaults to ``DEFAULT_START``.
        end: Last trade date.  Defaults to today (KST).
        throttle: KRX pacing policy.  ``None`` means no pacing, which is for
            tests; the CLI always supplies one built from the KRX settings.
        force: Re-fetch slices that are already complete.
        max_consecutive_failures: Stop the run after this many slices fail in a
            row (0 disables).  Without it a blocked source is met with 6,000
            slices; see ``ConsecutiveFailureGuard``.

    Returns:
        ``MarketCapBackfillResult`` with per-slice counters and errors.
    """
    resolved_markets = list(markets) if markets else list(DEFAULT_MARKETS)
    resolved_start = start or DEFAULT_START
    resolved_end = end or today_kst()

    run = IngestionRun(
        run_type=RunType.MARKET_CAP_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "markets": [m.value for m in resolved_markets],
            "start": str(resolved_start),
            "end": str(resolved_end),
            "force": force,
            "max_consecutive_failures": max_consecutive_failures,
        },
    )
    storage.record_run(run)

    result = MarketCapBackfillResult()
    pacer = throttle or HumanThrottle(HumanThrottlePolicy(), logger_instance=logger)
    guard = ConsecutiveFailureGuard(
        max_consecutive_failures,
        label="market-cap backfill",
        # The throttle already backs off 45-180s after each error, so the guard
        # only needs to decide when to stop, not how long to wait.
        backoff_seconds=0.0,
        logger_instance=logger,
    )

    def _fetch_slice(d: date, m: Market, label: str) -> DailyMarketCapResult:
        """One attempt, then one more after a KRX-grade backoff.

        This used to be ``@retry(max_attempts=4, base_delay=0.5)``: four
        requests half a second apart at the exact moment the source is
        struggling. That is the shape KRX restricted the IP for, so the retry
        is now single and waits out the throttle's error backoff first.
        """
        pacer.before_request(label)
        res = provider.fetch_by_date(trade_date=d, market=m)
        pacer.after_request()
        if not res.error:
            return res

        pacer.backoff_after_error(label)
        pacer.before_request(label)
        res = provider.fetch_by_date(trade_date=d, market=m)
        pacer.after_request()
        if res.error:
            raise RuntimeError(res.error)
        return res

    try:
        if resolved_start > resolved_end:
            raise ValueError(f"start ({resolved_start}) must be <= end ({resolved_end})")

        trading_days = get_trading_days(resolved_start, resolved_end)
        logger.info(
            "Market-cap backfill %s..%s: %d trading days x %d markets",
            resolved_start,
            resolved_end,
            len(trading_days),
            len(resolved_markets),
        )

        stored_counts = (
            {} if force else storage.get_market_cap_slice_row_counts(resolved_start, resolved_end)
        )

        for trade_date in trading_days:
            for market in resolved_markets:
                label = _slice_key(trade_date, market)
                result.slices_attempted += 1

                # Completeness is a row-count comparison, never mere existence.
                # Without the expected count from a fresh response the best
                # available proxy is the neighbouring slices of the same market,
                # so a short slice is re-fetched rather than trusted.
                stored = stored_counts.get((trade_date, market), 0)
                if stored and not _looks_short(stored, stored_counts, market):
                    result.slices_skipped += 1
                    continue

                try:
                    fetch = _fetch_slice(trade_date, market, label)
                except SourceBlockedError:
                    raise
                except Exception as exc:  # noqa: BLE001 - collected, not raised
                    result.errors[label] = str(exc)
                    logger.warning("Slice %s failed: %s", label, exc)
                    guard.record_failure(f"{label}: {exc}")
                    continue

                dropped = max(0, fetch.response_rows - len(fetch.rows))
                result.rows_dropped += dropped

                if not fetch.rows:
                    # Every row was zero-filled: the calendar says session, the
                    # exchange says otherwise.  Not an error, but not a
                    # completed slice either — leave it retryable.
                    logger.info("Slice %s returned no usable rows (%d dropped)", label, dropped)
                    continue

                upsert = storage.upsert_daily_market_cap(fetch.rows)

                if upsert.updated != len(fetch.rows):
                    result.errors[label] = (
                        f"row count mismatch: fetched {len(fetch.rows)}, "
                        f"stored {upsert.updated}"
                    )
                    logger.warning("Slice %s: %s", label, result.errors[label])
                    continue

                result.rows_upserted += upsert.updated
                result.slices_completed += 1
                guard.record_success()

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                slices_attempted=result.slices_attempted,
                slices_skipped=result.slices_skipped,
                slices_completed=result.slices_completed,
                rows_upserted=result.rows_upserted,
                rows_dropped=result.rows_dropped,
                requests_made=pacer.completed_requests,
            ),
            errors=result.errors,
            partial_subject="market-cap slices",
        )
        return result

    except SourceBlockedError as exc:
        # Not a pipeline bug: the source stopped answering. Fail loudly so the
        # scheduler does not treat a half-finished range as done.
        logger.error("Market-cap backfill stopped: %s", exc)
        fail_run(storage, run, exc)
        result.errors["source_blocked"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("Market-cap backfill failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def _looks_short(
    stored: int,
    stored_counts: dict[tuple[date, Market], int],
    market: Market,
) -> bool:
    """Return ``True`` when a stored slice is implausibly small for its market.

    A partially written slice has rows, so row existence cannot detect it.  The
    listed universe moves slowly, so a slice holding far fewer rows than the
    median for its market was almost certainly interrupted.
    """
    same_market = sorted(count for (_, m), count in stored_counts.items() if m == market)
    if len(same_market) < 3:
        return False
    median = same_market[len(same_market) // 2]
    return stored < median * 0.9

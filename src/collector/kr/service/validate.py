"""Use-case: Data validation.

**Missing-bar checks resolve the universe AS OF the validated date.** Comparing
a past date against today's active set is structurally blind: the tickers that
are missing are exactly the ones that have since delisted, so they are absent
from both sides. That is how 13.9% of the 2016 cross-section stayed
uncollected without any check reporting it (``poc/survivorship_gap.md``).

1. **OHLC sanity rules**
   - low <= open <= high
   - low <= close <= high
   - All prices > 0 (unless delisted/halted — volume == 0 allowed)
   - Volume >= 0

2. **Missing-day checks**
   - Compare stored trade dates against the trading calendar.
   - Flag tickers with gaps (excluding holidays and weekends).
   - Use ``infra.calendar.trading_days`` for the calendar strategy.

3. **Universe count drift checks** (implemented)
   - Compare consecutive snapshot ``record_count`` values within a source.
   - Alert when the change exceeds ``universe_drift_pct`` (default ±5%).
   - Sources are compared separately: a backfilled snapshot and a live one
     come from different collection paths, so a step between them says
     nothing about the market.

4. **Cross-source consistency** (future)
   - Compare FDR vs pykrx universe snapshots for the same date.
   - Flag discrepancies in ticker sets or names.
"""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.domain.enums import Market, RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import IngestionRun
from collector.kr.infra.calendar.trading_days import get_trading_days, load_holidays
from collector.kr.ports.storage import Storage
from collector.kr.service.collection_targets import resolve_price_targets
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

# How many recent snapshots the drift check looks back over. Two years of
# month-end snapshots, so a slow universe trend does not trip it while a
# collection failure between consecutive runs still does.
SNAPSHOT_DRIFT_WINDOW = 24


def _universe_count_drift(storage: Storage, threshold_pct: float) -> list[str]:
    """Return alerts for consecutive snapshots whose size moved too much.

    Compared within a source: a backfilled snapshot and a live one have
    different collection paths, so a step between them says nothing about the
    market.
    """
    if threshold_pct <= 0:
        return []

    snapshots = storage.get_snapshot_record_counts(limit=SNAPSHOT_DRIFT_WINDOW)
    by_source: dict[Source, list[tuple[date, int]]] = {}
    for as_of, source, count in snapshots:
        by_source.setdefault(source, []).append((as_of, count))

    alerts: list[str] = []
    for source, series in by_source.items():
        # newest-first from storage; walk oldest-first so "previous" is earlier
        ordered = sorted(series)
        for (prev_date, prev_count), (curr_date, curr_count) in zip(
            ordered, ordered[1:], strict=False
        ):
            if not prev_count:
                continue
            change_pct = (curr_count - prev_count) / prev_count * 100.0
            if abs(change_pct) > threshold_pct:
                alerts.append(
                    f"{source.value} {prev_date}->{curr_date}: "
                    f"{prev_count} -> {curr_count} ({change_pct:+.1f}%)"
                )
    return alerts


def validate(
    storage: Storage,
    market: Market | None = None,
    target_date: date | None = None,
    universe_drift_pct: float = 5.0,
) -> None:
    """Run data-quality validations."""
    resolved_date = target_date or today_kst()

    run = IngestionRun(
        run_type=RunType.VALIDATE,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "market": market.value if market else None,
            "target_date": str(resolved_date),
        },
    )
    storage.record_run(run)

    try:
        # 1. Load trading calendar
        holidays = load_holidays()
        trading_days = get_trading_days(resolved_date, resolved_date, holidays)
        is_trading_day = len(trading_days) > 0

        if not is_trading_day:
            logger.info("%s is not a trading day. Skipping missing-day check.", resolved_date)

        # 2. Query daily_ohlcv
        bars = storage.get_daily_bars(resolved_date, market)
        logger.info("Found %d bars for %s.", len(bars), resolved_date)

        # 3. Apply OHLC sanity rules
        violations = []
        for bar in bars:
            if not (bar.low <= bar.open <= bar.high):
                violations.append(
                    f"{bar.ticker}: open ({bar.open}) out of bounds [{bar.low}, {bar.high}]"
                )
            if not (bar.low <= bar.close <= bar.high):
                violations.append(
                    f"{bar.ticker}: close ({bar.close}) out of bounds [{bar.low}, {bar.high}]"
                )
            if bar.volume < 0:
                violations.append(f"{bar.ticker}: negative volume ({bar.volume})")

        if violations:
            logger.warning("Found %d OHLC violations.", len(violations))
            for v in violations[:10]:
                logger.warning("  - %s", v)
            if len(violations) > 10:
                logger.warning("  - ... and %d more", len(violations) - 10)

        # 4. Check for missing bars against the universe listed ON THAT DATE
        #
        # Comparing a past date against today's active set cannot find a gap:
        # the tickers that are missing are exactly the ones that have since
        # delisted, so they are absent from both sides and cancel out. That is
        # how 13.9% of the 2016 cross-section stayed uncollected without any
        # check reporting it (poc/survivorship_gap.md). Resolve the universe
        # as-of instead; today's active set is just the as-of universe for
        # today, and stays the fallback when no snapshot covers the date.
        missing_tickers: list[str] = []
        universe_source = "none"
        universe_size = 0
        if is_trading_day:
            expected: set[str] = set()
            snapshot = storage.get_universe_as_of(resolved_date, market)
            if snapshot is not None:
                snapshot_date, expected = snapshot
                universe_source = f"snapshot:{snapshot_date.isoformat()}"
            else:
                expected = {
                    s.ticker for s in resolve_price_targets(storage, UniverseScope.CURRENT, market)
                }
                universe_source = "active"

            universe_size = len(expected)
            fetched_tickers = {b.ticker for b in bars}
            missing_tickers = sorted(expected - fetched_tickers)

            if missing_tickers:
                logger.warning(
                    "Missing daily bars for %d of %d tickers on %s (universe=%s).",
                    len(missing_tickers),
                    universe_size,
                    resolved_date,
                    universe_source,
                )

        # 5. Universe count drift — a sudden change in snapshot size is a
        #    collection failure long before it is a market event.
        drift_alerts = _universe_count_drift(storage, universe_drift_pct)
        for alert in drift_alerts:
            logger.warning("Universe drift: %s", alert)

        # 6. Complete the run
        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "bars_checked": len(bars),
            "ohlc_violations": len(violations),
            "missing_tickers": len(missing_tickers),
            # Without this the missing count is uninterpretable: 0 against an
            # empty universe reads the same as 0 against a complete one.
            "universe_size": universe_size,
            "universe_drift_alerts": len(drift_alerts),
        }
        run.params["universe_source"] = universe_source

        if violations or missing_tickers or drift_alerts:
            run.error_summary = (
                f"Violations: {len(violations)}, Missing: {len(missing_tickers)}, "
                f"Drift: {len(drift_alerts)}"
            )

        storage.record_run(run)
        logger.info("Validation completed.")

    except Exception as exc:
        logger.exception("Validation failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)

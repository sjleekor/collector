"""Regular-session correction for ``daily_ohlcv`` high / low / close (KR).

Since 2026-09-14 (KRX after-market launch) naver's daily high / low / close include the
after-market session, while KRX's regular-session values do not. Open (0.09% differ) and
volume (0%) still match. ``daily_market_cap.source_*`` carries KRX's regular-session
*unadjusted* OHLC, and naver's series is split/merge adjusted. The validated rule is::

    f           = naver_open / krx_open            same ticker, same date
    corrected_x = round(krx_x * f)                 x in {high, low, close}

On 2025-01-02~2026-09-11 it reproduces the stored naver close in 98.3% of 1.09M rows
(137k of them with ``f != 1``). Open and volume are never written.

``f`` comes from the open price, which is rounded on both sides, so a single day can be
off. An isolated outlier (``f`` differs by more than ``OUTLIER_TOLERANCE`` from both the
previous and the next valid ``f`` of the ticker) takes the previous day's ``f``. On the last
day there is no next day to confirm, so a day that differs from the previous ``f`` is skipped
and recorded instead of being guessed.

The job is idempotent and has to be re-run: ``prices backfill --incremental --lookback-days 7``
(18:30) re-fetches and overwrites the last week with naver values every evening. Run this
after the 20:00 market-cap job. Rows whose values already match are left alone, and every
change is written together with an audit row (``daily_ohlcv_regular_fix_audit``).

The KRX input is pluggable (``RegularSessionSource``); the default reads ``daily_market_cap``,
which lags one trading day (at day T 20:00 the newest KRX row is T-1).
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from fractions import Fraction
from typing import Protocol

from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.models import FixPlan, IngestionRun, KrxSessionRow, StoredOhlc
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

#: The first session with after-market contamination. Earlier rows already match KRX.
REGULAR_SESSION_FIX_FROM = date(2026, 9, 14)
#: A day's ``f`` that differs by more than this fraction from its neighbours is an outlier.
OUTLIER_TOLERANCE = 0.002
#: Calendar days of context read before ``start`` and after ``end`` for the neighbour ``f``.
CONTEXT_DAYS = 14
#: How many skipped rows are kept (by reason) in ``ingestion_runs.params`` for a human.
MAX_RECORDED_SKIPS = 200

FACTOR_OWN = "own"
FACTOR_PREV_DAY = "prev_day"

SKIP_NO_KRX_ROW = "no_krx_row"
SKIP_KRX_INVALID = "krx_invalid"  # open/high/low/close missing or <= 0
SKIP_OHLCV_OPEN_INVALID = "ohlcv_open_invalid"  # naver open <= 0 -> f undefined
SKIP_LAST_DAY_OUTLIER = "last_day_outlier"
SKIP_STALE_READ = "stale_read"  # row changed between read and write (re-run fixes it)


@dataclass(frozen=True, slots=True)
class SkippedRow:
    ticker: str
    market: str
    trade_date: date
    reason: str


@dataclass(slots=True)
class FixReport:
    """Planned (and, when applied, written) corrections for one run."""

    plans: list[FixPlan] = field(default_factory=list)
    unchanged: int = 0
    skipped: list[SkippedRow] = field(default_factory=list)
    applied: int = 0
    audit_rows_new: int = 0

    def skipped_by_reason(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for s in self.skipped:
            counts[s.reason] += 1
        return dict(counts)


class RegularSessionSource(Protocol):
    """Where KRX regular-session OHLC comes from. Default: ``daily_market_cap``."""

    name: str

    def get_session_rows(self, since: date, until: date) -> list[KrxSessionRow]: ...


class FixStorage(Protocol):
    """The reads and writes this job needs. ``PostgresStorage`` implements them."""

    def get_stored_ohlc(self, since: date, until: date) -> list[StoredOhlc]: ...

    def get_market_cap_session_rows(self, since: date, until: date) -> list[KrxSessionRow]: ...

    def get_latest_ohlcv_trade_dates(self, limit: int) -> list[date]: ...

    def apply_regular_session_fixes(
        self,
        plans: list[FixPlan],
        run_id: uuid.UUID,
        applied_at: datetime,
        krx_source: str,
    ) -> tuple[list[tuple[str, str, date]], int]:
        """Write plans; return (keys actually updated, new audit rows)."""
        ...

    def record_run(self, run: IngestionRun) -> None: ...


class MarketCapSource:
    """Default source: ``daily_market_cap.source_open/high/low/close`` (KRX OpenAPI)."""

    name = "daily_market_cap"

    def __init__(self, storage: FixStorage) -> None:
        self._storage = storage

    def get_session_rows(self, since: date, until: date) -> list[KrxSessionRow]:
        return self._storage.get_market_cap_session_rows(since, until)


def _positive(*values: int | None) -> bool:
    return all(v is not None and v > 0 for v in values)


def _round_half_up(numerator: int, denominator: int) -> int:
    """``round(numerator / denominator)`` for positive ints, ties up, no float."""
    return (2 * numerator + denominator) // (2 * denominator)


def _scale(value: int, f: Fraction) -> int:
    return _round_half_up(value * f.numerator, f.denominator)


def _differs(a: Fraction, b: Fraction) -> bool:
    return abs(float(a) - float(b)) / float(b) > OUTLIER_TOLERANCE


def resolve_factors(
    series: list[tuple[date, Fraction]],
) -> dict[date, tuple[Fraction, str] | None]:
    """Choose the factor for each day of one ticker's date-sorted ``f`` series.

    Returns ``date -> (f, basis)``, or ``None`` for a last-day outlier that must be skipped.

    * interior day differing > tolerance from BOTH neighbours -> previous day's resolved ``f``
    * last day differing > tolerance from the previous ``f`` -> ``None`` (skip and record)
    * first day (no previous) and everything else -> own ``f``
    """
    out: dict[date, tuple[Fraction, str] | None] = {}
    n = len(series)
    for i, (day, f) in enumerate(series):
        prev_f = series[i - 1][1] if i > 0 else None
        next_f = series[i + 1][1] if i + 1 < n else None
        if prev_f is not None and next_f is not None:
            if _differs(f, prev_f) and _differs(f, next_f):
                prev_resolved = out[series[i - 1][0]]
                # A previous last-day skip cannot happen (it is not last), so this is set.
                assert prev_resolved is not None
                out[day] = (prev_resolved[0], FACTOR_PREV_DAY)
                continue
        elif prev_f is not None and next_f is None:
            if _differs(f, prev_f):
                out[day] = None
                continue
        out[day] = (f, FACTOR_OWN)
    return out


def plan_fixes(
    stored: Iterable[StoredOhlc],
    krx: Iterable[KrxSessionRow],
    start: date,
    end: date,
) -> FixReport:
    """Compute corrections for ``[start, end]``. Pure: no I/O.

    ``stored``/``krx`` should carry context days around the window so the neighbour ``f``
    exist. Context days are used to judge outliers but are never planned.
    """
    krx_by_key: dict[tuple[str, str, date], KrxSessionRow] = {
        (k.ticker, k.market, k.trade_date): k for k in krx
    }
    by_ticker: dict[tuple[str, str], list[StoredOhlc]] = defaultdict(list)
    for row in stored:
        by_ticker[(row.ticker, row.market)].append(row)

    report = FixReport()
    for (ticker, market), rows in sorted(by_ticker.items()):
        rows.sort(key=lambda r: r.trade_date)
        # Rows that can carry an f (valid on both sides), in date order.
        usable: dict[date, tuple[StoredOhlc, KrxSessionRow]] = {}
        for row in rows:
            in_window = start <= row.trade_date <= end
            k = krx_by_key.get((ticker, market, row.trade_date))
            if k is None:
                if in_window:
                    report.skipped.append(
                        SkippedRow(ticker, market, row.trade_date, SKIP_NO_KRX_ROW)
                    )
                continue
            if not _positive(k.open, k.high, k.low, k.close):
                if in_window:
                    report.skipped.append(
                        SkippedRow(ticker, market, row.trade_date, SKIP_KRX_INVALID)
                    )
                continue
            if row.open <= 0:
                if in_window:
                    report.skipped.append(
                        SkippedRow(ticker, market, row.trade_date, SKIP_OHLCV_OPEN_INVALID)
                    )
                continue
            usable[row.trade_date] = (row, k)

        series = [(d, Fraction(r.open, k.open)) for d, (r, k) in sorted(usable.items())]  # type: ignore[arg-type]
        resolved = resolve_factors(series)

        for day, (row, k) in sorted(usable.items()):
            if not (start <= day <= end):
                continue
            choice = resolved[day]
            if choice is None:
                report.skipped.append(SkippedRow(ticker, market, day, SKIP_LAST_DAY_OUTLIER))
                continue
            f, basis = choice
            assert k.high is not None and k.low is not None and k.close is not None
            new_high, new_low, new_close = _scale(k.high, f), _scale(k.low, f), _scale(k.close, f)
            if (new_high, new_low, new_close) == (row.high, row.low, row.close):
                report.unchanged += 1
                continue
            assert k.open is not None
            report.plans.append(
                FixPlan(
                    ticker=ticker,
                    market=market,
                    trade_date=day,
                    old_high=row.high,
                    old_low=row.low,
                    old_close=row.close,
                    new_high=new_high,
                    new_low=new_low,
                    new_close=new_close,
                    naver_open=row.open,
                    krx_open=k.open,
                    krx_high=k.high,
                    krx_low=k.low,
                    krx_close=k.close,
                    factor=float(f),
                    factor_basis=basis,
                )
            )
    return report


def resolve_window(
    storage: FixStorage,
    start: date | None,
    end: date | None,
    last_trading_days: int | None,
) -> tuple[date, date] | None:
    """Turn CLI arguments into ``(start, end)``; ``None`` when there is nothing stored."""
    if last_trading_days is not None:
        if last_trading_days <= 0:
            raise ValueError("last_trading_days must be positive")
        if start is not None or end is not None:
            raise ValueError("last_trading_days cannot be combined with start/end")
        days = storage.get_latest_ohlcv_trade_dates(last_trading_days)
        if not days:
            return None
        return min(days), max(days)
    if start is None or end is None:
        raise ValueError("start and end are required (or use last_trading_days)")
    if start > end:
        raise ValueError("start must be <= end")
    return start, end


def fix_regular_session(
    storage: FixStorage,
    start: date,
    end: date,
    *,
    source: RegularSessionSource | None = None,
    dry_run: bool = False,
    allow_before_cutoff: bool = False,
) -> tuple[FixReport, IngestionRun]:
    """Correct ``daily_ohlcv`` high/low/close over ``[start, end]``; return report and run."""
    source = source or MarketCapSource(storage)
    effective_start = start if allow_before_cutoff else max(start, REGULAR_SESSION_FIX_FROM)
    run = IngestionRun(
        run_type=RunType.REGULAR_SESSION_FIX,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "start": str(start),
            "effective_start": str(effective_start),
            "end": str(end),
            "dry_run": dry_run,
            "krx_source": source.name,
            "allow_before_cutoff": allow_before_cutoff,
            "outlier_tolerance": OUTLIER_TOLERANCE,
        },
    )
    report = FixReport()
    if not dry_run:
        storage.record_run(run)
    try:
        if effective_start > end:
            logger.info("Window %s..%s is before the cutoff; nothing to do.", start, end)
        else:
            ctx_since = effective_start - timedelta(days=CONTEXT_DAYS)
            ctx_until = end + timedelta(days=CONTEXT_DAYS)
            stored = storage.get_stored_ohlc(ctx_since, ctx_until)
            krx = source.get_session_rows(ctx_since, ctx_until)
            report = plan_fixes(stored, krx, effective_start, end)
            logger.info(
                "Regular-session fix %s..%s: planned=%d unchanged=%d skipped=%s",
                effective_start,
                end,
                len(report.plans),
                report.unchanged,
                report.skipped_by_reason(),
            )
            if not dry_run and report.plans:
                updated, audit_new = storage.apply_regular_session_fixes(
                    report.plans, run.run_id, now_kst(), source.name
                )
                report.applied = len(updated)
                report.audit_rows_new = audit_new
                done = set(updated)
                for plan in report.plans:
                    if plan.key not in done:
                        report.skipped.append(
                            SkippedRow(plan.ticker, plan.market, plan.trade_date, SKIP_STALE_READ)
                        )
        run.status = RunStatus.SUCCESS
    except Exception as exc:
        logger.exception("Regular-session fix failed")
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        raise
    finally:
        run.ended_at = now_kst()
        if report.skipped:
            run.params["skipped_examples"] = [
                {"ticker": s.ticker, "date": str(s.trade_date), "reason": s.reason}
                for s in report.skipped[:MAX_RECORDED_SKIPS]
            ]
        run.counts = {
            "planned": len(report.plans),
            "applied": report.applied,
            "unchanged": report.unchanged,
            "audit_rows_new": report.audit_rows_new,
            "skipped": report.skipped_by_reason(),
        }
        if not dry_run:
            storage.record_run(run)
    return report, run

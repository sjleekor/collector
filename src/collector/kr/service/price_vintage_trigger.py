"""Pick the tickers whose ``daily_ohlcv`` history needs a re-fetch (price vintage trigger).

``daily_ohlcv`` comes from naver's *adjusted* series, which rewrites the whole
past on every split / reverse split / bonus issue, while ``prices backfill``
without ``--refetch`` only appends rows after each ticker's last stored date.
An event therefore leaves two adjustment bases glued together at the event date
(``my/milestones/kr/research/data/quality/01_price_adjustment_vintage.md`` §2~§3).

``daily_market_cap`` is the detector. It carries KRX's own ``listed_shares`` and the
*unadjusted* ``source_close`` every session. Between two consecutive sessions::

    k = listed_shares_t / listed_shares_(t-1)      share-count ratio
    p = source_close_t  / source_close_(t-1)       raw price ratio

``k x p ~ 1`` means the market cap is continuous while the share count moved: a
mechanical event (split, reverse split, bonus issue, capital reduction). ``p ~ 1``
with ``k != 1`` is a real capital raise (new shares at the market price) and needs
no price re-fetch. Section 8 of the quality note names this test ("k x p ~ 1").

A mechanical event is only a *target* while ``daily_ohlcv`` still carries the old basis.
KRX's ``source_close`` is never adjusted, so ``R = daily_ohlcv.close / source_close`` is the
adjustment factor naver applied to that day. In a correctly rebased series ``R`` is ``R_S`` on
the event day ``S`` and ``R_S / k`` on every earlier day. Earlier days still sitting at
``R_S`` are the stale rows. Looking at the price jump on ``S`` is not enough: naver rebases
at the *halt* that precedes a reverse split, weeks before KRX moves ``listed_shares``, so
the stored jump is found at the halt and the share event comes later (measured: 227
of 299 jump tickers since 2026-04-11 on the 2026-09-08 snapshot).
Two outcomes are kept out of the target list on purpose:

* ``resolved`` - the rows before the event already sit at ``R_S / k``. Re-running the job
  must not queue the same ticker again, which is what makes it safe to schedule weekly.
* ``refetched_unresolved`` - the stale rows were written *after* the event's market cap
  row, so a re-fetch already happened and they are still wrong. naver does not adjust
  this event (003060, a 15:1 reverse split, 2026-05-08). Queueing it again would repeat
  the same call every week; it is reported for a human instead.

**Early signal.** A reverse split halts trading for one to three weeks. naver rebases at the
halt, KRX books the new ``listed_shares`` only when trading resumes, so the share event
arrives late (measured 2026-09-21~09-23: four of six tickers rebased by naver at the halt had
no share event yet). The same ``R`` shows it sooner: a step in ``R`` between two stored rows
that no share event explains means the rows before the step are on the old basis. Such steps
(``rebase_step``) are targets too, with the same ``refetched_unresolved`` guard.

This module only computes and writes the list. **It never re-fetches**: the re-fetch
is ``prices backfill --refetch`` and a person approves it.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Protocol

from collector.kr.domain.models import MarketCapPoint, OhlcvPoint

SCHEMA = "kr_price_vintage_trigger/v1"

#: Lookback for the weekly run. Two weeks so one missed run is still covered; resolved
#: events drop out by themselves, so the overlap costs nothing.
DEFAULT_LOOKBACK_DAYS = 14
#: ``max(k, 1/k)`` at or above this counts as a share-count change worth checking.
#: Smaller moves are dominated by ordinary price noise in ``k x p`` and by rights issues
#: whose ex-rights price drop happens to make ``k x p ~ 1`` (naver does not adjust those).
DEFAULT_MIN_SHARE_RATIO = 1.5
#: ``k x p`` within ``1 +/- tol`` counts as mechanical. KRX limits a day to +/-30% from the
#: adjusted reference price, so the continuous part of ``k x p`` cannot leave this band.
DEFAULT_KP_TOLERANCE = 0.3
#: How far before an event the stored rows are checked (calendar days), cut at the ticker's
#: previous mechanical event.
DEFAULT_CHECK_BACK_DAYS = 180
#: A stored row is stale when its ``R`` is off the expected ``R_S / k`` by more than this fraction.
#: naver rounds adjusted closes to the won, so the band must clear that on low-priced stocks.
DEFAULT_R_TOLERANCE = 0.15
#: The reference ratio ``R_S`` is taken from the first usable row within this many calendar days
#: from the event day on.
BASE_LOOKAHEAD_DAYS = 14
#: Rows written by one re-fetch pass are at most this far apart (incremental rows: a day).
REFETCH_PASS_GAP = timedelta(hours=1)
#: Stale rows needed to call an event stale. One odd row is a data glitch, not a vintage break.
DEFAULT_MIN_BAD_BARS = 2

STATUS_STALE = "stale"
STATUS_UNKNOWN = "unknown"
STATUS_RESOLVED = "resolved"
STATUS_REFETCHED_UNRESOLVED = "refetched_unresolved"
#: Statuses that go to ``targets.txt``.
TARGET_STATUSES = frozenset({STATUS_STALE})

TRIGGER_SHARES = "shares"
TRIGGER_REBASE_STEP = "rebase_step"

KIND_MECHANICAL = "mechanical"
KIND_CAPITAL_RAISE = "capital_change"
KIND_AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class ShareEvent:
    """A session whose share count moved by at least the configured amount."""

    ticker: str
    market: str
    trade_date: date
    prev_trade_date: date
    k: float
    p: float
    kind: str
    fetched_at: datetime | None

    @property
    def kp(self) -> float:
        return self.k * self.p


@dataclass(frozen=True, slots=True)
class TriggerEvent:
    """A mechanical event with the vintage verdict on ``daily_ohlcv``."""

    event: ShareEvent
    status: str
    checked_bars: int = 0  # stored rows before the event that were compared
    bad_bars: int = 0  # of those, rows still on the old basis
    bad_first: date | None = None
    bad_last: date | None = None


@dataclass(frozen=True, slots=True)
class RebaseStep:
    """A step in ``R = close / source_close`` between two rows, with no share event that day."""

    ticker: str
    market: str
    trade_date: date
    prev_trade_date: date
    rho: float  # R_t / R_(t-1)
    status: str  # stale | refetched_unresolved


class TriggerStorage(Protocol):
    """The reads this job needs. ``PostgresStorage`` implements them."""

    def get_latest_market_cap_date(self) -> date | None: ...

    def get_market_cap_points(self, since: date, until: date) -> list[MarketCapPoint]: ...

    def get_ohlcv_points(
        self, tickers: list[str] | None, since: date, until: date
    ) -> list[OhlcvPoint]:
        """Rows with the same day's ``source_close``; ``tickers=None`` means every ticker."""
        ...


@dataclass(frozen=True, slots=True)
class TriggerConfig:
    as_of: date
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    min_share_ratio: float = DEFAULT_MIN_SHARE_RATIO
    kp_tolerance: float = DEFAULT_KP_TOLERANCE
    check_back_days: int = DEFAULT_CHECK_BACK_DAYS
    r_tolerance: float = DEFAULT_R_TOLERANCE
    min_bad_bars: int = DEFAULT_MIN_BAD_BARS


@dataclass(frozen=True, slots=True)
class TriggerReport:
    config: TriggerConfig
    market_cap_latest_date: date | None
    window_start: date
    share_events: int  # sessions that passed the share-change filter
    events: tuple[TriggerEvent, ...]  # mechanical events only
    steps: tuple[RebaseStep, ...] = ()

    @property
    def targets(self) -> list[str]:
        """Sorted unique tickers to re-fetch."""
        found = {e.event.ticker for e in self.events if e.status in TARGET_STATUSES}
        found |= {s.ticker for s in self.steps if s.status in TARGET_STATUSES}
        return sorted(found)

    def refetched_unresolved_tickers(self) -> list[str]:
        """Tickers already re-fetched whose rows are still wrong; a person looks at these."""
        found = {e.event.ticker for e in self.events if e.status == STATUS_REFETCHED_UNRESOLVED}
        found |= {s.ticker for s in self.steps if s.status == STATUS_REFETCHED_UNRESOLVED}
        return sorted(found)

    def count_steps_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for s in self.steps:
            counts[s.status] += 1
        return dict(counts)

    def count_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for e in self.events:
            counts[e.status] += 1
        return dict(counts)


def classify_share_event(k: float, p: float, kp_tolerance: float) -> str:
    """Name what a share-count change was, from ``k`` and ``p``.

    ``mechanical``    ``k x p`` is inside ``1 +/- kp_tolerance`` - market cap continuous.
    ``capital_change`` ``p`` is inside the same band - the price did not move, shares were added
                      or removed at the market price (rights issue, conversion, buyback).
    ``ambiguous``     neither, e.g. a large price move on the same day as the share change.
    A change that satisfies both bands is mechanical only when ``k`` is farther from 1 than
    the tolerance, which the caller's ``min_share_ratio`` already guarantees.
    """
    lo, hi = 1.0 - kp_tolerance, 1.0 + kp_tolerance
    kp = k * p
    if lo <= kp <= hi and not (lo <= p <= hi):
        return KIND_MECHANICAL
    if lo <= p <= hi and not (lo <= kp <= hi):
        return KIND_CAPITAL_RAISE
    if lo <= kp <= hi and lo <= p <= hi:
        # 둘 다 안이면 어느 쪽이 1에 더 가까운지로 가른다.
        return KIND_MECHANICAL if abs(math.log(kp)) < abs(math.log(p)) else KIND_CAPITAL_RAISE
    return KIND_AMBIGUOUS


def detect_share_events(
    points: Iterable[MarketCapPoint],
    *,
    since: date,
    min_share_ratio: float = DEFAULT_MIN_SHARE_RATIO,
    kp_tolerance: float = DEFAULT_KP_TOLERANCE,
) -> list[ShareEvent]:
    """Share-count changes from consecutive ``daily_market_cap`` sessions of each ticker.

    ``points`` must include the session before ``since`` so the first day in the window has
    a predecessor. A row with a non-positive share count or close is skipped as a predecessor
    too: it carries no usable ratio, and bridging over it would blur ``p``.
    """
    by_key: dict[tuple[str, str], list[MarketCapPoint]] = defaultdict(list)
    for point in points:
        if point.listed_shares > 0 and point.source_close > 0:
            by_key[(point.ticker, point.market)].append(point)

    events: list[ShareEvent] = []
    for series in by_key.values():
        series.sort(key=lambda r: r.trade_date)
        for prev, cur in zip(series, series[1:], strict=False):
            if cur.trade_date < since:
                continue
            k = cur.listed_shares / prev.listed_shares
            if max(k, 1.0 / k) < min_share_ratio:
                continue
            p = cur.source_close / prev.source_close
            events.append(
                ShareEvent(
                    ticker=cur.ticker,
                    market=cur.market,
                    trade_date=cur.trade_date,
                    prev_trade_date=prev.trade_date,
                    k=k,
                    p=p,
                    kind=classify_share_event(k, p, kp_tolerance),
                    fetched_at=cur.fetched_at,
                )
            )
    events.sort(key=lambda e: (e.trade_date, e.ticker, e.market))
    return events


def assess_vintage(
    event: ShareEvent,
    bars: list[OhlcvPoint],
    *,
    floor_date: date | None = None,
    check_back_days: int = DEFAULT_CHECK_BACK_DAYS,
    r_tolerance: float = DEFAULT_R_TOLERANCE,
    min_bad_bars: int = DEFAULT_MIN_BAD_BARS,
) -> TriggerEvent:
    """Judge whether the rows of ``daily_ohlcv`` before ``event`` are still on the old basis.

    ``bars`` are that ticker's rows (any order) with the same day's ``source_close``. Halted
    rows (``open = high = low = 0``) count: their close is the last close in naver's current
    basis, so a rebase done during a halt shows there first. Rows without a positive
    ``source_close`` are skipped. The comparison covers
    ``[max(floor_date, event - check_back_days), event)``; ``floor_date`` is the ticker's
    previous mechanical event, before which another factor applies.
    """

    def ratio(bar: OhlcvPoint) -> float | None:
        if bar.close <= 0 or not bar.source_close or bar.source_close <= 0:
            return None
        return bar.close / bar.source_close

    # 기준 R_S는 사건일 이후 처음 쓸 수 있는 행에서 잡는다. 재상장 직후엔 사건일 행이
    # 정지 표식이거나 비어 있을 수 있다.
    base: float | None = None
    for bar in sorted(bars, key=lambda b: b.trade_date):
        if (
            event.trade_date
            <= bar.trade_date
            <= event.trade_date + timedelta(days=BASE_LOOKAHEAD_DAYS)
        ):
            base = ratio(bar)
            if base is not None:
                break
    if base is None:
        return TriggerEvent(event=event, status=STATUS_UNKNOWN)

    start = event.trade_date - timedelta(days=check_back_days)
    if floor_date is not None and floor_date > start:
        start = floor_date
    expected = base / event.k
    checked = 0
    bad: list[OhlcvPoint] = []
    for bar in bars:
        if not (start <= bar.trade_date < event.trade_date):
            continue
        r = ratio(bar)
        if r is None:
            continue
        checked += 1
        if abs(math.log(r / expected)) > math.log1p(r_tolerance):
            bad.append(bar)
    if checked == 0:
        return TriggerEvent(event=event, status=STATUS_UNKNOWN)

    bad.sort(key=lambda b: b.trade_date)
    if len(bad) < min(min_bad_bars, checked):
        status = STATUS_RESOLVED
    elif event.fetched_at is not None and all(
        b.fetched_at is not None and b.fetched_at > event.fetched_at for b in bad
    ):
        status = STATUS_REFETCHED_UNRESOLVED
    else:
        status = STATUS_STALE
    return TriggerEvent(
        event=event,
        status=status,
        checked_bars=checked,
        bad_bars=len(bad),
        bad_first=bad[0].trade_date if bad else None,
        bad_last=bad[-1].trade_date if bad else None,
    )


def detect_rebase_steps(
    bars: Iterable[OhlcvPoint],
    *,
    since: date,
    min_ratio: float = DEFAULT_MIN_SHARE_RATIO,
    explained_days: Collection[tuple[str, str, date]] = (),
) -> list[RebaseStep]:
    """Steps in ``R = close / source_close`` that no share event explains.

    ``bars`` must include the row before ``since``. Halted rows count (see ``assess_vintage``).
    ``explained_days`` holds the
    ``(ticker, market, trade_date)`` of every share event: ``R`` is *supposed* to step there.
    A step on any other day is a basis break in the stored series. If the row before the step
    was written no earlier than the row after it, one re-fetch has already run (it rewrites
    both in the same pass) and the step is still there, so it is ``refetched_unresolved``.
    """
    by_key: dict[tuple[str, str], list[OhlcvPoint]] = defaultdict(list)
    for bar in bars:
        if bar.close > 0 and bar.source_close and bar.source_close > 0:
            by_key[(bar.ticker, bar.market)].append(bar)

    steps: list[RebaseStep] = []
    for series in by_key.values():
        series.sort(key=lambda b: b.trade_date)
        for prev, cur in zip(series, series[1:], strict=False):
            if cur.trade_date < since:
                continue
            rho = (cur.close / cur.source_close) / (prev.close / prev.source_close)
            if max(rho, 1.0 / rho) < min_ratio:
                continue
            if (cur.ticker, cur.market, cur.trade_date) in explained_days:
                continue
            refetched = (
                prev.fetched_at is not None
                and cur.fetched_at is not None
                and prev.fetched_at >= cur.fetched_at - REFETCH_PASS_GAP
            )
            steps.append(
                RebaseStep(
                    ticker=cur.ticker,
                    market=cur.market,
                    trade_date=cur.trade_date,
                    prev_trade_date=prev.trade_date,
                    rho=rho,
                    status=STATUS_REFETCHED_UNRESOLVED if refetched else STATUS_STALE,
                )
            )
    steps.sort(key=lambda r: (r.trade_date, r.ticker, r.market))
    return steps


def build_trigger_report(storage: TriggerStorage, cfg: TriggerConfig) -> TriggerReport:
    """Read the window from storage and decide the re-fetch targets. Read-only."""
    latest = storage.get_latest_market_cap_date()
    window_start = cfg.as_of - timedelta(days=cfg.lookback_days)
    # 창 안 사건의 "직전 사건"까지 알아야 해서 점검 구간만큼 앞에서부터 읽는다.
    # 시작 날 직전 세션도 필요하니 열흘을 더 둔다(연휴 포함).
    history_start = window_start - timedelta(days=cfg.check_back_days)
    points = storage.get_market_cap_points(history_start - timedelta(days=10), cfg.as_of)
    all_events = detect_share_events(
        points,
        since=history_start,
        min_share_ratio=cfg.min_share_ratio,
        kp_tolerance=cfg.kp_tolerance,
    )
    in_window = [e for e in all_events if e.trade_date >= window_start]
    mechanical_all = [e for e in all_events if e.kind == KIND_MECHANICAL]
    mechanical = [e for e in mechanical_all if e.trade_date >= window_start]

    bars_by_ticker: dict[tuple[str, str], list[OhlcvPoint]] = defaultdict(list)
    if mechanical:
        earliest = min(e.trade_date for e in mechanical) - timedelta(days=cfg.check_back_days)
        tickers = sorted({e.ticker for e in mechanical})
        for bar in storage.get_ohlcv_points(tickers, earliest, cfg.as_of):
            bars_by_ticker[(bar.ticker, bar.market)].append(bar)

    # 주식 수 사건이 없는 날의 R 단계 — 정지 중에 naver가 먼저 조정한 경우를 잡는다.
    window_bars = storage.get_ohlcv_points(None, window_start - timedelta(days=10), cfg.as_of)
    steps = detect_rebase_steps(
        window_bars,
        since=window_start,
        min_ratio=cfg.min_share_ratio,
        explained_days={(e.ticker, e.market, e.trade_date) for e in all_events},
    )

    floors: dict[tuple[str, str, date], date] = {}
    last_seen: dict[tuple[str, str], date] = {}
    for event in mechanical_all:  # 날짜순
        key = (event.ticker, event.market)
        if key in last_seen:
            floors[(event.ticker, event.market, event.trade_date)] = last_seen[key]
        last_seen[key] = event.trade_date

    results = [
        assess_vintage(
            event,
            bars_by_ticker.get((event.ticker, event.market), []),
            floor_date=floors.get((event.ticker, event.market, event.trade_date)),
            check_back_days=cfg.check_back_days,
            r_tolerance=cfg.r_tolerance,
            min_bad_bars=cfg.min_bad_bars,
        )
        for event in mechanical
    ]
    return TriggerReport(
        config=cfg,
        market_cap_latest_date=latest,
        window_start=window_start,
        share_events=len(in_window),
        events=tuple(results),
        steps=tuple(steps),
    )


def _fmt(value: float | None) -> str:
    return "" if value is None else f"{value:.6g}"


def write_trigger_outputs(report: TriggerReport, output_dir: Path) -> Path:
    """Write ``targets.txt``, ``events.csv`` and ``summary.json`` under ``<output_dir>/<as_of>/``.

    ``targets.txt`` is one ticker per line, the shape ``paste -sd,`` turns into ``--tickers``.
    Re-running the same ``as_of`` overwrites the files, so the last run is the record.
    """
    day_dir = output_dir / report.config.as_of.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)

    targets = report.targets
    (day_dir / "targets.txt").write_text("".join(f"{t}\n" for t in targets), encoding="utf-8")

    with (day_dir / "events.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "trigger",
                "ticker",
                "market",
                "trade_date",
                "prev_trade_date",
                "k",
                "p",
                "kp",
                "rho",
                "status",
                "checked_bars",
                "bad_bars",
                "bad_first",
                "bad_last",
            ]
        )
        for item in report.events:
            e = item.event
            writer.writerow(
                [
                    TRIGGER_SHARES,
                    e.ticker,
                    e.market,
                    e.trade_date.isoformat(),
                    e.prev_trade_date.isoformat(),
                    _fmt(e.k),
                    _fmt(e.p),
                    _fmt(e.kp),
                    "",
                    item.status,
                    item.checked_bars,
                    item.bad_bars,
                    item.bad_first.isoformat() if item.bad_first else "",
                    item.bad_last.isoformat() if item.bad_last else "",
                ]
            )
        for step in report.steps:
            writer.writerow(
                [
                    TRIGGER_REBASE_STEP,
                    step.ticker,
                    step.market,
                    step.trade_date.isoformat(),
                    step.prev_trade_date.isoformat(),
                    "",
                    "",
                    "",
                    _fmt(step.rho),
                    step.status,
                    "",
                    "",
                    "",
                    "",
                ]
            )

    cfg = report.config
    summary = {
        "schema": SCHEMA,
        "as_of": cfg.as_of.isoformat(),
        "window_start": report.window_start.isoformat(),
        "market_cap_latest_date": (
            report.market_cap_latest_date.isoformat() if report.market_cap_latest_date else None
        ),
        "params": {
            "lookback_days": cfg.lookback_days,
            "min_share_ratio": cfg.min_share_ratio,
            "kp_tolerance": cfg.kp_tolerance,
            "check_back_days": cfg.check_back_days,
            "r_tolerance": cfg.r_tolerance,
            "min_bad_bars": cfg.min_bad_bars,
        },
        "share_change_sessions": report.share_events,
        "mechanical_events": len(report.events),
        "events_by_status": report.count_by_status(),
        "rebase_steps": len(report.steps),
        "rebase_steps_by_status": report.count_steps_by_status(),
        "target_tickers": len(targets),
        "refetched_unresolved_tickers": report.refetched_unresolved_tickers(),
        "note": (
            "targets.txt is a proposal. The re-fetch (prices backfill --refetch) needs a "
            "person's approval."
        ),
    }
    (day_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return day_dir

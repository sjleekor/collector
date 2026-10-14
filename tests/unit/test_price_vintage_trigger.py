"""Price vintage trigger: ``listed_shares`` diff (k x p ~ 1) -> daily_ohlcv re-fetch candidates.

Fixtures are synthetic but mirror the shapes measured on the 2026-09-08 and 2026-09-29 lake
snapshots (see ``my/milestones/kr/research/data/quality/01_price_adjustment_vintage.md`` §9):

* a reverse split booked by KRX on the day trading resumes, with naver rebased a day earlier;
* a rights issue (new shares at the market price, ``p`` ~ 1) that needs no re-fetch;
* a 15:1 reverse split that naver never adjusts (003060), still wrong after a re-fetch.
"""

from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from collector.kr.domain.models import MarketCapPoint, OhlcvPoint
from collector.kr.service.price_vintage_trigger import (
    KIND_AMBIGUOUS,
    KIND_CAPITAL_RAISE,
    KIND_MECHANICAL,
    STATUS_REFETCHED_UNRESOLVED,
    STATUS_RESOLVED,
    STATUS_STALE,
    STATUS_UNKNOWN,
    ShareEvent,
    TriggerConfig,
    assess_vintage,
    build_trigger_report,
    classify_share_event,
    detect_rebase_steps,
    detect_share_events,
    write_trigger_outputs,
)

D0 = date(2026, 9, 1)  # Tuesday
# 평일만 세션이다 (휴장일은 시험에서 따로 두지 않는다).
SESSIONS = [D0 + timedelta(days=i) for i in range(100) if (D0 + timedelta(days=i)).weekday() < 5]
SPLIT = SESSIONS[20]  # 사건일 S
AS_OF = SESSIONS[30]


def _ts(day: date, hour: int = 18) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def _mc(
    ticker: str,
    shares_before: int,
    shares_after: int,
    close_before: int,
    close_after: int,
    event_day: date = SPLIT,
    market: str = "KOSDAQ",
) -> list[MarketCapPoint]:
    return [
        MarketCapPoint(
            ticker,
            market,
            d,
            shares_before if d < event_day else shares_after,
            close_before if d < event_day else close_after,
            _ts(d + timedelta(days=1)),
        )
        for d in SESSIONS
    ]


def _bars(
    ticker: str,
    mc: list[MarketCapPoint],
    *,
    factor_before: float,
    factor_after: float = 1.0,
    switch_day: date = SPLIT,
    fetched_at: datetime | None = None,
    halted_from: date | None = None,
    halted_to: date | None = None,
) -> list[OhlcvPoint]:
    """Stored rows: close = source_close x factor, factor switching at ``switch_day``."""
    out = []
    for m in mc:
        f = factor_before if m.trade_date < switch_day else factor_after
        close = round(m.source_close * f)
        halted = (
            halted_from is not None
            and halted_to is not None
            and halted_from <= m.trade_date <= halted_to
        )
        out.append(
            OhlcvPoint(
                ticker=m.ticker,
                market=m.market,
                trade_date=m.trade_date,
                open=0 if halted else close,
                high=0 if halted else close,
                low=0 if halted else close,
                close=close,
                source_close=m.source_close,
                fetched_at=fetched_at or _ts(m.trade_date),
            )
        )
    return out


class FakeStorage:
    def __init__(self, mc: list[MarketCapPoint], bars: list[OhlcvPoint]) -> None:
        self.mc = mc
        self.bars = bars

    def get_latest_market_cap_date(self) -> date | None:
        return max((m.trade_date for m in self.mc), default=None)

    def get_market_cap_points(self, since: date, until: date) -> list[MarketCapPoint]:
        return [m for m in self.mc if since <= m.trade_date <= until]

    def get_ohlcv_points(
        self, tickers: list[str] | None, since: date, until: date
    ) -> list[OhlcvPoint]:
        return [
            b
            for b in self.bars
            if since <= b.trade_date <= until and (tickers is None or b.ticker in tickers)
        ]


# --- classification --------------------------------------------------------------------


def test_classify_reverse_split_is_mechanical() -> None:
    # 5:1 병합 — 주식 수 1/5, 가격 5배. 시총은 이어진다.
    assert classify_share_event(0.2, 5.0, 0.3) == KIND_MECHANICAL
    # 병합일 가격이 상한가(+30%)까지 움직여도 시총 연속성 허용 범위 안이다.
    assert classify_share_event(0.2, 5.0 * 1.25, 0.3) == KIND_MECHANICAL


def test_classify_capital_raise_is_not_mechanical() -> None:
    # 주식 수 2배, 가격은 그대로 — 시장가 신주 발행이다. 가격 재수집이 필요 없다.
    assert classify_share_event(2.0, 1.01, 0.3) == KIND_CAPITAL_RAISE


def test_classify_ambiguous_when_neither_band_fits() -> None:
    # 주식 수 3배인데 가격은 2배 — 시총이 6배로 뛰었다. 어느 쪽도 아니다.
    assert classify_share_event(3.0, 2.0, 0.3) == KIND_AMBIGUOUS


# --- share events ----------------------------------------------------------------------


def test_detect_share_events_filters_small_changes_and_old_days() -> None:
    mc = _mc("A", 1000, 200, 100, 500)  # 사건 1건
    mc += _mc("B", 1000, 1100, 100, 91)  # +10% — 문턱(1.5배) 아래
    events = detect_share_events(mc, since=SPLIT - timedelta(days=3))
    assert [(e.ticker, e.trade_date, e.kind) for e in events] == [("A", SPLIT, KIND_MECHANICAL)]
    assert events[0].k == pytest.approx(0.2)
    assert events[0].p == pytest.approx(5.0)
    assert detect_share_events(mc, since=SPLIT + timedelta(days=1)) == []


def test_detect_share_events_skips_unusable_rows_as_predecessor() -> None:
    mc = _mc("A", 1000, 200, 100, 500)
    # 사건 전날 행이 깨져 있으면(주식 수 0) 그 행은 버리고 그 전 행과 비교한다.
    broken = [m for m in mc if m.trade_date == SESSIONS[19]][0]
    mc[mc.index(broken)] = MarketCapPoint("A", "KOSDAQ", broken.trade_date, 0, 100)
    events = detect_share_events(mc, since=SPLIT)
    assert len(events) == 1
    assert events[0].prev_trade_date == SESSIONS[18]


# --- vintage assessment ----------------------------------------------------------------


def _event(k: float = 0.2, p: float = 5.0, fetched_at: datetime | None = None) -> ShareEvent:
    return ShareEvent("A", "KOSDAQ", SPLIT, SESSIONS[19], k, p, KIND_MECHANICAL, fetched_at)


def test_assess_stale_when_rows_before_event_are_on_old_basis() -> None:
    mc = _mc("A", 1000, 200, 100, 500)
    # 낡은 시계열: 사건 전후 모두 R=1 (조정이 소급되지 않았다). 맞는 값은 사건 전 R=5.
    bars = _bars("A", mc, factor_before=1.0)
    result = assess_vintage(_event(fetched_at=_ts(SPLIT, 20)), bars)
    assert result.status == STATUS_STALE
    assert result.bad_bars == result.checked_bars > 0
    assert result.bad_first == SESSIONS[0]
    assert result.bad_last == SESSIONS[19]


def test_assess_resolved_when_rows_before_event_are_rebased() -> None:
    mc = _mc("A", 1000, 200, 100, 500)
    bars = _bars("A", mc, factor_before=5.0)  # 사건 전 R = 1/k = 5
    result = assess_vintage(_event(), bars)
    assert result.status == STATUS_RESOLVED
    assert result.bad_bars == 0


def test_assess_refetched_unresolved_when_bad_rows_were_written_after_the_event() -> None:
    mc = _mc("A", 1000, 200, 100, 500)
    # 003060형: 재수집이 끝난 뒤(사건 시총 적재 이후)에 쓰인 행인데도 조정이 안 됐다.
    bars = _bars("A", mc, factor_before=1.0, fetched_at=_ts(SPLIT, 23) + timedelta(days=3))
    result = assess_vintage(_event(fetched_at=_ts(SPLIT, 20)), bars)
    assert result.status == STATUS_REFETCHED_UNRESOLVED


def test_assess_counts_halted_rows_and_takes_base_after_the_halt() -> None:
    # 정지 표식(open=high=low=0) 행도 R 은 의미가 있다.
    # 사건일 행이 정지 표식이면 기준은 그 뒤 첫 쓸 수 있는 행에서 잡는다.
    mc = _mc("A", 1000, 200, 100, 500)
    bars = _bars(
        "A",
        mc,
        factor_before=1.0,
        halted_from=SESSIONS[15],
        halted_to=SPLIT,
    )
    result = assess_vintage(_event(), bars)
    assert result.status == STATUS_STALE
    assert result.bad_last == SESSIONS[19]


def test_assess_unknown_without_bars() -> None:
    assert assess_vintage(_event(), []).status == STATUS_UNKNOWN
    mc = _mc("A", 1000, 200, 100, 500)
    only_after = [b for b in _bars("A", mc, factor_before=1.0) if b.trade_date >= SPLIT]
    assert assess_vintage(_event(), only_after).status == STATUS_UNKNOWN


def test_assess_ignores_a_single_odd_row_and_stops_at_previous_event() -> None:
    mc = _mc("A", 1000, 200, 100, 500)
    bars = _bars("A", mc, factor_before=5.0)
    odd = bars[3]
    bars[3] = OhlcvPoint(odd.ticker, odd.market, odd.trade_date, 1, 1, 1, 1, odd.source_close)
    assert assess_vintage(_event(), bars).status == STATUS_RESOLVED  # 한 행은 글리치
    # 직전 사건 이후만 본다: 낡은 구간이 floor 이전이면 사건 앞 구간만 점검한다.
    stale = _bars("A", mc, factor_before=1.0)
    result = assess_vintage(_event(), stale, floor_date=SESSIONS[12])
    assert result.checked_bars == 8  # floor 날(포함)부터 사건 전날까지의 세션
    assert result.status == STATUS_STALE


# --- rebase steps (early signal) --------------------------------------------------------


def test_rebase_step_without_share_event_is_flagged_and_event_day_is_not() -> None:
    mc = _mc("H", 1000, 1000, 100, 100)  # 정지 중이라 KRX 는 그대로
    # naver 가 정지 시작일에 먼저 5배로 다시 쓴 시계열: 그날부터 R=5 (이전 행은 R=1).
    switch = SESSIONS[10]
    bars = _bars("H", mc, factor_before=1.0, factor_after=5.0, switch_day=switch)
    steps = detect_rebase_steps(bars, since=SESSIONS[5])
    assert [(s.trade_date, round(s.rho, 2), s.status) for s in steps] == [
        (switch, 5.0, STATUS_STALE)
    ]
    # 같은 날 주식 수 사건이 있으면 R 이 움직이는 게 맞다 — 설명된 단계로 뺀다.
    assert (
        detect_rebase_steps(bars, since=SESSIONS[5], explained_days={("H", "KOSDAQ", switch)}) == []
    )
    # 창 시작 전 단계는 보지 않는다.
    assert detect_rebase_steps(bars, since=SESSIONS[11]) == []


def test_rebase_step_after_refetch_is_refetched_unresolved() -> None:
    mc = _mc("H", 1000, 1000, 100, 100)
    switch = SESSIONS[10]
    bars = _bars("H", mc, factor_before=1.0, factor_after=5.0, switch_day=switch)
    one_pass = _ts(SESSIONS[25], 10)
    bars = [
        OhlcvPoint(
            b.ticker,
            b.market,
            b.trade_date,
            b.open,
            b.high,
            b.low,
            b.close,
            b.source_close,
            one_pass,
        )
        for b in bars
    ]
    (step,) = detect_rebase_steps(bars, since=SESSIONS[5])
    assert step.status == STATUS_REFETCHED_UNRESOLVED


# --- end to end ------------------------------------------------------------------------


def _scenario() -> tuple[list[MarketCapPoint], list[OhlcvPoint]]:
    # A: 사건 뒤 증분만 붙어 낡은 시계열 -> 대상
    mc_a = _mc("A", 1000, 200, 100, 500)
    bars_a = _bars("A", mc_a, factor_before=1.0)
    # B: 이미 재수집해 맞춰진 시계열 -> 대상 아님
    mc_b = _mc("B", 1000, 200, 100, 500)
    bars_b = _bars("B", mc_b, factor_before=5.0)
    # C: 유상증자(주식 2배, 가격 그대로) -> 기계적 사건이 아니다
    mc_c = _mc("C", 1000, 2000, 100, 101)
    bars_c = _bars("C", mc_c, factor_before=1.0)
    # D: 정지 중 naver 가 먼저 5배로 고침(주식 수 사건 없음) -> R 단계로 잡힌다
    mc_d = _mc("D", 1000, 1000, 100, 100)
    bars_d = _bars("D", mc_d, factor_before=1.0, factor_after=5.0, switch_day=SESSIONS[27])
    return mc_a + mc_b + mc_c + mc_d, bars_a + bars_b + bars_c + bars_d


def test_build_report_picks_only_stale_tickers() -> None:
    mc, bars = _scenario()
    report = build_trigger_report(FakeStorage(mc, bars), TriggerConfig(as_of=AS_OF))
    assert report.market_cap_latest_date == SESSIONS[-1]
    assert report.targets == ["A", "D"]
    by_ticker = {e.event.ticker: e.status for e in report.events}
    assert by_ticker == {"A": STATUS_STALE, "B": STATUS_RESOLVED}
    assert [s.ticker for s in report.steps] == ["D"]
    assert report.share_events == 3  # A, B, C 의 주식 수 변화 (C 는 기계적이 아니라 제외)


def test_build_report_is_empty_once_the_series_are_fixed() -> None:
    mc, _ = _scenario()
    mc_a = [m for m in mc if m.ticker == "A"]
    mc_d = [m for m in mc if m.ticker == "D"]
    # 재수집 뒤: 과거 행이 새 기준(R=5)으로 다시 쓰여 두 시계열 모두 한 기준이다.
    fixed = _bars("A", mc_a, factor_before=5.0) + _bars(
        "D", mc_d, factor_before=5.0, factor_after=5.0
    )
    report = build_trigger_report(FakeStorage(mc, fixed), TriggerConfig(as_of=AS_OF))
    assert report.targets == []


def test_build_report_window_drops_old_events() -> None:
    mc, bars = _scenario()
    # 사건이 창(2주) 밖이면 새로 보지 않는다. 낡은 시계열이어도 이 주에는 대상이 아니다.
    late = TriggerConfig(as_of=SESSIONS[45], lookback_days=7)
    report = build_trigger_report(FakeStorage(mc, bars), late)
    assert report.events == ()
    assert report.targets == []  # D 의 R 단계도 창 밖이다


def test_write_outputs(tmp_path: Path) -> None:
    mc, bars = _scenario()
    report = build_trigger_report(FakeStorage(mc, bars), TriggerConfig(as_of=AS_OF))
    day_dir = write_trigger_outputs(report, tmp_path)
    assert day_dir == tmp_path / AS_OF.isoformat()
    assert (day_dir / "targets.txt").read_text() == "A\nD\n"
    with (day_dir / "events.csv").open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert {(r["trigger"], r["ticker"], r["status"]) for r in rows} == {
        ("shares", "A", STATUS_STALE),
        ("shares", "B", STATUS_RESOLVED),
        ("rebase_step", "D", STATUS_STALE),
    }
    summary = json.loads((day_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["schema"] == "kr_price_vintage_trigger/v1"
    assert summary["target_tickers"] == 2
    assert summary["rebase_steps"] == 1
    assert summary["params"]["lookback_days"] == 14
    # 같은 as_of 로 다시 쓰면 덮어쓴다(누적되지 않는다).
    write_trigger_outputs(report, tmp_path)
    assert (day_dir / "targets.txt").read_text() == "A\nD\n"

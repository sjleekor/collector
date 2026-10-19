"""Regular-session correction: round(KRX x naver_open / KRX_open) for high/low/close.

Synthetic fixtures only. They mirror the shapes measured on 2026-10-19: no-adjustment days
(f = 1), an adjusted history (f != 1, naver prices already restated for a split) and the
rounding noise on a day's open that the isolated-outlier rule has to absorb.
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date, datetime, timedelta
from fractions import Fraction

import pytest

from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.models import FixPlan, IngestionRun, KrxSessionRow, StoredOhlc
from collector.kr.service.fix_regular_session import (
    FACTOR_OWN,
    FACTOR_PREV_DAY,
    REGULAR_SESSION_FIX_FROM,
    SKIP_KRX_INVALID,
    SKIP_LAST_DAY_OUTLIER,
    SKIP_NO_KRX_ROW,
    SKIP_STALE_READ,
    fix_regular_session,
    plan_fixes,
    resolve_factors,
    resolve_window,
)

D0 = REGULAR_SESSION_FIX_FROM  # Monday 2026-09-14
DAYS = [D0 + timedelta(days=i) for i in range(30) if (D0 + timedelta(days=i)).weekday() < 5]


def _stored(day: date, o: int, h: int, low: int, c: int, ticker: str = "005930") -> StoredOhlc:
    return StoredOhlc(ticker, "KOSPI", day, o, h, low, c)


def _krx(
    day: date,
    o: int | None,
    h: int | None,
    low: int | None,
    c: int | None,
    ticker: str = "005930",
) -> KrxSessionRow:
    return KrxSessionRow(ticker, "KOSPI", day, o, h, low, c)


class FakeStorage:
    """In-memory FixStorage with the same optimistic UPDATE and audit-dedupe semantics."""

    def __init__(self, stored: list[StoredOhlc], krx: list[KrxSessionRow]) -> None:
        self.ohlc = {(r.ticker, r.market, r.trade_date): r for r in stored}
        self.krx = list(krx)
        self.audit: list[dict] = []
        self.runs: list[IngestionRun] = []
        self.before_apply = None  # hook to simulate a concurrent naver overwrite

    def get_stored_ohlc(self, since: date, until: date) -> list[StoredOhlc]:
        return [r for r in self.ohlc.values() if since <= r.trade_date <= until]

    def get_market_cap_session_rows(self, since: date, until: date) -> list[KrxSessionRow]:
        return [r for r in self.krx if since <= r.trade_date <= until]

    def get_latest_ohlcv_trade_dates(self, limit: int) -> list[date]:
        return sorted({r.trade_date for r in self.ohlc.values()}, reverse=True)[:limit]

    def apply_regular_session_fixes(
        self, plans: list[FixPlan], run_id: uuid.UUID, applied_at: datetime, krx_source: str
    ) -> tuple[list[tuple[str, str, date]], int]:
        if self.before_apply:
            self.before_apply(self)
        updated, new_audit = [], 0
        for p in plans:
            cur = self.ohlc[p.key]
            if (cur.high, cur.low, cur.close) != (p.old_high, p.old_low, p.old_close):
                continue
            self.ohlc[p.key] = StoredOhlc(
                cur.ticker, cur.market, cur.trade_date, cur.open, p.new_high, p.new_low, p.new_close
            )
            updated.append(p.key)
            ident = (p.key, p.old_high, p.old_low, p.old_close, p.new_high, p.new_low, p.new_close)
            if all(a["ident"] != ident for a in self.audit):
                self.audit.append(
                    {"ident": ident, "run_id": run_id, "plan": p, "krx_source": krx_source}
                )
                new_audit += 1
        return updated, new_audit

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(replace(run))  # snapshot: the service mutates one object


def _series(
    n: int = 6, factor: Fraction = Fraction(1), ticker: str = "005930"
) -> tuple[list[StoredOhlc], list[KrxSessionRow]]:
    stored, krx = [], []
    for i, day in enumerate(DAYS[:n]):
        ko = 1000 * (1 + i)
        kh, kl, kc = ko + 50, ko - 30, ko + 20
        krx.append(_krx(day, ko, kh, kl, kc, ticker))
        stored.append(
            _stored(
                day,
                round(ko * factor),
                round(kh * factor) + 7,
                round(kl * factor),
                round(kc * factor) + 9,
                ticker,
            )
        )
    return stored, krx


START, END = DAYS[0], DAYS[5]


def test_no_adjustment_day_uses_krx_values_directly() -> None:
    stored, krx = _series(6)
    report = plan_fixes(stored, krx, START, END)
    assert len(report.plans) == 6 and not report.skipped
    p = report.plans[0]
    assert (p.new_high, p.new_low, p.new_close) == (1050, 970, 1020)
    assert p.factor == 1.0 and p.factor_basis == FACTOR_OWN
    assert (p.old_high, p.old_close) == (1057, 1029)


def test_adjusted_history_scales_by_naver_open_over_krx_open() -> None:
    # naver restated the whole history by 1/4 (4:1 reverse split in reverse: prices x 0.25).
    stored, krx = _series(6, factor=Fraction(1, 4))
    report = plan_fixes(stored, krx, START, END)
    assert report.plans, "contaminated rows must be planned"
    for p, day_krx in zip(report.plans, krx, strict=True):
        assert p.factor == pytest.approx(0.25)
        # ties round up: 1050 x 0.25 = 262.5 -> 263 (Python's round() would give 262)
        assert p.new_close == int(day_krx.close * 0.25 + 0.5)
        assert p.new_high == int(day_krx.high * 0.25 + 0.5)


def test_open_and_volume_are_never_written() -> None:
    stored, krx = _series(6)
    storage = FakeStorage(stored, krx)
    before = {k: v.open for k, v in storage.ohlc.items()}
    fix_regular_session(storage, START, END)
    assert {k: v.open for k, v in storage.ohlc.items()} == before
    assert not hasattr(FixPlan, "new_open") and not hasattr(FixPlan, "new_volume")


def test_isolated_open_outlier_takes_previous_day_factor() -> None:
    stored, krx = _series(6)
    # Day 3: naver open is off by 1% (rounding/adjustment noise) -> f = 1.01, both neighbours 1.0.
    bad = stored[3]
    stored[3] = _stored(bad.trade_date, round(bad.open * 1.01), bad.high, bad.low, bad.close)
    report = plan_fixes(stored, krx, START, END)
    by_day = {p.trade_date: p for p in report.plans}
    assert by_day[DAYS[3]].factor_basis == FACTOR_PREV_DAY
    assert by_day[DAYS[3]].factor == 1.0
    assert by_day[DAYS[3]].new_close == krx[3].close  # not scaled by 1.01
    assert by_day[DAYS[2]].factor_basis == FACTOR_OWN


def test_two_consecutive_off_days_are_not_isolated() -> None:
    f = [Fraction(1)] * 2 + [Fraction(101, 100)] * 2 + [Fraction(1)] * 2
    series = [(DAYS[i], x) for i, x in enumerate(f)]
    resolved = resolve_factors(series)
    assert all(v is not None and v[1] == FACTOR_OWN for v in resolved.values())


def test_last_day_outlier_is_skipped_and_recorded() -> None:
    stored, krx = _series(6)
    bad = stored[5]
    stored[5] = _stored(bad.trade_date, round(bad.open * 1.01), bad.high, bad.low, bad.close)
    storage = FakeStorage(stored, krx)
    report, run = fix_regular_session(storage, START, END)
    assert [s.reason for s in report.skipped] == [SKIP_LAST_DAY_OUTLIER]
    assert report.skipped[0].trade_date == DAYS[5]
    assert storage.ohlc[("005930", "KOSPI", DAYS[5])].close == bad.close  # untouched
    assert run.counts["skipped"] == {SKIP_LAST_DAY_OUTLIER: 1}
    assert run.params["skipped_examples"][0]["reason"] == SKIP_LAST_DAY_OUTLIER


def test_last_day_with_following_row_in_context_is_not_last() -> None:
    # The window ends at DAYS[4], but DAYS[5] exists as context: day 4 is judged against it.
    stored, krx = _series(6)
    bad = stored[4]
    stored[4] = _stored(bad.trade_date, round(bad.open * 1.01), bad.high, bad.low, bad.close)
    report = plan_fixes(stored, krx, START, DAYS[4])
    p = next(p for p in report.plans if p.trade_date == DAYS[4])
    assert p.factor_basis == FACTOR_PREV_DAY
    assert not report.skipped


def test_missing_and_zero_krx_rows_are_skipped_not_fixed() -> None:
    stored, krx = _series(6)
    del krx[1]  # no KRX row (e.g. day T before the one-day lag catches up)
    krx[1] = _krx(DAYS[2], 3000, 0, 2970, 3020)  # zero high -> invalid
    krx[2] = _krx(DAYS[3], None, None, None, None)  # NULL columns (pykrx-path row)
    report = plan_fixes(stored, krx, START, END)
    reasons = {(s.trade_date, s.reason) for s in report.skipped}
    assert reasons == {
        (DAYS[1], SKIP_NO_KRX_ROW),
        (DAYS[2], SKIP_KRX_INVALID),
        (DAYS[3], SKIP_KRX_INVALID),
    }
    assert {p.trade_date for p in report.plans} == {DAYS[0], DAYS[4], DAYS[5]}


def test_rows_before_cutoff_are_not_touched_by_default() -> None:
    early = D0 - timedelta(days=7)
    stored = [_stored(early, 1000, 1100, 900, 1050)]
    krx = [_krx(early, 1000, 1080, 900, 1040)]
    storage = FakeStorage(stored, krx)
    report, _ = fix_regular_session(storage, early, early)
    assert not report.plans and storage.ohlc[("005930", "KOSPI", early)].close == 1050
    report, _ = fix_regular_session(storage, early, early, allow_before_cutoff=True)
    assert storage.ohlc[("005930", "KOSPI", early)].close == 1040


def test_idempotent_and_survives_naver_overwrite_reapply() -> None:
    stored, krx = _series(6, factor=Fraction(1, 4))
    storage = FakeStorage(stored, krx)
    first, _ = fix_regular_session(storage, START, END)
    assert first.applied == 6 and first.audit_rows_new == 6
    snapshot = dict(storage.ohlc)

    second, run2 = fix_regular_session(storage, START, END)
    assert second.applied == 0 and second.unchanged == 6 and not second.plans
    assert storage.ohlc == snapshot
    assert len(storage.audit) == 6  # nothing new logged

    # 18:30 naver job overwrites again; the 20:00+ fix restores the same values and the
    # identical old -> new is not logged a second time.
    storage.ohlc = {(r.ticker, r.market, r.trade_date): r for r in stored}
    third, _ = fix_regular_session(storage, START, END)
    assert third.applied == 6 and third.audit_rows_new == 0
    assert {k: (v.high, v.low, v.close) for k, v in storage.ohlc.items()} == {
        k: (v.high, v.low, v.close) for k, v in snapshot.items()
    }
    assert len(storage.audit) == 6


def test_audit_rows_record_old_new_and_evidence() -> None:
    stored, krx = _series(6)
    storage = FakeStorage(stored, krx)
    _, run = fix_regular_session(storage, START, END)
    row = next(a for a in storage.audit if a["plan"].trade_date == DAYS[0])
    p = row["plan"]
    assert (p.old_high, p.old_low, p.old_close) == (1057, 970, 1029)
    assert (p.new_high, p.new_low, p.new_close) == (1050, 970, 1020)
    assert (p.krx_open, p.krx_high, p.krx_low, p.krx_close) == (1000, 1050, 970, 1020)
    assert p.naver_open == 1000 and p.factor == 1.0
    assert row["run_id"] == run.run_id and row["krx_source"] == "daily_market_cap"


def test_concurrent_overwrite_is_left_for_the_next_run() -> None:
    stored, krx = _series(6)
    storage = FakeStorage(stored, krx)

    def overwrite(s: FakeStorage) -> None:
        key = ("005930", "KOSPI", DAYS[2])
        cur = s.ohlc[key]
        s.ohlc[key] = StoredOhlc(cur.ticker, cur.market, cur.trade_date, cur.open, 1, 1, 1)

    storage.before_apply = overwrite
    report, run = fix_regular_session(storage, START, END)
    assert report.applied == 5
    assert [s.reason for s in report.skipped] == [SKIP_STALE_READ]
    assert storage.ohlc[("005930", "KOSPI", DAYS[2])].close == 1  # not clobbered


def test_dry_run_writes_nothing_and_records_no_run() -> None:
    stored, krx = _series(6)
    storage = FakeStorage(stored, krx)
    before = dict(storage.ohlc)
    report, _ = fix_regular_session(storage, START, END, dry_run=True)
    assert len(report.plans) == 6 and report.applied == 0
    assert storage.ohlc == before and not storage.audit and not storage.runs


def test_run_is_recorded_with_counts_and_type() -> None:
    stored, krx = _series(6)
    storage = FakeStorage(stored, krx)
    fix_regular_session(storage, START, END)
    assert [r.status for r in storage.runs] == [RunStatus.RUNNING, RunStatus.SUCCESS]
    final = storage.runs[-1]
    assert final.run_type is RunType.REGULAR_SESSION_FIX
    assert final.counts["applied"] == 6 and final.counts["planned"] == 6
    assert final.params["krx_source"] == "daily_market_cap"


def test_pluggable_source_is_used() -> None:
    stored, _ = _series(6)
    storage = FakeStorage(stored, [])  # daily_market_cap empty

    class Fixed:
        name = "same_day_krx"

        def get_session_rows(self, since: date, until: date) -> list[KrxSessionRow]:
            return _series(6)[1]

    report, run = fix_regular_session(storage, START, END, source=Fixed())
    assert report.applied == 6 and run.params["krx_source"] == "same_day_krx"


def test_ticker_series_are_independent() -> None:
    s1, k1 = _series(6, ticker="005930")
    s2, k2 = _series(6, factor=Fraction(1, 2), ticker="000660")
    report = plan_fixes(s1 + s2, k1 + k2, START, END)
    factors = {(p.ticker, p.factor) for p in report.plans}
    assert factors == {("005930", 1.0), ("000660", 0.5)}


def test_resolve_window() -> None:
    stored, krx = _series(6)
    storage = FakeStorage(stored, krx)
    assert resolve_window(storage, None, None, 3) == (DAYS[3], DAYS[5])
    assert resolve_window(storage, DAYS[0], DAYS[1], None) == (DAYS[0], DAYS[1])
    with pytest.raises(ValueError):
        resolve_window(storage, DAYS[0], None, 3)
    with pytest.raises(ValueError):
        resolve_window(storage, None, None, None)
    assert resolve_window(FakeStorage([], []), None, None, 3) is None

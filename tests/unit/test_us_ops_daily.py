"""상시 운영 하루 실행 (04 C8). 네트워크 없이 돈다 — 할 일 계산만 본다."""

from __future__ import annotations

import datetime as dt
import os

import pytest

from collector.lake import DataRoot
from collector.us import calendars
from collector.us.ops import daily
from collector.us.sources import finra, nasdaq
from collector.us.store.writer import latest_snapshot, snapshot_path


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


def _calendar(root: DataRoot, snapshot_date: str, start: str, end: str) -> None:
    calendars.load_trading_calendar(
        root, snapshot_date=snapshot_date, start=start, end=end
    )


# --- 스냅샷 고르기 -----------------------------------------------------------


def test_latest_snapshot_picks_the_newest_partition(tmp_path):
    """오늘 날짜로 찾으면 늘 없다 — 캘린더는 매일 다시 굳히지 않는다."""
    root = _lake(tmp_path)
    assert latest_snapshot(root, "trading_calendar") is None
    for day in ("2026-01-05", "2026-09-19", "2026-03-02"):
        p = snapshot_path(root, "trading_calendar", day)
        p.parent.mkdir(parents=True)
        p.write_bytes(b"x")
    assert latest_snapshot(root, "trading_calendar").parent.name == "snapshot_date=2026-09-19"


def test_sessions_through_says_what_to_do_when_the_calendar_is_missing(tmp_path):
    with pytest.raises(FileNotFoundError, match="us-calendar build"):
        daily.sessions_through(_lake(tmp_path), until=dt.date(2026, 9, 19))


# --- 할 일 계산 --------------------------------------------------------------


def test_daily_counts_only_days_it_does_not_have(tmp_path):
    """할 일을 일정이 아니라 raw/ 에 무엇이 있나로 만든다 (04 §2.4)."""
    root = _lake(tmp_path)
    _calendar(root, "2026-09-19", "2026-09-01", "2026-09-30")

    # 9/1~9/8 은 이미 받아 뒀다고 두고, 나머지가 할 일로 잡히는지 본다
    for day in (dt.date(2026, 9, 1), dt.date(2026, 9, 2), dt.date(2026, 9, 3)):
        for path in (nasdaq.earnings_path(root, day), finra.regsho_path(root, day)):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}")

    result = daily.run_daily(
        root,
        snapshot_date="2026-09-19",
        today=dt.date(2026, 9, 9),
        dry_run=True,
        sources=("nasdaq_earnings", "finra_regsho"),
    )
    assert result["until"] == dt.date(2026, 9, 8)
    by = {s["name"]: s for s in result["sources"]}
    # 9/1~9/8 중 거래일은 9/1,2,3,4,8 다섯 (9/7 은 노동절)
    assert by["nasdaq_earnings"]["skipped"] == 3
    assert by["nasdaq_earnings"]["pending"] == 2
    assert by["finra_regsho"]["pending"] == 2


def test_daily_ignores_days_before_a_source_starts(tmp_path):
    """캘린더는 2011년부터인데 실적 캘린더는 2018-07부터다."""
    root = _lake(tmp_path)
    _calendar(root, "2026-09-19", "2011-01-01", "2011-12-31")
    result = daily.run_daily(
        root,
        snapshot_date="2026-09-19",
        today=dt.date(2011, 12, 31),
        dry_run=True,
        sources=("nasdaq_earnings", "finra_regsho"),
    )
    assert all(s["pending"] == 0 for s in result["sources"])


def test_daily_rejects_an_unknown_source(tmp_path):
    root = _lake(tmp_path)
    _calendar(root, "2026-09-19", "2026-09-01", "2026-09-30")
    with pytest.raises(ValueError, match="모르는 원천"):
        daily.run_daily(
            root, snapshot_date="2026-09-19", today=dt.date(2026, 9, 9),
            dry_run=True, sources=("nope",),
        )


def test_weekly_macro_uses_the_age_of_the_newest_snapshot(tmp_path):
    """오늘 날짜 경로로 찾으면 주 1회가 매일 1회가 된다."""
    root = _lake(tmp_path)
    _calendar(root, "2026-09-19", "2026-09-01", "2026-09-30")
    for table in ("macro_series", "index_constituents"):
        p = snapshot_path(root, table, "2026-09-14")
        p.parent.mkdir(parents=True)
        p.write_bytes(b"x")
        os.utime(p, (0, dt.datetime(2026, 9, 14).timestamp()))

    fresh = daily.run_weekly_macro(root, today=dt.date(2026, 9, 18), snapshot_date="2026-09-19",
                                  dry_run=True)
    assert fresh.pending == 0 and fresh.skipped == 2

    stale = daily.run_weekly_macro(root, today=dt.date(2026, 9, 25), snapshot_date="2026-09-25",
                                   dry_run=True)
    assert stale.pending == 2


def test_last_closed_quarter_does_not_ask_for_an_open_one():
    assert daily._last_closed_quarter(dt.date(2026, 9, 20)) == (2026, 2)
    assert daily._last_closed_quarter(dt.date(2026, 1, 5)) == (2025, 4)
    assert daily._last_closed_quarter(dt.date(2026, 7, 1)) == (2026, 2)


def test_dolt_reports_a_missing_clone_as_not_ok(tmp_path):
    run = daily.run_dolt(_lake(tmp_path), dry_run=True)
    assert run.ok is False and len(run.missing) == 3


def test_source_run_serialises():
    run = daily.SourceRun("x", fetched=1, pending=2, note="n")
    assert run.as_dict() == {
        "name": "x", "fetched": 1, "skipped": 0, "missing": [],
        "pending": 2, "ok": True, "note": "n",
    }


# --- 스냅샷 보존 (04 C8 · 05 §5.1) -------------------------------------------


def _calendar_snapshot(root: DataRoot, snapshot_date: str, end: str) -> None:
    calendars.load_trading_calendar(
        root, snapshot_date=snapshot_date, start="2026-09-01", end=end
    )


def test_prune_drops_a_snapshot_whose_content_did_not_change(tmp_path):
    """`observed_at`이 달라 파일은 다르다. **내용으로 봐야 한다.**"""
    from collector.us.ops import retention

    root = _lake(tmp_path)
    _calendar_snapshot(root, "2026-09-10", "2026-09-30")
    _calendar_snapshot(root, "2026-09-11", "2026-09-30")  # 내용 같음
    _calendar_snapshot(root, "2026-09-12", "2026-10-31")  # 내용 다름
    _calendar_snapshot(root, "2026-09-13", "2026-10-31")  # 마지막 — 안 지운다

    a, b = retention.snapshot_paths(root, "trading_calendar")[:2]
    assert a.read_bytes() != b.read_bytes()  # observed_at 때문에 바이트는 다르다
    assert retention.fingerprint(a, "trading_calendar") == retention.fingerprint(
        b, "trading_calendar"
    )

    dry = retention.prune_unchanged(root, "trading_calendar")
    assert dry["removed"] == ["snapshot_date=2026-09-11"]
    assert len(retention.snapshot_paths(root, "trading_calendar")) == 4  # 안 지웠다

    applied = retention.prune_unchanged(root, "trading_calendar", dry_run=False)
    assert applied["removed"] == ["snapshot_date=2026-09-11"]
    left = [p.parent.name for p in retention.snapshot_paths(root, "trading_calendar")]
    assert left == [
        "snapshot_date=2026-09-10",
        "snapshot_date=2026-09-12",
        "snapshot_date=2026-09-13",
    ]


def test_prune_never_touches_the_only_snapshot(tmp_path):
    from collector.us.ops import retention

    root = _lake(tmp_path)
    _calendar_snapshot(root, "2026-09-10", "2026-09-30")
    r = retention.prune_unchanged(root, "trading_calendar", dry_run=False)
    assert r["removed"] == [] and r["kept"] == 1

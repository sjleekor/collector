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


def test_sec_13f_is_a_known_source():
    assert "sec_13f" in daily.SOURCES


def test_run_sec_13f_downloads_new_periods_and_skips_known(tmp_path, monkeypatch):
    """목록에 있는 것 중 ``raw/``에 없는 기간만 받는다 — ``sec_ftd``와 같은 모양이다."""
    from collector.us.sources import sec_13f

    root = _lake(tmp_path)
    monkeypatch.setenv("SEC_USER_AGENT", "x/1 (a@b.c)")
    monkeypatch.setattr(
        sec_13f,
        "list_13f_files",
        lambda client: sec_13f.ThirteenFListing(
            files={
                "2018q4": "https://x/a.zip",
                "2019q1": "https://x/b.zip",
            },
            duplicates={},
            unparsed=[],
        ),
    )
    monkeypatch.setattr(sec_13f, "raw_periods", lambda _root: {"2018q4"})
    calls: list[str] = []
    monkeypatch.setattr(
        sec_13f,
        "download_13f",
        lambda client, root, period, url, **kw: calls.append(period) or {"period": period},
    )

    run = daily.run_sec_13f(root, budget=daily._Budget(None))
    assert run.skipped == 1
    assert run.fetched == 1
    assert calls == ["2019q1"]
    assert run.ok


def test_run_sec_13f_reports_a_missing_listing_as_not_ok(tmp_path, monkeypatch):
    from collector.us.sources import sec_13f

    root = _lake(tmp_path)
    monkeypatch.setenv("SEC_USER_AGENT", "x/1 (a@b.c)")

    def _boom(_client):
        raise sec_13f.ThirteenFError("페이지 구조가 바뀌었다")

    monkeypatch.setattr(sec_13f, "list_13f_files", _boom)
    run = daily.run_sec_13f(root, budget=daily._Budget(None))
    assert not run.ok
    assert "목록 페이지" in run.missing[0]


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


def test_weekly_macro_force_bypasses_the_age_gate(tmp_path, monkeypatch):
    from collector.us.sources import fred, sec
    from collector.us.sources import wikipedia as wp

    root = _lake(tmp_path)
    for table in ("macro_series", "index_constituents"):
        p = snapshot_path(root, table, "2026-09-14")
        p.parent.mkdir(parents=True)
        p.write_bytes(b"x")
        os.utime(p, (0, dt.datetime(2026, 9, 14).timestamp()))
    calls = []
    monkeypatch.setattr(fred, "api_key_from_env", lambda: "k")
    monkeypatch.setattr(fred, "FredClient", lambda **kw: object())
    monkeypatch.setattr(fred, "load_macro_series", lambda *a, **kw: calls.append("fred"))
    monkeypatch.setattr(sec, "user_agent_from_env", lambda: "x/1 (a@b.c)")
    monkeypatch.setattr(wp, "WikipediaClient", lambda **kw: object())
    monkeypatch.setattr(wp, "load_index_constituents", lambda *a, **kw: calls.append("wp"))

    kw = dict(today=dt.date(2026, 9, 18), snapshot_date="2026-09-19")
    plain = daily.run_weekly_macro(root, **kw)
    assert plain.fetched == 0 and calls == []
    forced = daily.run_weekly_macro(root, force=True, **kw)
    assert forced.fetched == 2 and calls == ["fred", "wp"]


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


# --- sec_quarterly: 마감 직후 분기의 404 -------------------------------------


def _fake_sec(monkeypatch, *, status=404, only=None):
    """네트워크 없이 ``download_quarterly``를 대신한다. ``only``(갈래 집합)만 실패한다."""
    from collector.us.sources import sec

    monkeypatch.setenv("SEC_USER_AGENT", "x/1 (a@b.c)")
    calls: list[tuple[str, int, int]] = []

    def _download(client, root, kind, year, quarter, **kw):
        calls.append((kind, year, quarter))
        if only is None or kind in only:
            raise sec.SecAccessError(f"{status} https://x/{kind}", status_code=status)
        path = sec.quarterly_path(root, kind, year, quarter)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"zip")
        return {}

    monkeypatch.setattr(sec, "download_quarterly", _download)
    # 마감 분기 하나만 남기고 나머지는 이미 받은 것으로 둔다
    return calls


def _have_all_but_last(root, today):
    from collector.us.sources import sec

    last = daily._last_closed_quarter(today)
    for kind in sec.QUARTERLY_KINDS:
        for y, q in sec.quarters((2018, 3), last):
            if (y, q) != last:
                p = sec.quarterly_path(root, kind, y, q)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(b"zip")


def test_quarter_end_is_the_last_calendar_day():
    assert daily._quarter_end(2026, 3) == dt.date(2026, 9, 30)
    assert daily._quarter_end(2025, 4) == dt.date(2025, 12, 31)
    assert daily._quarter_end(2026, 1) == dt.date(2026, 3, 31)


def test_sec_quarterly_404_within_grace_is_not_a_failure(tmp_path, monkeypatch):
    """마감 5일 뒤의 2026q3 404 셋은 발표 전이다 — 실패가 아니다 (2026-10-01 이후 매일 exit 1)."""
    root = _lake(tmp_path)
    today = dt.date(2026, 10, 5)
    _have_all_but_last(root, today)
    _fake_sec(monkeypatch)

    run = daily.run_sec_quarterly(root, today=today)
    assert run.ok is True
    assert run.missing == []
    assert run.fetched == 0
    assert run.pending == 0  # 발표 전인 것은 "예산이 남았다"가 아니다
    for kind in ("financial", "insider", "midas"):
        assert f"{kind} 2026q3" in run.note
    assert run.note.startswith("발표 전인 분기")


def test_sec_quarterly_404_past_grace_fails_with_the_old_message(tmp_path, monkeypatch):
    """마감 92일(2026-12-31): FSDS·내부자는 유예(90일) 밖이라 실패, MIDAS(300일)는 발표 전이다."""
    root = _lake(tmp_path)
    today = dt.date(2026, 12, 31)
    assert daily._last_closed_quarter(today) == (2026, 3)
    assert (today - dt.date(2026, 9, 30)).days == 92
    _have_all_but_last(root, today)
    _fake_sec(monkeypatch)

    run = daily.run_sec_quarterly(root, today=today)
    assert run.ok is False
    assert sorted(m.split(":")[0] for m in run.missing) == [
        "financial 2026q3",
        "insider 2026q3",
    ]
    assert all("404" in m for m in run.missing)
    assert "midas 2026q3" in run.note and "financial" not in run.note


def test_sec_quarterly_midas_grace_ends_after_300_days(tmp_path, monkeypatch):
    """2025q1 MIDAS는 289일 뒤에 나왔다 — 300일까지는 기다리고 그 뒤는 실패다."""
    from collector.us.sources import sec

    today = dt.date(2025, 3, 31) + dt.timedelta(days=301)  # 2026-01-26, 마감 분기는 2025q4
    root = _lake(tmp_path)
    last = daily._last_closed_quarter(today)
    for kind in sec.QUARTERLY_KINDS:
        for y, q in sec.quarters((2018, 3), last):
            if not (kind == "midas" and (y, q) == (2025, 1)):
                p = sec.quarterly_path(root, kind, y, q)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(b"zip")
    _fake_sec(monkeypatch, only={"midas"})

    run = daily.run_sec_quarterly(root, today=today)
    assert run.ok is False
    assert [m.split(":")[0] for m in run.missing] == ["midas 2025q1"]
    assert run.note == ""


def test_sec_quarterly_non_404_is_always_a_failure(tmp_path, monkeypatch):
    """403·서버 오류는 마감 직후여도 실패다 — 발표 전과 다르다."""
    root = _lake(tmp_path)
    today = dt.date(2026, 10, 5)
    _have_all_but_last(root, today)
    _fake_sec(monkeypatch, status=403)

    run = daily.run_sec_quarterly(root, today=today)
    assert run.ok is False
    assert len(run.missing) == 3 and run.note == ""


def test_sec_quarterly_grace_does_not_touch_other_kinds(tmp_path, monkeypatch):
    """404가 한 갈래에만 와도 다른 갈래의 정상 다운로드는 그대로 센다."""
    root = _lake(tmp_path)
    today = dt.date(2026, 10, 5)
    _have_all_but_last(root, today)
    _fake_sec(monkeypatch, only={"midas"})

    run = daily.run_sec_quarterly(root, today=today)
    assert run.ok is True and run.fetched == 2
    assert "midas 2026q3" in run.note and "financial" not in run.note


def test_daily_summary_ok_stays_true_when_only_unpublished(tmp_path, monkeypatch):
    """run_daily의 최상위 ``ok``(=exit code)가 발표 전 404로 내려가지 않는다."""
    root = _lake(tmp_path)
    today = dt.date(2026, 10, 5)
    _have_all_but_last(root, today)
    _fake_sec(monkeypatch)
    _calendar(root, "2026-09-01", "2026-01-01", "2026-12-31")

    result = daily.run_daily(
        root, snapshot_date="2026-10-05", today=today, sources=("sec_quarterly",)
    )
    assert result["ok"] is True and result["pending"] == 0
    assert result["sources"][0]["note"].startswith("발표 전인 분기")

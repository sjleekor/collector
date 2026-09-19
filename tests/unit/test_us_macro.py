"""C5 — 캘린더·FRED·Wikipedia. 네트워크 없이 돈다 (04 C5)."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from collector.lake import DataRoot
from collector.us import calendars
from collector.us.sources import fred
from collector.us.sources import wikipedia as wp


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


# --- 거래일 캘린더 -----------------------------------------------------------


def test_trading_calendar_marks_early_closes(tmp_path):
    """조기 종료일을 표시한다 — 그날 거래량이 절반인 것이 이상값이 아니다."""
    root = _lake(tmp_path)
    r = calendars.load_trading_calendar(
        root, snapshot_date="2026-01-01", start="2018-11-01", end="2018-12-31"
    )
    con = duckdb.connect()
    early = con.execute(
        f"SELECT date, close_local FROM '{r['path']}' WHERE is_early_close ORDER BY date"
    ).fetchall()
    assert (dt.date(2018, 11, 23), dt.time(13, 0)) in early  # 추수감사절 다음 날
    assert (dt.date(2018, 12, 24), dt.time(13, 0)) in early
    # 휴장일은 행 자체가 없다
    dates = {d for (d,) in con.execute(f"SELECT date FROM '{r['path']}'").fetchall()}
    assert dt.date(2018, 11, 22) not in dates  # 추수감사절
    assert dt.date(2018, 12, 25) not in dates
    assert "exchange_calendars" in con.execute(
        f"SELECT source_rev FROM '{r['path']}' LIMIT 1"
    ).fetchone()[0]


# --- FRED --------------------------------------------------------------------


def test_fred_key_is_required():
    with pytest.raises(fred.FredError, match="FRED_API_KEY"):
        fred.api_key_from_env({})


def test_dedupe_folds_repeats_but_keeps_reverts():
    """끊어 받으면 같은 값이 다시 온다. 값이 되돌아간 것은 중복이 아니다."""
    d1, d2 = dt.date(2020, 1, 1), dt.date(2020, 2, 1)
    rows = [
        (dt.date(2020, 2, 10), d1, 1.0),
        (dt.date(2020, 3, 10), d1, 1.0),  # 구간 경계에서 잘려 다시 온 것
        (dt.date(2020, 4, 10), d1, 2.0),  # 개정
        (dt.date(2020, 5, 10), d1, 1.0),  # 되돌아갔다 — 남는다
        (dt.date(2020, 3, 10), d2, 5.0),
    ]
    out = fred._dedupe(rows)
    assert [(r[0], r[2]) for r in out if r[1] == d1] == [
        (dt.date(2020, 2, 10), 1.0),
        (dt.date(2020, 4, 10), 2.0),
        (dt.date(2020, 5, 10), 1.0),
    ]
    assert len([r for r in out if r[1] == d2]) == 1


def test_dedupe_handles_null_realtime_start():
    """ALFRED에 없는 series는 realtime_start가 null이다 (SP500)."""
    rows = [(None, dt.date(2020, 1, 2), 3.0), (None, dt.date(2020, 1, 1), 2.0)]
    assert [r[1] for r in fred._dedupe(rows)] == [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]


def test_series_list_covers_the_five_axes():
    axes = set(fred.FRED_SERIES.values())
    assert {"금리", "유가", "환율", "신용", "변동성"} <= axes
    # 신용 스프레드를 term spread로 대체하면 안 된다 (연구 §1)
    assert fred.FRED_SERIES["T10Y2Y"] == "금리"
    assert fred.FRED_SERIES["BAMLH0A0HYM2"] == "신용"


# --- Wikipedia ---------------------------------------------------------------


def test_clean_cell_strips_wiki_markup():
    assert wp.clean_cell("{{NyseSymbol|MMM}}") == "MMM"
    assert wp.clean_cell("[[3M|3M Company]]") == "3M Company"
    assert wp.clean_cell("[[Zoetis]]") == "Zoetis"
    assert wp.clean_cell("[https://www.sec.gov/x reports]") == "reports"
    assert wp.clean_cell("Health Care<ref name=a>x</ref>") == "Health Care"


_OLD = """
{|  class="wikitable sortable"
|-
! [[Ticker symbol]] !! Security !! [[SEC filing]]s !! GICS Sector !! GICS Sub Industry !! CIK
|-
|	{{NyseSymbol|MMM}}	||	[[3M|3M Company]]	||	[https://x r]	||	Industrials
 || Industrial Conglomerates || 0000066740
|-
|	{{NyseSymbol|ABT}}	||	[[Abbott Laboratories]]	||	[https://x r]	||	Health Care
 || Health Care Equipment || 0000001800
|}
"""

_NEW = """
{| class="wikitable sortable" id="constituents"
|-
![[Ticker symbol|Ticker Symbol]]
! Security !![[SEC filing]]s !! GICS Sector !! GICS Sub Industry !! CIK !! Founded
|-
|{{NyseSymbol|MMM}}
|[[3M|3M Company]]||[https://x r]	||	Industrials
| Industrial Conglomerates || 0000066740 || 1902
|}
"""


@pytest.mark.parametrize("text", [_OLD, _NEW])
def test_parse_constituents_handles_both_table_shapes(text):
    """머리글 이름으로 칸을 잡는다. 위치로 잡으면 해가 바뀔 때 깨진다."""
    rows = wp.parse_constituents(text)
    assert rows[0] == {
        "symbol": "MMM",
        "security": "3M Company",
        "gics_sector": "Industrials",
        "gics_sub_industry": "Industrial Conglomerates",
        "cik": "0000066740",
    }


def test_parse_constituents_returns_nothing_when_there_is_no_table():
    assert wp.parse_constituents("본문만 있고 표가 없다") == []


def test_weekly_sample_keeps_the_last_revision_of_each_week():
    revs = [
        {"revid": 1, "timestamp": "2024-01-02T00:00:00Z"},
        {"revid": 2, "timestamp": "2024-01-05T00:00:00Z"},  # 같은 주 — 이쪽이 남는다
        {"revid": 3, "timestamp": "2024-01-09T00:00:00Z"},
    ]
    assert [r["revid"] for r in wp.weekly_sample(revs)] == [2, 3]

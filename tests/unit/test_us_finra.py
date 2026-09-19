"""FINRA — 공매도 잔고·off-exchange 거래량 (04 C7). 네트워크 없이 돈다."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from collector.lake import DataRoot
from collector.us.sources import finra


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


# --- 결제일 -----------------------------------------------------------------


def _sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    d, out = start, []
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def test_settlement_dates_pull_back_to_the_prior_session():
    """2018-09-15가 토요일이라 결제일이 2018-09-14다 (01 §2.4 실측)."""
    sessions = _sessions(dt.date(2018, 8, 1), dt.date(2018, 12, 31))
    got = finra.settlement_dates(
        sessions, start=finra.SHORT_INTEREST_START, end=dt.date(2018, 10, 31)
    )
    assert got[:4] == [
        dt.date(2018, 9, 14),   # 15일이 토요일
        dt.date(2018, 9, 28),   # 9월 말일(30일)이 일요일
        dt.date(2018, 10, 15),  # 월요일 — 그대로
        dt.date(2018, 10, 31),
    ]


def test_settlement_dates_skip_a_month_that_has_not_ended():
    """아직 안 지난 기준일을 후보로 만들면 그 달 마지막 거래일이 결제일로 샌다."""
    sessions = _sessions(dt.date(2026, 9, 1), dt.date(2026, 9, 18))
    got = finra.settlement_dates(sessions, start=dt.date(2026, 9, 1), end=dt.date(2026, 9, 18))
    assert got == [dt.date(2026, 9, 15)]  # 9월 말일은 아직 안 왔다


# --- 잔고 -------------------------------------------------------------------


class _Api:
    """``record-total``을 주고 페이지로 잘라 주는 서버 흉내."""

    def __init__(self, rows: int):
        self.rows = rows
        self.calls: list[dict] = []

    def post(self, url, json=None, headers=None, **kw):
        self.calls.append(json)
        offset = json["offset"]
        n = max(0, min(finra.PAGE_LIMIT, self.rows - offset))
        if self.rows == 0:
            return _Resp(204, "", {})
        body = ['"symbolCode","settlementDate","revisionFlag"']
        body += [f'"S{offset + i}","2018-09-14","R"' for i in range(n)]
        return _Resp(200, "\n".join(body), {"record-total": str(self.rows)})


class _Resp:
    def __init__(self, status, text, headers):
        self.status_code = status
        self.text = text
        self.headers = headers


def test_fetch_short_interest_pages_until_record_total(tmp_path):
    root = _lake(tmp_path)
    api = _Api(12_000)
    client = finra.FinraClient(api_interval_seconds=0, session=api)
    r = finra.fetch_short_interest(client, root, dt.date(2018, 9, 14))
    assert r["pages"] == 3  # 5000 + 5000 + 2000
    assert r["rows"] == 12_000
    assert [c["offset"] for c in api.calls] == [0, 5000, 10000]


def test_fetch_short_interest_treats_204_as_zero_rows(tmp_path):
    """아직 발표 안 된 결제일이 204로 온다. 실패가 아니다 (06 §1)."""
    root = _lake(tmp_path)
    client = finra.FinraClient(api_interval_seconds=0, session=_Api(0))
    r = finra.fetch_short_interest(client, root, dt.date(2026, 9, 15))
    assert r["rows"] == 0 and r["path"] is None


def test_fetch_short_interest_skips_a_file_it_already_has(tmp_path):
    root = _lake(tmp_path)
    dest = finra.short_interest_path(root, "2018-09-14")
    dest.parent.mkdir(parents=True)
    dest.write_text("h\na\nb\n")
    api = _Api(12_000)
    client = finra.FinraClient(api_interval_seconds=0, session=api)
    r = finra.fetch_short_interest(client, root, dt.date(2018, 9, 14))
    assert r["skipped"] is True and r["rows"] == 2
    assert api.calls == []


@pytest.mark.parametrize(
    "value,expected",
    [("R", True), ("S", True), ("Y", True), ("", False), (None, False), ("N", False)],
)
def test_flag_reads_the_source_markers(value, expected):
    """**`Y`가 아니라 `R`·`S`로 온다.** Y만 참으로 보면 정정 행이 전부 거짓이 된다."""
    assert finra._flag(value) is expected


def test_load_short_interest_builds_the_table(tmp_path):
    root = _lake(tmp_path)
    p = finra.short_interest_path(root, "2018-09-14")
    p.parent.mkdir(parents=True)
    p.write_text(
        '"symbolCode","currentShortPositionQuantity","previousShortPositionQuantity",'
        '"averageDailyVolumeQuantity","daysToCoverQuantity","changePercent",'
        '"revisionFlag","stockSplitFlag","marketClassCode","settlementDate"\n'
        '"A","4281663","4233272","1669848","2.56","1.14","R",,"NYSE","2018-09-14"\n'
        '"B","10","20","5","2.00",,,"S","NNM","2018-09-14"\n'
        # 같은 결제일에 같은 심볼이 또 오면 접는다
        '"A","999","1","1","1.00",,,,"NYSE","2018-09-14"\n'
    )
    r = finra.load_short_interest(root, snapshot_date="2026-09-19")
    assert r["rows"] == 2 and r["duplicate_rows"] == 1
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT symbol, current_short_qty, days_to_cover, change_percent,"
        f" revision_flag, stock_split_flag FROM '{r['path']}' ORDER BY symbol"
    ).fetchall()
    assert rows[0] == ("A", 4281663, 2.56, 1.14, True, False)
    assert rows[1] == ("B", 10, 2.0, None, False, True)


# --- 거래량 -----------------------------------------------------------------


_NEW = (
    "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
    "20260828|AAPL|1234.5|0.0|3000.0|B,Q,N\n"
    "20260828|BF.A|10|0|20|Q\n"
    "2\n"
)
_OLD = (
    "Date|Symbol|ShortVolume|TotalVolume|Market\n"
    "20091109|AGM.A|100|300|Q\n"
    "1\n"
)


def test_parse_regsho_reads_both_formats_and_the_trailer():
    rows, trailer = finra.parse_regsho(_NEW)
    assert trailer == 2 and len(rows) == 2
    assert rows[0]["ShortVolume"] == "1234.5"  # 소수점이 있다 (연구 §2.3)
    assert rows[0]["Market"] == "B,Q,N"        # venue 목록이다 (연구 §2.4)
    assert rows[1]["Symbol"] == "BF.A"         # 심볼의 점을 소수점으로 읽으면 안 된다

    old_rows, old_trailer = finra.parse_regsho(_OLD)
    assert old_trailer == 1 and len(old_rows) == 1
    # 옛 파일에는 ShortExemptVolume 칸이 아예 없다 (연구 §2.1)
    assert "ShortExemptVolume" not in old_rows[0]


def test_load_short_volume_keeps_null_for_missing_short_exempt(tmp_path):
    root = _lake(tmp_path)
    for day, text in (("2026-08-28", _NEW), ("2009-11-09", _OLD)):
        p = finra.regsho_path(root, day)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    r = finra.load_short_volume(root, snapshot_date="2026-09-19")
    assert r["rows"] == 3
    assert r["files_without_short_exempt"] == 1
    assert r["trailer_mismatch"] == []
    con = duckdb.connect()
    got = con.execute(
        f"SELECT date, symbol, short_volume, short_exempt_volume, market"
        f" FROM '{r['path']}' ORDER BY date, symbol"
    ).fetchall()
    assert got[0] == (dt.date(2009, 11, 9), "AGM.A", 100.0, None, "Q")
    assert got[1] == (dt.date(2026, 8, 28), "AAPL", 1234.5, 0.0, "B,Q,N")


def test_fetch_regsho_reports_a_missing_day(tmp_path):
    class _Cdn:
        def get(self, url, **kw):
            return _Resp(404, "", {})

    root = _lake(tmp_path)
    client = finra.FinraClient(cdn_interval_seconds=0, session=_Cdn())
    r = finra.fetch_regsho(client, root, dt.date(2020, 1, 1))
    assert r["missing"] is True and r["path"] is None

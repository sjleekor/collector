"""Nasdaq 실적 캘린더 — 미국 계획 01 §2.3, 04 C6 §2.1. 네트워크 없이 돈다."""

from __future__ import annotations

import datetime as dt
import json

import pytest

from collector.lake import DataRoot
from collector.us.sources import nasdaq


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


class _Session:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status
        self.calls: list[dict] = []

    def get(self, url, params=None, headers=None, **kw):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return _Resp(self.status, self.payload)


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self.payload = payload
        self.text = json.dumps(payload)

    def json(self):
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


_ROWS = {
    "data": {
        "asOf": "Thu, Feb 1, 2024",
        "rows": [
            {"symbol": "AAPL", "eps": "$2.18", "epsForecast": "$2.09",
             "surprise": "4.31", "noOfEsts": "11", "marketCap": "$4,918,238,800,000"},
            # 컨센서스가 없는 행이 실제로 있다 — 채움률을 컬럼별로 센다
            {"symbol": "ZZZ", "eps": "$0.10", "epsForecast": "", "surprise": "",
             "noOfEsts": "", "marketCap": "$1"},
        ],
    }
}


# --- 표본 날짜 ---------------------------------------------------------------


def test_sample_dates_is_64_thursdays_in_earnings_season():
    ds = nasdaq.sample_dates()
    assert len(ds) == 64  # 32분기 × 2일 (04 C6 §2.1)
    assert {d.weekday() for d in ds} == {3}
    assert ds[0] == dt.date(2018, 7, 26) and ds[1] == dt.date(2018, 8, 2)
    assert ds[-1] == dt.date(2026, 5, 7)
    assert len(set(ds)) == 64


def test_month_helpers_wrap_the_year():
    assert nasdaq._last_weekday(2018, 12, 3) == dt.date(2018, 12, 27)
    assert nasdaq._first_weekday(2019, 1, 3) == dt.date(2019, 1, 3)


# --- 요약 --------------------------------------------------------------------


def test_summarize_counts_columns_not_rows():
    s = nasdaq.summarize_earnings(_ROWS)
    assert s["rows"] == 2 and s["symbols"] == 2
    assert s["n_eps"] == 2
    assert s["n_epsForecast"] == 1 and s["n_surprise"] == 1 and s["n_noOfEsts"] == 1


def test_summarize_treats_null_data_as_zero_rows():
    """200인데 ``data: null``이 온다 — 성공으로 세면 빈 날짜를 놓친다 (06 §1)."""
    assert nasdaq.summarize_earnings({"data": None})["rows"] == 0


# --- 받기 --------------------------------------------------------------------


def test_fetch_earnings_keeps_the_raw_json(tmp_path):
    root = _lake(tmp_path)
    session = _Session(_ROWS)
    r = nasdaq.fetch_earnings(
        nasdaq.NasdaqClient(interval_seconds=0, session=session), root, dt.date(2024, 2, 1)
    )
    assert r["rows"] == 2
    dest = nasdaq.earnings_path(root, "2024-02-01")
    assert json.loads(dest.read_text())["data"]["rows"][0]["symbol"] == "AAPL"
    assert session.calls[0]["params"] == {"date": "2024-02-01"}
    # 브라우저 UA가 아니면 403도 아니고 읽기 타임아웃이다
    assert "Mozilla" in session.calls[0]["headers"]["User-Agent"]


def test_fetch_earnings_refetches_by_default(tmp_path):
    """같은 URL이 나중에 다른 값을 준다. 캐시로 갈음하면 정정을 못 본다 (05 §6.1)."""
    root = _lake(tmp_path)
    session = _Session(_ROWS)
    client = nasdaq.NasdaqClient(interval_seconds=0, session=session)
    nasdaq.fetch_earnings(client, root, "2024-02-01")
    nasdaq.fetch_earnings(client, root, "2024-02-01")
    assert len(session.calls) == 2
    nasdaq.fetch_earnings(client, root, "2024-02-01", skip_existing=True)
    assert len(session.calls) == 2


def test_non_200_raises(tmp_path):
    root = _lake(tmp_path)
    client = nasdaq.NasdaqClient(interval_seconds=0, session=_Session(_ROWS, status=500))
    with pytest.raises(nasdaq.NasdaqError, match="500"):
        nasdaq.fetch_earnings(client, root, "2024-02-01")


def test_scan_writes_a_csv(tmp_path):
    root = _lake(tmp_path)
    client = nasdaq.NasdaqClient(interval_seconds=0, session=_Session(_ROWS))
    r = nasdaq.scan_earnings_sample(
        client, root, snapshot_date="2026-09-19",
        dates=[dt.date(2024, 2, 1), dt.date(2024, 5, 2)],
    )
    text = r["path"].read_text()
    assert r["dates"] == 2
    assert text.splitlines()[0].startswith("date,as_of,rows,symbols,n_eps")
    assert "2024-05-02" in text


# --- 값 파싱 -----------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("$2.18", 2.18),
        ("($0.02)", -0.02),  # 괄호가 음수다. 빼먹으면 적자 종목 부호가 뒤집힌다
        ("-$0.15", -0.15),
        ("4.31", 4.31),
        ("N/A", None),
        ("", None),
        (None, None),
        ("$1,234.50", 1234.5),
    ],
)
def test_parse_money(text, expected):
    assert nasdaq.parse_money(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Dec/2023", dt.date(2023, 12, 31)),
        ("Feb/2020", dt.date(2020, 2, 29)),  # 윤년
        ("Jun/2024", dt.date(2024, 6, 30)),
        ("", None),
        ("Xyz/2024", None),
    ],
)
def test_parse_fiscal_quarter(text, expected):
    assert nasdaq.parse_fiscal_quarter(text) == expected


def test_load_earnings_calendar_drops_market_cap(tmp_path):
    """`marketCap`은 과거 행에도 오늘 값이 들어 있다 (01 §2.3). 담지 않는다."""
    import duckdb

    root = _lake(tmp_path)
    for day, rows in (
        ("2024-02-01", _ROWS["data"]["rows"]),
        ("2024-02-02", []),  # 실적이 하나도 없는 날이 있다 (연말·독립기념일 앞뒤)
    ):
        p = nasdaq.earnings_path(root, day)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"data": {"asOf": f"Thu, Feb {day[-1]}, 2024", "rows": rows}}))

    r = nasdaq.load_earnings_calendar(root, snapshot_date="2026-09-19")
    assert r["rows"] == 2 and r["empty_days"] == 1 and r["asof_mismatch"] == 0
    con = duckdb.connect()
    assert "marketCap" not in con.execute(f"SELECT * FROM '{r['path']}' LIMIT 0").df().columns
    got = con.execute(
        f"SELECT symbol, eps, eps_forecast, surprise_pct, n_estimates FROM '{r['path']}'"
        " ORDER BY symbol"
    ).fetchall()
    assert got[0] == ("AAPL", 2.18, 2.09, 4.31, 11)
    assert got[1] == ("ZZZ", 0.10, None, None, None)


def test_load_earnings_calendar_counts_asof_mismatch(tmp_path):
    """`date=`를 조용히 무시하는 엔드포인트가 같은 서버에 있다 (06 §1)."""
    root = _lake(tmp_path)
    p = nasdaq.earnings_path(root, "2024-02-01")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"data": {"asOf": "Fri, Mar 8, 2024", "rows": _ROWS["data"]["rows"]}}))
    r = nasdaq.load_earnings_calendar(root, snapshot_date="2026-09-19")
    assert r["asof_mismatch"] == 1

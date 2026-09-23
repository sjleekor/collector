"""Nasdaq 애널리스트 추정치 — source_expansion 04·99 §3.4. 네트워크 없이 돈다.

fixture 값은 2026-09-24 sj2-server 실측 그대로다 (``AAPL``·``ATER``·``ZZZZZ``).
"""

from __future__ import annotations

import datetime as dt
import json

import duckdb
import pytest

from collector.lake import DataRoot
from collector.us.sources import nasdaq_analyst as na


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


class _Session:
    def __init__(self, payloads: dict[str, dict], status=200):
        """``payloads``는 항상 심볼 → 응답 딕셔너리다 — 모호함이 없게 강제한다."""
        self.payloads = payloads
        self.status = status
        self.calls: list[dict] = []

    def get(self, url, params=None, headers=None, **kw):
        self.calls.append({"url": url, "params": params, "headers": headers})
        symbol = url.rsplit("/", 2)[1]
        return _Resp(self.status, self.payloads[symbol])


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self.payload = payload
        self.text = json.dumps(payload)

    def json(self):
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


# --- 실측 fixture (2026-09-24, sj2-server) -----------------------------------

AAPL_DOC = {
    "data": {
        "symbol": "aapl",
        "quarterlyForecast": {
            "asOf": None,
            "headers": {},
            "rows": [
                {
                    "fiscalEnd": "Sep 2026",
                    "consensusEPSForecast": 1.98,
                    "highEPSForecast": 2.09,
                    "lowEPSForecast": 1.91,
                    "noOfEstimates": 8,
                    "up": 0,
                    "down": 0,
                },
                {
                    "fiscalEnd": "Dec 2026",
                    "consensusEPSForecast": 2.91,
                    "highEPSForecast": 3.1,
                    "lowEPSForecast": 2.68,
                    "noOfEstimates": 7,
                    "up": 0,
                    "down": 0,
                },
            ],
        },
        "yearlyForecast": {
            "asOf": None,
            "headers": {},
            "rows": [
                {
                    "fiscalEnd": "Sep 2026",
                    "consensusEPSForecast": 8.74,
                    "highEPSForecast": 8.77,
                    "lowEPSForecast": 8.73,
                    "noOfEstimates": 5,
                    "up": 0,
                    "down": 0,
                },
                {
                    "fiscalEnd": "Sep 2027",
                    "consensusEPSForecast": 9.53,
                    "highEPSForecast": 10.46,
                    "lowEPSForecast": 9.01,
                    "noOfEstimates": 12,
                    "up": 2,
                    "down": 1,
                },
            ],
        },
    },
    "message": None,
    "status": {"rCode": 200, "bCodeMessage": None, "developerMessage": None},
}

# 분석 커버리지가 없는 심볼. 둘 다 200이다 — 실패가 아니다.
ATER_DOC = {
    "data": {"symbol": "ater", "quarterlyForecast": None, "yearlyForecast": None},
    "message": {
        "QuarterlyForecast": "Quarterly Earnings Forecast is not available",
        "YearlyForecast": "Yearly Earnings Forecast is not available",
    },
    "status": {"rCode": 200, "bCodeMessage": None, "developerMessage": None},
}

# 존재하지 않는 심볼. 여전히 HTTP 200 인데 본문 status.rCode 가 400 이다.
ZZZZZ_DOC = {
    "data": None,
    "message": None,
    "status": {
        "rCode": 400,
        "bCodeMessage": [{"code": 1001, "errorMessage": "Symbol not exists."}],
        "developerMessage": None,
    },
}


# --- 주 시작일 ----------------------------------------------------------------


@pytest.mark.parametrize(
    "day,monday",
    [
        (dt.date(2026, 9, 24), dt.date(2026, 9, 21)),  # 목요일
        (dt.date(2026, 9, 21), dt.date(2026, 9, 21)),  # 이미 월요일
        (dt.date(2026, 9, 27), dt.date(2026, 9, 21)),  # 일요일 — 같은 주
        (dt.date(2026, 9, 28), dt.date(2026, 9, 28)),  # 다음 주 월요일
    ],
)
def test_week_start_is_the_monday_of_the_same_iso_week(day, monday):
    assert na.week_start(day) == monday


# --- 경로 ---------------------------------------------------------------------


def test_earnings_forecast_path_partitions_by_week_start(tmp_path):
    root = _lake(tmp_path)
    p = na.earnings_forecast_path(root, dt.date(2026, 9, 21), "AAPL")
    assert p.as_posix().endswith(
        "raw/nasdaq/analyst_earnings_forecast/snapshot_date=2026-09-21/AAPL.json"
    )


# --- 응답 요약 — 200 이 성공이 아니다 -----------------------------------------


def test_summarize_counts_both_sections():
    s = na.summarize_earnings_forecast(AAPL_DOC)
    assert s["rows"] == 4
    assert s["sections"] == ["quarterlyForecast", "yearlyForecast"]
    assert s["status_rcode"] == 200


def test_summarize_treats_null_forecast_sections_as_zero_rows_not_a_failure():
    """분석 커버리지가 없어도 200이다 — 실패로 세면 안 된다 (04 §3)."""
    s = na.summarize_earnings_forecast(ATER_DOC)
    assert s["rows"] == 0
    assert s["status_rcode"] == 200


def test_summarize_surfaces_the_body_level_failure_for_an_unknown_symbol():
    """존재하지 않는 심볼도 HTTP 200 이다 — 본문 rCode 로만 구분된다."""
    s = na.summarize_earnings_forecast(ZZZZZ_DOC)
    assert s["rows"] == 0
    assert s["status_rcode"] == 400


# --- 값 파싱 -------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Sep 2026", dt.date(2026, 9, 30)),
        ("Dec 2026", dt.date(2026, 12, 31)),
        ("Feb 2020", dt.date(2020, 2, 29)),  # 윤년
        ("", None),
        (None, None),
        ("Dec/2023", None),  # earnings_calendar 쪽 구분자(슬래시)는 여기 형식이 아니다
    ],
)
def test_parse_fiscal_end(text, expected):
    assert na.parse_fiscal_end(text) == expected


def test_num_accepts_native_numbers_and_falls_back_to_money_strings():
    """실측 응답은 숫자 그대로 온다. 그래도 문자열이 섞이면 parse_money로 받는다."""
    assert na._num(1.98) == 1.98
    assert na._num(8) == 8.0
    assert na._num(None) is None
    assert na._num("$1.98") == 1.98
    assert na._num("N/A") is None


# --- 받기 -----------------------------------------------------------------


def test_fetch_one_wraps_the_raw_response_with_a_fetch_timestamp(tmp_path):
    root = _lake(tmp_path)
    session = _Session({"AAPL": AAPL_DOC})
    client = na.NasdaqClient(interval_seconds=0, session=session)

    r = na.fetch_one(client, root, dt.date(2026, 9, 21), "AAPL")

    assert r["status"] == "fetched"
    assert r["rows"] == 4
    dest = na.earnings_forecast_path(root, "2026-09-21", "AAPL")
    envelope = json.loads(dest.read_text())
    assert set(envelope) == {"fetched_at", "response"}
    assert envelope["response"] == AAPL_DOC
    dt.datetime.fromisoformat(envelope["fetched_at"])  # 파싱되면 통과
    assert "Mozilla" in session.calls[0]["headers"]["User-Agent"]


def test_fetch_one_skips_an_already_collected_symbol_by_default(tmp_path):
    """같은 주에 다시 돌려도 이미 받은 심볼은 건드리지 않는다."""
    root = _lake(tmp_path)
    session = _Session({"AAPL": AAPL_DOC})
    client = na.NasdaqClient(interval_seconds=0, session=session)

    na.fetch_one(client, root, "2026-09-21", "AAPL")
    r = na.fetch_one(client, root, "2026-09-21", "AAPL")

    assert r["status"] == "skipped"
    assert len(session.calls) == 1  # 두 번째는 세션을 안 건드렸다


def test_fetch_one_writes_the_body_level_failure_too(tmp_path):
    """존재하지 않는 심볼도 원문을 남긴다 — 재시도해도 안 바뀌는 응답이다."""
    root = _lake(tmp_path)
    session = _Session({"ZZZZZ": ZZZZZ_DOC})
    client = na.NasdaqClient(interval_seconds=0, session=session)

    r = na.fetch_one(client, root, "2026-09-21", "ZZZZZ")
    assert r["status"] == "fetched"
    assert r["status_rcode"] == 400
    assert r["rows"] == 0
    assert na.earnings_forecast_path(root, "2026-09-21", "ZZZZZ").is_file()


def test_fetch_one_raises_on_a_real_transport_failure(tmp_path):
    root = _lake(tmp_path)
    client = na.NasdaqClient(interval_seconds=0, session=_Session({"AAPL": AAPL_DOC}, status=500))
    with pytest.raises(na.NasdaqError, match="500"):
        na.fetch_one(client, root, "2026-09-21", "AAPL")


# --- 유니버스 대상 ------------------------------------------------------------


def _write_universe(root: DataRoot, rows: list[dict]) -> None:
    """``target_symbols``만 시험하는 최소 fixture 다 — arrow 배열을 직접 만든다."""
    import pyarrow as pyar
    import pyarrow.parquet as pq

    from collector.us.store.schema import UNIVERSE_DAILY_ARROW
    from collector.us.store.writer import snapshot_path

    observed = dt.datetime(2026, 9, 22, 6, tzinfo=dt.UTC)
    n = len(rows)
    table = pyar.table(
        {
            "date": pyar.array([r["date"] for r in rows], type=pyar.date32()),
            "symbol": pyar.array([r["symbol"] for r in rows], type=pyar.string()),
            "cik": pyar.array([None] * n, type=pyar.int64()),
            "in_prices": pyar.array([True] * n, type=pyar.bool_()),
            "in_listing": pyar.array([True] * n, type=pyar.bool_()),
            "listing_source": pyar.array(["wayback"] * n, type=pyar.string()),
            "is_etf": pyar.array([False] * n, type=pyar.bool_()),
            "test_issue": pyar.array([False] * n, type=pyar.bool_()),
            "exchange": pyar.array(["XNAS"] * n, type=pyar.string()),
            "sic": pyar.array([None] * n, type=pyar.string()),
            "sic_source": pyar.array([None] * n, type=pyar.string()),
            "mcap_rank": pyar.array([None] * n, type=pyar.int32()),
            "adv_20d": pyar.array([5_000_000.0] * n, type=pyar.float64()),
            "in_universe": pyar.array([r["in_universe"] for r in rows], type=pyar.bool_()),
            "usable_from": pyar.array([dt.date(2018, 9, 7)] * n, type=pyar.date32()),
            "observed_at": pyar.array([observed] * n, type=pyar.timestamp("us", tz="UTC")),
        },
        schema=UNIVERSE_DAILY_ARROW,
    )
    dest = snapshot_path(root, "universe_daily", "2026-09-22")
    dest.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dest, compression="zstd")


def test_target_symbols_uses_the_latest_date_and_in_universe_only(tmp_path):
    root = _lake(tmp_path)
    _write_universe(
        root,
        [
            {"date": dt.date(2026, 9, 18), "symbol": "OLD", "in_universe": True},
            {"date": dt.date(2026, 9, 21), "symbol": "AAPL", "in_universe": True},
            {"date": dt.date(2026, 9, 21), "symbol": "MSFT", "in_universe": True},
            {"date": dt.date(2026, 9, 21), "symbol": "ATER", "in_universe": False},
        ],
    )
    assert na.target_symbols(root) == ["AAPL", "MSFT"]


def test_target_symbols_needs_a_universe_snapshot_first(tmp_path):
    with pytest.raises(FileNotFoundError, match="us-universe rebuild"):
        na.target_symbols(_lake(tmp_path))


# --- derive: raw → derived ----------------------------------------------------


def _write_raw(root: DataRoot, week: str, symbol: str, doc: dict, fetched_at: str) -> None:
    p = na.earnings_forecast_path(root, week, symbol)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"fetched_at": fetched_at, "response": doc}))


def test_load_builds_one_row_per_period_and_fiscal_end(tmp_path):
    root = _lake(tmp_path)
    _write_raw(root, "2026-09-21", "AAPL", AAPL_DOC, "2026-09-24T01:00:00+00:00")

    r = na.load_nasdaq_analyst_estimates(root, snapshot_date="2026-09-24")
    assert r["rows"] == 4
    assert r["files"] == 1
    assert r["empty_files"] == 0
    assert r["fiscal_end_unparsed"] == 0

    con = duckdb.connect()
    got = con.execute(
        f"SELECT collected_week, symbol, period_type, fiscal_end_raw, fiscal_end, "
        f"consensus_eps_forecast, n_estimates, up, down, observed_at "
        f"FROM read_parquet('{r['path']}') ORDER BY period_type, fiscal_end"
    ).fetchall()
    assert len(got) == 4
    quarterly_sep = next(
        row for row in got if row[2] == "quarterlyForecast" and row[3] == "Sep 2026"
    )
    assert quarterly_sep[0] == dt.date(2026, 9, 21)  # collected_week
    assert quarterly_sep[1] == "AAPL"
    assert quarterly_sep[4] == dt.date(2026, 9, 30)  # fiscal_end
    assert quarterly_sep[5] == 1.98
    assert quarterly_sep[6] == 8
    assert quarterly_sep[7] == 0 and quarterly_sep[8] == 0
    # observed_at은 derive를 돌린 지금이 아니라 fetched_at 이다.
    assert quarterly_sep[9] == dt.datetime(2026, 9, 24, 1, tzinfo=dt.UTC)

    # 같은 fiscalEnd("Sep 2026"·"Sep 2027")가 분기·연간에 같이 나와도
    # period_type이 달라 유일성이 안 깨진다.
    yearly_sep_2026 = [row for row in got if row[2] == "yearlyForecast" and row[3] == "Sep 2026"]
    assert len(yearly_sep_2026) == 1


def test_load_treats_no_coverage_symbols_as_zero_rows(tmp_path):
    """``ATER``처럼 커버리지가 없는 심볼은 파일은 남지만 행은 안 만든다."""
    root = _lake(tmp_path)
    _write_raw(root, "2026-09-21", "AAPL", AAPL_DOC, "2026-09-24T01:00:00+00:00")
    _write_raw(root, "2026-09-21", "ATER", ATER_DOC, "2026-09-24T01:00:05+00:00")
    _write_raw(root, "2026-09-21", "ZZZZZ", ZZZZZ_DOC, "2026-09-24T01:00:10+00:00")

    r = na.load_nasdaq_analyst_estimates(root, snapshot_date="2026-09-24")
    assert r["files"] == 3
    assert r["rows"] == 4  # AAPL 뿐이다
    assert r["empty_files"] == 2  # ATER·ZZZZZ


def test_load_accumulates_across_multiple_weeks(tmp_path):
    """`nasdaq.load_earnings_calendar`와 같은 모양 — 쌓인 파티션을 전부 다시 읽는다."""
    root = _lake(tmp_path)
    _write_raw(root, "2026-09-14", "AAPL", AAPL_DOC, "2026-09-14T09:00:00+00:00")
    _write_raw(root, "2026-09-21", "AAPL", AAPL_DOC, "2026-09-21T09:00:00+00:00")

    r = na.load_nasdaq_analyst_estimates(root, snapshot_date="2026-09-24")
    assert r["files"] == 2
    assert r["rows"] == 8

    con = duckdb.connect()
    weeks = con.execute(
        f"SELECT DISTINCT collected_week FROM read_parquet('{r['path']}') ORDER BY 1"
    ).fetchall()
    assert weeks == [(dt.date(2026, 9, 14),), (dt.date(2026, 9, 21),)]


def test_load_requires_raw_files_first(tmp_path):
    with pytest.raises(na.NasdaqError, match="us-nasdaq-analyst run"):
        na.load_nasdaq_analyst_estimates(_lake(tmp_path), snapshot_date="2026-09-24")


def test_load_falls_back_to_mtime_when_the_envelope_is_missing(tmp_path):
    """봉투 없는 파일이 있어서는 안 되지만, 조용히 죽는 대신 mtime으로 받는다."""
    import os

    root = _lake(tmp_path)
    p = na.earnings_forecast_path(root, "2026-09-21", "AAPL")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(AAPL_DOC))  # 봉투 없이 원문만
    os.utime(p, (0, dt.datetime(2026, 9, 21, 12, tzinfo=dt.UTC).timestamp()))

    r = na.load_nasdaq_analyst_estimates(root, snapshot_date="2026-09-24")
    assert r["missing_envelope"] == 1
    assert r["rows"] == 4

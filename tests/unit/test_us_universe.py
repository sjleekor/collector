"""유니버스 조립 규칙 — 미국 계획 03 §5, 06 §4.1. 실제 lake 없이 돈다."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from collector.us.universe import build


def test_thresholds_match_the_plan():
    assert build.ENTRY_ADV_USD == 1_000_000
    assert build.MAINTAIN_ADV_USD == 700_000
    # 유지가 진입보다 낮아야 경계에서 덜 흔들린다 (03 §5.3)
    assert build.MAINTAIN_ADV_USD < build.ENTRY_ADV_USD
    assert build.MIN_TRADED_DAYS_20 == 10
    assert build.USABLE_FROM == dt.date(2018, 9, 7)


def test_warmup_exists():
    """구간 첫날부터 바로 재면 adv_20d 가 하루치가 되어 유니버스가 통째로 빈다."""
    assert build.WARMUP_DAYS >= 40


@pytest.mark.parametrize(
    "symbol,name,excluded",
    [
        ("GOOG", "Alphabet Inc. - Class C Capital Stock", False),
        ("V", "Visa Inc.", False),
        ("TSM", "Taiwan Semiconductor Manufacturing Company Ltd.", False),
        ("ASML", "ASML Holding N.V. - New York Registry Shares", False),
        ("SLB", "SLB Limited Common Shares", False),
        ("OKLO", "Oklo Inc. Class A common stock", False),
        ("ACME", "Acme Corp - Warrant", True),
        ("ACME", "Acme Corp 6.5% Preferred Series A", True),
        ("ACME", "Acme Corp - Right", True),
        ("ACME", "Acme Corp - Unit", True),
        ("ACME", "Acme Corp Depositary Shares", True),
        # 원천이 분리상장 뒤에도 이름의 When-Issued를 안 지운다. 이름만 보면
        # 거래대금 $556M·$8.0B짜리 본주가 통째로 빠진다 (2026-09-19)
        ("CEG", "Constellation Energy Corporation - Common Stock When-Issued", False),
        ("SNDK", "Sandisk Corporation - Common Stock When-Issued", False),
        # 진짜 WI 회선은 심볼이 V로 끝난다
        ("LILAV", "Liberty Latin America Ltd. - Class A Common Stock When Issued", True),
    ],
)
def test_name_exclusion_is_a_denylist_not_an_allowlist(symbol, name, excluded):
    """포함 목록으로 거르면 GOOG·V·TSM·ASML 이 빠진다 (03 §5.1)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t (symbol VARCHAR, security_name VARCHAR)")
    con.execute("INSERT INTO t VALUES (?, ?)", [symbol, name])
    sql = build._name_exclusion_sql("security_name", "symbol")
    got = con.execute(f"SELECT {sql} FROM t").fetchone()[0]
    assert bool(got) is excluded, name


def _membership(monthly_rows, prev):
    """build_universe_daily 의 월 이어달리기 판정과 같은 규칙."""
    keep = set()
    for symbol, adv, traded, is_etf, is_test, excluded in monthly_rows:
        if is_etf or is_test or excluded:
            continue
        if adv is None or (traded or 0) < build.MIN_TRADED_DAYS_20:
            continue
        threshold = build.MAINTAIN_ADV_USD if symbol in prev else build.ENTRY_ADV_USD
        if adv >= threshold:
            keep.add(symbol)
    return keep


def test_hysteresis_keeps_an_existing_member_below_the_entry_threshold():
    row = [("AAA", 800_000, 20, False, False, False)]
    assert _membership(row, prev=set()) == set()  # 새로 들어오려면 $1M
    assert _membership(row, prev={"AAA"}) == {"AAA"}  # 이미 있으면 $700K로 버틴다


def test_below_maintain_threshold_leaves_even_if_a_member():
    row = [("AAA", 600_000, 20, False, False, False)]
    assert _membership(row, prev={"AAA"}) == set()


def test_etf_test_and_name_exclusions_win_over_volume():
    rows = [
        ("ETF1", 50_000_000, 20, True, False, False),
        ("TST1", 50_000_000, 20, False, True, False),
        ("WAR1", 50_000_000, 20, False, False, True),
    ]
    assert _membership(rows, prev={"ETF1", "TST1", "WAR1"}) == set()


def test_thin_trading_is_excluded():
    assert _membership([("AAA", 50_000_000, 9, False, False, False)], prev={"AAA"}) == set()
    assert _membership([("AAA", 50_000_000, 10, False, False, False)], prev=set()) == {"AAA"}


# --- 월 단위 플래그 집계 (2026-09-19에 새던 곳) -----------------------------


def test_month_flags_catch_a_symbol_that_misses_the_first_trading_day():
    """월 첫 거래일 하루에 기대면 그날 가격이 없는 종목이 NULL -> FALSE 로 샌다.

    ZWZZT(나스닥 테스트 심볼)가 그 틈으로 95일 들어와 있었다.
    """
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE listing_daily (date DATE, symbol VARCHAR, "
        "is_etf BOOLEAN, test_issue BOOLEAN, security_name VARCHAR, listing_age_days INT)"
    )
    # 2월 첫 거래일(2/1)에는 이 종목 행이 없고, 2/15 에만 있다
    con.execute(
        "INSERT INTO listing_daily VALUES "
        "(DATE '2020-02-15','ZWZZT',FALSE,TRUE,'NASDAQ TEST STOCK',3)"
    )
    flags = con.execute("""
        SELECT bool_or(test_issue) FROM listing_daily
        WHERE date_trunc('month', date) = DATE '2020-02-01' AND symbol = 'ZWZZT'
        """).fetchone()[0]
    assert flags is True

    # 첫 거래일만 보면 아무것도 안 나온다 — 그래서 샜다
    first_day = con.execute(
        "SELECT test_issue FROM listing_daily WHERE date = DATE '2020-02-03'"
    ).fetchall()
    assert first_day == []


def test_current_flags_are_a_fallback_not_an_override():
    """Wayback PIT 값이 있으면 그것이 이긴다. 없을 때만 dolt 현재값을 쓴다."""
    con = duckdb.connect()
    got = con.execute("SELECT COALESCE(?, ?), COALESCE(?, ?)", [None, True, False, True]).fetchone()
    assert got == (True, False)  # (PIT 없음 -> 현재값, PIT 있음 -> PIT)


# --- 손상 스캔 규칙 (03 §6) --------------------------------------------------


def test_spike_rule_is_symmetric():
    """`> 3` 과 짝이 되는 하한은 `1/3` 이다. 처음에 0.34 로 적었던 것을 고쳤다."""
    from collector.us.validate import coverage

    assert coverage.SPIKE_RATIO == 3.0
    assert 1 / coverage.SPIKE_RATIO == pytest.approx(0.3333333, rel=1e-6)
    assert coverage.NEIGHBOUR_TOLERANCE == 0.30
    assert coverage.FLAT_RUN_DAYS == 5


# --- 티커 → CIK 를 PIT 로 붙인다 (2026-09-21) --------------------------------


def _pit_cik(rows, ticker_rows):
    """``build_universe_daily`` 와 **같은 조인 문자열**로 붙여 본다.

    `TICKER_PIT_JOIN` 을 공유하므로 조인 조건이 바뀌면 이 시험이 같이 바뀐다.
    """
    con = duckdb.connect()
    con.execute("CREATE TABLE daily_base (date DATE, symbol VARCHAR)")
    con.executemany("INSERT INTO daily_base VALUES (?, ?)", rows)
    con.execute("CREATE TABLE ticker_pit (symbol VARCHAR, cik BIGINT, as_of DATE)")
    con.executemany("INSERT INTO ticker_pit VALUES (?, ?, ?)", ticker_rows)
    return con.execute(
        "SELECT b.date, b.symbol, ct.cik FROM daily_base b "
        + build.TICKER_PIT_JOIN.format(left="b")
        + " ORDER BY b.date"
    ).fetchall()


def test_pit_join_uses_the_snapshot_in_force_on_that_date():
    """**티커 재사용.** 같은 심볼이 2019년엔 111, 2023년엔 222 다."""
    rows = [
        (dt.date(2018, 6, 1), "XYZ"),
        (dt.date(2020, 6, 1), "XYZ"),
        (dt.date(2024, 6, 1), "XYZ"),
    ]
    tickers = [
        ("XYZ", 111, dt.date(2019, 1, 19)),
        ("XYZ", 222, dt.date(2023, 1, 3)),
    ]
    got = _pit_cik(rows, tickers)
    assert got == [
        (dt.date(2018, 6, 1), "XYZ", None),  # 첫 스냅샷 이전 — 모른다
        (dt.date(2020, 6, 1), "XYZ", 111),
        (dt.date(2024, 6, 1), "XYZ", 222),
    ]


def test_pit_join_does_not_use_a_future_snapshot():
    """**이게 핵심이다.** 미래 맵을 끌어다 쓰면 룩어헤드가 된다."""
    rows = [(dt.date(2018, 6, 1), "LATER")]
    tickers = [("LATER", 999, dt.date(2026, 9, 19))]
    assert _pit_cik(rows, tickers) == [(dt.date(2018, 6, 1), "LATER", None)]


def test_pit_join_keeps_a_delisted_symbol_attached_after_it_is_gone():
    """상폐된 뒤에도 **그 시점 행에는** cik 이 붙어 있어야 한다."""
    rows = [(dt.date(2019, 6, 3), "AABA"), (dt.date(2020, 6, 1), "AABA")]
    tickers = [("AABA", 1011006, dt.date(2019, 1, 19))]
    assert [c for *_, c in _pit_cik(rows, tickers)] == [1011006, 1011006]


def test_pit_join_leaves_an_unknown_symbol_null():
    assert _pit_cik([(dt.date(2020, 6, 1), "NOPE")], [("XYZ", 1, dt.date(2019, 1, 1))]) == [
        (dt.date(2020, 6, 1), "NOPE", None)
    ]


def _fake_snapshots(root, spec):
    for name, days in spec.items():
        for day in days:
            d = root.derived / "snapshots" / name / f"snapshot_date={day}"
            d.mkdir(parents=True, exist_ok=True)
            (d / "part.parquet").write_bytes(b"")


ALL_AT = {n: ["2026-09-18"] for n in build.INPUT_TABLES}


def test_resolve_inputs_takes_the_latest_of_each_table(tmp_path):
    """**입력을 출력 날짜로 찾으면 안 된다** (2026-09-21).

    표마다 굳는 주기가 달라 오늘 날짜로 찾으면 거의 늘 없다 — C8 이
    `weekly_macro` 에서 겪은 것과 같은 버그다 (04 §2.17). 실제로 2026-09-21 에
    `prices_daily` 만 09-21 이고 나머지 셋이 09-18 이라 재판정이 안 돌았다.
    """
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    spec = dict(ALL_AT)
    spec["prices_daily"] = ["2026-09-18", "2026-09-21"]
    _fake_snapshots(root, spec)

    got = {n: p.parent.name for n, p in build.resolve_inputs(root).items()}
    assert got["prices_daily"] == "snapshot_date=2026-09-21"
    assert got["listing_snapshots"] == "snapshot_date=2026-09-18"
    assert set(got) == set(build.INPUT_TABLES)


def test_resolve_inputs_says_which_table_is_missing(tmp_path):
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    spec = {n: ["2026-09-18"] for n in build.INPUT_TABLES if n != "filings_sub"}
    _fake_snapshots(root, spec)
    with pytest.raises(FileNotFoundError, match="filings_sub"):
        build.resolve_inputs(root)

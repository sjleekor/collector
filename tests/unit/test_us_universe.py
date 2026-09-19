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
    "name,excluded",
    [
        ("Alphabet Inc. - Class C Capital Stock", False),
        ("Visa Inc.", False),
        ("Taiwan Semiconductor Manufacturing Company Ltd.", False),
        ("ASML Holding N.V. - New York Registry Shares", False),
        ("SLB Limited Common Shares", False),
        ("Oklo Inc. Class A common stock", False),
        ("Acme Corp - Warrant", True),
        ("Acme Corp 6.5% Preferred Series A", True),
        ("Acme Corp - Right", True),
        ("Acme Corp - Unit", True),
        ("Acme Corp Depositary Shares", True),
    ],
)
def test_name_exclusion_is_a_denylist_not_an_allowlist(name, excluded):
    """포함 목록으로 거르면 GOOG·V·TSM·ASML 이 빠진다 (03 §5.1)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t (security_name VARCHAR)")
    con.execute("INSERT INTO t VALUES (?)", [name])
    sql = build._name_exclusion_sql("security_name")
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

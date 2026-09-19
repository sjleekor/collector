"""조정 계산 — 미국 계획 03 §2, 06 §2.2.

실제 lake 없이 돈다. 값은 계획이 못박은 사례를 합성으로 옮긴 것이다.
"""

from __future__ import annotations

import duckdb
import pytest

from collector.us.adjust import adjusted_prices_sql


def _con(prices, splits):
    """prices: (date, symbol, close, volume) / splits: (symbol, ex_date, to, for)"""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE prices_daily (date DATE, symbol VARCHAR, open DECIMAL(14,4), "
        "high DECIMAL(14,4), low DECIMAL(14,4), close DECIMAL(14,4), volume BIGINT)"
    )
    for d, s, c, v in prices:
        con.execute("INSERT INTO prices_daily VALUES (?,?,?,?,?,?,?)", [d, s, c, c, c, c, v])
    con.execute(
        "CREATE TABLE corp_actions (symbol VARCHAR, ex_date DATE, kind VARCHAR, "
        "to_factor DECIMAL(10,5), for_factor DECIMAL(10,5))"
    )
    for s, d, to, fr in splits:
        con.execute("INSERT INTO corp_actions VALUES (?,?,'split',?,?)", [s, d, to, fr])
    return con


def _adj(con, as_of="2026-09-09"):
    return {
        (r[0].isoformat(), r[1]): r[2:]
        for r in con.execute(
            # 반올림하지 않는다 — 1/28·1/15 같은 값이 깎이면 검산이 무의미해진다
            "SELECT date, symbol, close_adj, volume_adj, adj_factor "
            "FROM (" + adjusted_prices_sql(as_of=as_of) + ") ORDER BY date"
        ).fetchall()
    }


def test_split_does_not_break_the_series():
    """AAPL 2020-08-31 4:1 — 주 판정 (06 §2.2)."""
    con = _con(
        [("2020-08-28", "AAPL", 499.23, 46907479), ("2020-08-31", "AAPL", 129.04, 225702688)],
        [("AAPL", "2020-08-31", 4, 1)],
    )
    a = _adj(con)
    before = float(a[("2020-08-28", "AAPL")][0])
    after = float(a[("2020-08-31", "AAPL")][0])
    assert before == pytest.approx(499.23 / 4, rel=1e-9)
    # 남은 간격은 그날의 실제 등락(+3.4%)이지 끊긴 것이 아니다
    assert abs(after - before) / before < 0.05


def test_volume_goes_the_other_way():
    """같은 계수를 걸면 거래대금이 16분의 1이 된다 — 값이 그럴듯해 눈으로 안 잡힌다."""
    con = _con([("2020-08-28", "AAPL", 499.23, 46907479)], [("AAPL", "2020-08-31", 4, 1)])
    close_adj, volume_adj, factor = _adj(con)[("2020-08-28", "AAPL")]
    assert float(factor) == pytest.approx(0.25)
    assert float(volume_adj) == pytest.approx(46907479 * 4, rel=1e-9)
    # 거래대금은 보존된다
    assert float(close_adj) * float(volume_adj) == pytest.approx(499.23 * 46907479, rel=1e-6)


def test_ten_for_one():
    """NVDA 2024-06-10 10:1 — 주 판정."""
    con = _con([("2024-06-07", "NVDA", 1208.88, 40552650)], [("NVDA", "2024-06-10", 10, 1)])
    close_adj, volume_adj, factor = _adj(con)[("2024-06-07", "NVDA")]
    assert float(factor) == pytest.approx(0.1)
    assert float(close_adj) == pytest.approx(120.888, rel=1e-9)
    assert float(volume_adj) == pytest.approx(405526500, rel=1e-9)


def test_split_on_the_day_itself_is_not_applied():
    """식이 t < ex_date 다. 분할 당일 가격은 이미 분할 뒤 값이다."""
    con = _con([("2020-08-31", "AAPL", 129.04, 1)], [("AAPL", "2020-08-31", 4, 1)])
    assert float(_adj(con)[("2020-08-31", "AAPL")][2]) == pytest.approx(1.0)


def test_splits_after_the_as_of_date_are_ignored():
    """미래 분할로 과거를 조정하면 그 시점에 몰랐던 정보가 들어간다."""
    con = _con([("2020-01-02", "AAPL", 300.0, 1)], [("AAPL", "2020-08-31", 4, 1)])
    assert float(_adj(con, as_of="2020-06-30")[("2020-01-02", "AAPL")][2]) == pytest.approx(1.0)
    assert float(_adj(con, as_of="2020-12-31")[("2020-01-02", "AAPL")][2]) == pytest.approx(0.25)


def test_multiple_splits_compound():
    """AAPL 7:1(2014) 뒤에 4:1(2020) — 누적 1/28."""
    con = _con(
        [("2014-06-06", "AAPL", 645.57, 12517273)],
        [("AAPL", "2014-06-09", 7, 1), ("AAPL", "2020-08-31", 4, 1)],
    )
    close_adj, _, factor = _adj(con)[("2014-06-06", "AAPL")]
    assert float(factor) == pytest.approx(1 / 28, rel=1e-9)
    assert float(close_adj) == pytest.approx(645.57 / 28, rel=1e-9)


def test_symbol_without_splits_is_untouched():
    con = _con([("2024-01-02", "MSFT", 370.0, 100)], [("AAPL", "2020-08-31", 4, 1)])
    close_adj, volume_adj, factor = _adj(con)[("2024-01-02", "MSFT")]
    assert float(factor) == pytest.approx(1.0)
    assert float(close_adj) == pytest.approx(370.0)
    assert float(volume_adj) == pytest.approx(100)


def test_reverse_split():
    """FBP 2011-01-07 은 1:15 다 — to < for 인 경우도 있다 (03 §2.1)."""
    con = _con([("2011-01-06", "FBP", 0.3598, 1000)], [("FBP", "2011-01-07", 1, 15)])
    close_adj, volume_adj, factor = _adj(con)[("2011-01-06", "FBP")]
    assert float(factor) == pytest.approx(15.0)
    assert float(close_adj) == pytest.approx(0.3598 * 15, rel=1e-9)
    assert float(volume_adj) == pytest.approx(1000 / 15, rel=1e-9)

"""구멍 지도와 손상 스캔 — 미국 계획 03 §6, 04 C6, 06 §4·§5.

**값어치는 없는 것을 아는 데 있다.** 어느 날짜가 비었고 어느 행이 못 믿을
값인지 표로 남긴다. 버리는 판단은 여기서 하지 않는다 — 표시만 한다.

유니버스 기준으로 좁혀서 본다. 21,592종목 전체에 돌리면 상폐된 동전주
노이즈가 대부분이라 읽을 수 없다.
"""

from __future__ import annotations

import datetime as _dt

#: 하루짜리 이상값 판정 (03 §6.1). `1/3`이다 — `> 3`과 대칭이어야 한다.
SPIKE_RATIO = 3.0
#: 전일과 익일이 이만큼 안에서 붙어 있어야 "그날만 무너진 것"이다.
NEIGHBOUR_TOLERANCE = 0.30
#: 같은 종가가 이만큼 이어지면 표시한다 (03 §6.2). 버리지 않는다.
FLAT_RUN_DAYS = 5


def one_day_spike_sql(*, prices: str = "px", actions: str = "corp_actions") -> str:
    """전일·익일은 붙어 있는데 당일만 3배 넘게 튀는 행.

    **분할·배당이 그날 있으면 뺀다** — 진짜 이벤트도 같은 모양이 나온다.
    """
    return f"""
    WITH n AS (
        SELECT date, symbol, close,
               lag(close)  OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
               lead(close) OVER (PARTITION BY symbol ORDER BY date) AS next_close
        FROM {prices}
    )
    SELECT n.date, n.symbol,
           CAST(n.prev_close AS DOUBLE) AS prev_close,
           CAST(n.close      AS DOUBLE) AS close,
           CAST(n.next_close AS DOUBLE) AS next_close,
           CAST(n.prev_close AS DOUBLE) / NULLIF(CAST(n.close AS DOUBLE), 0) AS prev_over_cur
    FROM n
    WHERE n.prev_close IS NOT NULL AND n.next_close IS NOT NULL AND n.close > 0
      AND abs(CAST(n.prev_close AS DOUBLE) / NULLIF(CAST(n.next_close AS DOUBLE), 0) - 1)
            < {NEIGHBOUR_TOLERANCE}
      AND (
            CAST(n.prev_close AS DOUBLE) / n.close > {SPIKE_RATIO}
         OR CAST(n.prev_close AS DOUBLE) / n.close < {1 / SPIKE_RATIO}
      )
      AND NOT EXISTS (
            SELECT 1 FROM {actions} a
            WHERE a.symbol = n.symbol AND a.ex_date = n.date
      )
    """


def flat_run_sql(*, prices: str = "px") -> str:
    """같은 종가가 이어지는 구간. 표시만 한다 — 실제로 안 움직였을 수 있다."""
    return f"""
    WITH g AS (
        SELECT date, symbol, close,
               row_number() OVER (PARTITION BY symbol ORDER BY date)
             - row_number() OVER (PARTITION BY symbol, close ORDER BY date) AS grp
        FROM {prices}
    )
    SELECT symbol, close, min(date) AS start_date, max(date) AS end_date, count(*) AS run_days
    FROM g
    GROUP BY symbol, close, grp
    HAVING count(*) >= {FLAT_RUN_DAYS}
    """


def scan(
    root,
    *,
    snapshot_date,
    start: str = "2018-09-07",
    end: str | None = None,
    universe_only: bool = True,
) -> dict[str, object]:
    """구멍 지도와 손상 스캔을 ``output/scan/``에 CSV로 남긴다.

    ``end`` 를 안 주면 ``prices_daily`` 의 마지막 날까지 본다.
    **고정 날짜를 기본값에 두지 않는다** — 그 날짜가 지나면 뒤쪽을 조용히
    안 보게 된다 (``universe/build.py`` 와 같은 실수였다).
    """
    import duckdb

    from collector.us.store.writer import snapshot_path

    con = duckdb.connect()
    for name in ("prices_daily", "corp_actions", "universe_daily"):
        con.execute(
            f"CREATE VIEW {name} AS SELECT * FROM "
            f"read_parquet('{snapshot_path(root, name, snapshot_date)}')"
        )
    if end is None:
        end = str(con.execute("SELECT max(date) FROM prices_daily").fetchone()[0])
    scope = "AND u.in_universe" if universe_only else ""
    con.execute(f"""
        CREATE TABLE px AS
        SELECT p.date, p.symbol, p.close, p.volume
        FROM prices_daily p
        JOIN universe_daily u ON u.date = p.date AND u.symbol = p.symbol {scope}
        WHERE p.date BETWEEN DATE '{start}' AND DATE '{end}'
        """)

    out = root.output / "scan" / f"snapshot_date={snapshot_date}"
    out.mkdir(parents=True, exist_ok=True)

    def dump(name: str, sql: str) -> int:
        con.execute(f"COPY ({sql}) TO '{out / name}' (HEADER, DELIMITER ',')")
        return con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]

    # 구멍 지도: 날짜별 행 수와 중앙값 대비 비율
    n_daily = dump(
        "coverage_by_date.csv",
        """
        SELECT date, count(*) AS symbols,
               round(100.0 * count(*) / median(count(*)) OVER (), 1) AS pct_of_median
        FROM px GROUP BY date ORDER BY date
        """,
    )
    thin = con.execute("""
        SELECT date, symbols FROM (
            SELECT date, count(*) AS symbols,
                   median(count(*)) OVER () AS med
            FROM px GROUP BY date
        ) WHERE symbols < med * 0.6 ORDER BY date
        """).fetchall()
    n_spike = dump("one_day_spikes.csv", one_day_spike_sql())
    n_flat = dump("flat_runs.csv", flat_run_sql())

    return {
        "out_dir": out,
        "dates": n_daily,
        "thin_dates": [(str(d), n) for d, n in thin],
        "one_day_spikes": n_spike,
        "flat_runs": n_flat,
        "scanned_rows": con.execute("SELECT count(*) FROM px").fetchone()[0],
        "scanned_at": _dt.datetime.now(_dt.UTC),
    }

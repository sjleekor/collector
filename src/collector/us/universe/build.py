"""``universe_daily`` 조립 — 미국 계획 03 §4.5·§5, 04 C4.

**그날의 판정을 행에 박아 둔다.** 나중에 ``symbol`` 테이블이 바뀌어도 과거가
안 흔들린다 (03 §5.4).

필터는 03 §5의 다섯 조건이다. 다만 **진입과 이탈 기준을 다르게 두고 월 1회만
재판정한다** (§5.3) — 매일 재판정하면 하루짜리 거래량 급증에 유니버스가 흔들리고
경계 종목이 들락거려 피쳐가 끊긴다.
"""

from __future__ import annotations

import datetime as _dt
import json as _json

#: 진입·유지 문턱 (03 §5.3). 유지가 낮아 경계에서 덜 흔들린다.
ENTRY_ADV_USD = 1_000_000
MAINTAIN_ADV_USD = 700_000

#: 20거래일 중 이만큼은 거래돼야 한다 (03 §5 조건 5).
MIN_TRADED_DAYS_20 = 10

#: 생존편향 하한. 이 앞 구간은 상폐가 기록되지 않아 횡단면 통계를 만들면 안 된다
#: (07 X3). 값을 행에 박아 학습 코드가 읽게 한다.
USABLE_FROM = _dt.date(2018, 9, 7)

#: 증권종류 배제 목록. **포함 목록("이름에 Common Stock")으로 거르면 안 된다** —
#: GOOG(Class C Capital Stock)·V(Visa Inc.)·TSM·ASML 이 빠진다 (03 §5.1).
EXCLUDED_NAME_TOKENS = (
    "warrant",
    "preferred",
    " right",
    " unit",
    "%  note",
    " note due",
    " bond",
    "depositary",
    "when issued",
    "when-issued",
)


def _name_exclusion_sql(column: str) -> str:
    """이름에 증권종류가 드러나면 뺀다. 소문자로 맞춰 비교한다."""
    tests = " OR ".join(f"lower({column}) LIKE '%{t}%'" for t in EXCLUDED_NAME_TOKENS)
    return f"({tests})"


#: 20거래일 롤링을 예열하는 데 쓰는 달력일. 구간 첫날부터 바로 재면
#: adv_20d 가 하루치로 계산돼 유니버스가 통째로 빈다.
WARMUP_DAYS = 60


def daily_base_sql(*, prices: str = "prices_daily", warmup_start: str, end: str) -> str:
    """일별 거래대금과 거래일 수. 20거래일 롤링이다.

    거래대금은 **원시 종가 × 원시 거래량**이다 — 조정하면 과거 시점의 실제
    거래 규모가 아니게 된다.

    ``warmup_start``부터 읽는다. 검정 구간 첫날에도 20거래일이 차 있어야 한다.
    """
    return f"""
    SELECT
        date,
        symbol,
        close,
        volume,
        CAST(close AS DOUBLE) * volume AS dollar_volume,
        avg(CAST(close AS DOUBLE) * volume) OVER w AS adv_20d,
        count(*) FILTER (WHERE volume > 0) OVER w AS traded_days_20
    FROM {prices}
    WHERE date BETWEEN DATE '{warmup_start}' AND DATE '{end}'
    WINDOW w AS (
        PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
    """


def listing_asof_sql(
    *,
    listing: str = "listing_snapshots",
    calendar: str = "trading_days",
) -> str:
    """거래일마다 **그 앞의 가장 가까운 스냅샷**을 붙인다.

    두 갈래(``nasdaqlisted``·``otherlisted``)를 따로 ASOF 한 뒤 합친다 —
    갈래마다 캡처 시점이 달라 같이 묶으면 한쪽이 다른 쪽을 덮는다.

    **``otherlisted``는 2021·2023에 캡처가 아예 없다** (아카이브에 없다,
    2026-09-19 CDX 확인). 그 구간은 앞선 스냅샷이 최장 708일까지 끌린다.
    """
    return f"""
    SELECT
        c.date,
        l.symbol,
        l.kind,
        l.as_of                     AS listing_as_of,
        date_diff('day', l.as_of, c.date) AS listing_age_days,
        l.security_name,
        l.exchange,
        l.market_category,
        l.is_etf,
        l.test_issue,
        l.financial_status
    FROM {calendar} c
    ASOF JOIN (
        SELECT * FROM {listing}
    ) l ON c.date >= l.as_of
    """


def monthly_candidates_sql(*, base: str = "daily_base", listing: str = "listing_daily") -> str:
    """월 재판정의 후보 상태.

    **그 달의 중앙값 ``adv_20d``로 잰다. 첫 거래일 값 하나로 재지 않는다.**
    한 날짜에 기대면 §5.3이 피하려던 것(하루짜리 거래량 급증에 유니버스가
    흔들리는 것)이 월 단위로 되돌아온다 — 문턱 근처 종목이 매달 들락거린다.
    상장 상태는 그 달 첫 거래일 기준이다.

    ``in_universe``를 여기서 정하지는 않는다 — 진입·유지 문턱이 달라서
    직전 달 결과가 필요하다. 그 이어달리기는 :func:`build_universe_daily`가 한다.
    """
    return f"""
    WITH month_first AS (
        SELECT date_trunc('month', date) AS ym, min(date) AS judge_date
        FROM {base} GROUP BY 1
    ),
    month_stat AS (
        SELECT date_trunc('month', date) AS ym,
               symbol,
               median(adv_20d)        AS adv_20d,
               median(traded_days_20) AS traded_days_20
        FROM {base} GROUP BY 1, 2
    )
    SELECT
        m.ym,
        m.judge_date,
        b.symbol,
        b.adv_20d,
        b.traded_days_20,
        l.is_etf,
        l.test_issue,
        l.security_name,
        l.listing_age_days,
        COALESCE(l.is_etf, FALSE)      AS etf_assumed,
        COALESCE(l.test_issue, FALSE)  AS test_assumed,
        {_name_exclusion_sql("COALESCE(l.security_name, '')")} AS name_excluded
    FROM month_first m
    JOIN month_stat b     ON b.ym = m.ym
    LEFT JOIN {listing} l ON l.date = m.judge_date AND l.symbol = b.symbol
    """


def build_universe_daily(
    root,
    *,
    snapshot_date,
    start: str = "2018-09-07",
    end: str = "2026-09-09",
    observed_at=None,
) -> dict[str, object]:
    """``universe_daily``를 굳힌다.

    행은 **그날 가격이 있는 종목**이 기준이다. 상장 기록에만 있고 가격이 없는
    종목은 행으로 만들지 않고 커버리지 수치로 따로 센다 — 그쪽은 유니버스
    판정 대상이 아니라 구멍 지도의 재료다 (04 C6).
    """
    import duckdb

    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    con = duckdb.connect()

    def snap(name):
        return snapshot_path(root, name, snapshot_date)

    for name in ("prices_daily", "listing_snapshots", "filings_sub", "midas_security_daily"):
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{snap(name)}')")
    tickers = root.raw / "sec" / "company_tickers" / "company_tickers.json"
    # company_tickers.json 은 {"0": {...}, "1": {...}} 꼴이라 read_json 으로
    # 바로 안 열린다. 1만 건뿐이니 파이썬으로 읽어 등록한다.
    entries = _json.loads(tickers.read_text())
    pairs = sorted({(str(v["ticker"]).upper(), int(v["cik_str"])) for v in entries.values()})
    con.execute("CREATE TABLE company_tickers (symbol VARCHAR, cik BIGINT)")
    con.executemany("INSERT INTO company_tickers VALUES (?, ?)", pairs)

    warmup_start = (_dt.date.fromisoformat(start) - _dt.timedelta(days=WARMUP_DAYS)).isoformat()
    # 원천에 비정규장 데이터가 섞여 있다 — 2020-02-17(Presidents' Day)에 3,432행이
    # 있었다 (2026-09-19 확인). 거래소 캘린더로 거른다.
    import exchange_calendars as _xcals

    sessions = [
        d.date().isoformat() for d in _xcals.get_calendar("XNYS").sessions_in_range(start, end)
    ]
    con.execute("CREATE TABLE xnys_sessions (date DATE)")
    con.executemany("INSERT INTO xnys_sessions VALUES (?)", [(d,) for d in sessions])
    con.execute("CREATE VIEW trading_days AS SELECT date FROM xnys_sessions")
    # 예열 구간까지 읽어 롤링을 채운 뒤, 검정 구간만 남긴다.
    con.execute(
        "CREATE TABLE daily_base_warm AS " + daily_base_sql(warmup_start=warmup_start, end=end)
    )
    con.execute(
        f"CREATE TABLE daily_base AS SELECT w.* FROM daily_base_warm w "
        f"JOIN xnys_sessions s ON s.date = w.date WHERE w.date >= DATE '{start}'"
    )
    dropped = con.execute(
        f"SELECT count(DISTINCT date) FROM daily_base_warm w "
        f"WHERE w.date >= DATE '{start}' "
        f"AND NOT EXISTS (SELECT 1 FROM xnys_sessions s WHERE s.date = w.date)"
    ).fetchone()[0]

    # 한 as_of 에 같은 symbol 이 두 갈래로 오면 하나만 남긴다 (드물다).
    con.execute("""
        CREATE TABLE listing_flat AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY symbol, as_of ORDER BY kind) AS rn
            FROM listing_snapshots
        ) WHERE rn = 1
        """)
    con.execute("""
        CREATE TABLE listing_daily AS
        SELECT b.date, b.symbol, l.as_of AS listing_as_of,
               date_diff('day', l.as_of, b.date) AS listing_age_days,
               l.kind, l.security_name, l.exchange, l.market_category,
               l.is_etf, l.test_issue, l.financial_status
        FROM daily_base b
        ASOF LEFT JOIN listing_flat l
          ON b.symbol = l.symbol AND b.date >= l.as_of
        """)

    con.execute("CREATE TABLE monthly AS " + monthly_candidates_sql())

    # --- 월 재판정 이어달리기 (03 §5.3) ---------------------------------
    months = [r[0] for r in con.execute("SELECT DISTINCT ym FROM monthly ORDER BY ym").fetchall()]
    con.execute("CREATE TABLE membership (ym DATE, symbol VARCHAR)")
    prev: set[str] = set()
    for ym in months:
        rows = con.execute(
            """
            SELECT symbol, adv_20d, traded_days_20, etf_assumed, test_assumed, name_excluded
            FROM monthly WHERE ym = ?
            """,
            [ym],
        ).fetchall()
        keep = set()
        for symbol, adv, traded, is_etf, is_test, excluded in rows:
            if is_etf or is_test or excluded:
                continue
            if adv is None or (traded or 0) < MIN_TRADED_DAYS_20:
                continue
            threshold = MAINTAIN_ADV_USD if symbol in prev else ENTRY_ADV_USD
            if adv >= threshold:
                keep.add(symbol)
        if keep:
            con.executemany("INSERT INTO membership VALUES (?, ?)", [(ym, s) for s in sorted(keep)])
        prev = keep

    dest = snapshot_path(root, "universe_daily", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                b.date,
                b.symbol,
                ct.cik,
                TRUE                                   AS in_prices,
                (ld.listing_as_of IS NOT NULL)         AS in_listing,
                CASE WHEN ld.listing_as_of IS NULL THEN 'none'
                     WHEN ld.listing_age_days <= 120  THEN 'wayback'
                     ELSE 'wayback_stale' END          AS listing_source,
                COALESCE(ld.is_etf, FALSE)             AS is_etf,
                COALESCE(ld.test_issue, FALSE)         AS test_issue,
                ld.exchange,
                f.sic,
                CASE WHEN f.sic IS NULL THEN NULL ELSE 'sec_sub' END AS sic_source,
                CAST(md.mcap_rank AS INTEGER)          AS mcap_rank,
                b.adv_20d,
                (m.symbol IS NOT NULL)                 AS in_universe,
                DATE '{USABLE_FROM.isoformat()}'       AS usable_from,
                CAST(? AS TIMESTAMP WITH TIME ZONE)    AS observed_at
            FROM daily_base b
            LEFT JOIN listing_daily ld ON ld.date = b.date AND ld.symbol = b.symbol
            LEFT JOIN company_tickers ct ON ct.symbol = b.symbol
            LEFT JOIN membership m
                   ON m.ym = date_trunc('month', b.date) AND m.symbol = b.symbol
            LEFT JOIN midas_security_daily md
                   ON md.date = b.date AND md.ticker = b.symbol
                  AND md.security_type = 'Stock'
            LEFT JOIN LATERAL (
                SELECT sic FROM filings_sub fs
                WHERE fs.cik = ct.cik AND fs.filed <= b.date AND fs.sic IS NOT NULL
                ORDER BY fs.filed DESC LIMIT 1
            ) f ON TRUE
        ) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [observed_at],
    )
    stats = verify_snapshot(dest, "universe_daily", unique_on=("date", "symbol"))
    return {
        "path": dest,
        "months": len(months),
        "sessions": len(sessions),
        "non_session_dates_dropped": dropped,
        **stats,
    }

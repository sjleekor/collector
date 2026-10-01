"""``universe_daily`` 조립 — 미국 계획 03 §4.5·§5, 04 C4.

**그날의 판정을 행에 박아 둔다.** 나중에 ``symbol`` 테이블이 바뀌어도 과거가
안 흔들린다 (03 §5.4).

필터는 03 §5의 다섯 조건이다. 다만 **진입과 이탈 기준을 다르게 두고 월 1회만
재판정한다** (§5.3) — 매일 재판정하면 하루짜리 거래량 급증에 유니버스가 흔들리고
경계 종목이 들락거려 피쳐가 끊긴다.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path


def _bounded_duckdb(root, *, bounded: bool = True):
    """Open DuckDB under the shared sj2-server resource budget.

    ``SDC_US_DUCKDB_MEMORY_LIMIT`` and ``SDC_US_DUCKDB_THREADS`` override the
    defaults. ``bounded=False`` (the full rebuild) applies no limit unless one
    of them is set, which keeps the historical unlimited behavior. Incremental
    runs default to ``4GB`` and ``2`` threads.
    """
    import os

    import duckdb

    memory = os.environ.get("SDC_US_DUCKDB_MEMORY_LIMIT") or ("4GB" if bounded else None)
    threads = os.environ.get("SDC_US_DUCKDB_THREADS") or ("2" if bounded else None)
    if memory is None and threads is None:
        return duckdb.connect()
    config: dict[str, str] = {}
    if threads is not None:
        config["threads"] = str(threads)
    if memory is not None:
        # Spill files go under ``output/``, never ``derived/``: the Mac mirror
        # (``us-mirror.sh``) pulls ``derived/`` with ``--delete`` and would copy
        # leftovers from an aborted run. ``output/`` is not mirrored except
        # ``output/scan``.
        temporary = root.output / "duckdb_tmp" / "universe"
        temporary.mkdir(parents=True, exist_ok=True)
        config["memory_limit"] = str(memory)
        config["temp_directory"] = str(temporary)
    return duckdb.connect(config=config)


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
)

#: 이름만으로는 못 거르는 것. **nasdaqtrader가 이름의 "When Issued"를 안 지운다** —
#: 분리상장이 끝난 뒤에도 몇 해씩 남아 있다 (2026-09-19 실측: 이름에 when issued가
#: 든 74종목 중 64개는 심볼이 ``V``로 끝나는 진짜 WI 회선이고, 나머지 10개 중
#: 9개는 DFS·DAN·AMCX·CVCO·BCH·CEG·SNDK 같은 **멀쩡히 거래되는 본주**다).
#:
#: **그래서 이름과 심볼을 함께 본다.** Nasdaq의 WI 회선은 심볼이 ``V``로 끝난다.
WHEN_ISSUED_NAME_TOKENS = ("when issued", "when-issued")


def _name_exclusion_sql(column: str, symbol_column: str = "b.symbol") -> str:
    """이름에 증권종류가 드러나면 뺀다. 소문자로 맞춰 비교한다.

    **when-issued만 심볼을 같이 본다.** 이름에만 기대면 CEG(거래대금 $556M)와
    SNDK($8.0B)가 통째로 빠진다 — 원천이 이름을 안 고쳤을 뿐이다.
    """
    tests = [f"lower({column}) LIKE '%{t}%'" for t in EXCLUDED_NAME_TOKENS]
    wi = " OR ".join(f"lower({column}) LIKE '%{t}%'" for t in WHEN_ISSUED_NAME_TOKENS)
    tests.append(f"(({wi}) AND {symbol_column} LIKE '%V')")
    return "(" + " OR ".join(tests) + ")"


#: 20거래일 롤링을 예열하는 데 쓰는 달력일. 구간 첫날부터 바로 재면
#: adv_20d 가 하루치로 계산돼 유니버스가 통째로 빈다.
#:
#: **60 에서 90 으로 늘렸다** (2026-09-22). 이제 달 M 의 판정에 달 M-1 을
#: 쓰므로(:data:`JUDGE_LAG_MONTHS`), 첫 달(2018-09)의 근거가 되는 2018-08 이
#: **완전히 예열돼 있어야** 한다. 60일이면 8월 초 며칠이 덜 찬 채로 들어간다.
WARMUP_DAYS = 90

#: **달 M 의 멤버십을 달 M-1 의 통계로 정한다** (2026-09-22).
#:
#: 원래는 달 M 자신의 중앙값으로 정했다. 그런데 패널은 그 달 **첫 거래일**을
#: 리밸런스일로 쓰므로, 9월 1일에 서는 포트폴리오가 9월 한 달의 유동성을
#: 이미 아는 셈이었다 — 룩어헤드다. 멤버십의 **8.72%** 가 여기서 갈렸다
#: (미국 2차 후속 ``06_universe_lookahead.md``).
#:
#: **안정성은 안 잃는다.** 한 날짜가 아니라 한 달 중앙값을 쓰는 것은 그대로다
#: (04 §2.11). 보는 달만 하나 앞으로 민다.
JUDGE_LAG_MONTHS = 1


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

    **여기는 "달 ym 에 무슨 일이 있었나"만 낸다.** 그 통계를 **어느 달의**
    멤버십에 쓸지는 :func:`build_universe_daily` 가 정한다 — 달 M 의 멤버십은
    달 M-1 의 행을 본다 (:data:`JUDGE_LAG_MONTHS`). 그래서 이 함수의
    ``judge_date`` 는 통계를 낸 달의 첫 거래일이지 판정 대상 달이 아니다.

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
    ),
    -- 플래그는 그 달 **아무 날에라도** 켜졌으면 켜진 것으로 본다.
    -- 월 첫 거래일 하루에 기대면, 그날 가격이 없는 종목(테스트 심볼처럼
    -- 띄엄띄엄 거래되는 것)이 NULL -> FALSE 로 새어 들어온다.
    month_flags AS (
        SELECT date_trunc('month', date) AS ym,
               symbol,
               bool_or(is_etf)      AS is_etf,
               bool_or(test_issue)  AS test_issue,
               max(security_name)   AS security_name,
               min(listing_age_days) AS listing_age_days
        FROM {listing} GROUP BY 1, 2
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
    JOIN month_stat b   ON b.ym = m.ym
    LEFT JOIN month_flags l ON l.ym = m.ym AND l.symbol = b.symbol
    """


#: ``universe_daily`` 를 굳히는 데 필요한 표. **가장 최근 스냅샷을 쓴다.**
INPUT_TABLES: tuple[str, ...] = (
    "prices_daily",
    "listing_snapshots",
    "filings_sub",
    "midas_security_daily",
)

def _shift_months(ym: _dt.date, months: int) -> _dt.date:
    """달의 첫날을 ``months`` 만큼 민다. ``_shift_months(2019-01-01, -1) == 2018-12-01``."""
    total = ym.year * 12 + (ym.month - 1) + months
    return _dt.date(total // 12, total % 12 + 1, 1)


def resolve_inputs(root) -> dict[str, Path]:
    """``INPUT_TABLES`` 마다 **가장 최근 스냅샷** 경로.

    **출력 ``snapshot_date`` 로 찾으면 안 된다.** 표마다 굳는 주기가 달라
    오늘 날짜로 찾으면 거의 늘 없다 — C8 이 ``weekly_macro`` 에서 이미 겪은
    그 버그다 (04 §2.17). 2026-09-21 에 ``prices_daily`` 만 09-21 이고 나머지
    셋이 09-18 이라 재판정이 아예 안 돌았다.
    """
    from collector.us.store.writer import latest_snapshot

    out = {}
    for name in INPUT_TABLES:
        path = latest_snapshot(root, name)
        if path is None:
            raise FileNotFoundError(
                f"{name} 스냅샷이 없다. 먼저 굳힌다 — collector us-derive run"
            )
        out[name] = path
    return out


#: 티커 → CIK 를 **PIT 로** 붙이는 조인 (2026-09-21).
#:
#: 오늘자 맵 한 벌을 쓰면 상폐·피인수·개명한 회사가 통째로 빠져서 `cik` 이
#: 붙었나가 곧 "2026년에도 살아 있나"가 된다 — 끝까지 남은 종목 98.9% 대
#: 사라진 종목 26.5% (2026-09-21 실측 · 2차 후속 `03_cik_pit_probe.md`).
#:
#: **시험이 이 문자열을 그대로 쓴다.** 조인 조건을 여기서만 고치면 된다.
TICKER_PIT_JOIN = (
    "ASOF LEFT JOIN ticker_pit ct\n"
    "                   ON ct.symbol = {left}.symbol AND {left}.date >= ct.as_of"
)


def build_universe_daily(
    root,
    *,
    snapshot_date,
    start: str = "2018-09-07",
    end: str | None = None,
    observed_at=None,
    seed_previous_members: set[str] | None = None,
    fixed_month_members: dict[str, set[str]] | None = None,
    prefix_snapshot: Path | None = None,
    resolved_inputs: dict[str, Path] | None = None,
    destination_path: Path | None = None,
    ticker_source_paths: list[Path] | None = None,
    bounded: bool = False,
) -> dict[str, object]:
    """``universe_daily``를 굳힌다.

    행은 **그날 가격이 있는 종목**이 기준이다. 상장 기록에만 있고 가격이 없는
    종목은 행으로 만들지 않고 커버리지 수치로 따로 센다 — 그쪽은 유니버스
    판정 대상이 아니라 구멍 지도의 재료다 (04 C6).

    ``end`` 를 안 주면 **``prices_daily`` 에 있는 마지막 날**까지 간다.
    **고정 날짜를 기본값에 두지 않는다** — 원래 ``"2026-09-09"`` 였다(C4 를
    돌린 날의 가격 최대일). 그대로 두면 재판정을 몇 번 돌려도 유니버스가
    **영원히 그 날짜에서 끊긴다.** 2026-09-21 재판정이 실제로 그랬다:
    가격은 09-18 까지인데 결과가 09-09 에서 멈췄다.
    """
    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    con = _bounded_duckdb(root, bounded=bounded)

    try:
        resolved = resolved_inputs if resolved_inputs is not None else resolve_inputs(root)
        inputs = {n: p.parent.name.removeprefix("snapshot_date=") for n, p in resolved.items()}
        for name, path in resolved.items():
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')")

        if end is None:
            end = str(con.execute("SELECT max(date) FROM prices_daily").fetchone()[0])
        # **티커 → CIK 는 PIT 다** (2026-09-21). 오늘자 맵 한 벌을 쓰면
        # 상폐·피인수·개명한 회사가 통째로 빠져 `cik` 이 붙었나가 곧 "2026년에도
        # 살아 있나"가 된다 — 끝까지 남은 종목 98.9% 대 사라진 종목 26.5%.
        # 아래 ASOF 조인이 `date >= as_of` 중 최신 스냅샷을 쓴다.
        from collector.us.sources import wayback

        ticker_rows = wayback.ticker_cik_map(root, source_paths=ticker_source_paths)
        import pyarrow as pa

        # DuckDB executemany inserts one row at a time and took over ten
        # minutes for the historical Wayback PIT map. Register one Arrow batch
        # so the same rows enter DuckDB through a vectorized scan.
        ticker_table = pa.table({
            "symbol": pa.array([row[0] for row in ticker_rows], type=pa.string()),
            "cik": pa.array([row[1] for row in ticker_rows], type=pa.int64()),
            "as_of": pa.array([row[2] for row in ticker_rows], type=pa.date32()),
        })
        con.register("company_tickers_arrow", ticker_table)
        # ASOF 조인은 오른쪽이 키별로 정렬돼 있어야 싸다.
        con.execute(
            "CREATE TABLE ticker_pit AS "
            "SELECT symbol, cik, as_of FROM company_tickers_arrow ORDER BY symbol, as_of"
        )
        con.unregister("company_tickers_arrow")
        del ticker_table

        # dolt symbol 의 현재값. Wayback 이 그 날짜를 못 덮을 때만 쓴다 — PIT 는
        # 아니지만 "모르면 FALSE" 보다 낫다. ZWZZT(나스닥 테스트 심볼)와
        # SGOV(ETF)가 그 틈으로 들어왔었다 (2026-09-19).
        dolt_symbol = root.raw / "dolt" / "stocks"
        con.execute(
            "CREATE TABLE symbol_current (symbol VARCHAR, is_etf BOOLEAN, "
            "test_issue BOOLEAN, security_name VARCHAR)"
        )
        if (dolt_symbol / ".dolt").is_dir():
            import subprocess

            proc = subprocess.run(
                [
                    "dolt",
                    "sql",
                    "-q",
                    "select act_symbol, is_etf, is_test_issue, security_name from symbol",
                    "-r",
                    "csv",
                ],
                cwd=dolt_symbol,
                capture_output=True,
                text=True,
                check=True,
            )
            import csv as _csv
            import io as _io

            reader = _csv.DictReader(_io.StringIO(proc.stdout))
            con.executemany(
                "INSERT INTO symbol_current VALUES (?, ?, ?, ?)",
                [
                    (
                        r["act_symbol"],
                        (r["is_etf"] or "0") not in ("0", ""),
                        (r["is_test_issue"] or "0") not in ("0", ""),
                        r["security_name"],
                    )
                    for r in reader
                ],
            )

        warmup_start = (_dt.date.fromisoformat(start) - _dt.timedelta(days=WARMUP_DAYS)).isoformat()
        # 원천에 비정규장 데이터가 섞여 있다 — 2020-02-17(Presidents' Day)에 3,432행이
        # 있었다 (2026-09-19 확인). 거래소 캘린더로 거른다.
        import exchange_calendars as _xcals

        _cal = _xcals.get_calendar("XNYS")
        sessions = [d.date().isoformat() for d in _cal.sessions_in_range(start, end)]
        # 예열 구간의 세션도 필요하다 — 판정 바닥이 거기까지 간다.
        sessions_all = [d.date().isoformat() for d in _cal.sessions_in_range(warmup_start, end)]
        con.execute("CREATE TABLE xnys_sessions (date DATE)")
        con.executemany("INSERT INTO xnys_sessions VALUES (?)", [(d,) for d in sessions])
        con.execute("CREATE TABLE xnys_sessions_all (date DATE)")
        con.executemany("INSERT INTO xnys_sessions_all VALUES (?)", [(d,) for d in sessions_all])
        con.execute("CREATE VIEW trading_days AS SELECT date FROM xnys_sessions")
        # 예열 구간까지 읽어 롤링을 채운 뒤, 검정 구간만 남긴다.
        con.execute(
            "CREATE TABLE daily_base_warm AS " + daily_base_sql(warmup_start=warmup_start, end=end)
        )
        # **판정에 쓰는 바닥은 예열 구간까지 포함한다.** 달 M 의 멤버십이 M-1 을
        # 보므로, 첫 달(2018-09)의 근거가 될 2018-08 이 여기 있어야 한다.
        # 세션 필터는 둘 다 건다 — 원천에 비정규장 데이터가 섞여 있다.
        con.execute(
            "CREATE TABLE daily_base_judge AS SELECT w.* FROM daily_base_warm w "
            "JOIN xnys_sessions_all s ON s.date = w.date"
        )
        con.execute(
            "CREATE TABLE daily_base AS SELECT * FROM daily_base_judge "
            f"WHERE date >= DATE '{start}'"
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
                   l.kind,
                   COALESCE(l.security_name, sc.security_name) AS security_name,
                   l.exchange, l.market_category,
                   -- Wayback PIT 값이 있으면 그것, 없으면 dolt 현재값.
                   COALESCE(l.is_etf, sc.is_etf)           AS is_etf,
                   COALESCE(l.test_issue, sc.test_issue)   AS test_issue,
                   (l.is_etf IS NULL AND sc.is_etf IS NOT NULL) AS flags_from_current,
                   l.financial_status
            FROM daily_base_judge b
            ASOF LEFT JOIN listing_flat l
              ON b.symbol = l.symbol AND b.date >= l.as_of
            LEFT JOIN symbol_current sc ON sc.symbol = b.symbol
            """)

        con.execute(
            "CREATE TABLE monthly AS " + monthly_candidates_sql(base="daily_base_judge")
        )

        # --- 월 재판정 이어달리기 (03 §5.3) ---------------------------------
        #
        # **달 M 의 멤버십은 달 M-1 의 통계로 정한다** (JUDGE_LAG_MONTHS).
        # 패널이 그 달 첫 거래일을 리밸런스일로 쓰므로, 달 M 자신의 중앙값으로
        # 정하면 그 시점에 알 수 없는 것을 쓰게 된다 (06_universe_lookahead.md).
        target_months = [
            r[0].date() if isinstance(r[0], _dt.datetime) else r[0]
            for r in con.execute(
                "SELECT DISTINCT date_trunc('month', date) AS ym FROM daily_base ORDER BY ym"
            ).fetchall()
        ]
        con.execute("CREATE TABLE membership (ym DATE, symbol VARCHAR)")
        prev: set[str] = set(seed_previous_members or ())
        unjudged: list[str] = []
        for ym in target_months:
            if fixed_month_members and str(ym) in fixed_month_members:
                keep = fixed_month_members[str(ym)]
                if keep:
                    con.executemany(
                        "INSERT INTO membership VALUES (?, ?)", [(ym, s) for s in sorted(keep)]
                    )
                prev = keep
                continue
            src = _shift_months(ym, -JUDGE_LAG_MONTHS)
            rows = con.execute(
                """
                SELECT symbol, adv_20d, traded_days_20, etf_assumed, test_assumed, name_excluded
                FROM monthly WHERE ym = ?
                """,
                [src],
            ).fetchall()
            if not rows:
                # 근거가 될 달이 없다. **조용히 빈 달로 두지 않는다** — 결과에 적는다.
                unjudged.append(str(ym))
                prev = set()
                continue
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
                con.executemany(
                    "INSERT INTO membership VALUES (?, ?)", [(ym, s) for s in sorted(keep)]
                )
            prev = keep

        if prefix_snapshot is not None and unjudged:
            raise ValueError(f"incremental universe has unjudged months: {unjudged}")

        dest = destination_path or snapshot_path(root, "universe_daily", snapshot_date)
        if dest.exists():
            raise FileExistsError(f"universe_daily snapshot already exists: {dest}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        import os

        temporary_dest = dest.parent / f".part.{os.getpid()}.parquet"
        if temporary_dest.exists():
            raise FileExistsError(
                f"universe_daily temporary snapshot already exists: {temporary_dest}"
            )
        ticker_pit_join = TICKER_PIT_JOIN.format(left="b")
        prefix_sql = (
            "SELECT date, symbol, cik, in_prices, in_listing, listing_source, is_etf, "
            "test_issue, exchange, sic, sic_source, mcap_rank, adv_20d, in_universe, "
            "usable_from, observed_at "
            f"FROM read_parquet('{prefix_snapshot}') WHERE date < DATE '{start}' UNION ALL "
            if prefix_snapshot is not None else ""
        )
        con.execute(
            f"""
            COPY (
                {prefix_sql}
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
                {ticker_pit_join}
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
            ) TO '{temporary_dest}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [observed_at],
        )
        stats = verify_snapshot(temporary_dest, "universe_daily", unique_on=("date", "symbol"),
                                connection=con)
        os.link(temporary_dest, dest)
        temporary_dest.unlink()
        # **무엇으로 만들었는지 같이 남긴다.** 입력 스냅샷이 표마다 다른 날짜일
        # 수 있으므로 어느 것을 썼는지 적어야 재현이 된다.
        ticker_as_of = sorted({row[2] for row in ticker_rows})
        return {
            "path": dest,
            "months": len(target_months),
            "sessions": len(sessions),
            "non_session_dates_dropped": dropped,
            "input_snapshots": inputs,
            "start": start,
            "end": end,
            "judge_lag_months": JUDGE_LAG_MONTHS,
            "unjudged_months": unjudged,
            "ticker_map_snapshots": len(ticker_as_of),
            "ticker_map_first": str(ticker_as_of[0]) if ticker_as_of else None,
            "ticker_map_last": str(ticker_as_of[-1]) if ticker_as_of else None,
            "prefix_snapshot": str(prefix_snapshot) if prefix_snapshot else None,
            "seed_previous_members": len(seed_previous_members or ()),
            "fixed_months": sorted((fixed_month_members or {}).keys()),
            **stats,
        }
    finally:
        con.close()


def build_universe_incremental(
    root, *, snapshot_date, observed_at=None, if_new: bool = False, dry_run: bool = False
) -> dict[str, object]:
    """Extend a complete prior snapshot while freezing established month members.

    The prior last session and previous month are required. The output retains
    every earlier row, records its provenance, and adds only later price dates.
    An incomplete prior month fails rather than silently resetting hysteresis.

    ``if_new`` turns the two expected no-op cases into a skip result instead of
    an exception: no new XNYS session, or the ``snapshot_date`` partition
    already exists. ``dry_run`` runs every check and reports what would be
    added without writing a snapshot.
    """
    import hashlib
    import json
    import os
    import tempfile

    import exchange_calendars as xcals

    from collector.us.store.writer import latest_snapshot, snapshot_path

    def sha(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    from collector.us.sources import wayback

    previous = latest_snapshot(root, "universe_daily")
    input_files = resolve_inputs(root)
    prices = input_files["prices_daily"]
    if previous is None or prices is None:
        raise FileNotFoundError(
            "incremental universe requires prior universe_daily and prices_daily"
        )
    destination = snapshot_path(root, "universe_daily", snapshot_date)
    if destination.parent.exists() and not if_new:
        raise FileExistsError(f"universe_daily partition already exists: {destination.parent}")
    con = _bounded_duckdb(root)
    try:
        previous_end = con.execute(
            "SELECT max(date) FROM read_parquet(?)", [str(previous)]
        ).fetchone()[0]
        prices_end = con.execute(
            "SELECT max(date) FROM read_parquet(?)", [str(prices)]
        ).fetchone()[0]
        if previous_end is None or prices_end is None or prices_end <= previous_end:
            if if_new:
                return {"skipped": True, "reason": "no new price session"}
            raise ValueError("new completed price session is required")
        cal = xcals.get_calendar("XNYS")
        next_sessions = [d.date() for d in cal.sessions_in_range(
            previous_end + _dt.timedelta(days=1), prices_end
        )]
        if not next_sessions:
            if if_new:
                return {"skipped": True, "reason": "no new XNYS session"}
            raise ValueError("no new XNYS session is present")
        if if_new and destination.parent.exists():
            return {
                "skipped": True,
                "reason": f"universe_daily partition already exists: {destination.parent.name}",
            }
        recorded_new_dates = {row[0] for row in con.execute(
            "SELECT DISTINCT date FROM read_parquet(?) WHERE date > ? AND date <= ?",
            [str(prices), previous_end, prices_end],
        ).fetchall()}
        missing_new_dates = sorted(set(next_sessions) - recorded_new_dates)
        if missing_new_dates:
            raise ValueError(f"prices snapshot has missing new XNYS sessions: {missing_new_dates}")
        start = next_sessions[0]
        previous_month = _shift_months(start.replace(day=1), -1)
        prior_month_sessions = [d.date() for d in cal.sessions_in_range(
            previous_month, start - _dt.timedelta(days=1)
        ) if d.date().month == previous_month.month]
        if not prior_month_sessions:
            raise ValueError("previous month has no confirmed XNYS session")
        previous_month_last = prior_month_sessions[-1]
        if previous_end < previous_month_last:
            raise ValueError("prior universe snapshot does not cover the previous month")
        expected_prior_dates = {
            d.date() for d in cal.sessions_in_range(previous_month, previous_end)
        }
        recorded_prior_dates = {row[0] for row in con.execute(
            "SELECT DISTINCT date FROM read_parquet(?) WHERE date BETWEEN ? AND ?",
            [str(previous), previous_month, previous_end],
        ).fetchall()}
        missing_prior_dates = sorted(expected_prior_dates - recorded_prior_dates)
        if missing_prior_dates:
            raise ValueError(
                f"prior universe snapshot has missing XNYS sessions: {missing_prior_dates}"
            )
        seed = {row[0] for row in con.execute(
            "SELECT DISTINCT symbol FROM read_parquet(?) "
            "WHERE date >= ? AND date < ? AND in_universe",
            [str(previous), previous_month, start.replace(day=1)],
        ).fetchall()}
        if not seed:
            raise ValueError("previous month membership seed is missing")
        fixed: dict[str, set[str]] = {}
        current_month = start.replace(day=1)
        if previous_end >= current_month:
            fixed[str(current_month)] = {row[0] for row in con.execute(
                "SELECT DISTINCT symbol FROM read_parquet(?) "
                "WHERE date >= ? AND date <= ? AND in_universe",
                [str(previous), current_month, previous_end],
            ).fetchall()}
            if not fixed[str(current_month)]:
                raise ValueError("current month frozen membership is missing")
        if dry_run:
            return {
                "dry_run": True,
                "would_build": True,
                "added_start": start.isoformat(),
                "added_end": prices_end.isoformat(),
                "sessions": len(next_sessions),
            }
    finally:
        con.close()
    ticker_files = sorted(wayback.company_tickers_dir(root).glob("company_tickers_*.json"))
    ticker_paths = ticker_files or [root.raw / "sec" / "company_tickers" / "company_tickers.json"]
    if any(not path.is_file() for path in ticker_paths):
        raise FileNotFoundError("pinned Wayback ticker mapping is missing")
    ticker_hashes_before = {str(path): sha(path) for path in ticker_paths}
    input_hashes_before = {name: sha(path) for name, path in input_files.items()}
    previous_hash = sha(previous)
    destination.parent.parent.mkdir(parents=True, exist_ok=True)
    staged_dir = Path(
        tempfile.mkdtemp(prefix=".universe-incremental-", dir=destination.parent.parent)
    )
    # mkdtemp is 0700 and the rename keeps it; readers outside the container
    # (briefing prepare, us-mirror) run as another user and need 0755 like
    # every other snapshot partition.
    staged_dir.chmod(0o755)
    import shutil
    try:
        staged_part = staged_dir / "part.parquet"
        result = build_universe_daily(
            root, snapshot_date=snapshot_date, start=start.isoformat(), end=prices_end.isoformat(),
            observed_at=observed_at, seed_previous_members=seed,
            fixed_month_members=fixed, prefix_snapshot=previous,
            resolved_inputs=input_files, destination_path=staged_part,
            ticker_source_paths=ticker_files, bounded=True,
        )
        # A prior month's already-published membership is an invariant.  Do
        # not mistake the presence of fixed_months in the marker for proof
        # that the SQL lookup actually applied the fixed set.
        check = _bounded_duckdb(root)
        try:
            for month, members in fixed.items():
                rows = check.execute(
                    "SELECT symbol, in_universe FROM read_parquet(?) "
                    "WHERE date >= ? AND date < (?::DATE + INTERVAL 1 MONTH)",
                    [str(staged_part), max(start, _dt.date.fromisoformat(month)), month],
                ).fetchall()
                if not rows or any(bool(in_universe) != (symbol in members)
                                   for symbol, in_universe in rows):
                    raise ValueError(f"frozen monthly membership was not applied: {month}")
        finally:
            check.close()
        input_hashes_after = {name: sha(path) for name, path in input_files.items()}
        if (input_hashes_after != input_hashes_before or sha(previous) != previous_hash or
                {str(path): sha(path) for path in ticker_paths} != ticker_hashes_before):
            raise ValueError("universe input snapshot changed during incremental build")
        # The marker and parquet become visible in one directory rename.
        manifest = {
            "schema_version": 1,
            "table": "universe_daily",
            "snapshot_date": str(snapshot_date),
            "snapshot_sha256": sha(staged_part),
            "previous_snapshot": str(previous),
            "previous_snapshot_sha256": previous_hash,
            "previous_end": previous_end.isoformat(),
            "added_start": start.isoformat(),
            "added_end": prices_end.isoformat(),
            "previous_month_seed_count": len(seed),
            "fixed_months": sorted(fixed),
            "input_snapshots": result["input_snapshots"],
            "input_snapshot_sha256": input_hashes_before,
            "ticker_source_sha256": {
                str(Path(name).relative_to(root.base)): digest
                for name, digest in ticker_hashes_before.items()
            },
            "unjudged_months": result["unjudged_months"],
        }
        if manifest["unjudged_months"]:
            raise ValueError("incremental universe contains an unjudged month")
        staged_marker = staged_dir / "completion.json"
        staged_marker.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        os.rename(staged_dir, destination.parent)
    except BaseException:
        shutil.rmtree(staged_dir, ignore_errors=True)
        raise
    return {
        **result,
        "path": destination,
        "completion_path": destination.parent / "completion.json",
    }

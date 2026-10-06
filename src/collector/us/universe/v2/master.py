"""증권 마스터 — 구간(security id)마다 증권 종류와 포함 여부 (설계 02 §2.2).

판정 시점 ``t`` 는 달 M 의 첫 거래일이다. **t 에 쓸 수 있던 근거만 쓴다** (T7).

| 원천 | 쓰기 시작하는 시점 |
|---|---|
| SEC 공시 | ``acceptance_datetime``(미 동부). **t 전 거래일 16:00 이전** 접수만 쓴다 |
| (시각 없음) | ``filing_date`` 다음 거래일부터 |
| 8-K item 5.06 | SEC 공시와 같다 |
| 상장 목록 | ``as_of`` 다음 거래일부터 (``as_of_time`` 의 시간대를 몰라서) |
| FTD 설명 | 결제일 반월 구간 끝 + ``LAG_FTD`` 뒤 다음 거래일부터 |

이 모듈은 둘로 나뉜다. ``prepare_evidence``·``gather`` 는 DuckDB 에서 근거를 모으고,
``classify`` 는 **순수 pandas** 라 시험이 규칙 하나씩을 합성 표로 직접 부른다.
"""

from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd

from collector.us.universe.v2.config import V2Rules
from collector.us.universe.v2.names import (
    COMMODITY_RE,
    FTD_EXCLUDE_KINDS,
    FUND_RE,
    KIND_BOND,
    KIND_COMMON,
    KIND_FUND,
    KIND_PREFERRED,
    KIND_SPAC,
    KIND_UNKNOWN,
    classify_name,
    common_subtype,
    ftd_kind,
    issuer_name_kind,
    jaccard,
    suffix_noncommon,
    token_other,
)

FUND_FORMS = ("N-CSR", "N-CSRS", "N-CEN", "NPORT-P", "N-CSR/A", "N-CSRS/A", "N-CEN/A", "NPORT-P/A")
PERIODIC_FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A", "20-F", "40-F", "20-F/A", "40-F/A")
DOMESTIC_BDC_FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A")
SIX_K_FORMS = ("6-K", "6-K/A")


def _in(forms: tuple[str, ...]) -> str:
    return ",".join(f"'{f}'" for f in forms)


#: 우선순위 순서 (설계 2.2). 위에서 먼저 걸린 것이 이긴다.
REASON_ORDER = (
    "fund_sec",
    "bdc_sec",
    "etf_list",
    "test_list",
    "name_bond",
    "name_pref",
    "name_spac",
    "spac_sic",
    "token_other",
    "commodity_trust",
    "name_fund_strong",
    "common_weak_issuer",
    "name_fund_weak",
    "common_name",
    "common_name_stale",
    "issuer_name_noncommon",
    "issuer_only_ftd",
    "issuer_only_suffix",
    "issuer_only",
    "f6k_only",
    "unknown",
)

INCLUDED = frozenset({"common_weak_issuer", "common_name", "common_name_stale", "issuer_only"})

SOURCE_CODE = {
    "fund_sec": "sec_filings",
    "bdc_sec": "sec_filings",
    "etf_list": "listing",
    "test_list": "listing",
    "name_bond": "listing_name",
    "name_pref": "listing_name",
    "name_spac": "listing_name",
    "spac_sic": "sec_sic",
    "token_other": "listing_name",
    "commodity_trust": "sec_sic+issuer_name",
    "name_fund_strong": "listing_name",
    "common_weak_issuer": "listing_name+sec_filings",
    "name_fund_weak": "listing_name",
    "common_name": "listing_name",
    "common_name_stale": "listing_name",
    "issuer_name_noncommon": "issuer_name",
    "issuer_only_ftd": "ftd_description",
    "issuer_only_suffix": "symbol_suffix",
    "issuer_only": "sec_filings",
    "f6k_only": "sec_filings",
    "unknown": "none",
}

_ETN_RE = re.compile(r"\betns?\b", re.I)


def _truthy(series: pd.Series) -> pd.Series:
    """None·NaN 을 False 로. object 열의 ``fillna`` 가 내는 downcast 경고를 피한다."""
    return pd.Series([bool(v) if pd.notna(v) else False for v in series], index=series.index)


# --- 근거 표 (DuckDB) --------------------------------------------------------------------


def prepare_evidence(con, *, rules: V2Rules, inputs: dict) -> None:
    """공시·발행사 이름·SIC·FTD 설명·상장 목록 표를 만든다.

    전제 표: ``px``·``ticker_pit``·``lst``·``nxt``·``xcal``. ``inputs`` 는 스냅샷 경로.
    """
    fi = str(inputs["filings_index"])
    con.execute("CREATE OR REPLACE TABLE tk_ciks AS SELECT DISTINCT cik FROM ticker_pit")
    con.execute(f"""
        CREATE OR REPLACE TABLE fi0 AS
        SELECT cik, form, filing_date, file_number, items,
               (acceptance_datetime AT TIME ZONE 'America/New_York') AS acc_et
        FROM read_parquet('{fi}')
        WHERE cik IN (SELECT cik FROM tk_ciks)
        """)
    # 접수 시각이 있으면 t 전 거래일 16:00 cutoff, 없으면 filing_date 다음 거래일.
    # n1 = 접수일 뒤 첫 거래일, n2 = 그 다음 거래일.
    usable = """
        CASE WHEN u.no_time THEN n.n1
             WHEN x.date IS NOT NULL AND CAST(u.ts AS TIME) <= TIME '16:00:00' THEN n.n1
             ELSE n.n2 END
        """
    con.execute(f"""
        CREATE OR REPLACE TABLE ev AS
        WITH base AS (
            SELECT cik, form, filing_date, file_number, acc_et,
                   CASE WHEN form IN ({_in(FUND_FORMS)}) THEN 'fund'
                        WHEN form = 'N-54A'
                          OR (form IN ({_in(DOMESTIC_BDC_FORMS)}) AND file_number LIKE '814-%')
                          THEN 'bdc'
                        WHEN form = 'N-54C' THEN 'n54c'
                        WHEN form IN ({_in(PERIODIC_FORMS)}) THEN 'op'
                        WHEN form IN ({_in(SIX_K_FORMS)}) THEN 'k6' END AS kind,
                   coalesce(acc_et, CAST(filing_date AS TIMESTAMP) + INTERVAL 12 HOUR) AS ts,
                   acc_et IS NULL AS no_time
            FROM fi0
        ),
        u AS (
            SELECT cik, kind, filing_date, ts, no_time FROM base WHERE kind IS NOT NULL
            UNION ALL
            -- 814- 번호 10-K·10-Q 는 BDC 근거이면서 정기보고서 근거이기도 하다
            SELECT cik, 'op', filing_date, ts, no_time FROM base
            WHERE kind = 'bdc' AND form IN ({_in(DOMESTIC_BDC_FORMS)})
        )
        SELECT u.cik, u.kind, u.filing_date, u.ts, {usable} AS usable
        FROM u JOIN nxt n ON n.d = CAST(u.ts AS DATE)
        LEFT JOIN xcal x ON x.date = CAST(u.ts AS DATE)
        """)
    # 8-K item 5.06 (합병 완료): SPAC 표시를 푸는 사건 (B3)
    con.execute(f"""
        CREATE OR REPLACE TABLE ev506 AS
        WITH b AS (
            SELECT cik, filing_date, acc_et,
                   coalesce(acc_et, CAST(filing_date AS TIMESTAMP) + INTERVAL 12 HOUR) AS ts,
                   acc_et IS NULL AS no_time
            FROM fi0
            WHERE form IN ('8-K', '8-K/A') AND items IS NOT NULL
              AND regexp_matches(items, '(^|,)\\s*5\\.06\\s*(,|$)')
        ),
        u AS (SELECT * FROM b)
        SELECT cik, min({usable}) AS usable
        FROM u JOIN nxt n ON n.d = CAST(u.ts AS DATE)
        LEFT JOIN xcal x ON x.date = CAST(u.ts AS DATE)
        GROUP BY cik
        """)
    # 마지막 공시(양식 무관) — 분리 뒤 구간의 cik 가 휴면인지 본다. 날짜 기준 다음 거래일.
    con.execute("""
        CREATE OR REPLACE TABLE fa AS
        SELECT DISTINCT f.cik, n.n1 AS usable
        FROM (SELECT DISTINCT cik, filing_date FROM fi0) f JOIN nxt n ON n.d = f.filing_date
        """)
    con.execute("DROP TABLE fi0")

    # 발행사 이름: 시점별. 지금 이름(f IS NULL)과 옛 이름(from~to)
    cm = con.execute(f"""
        SELECT cik, name, former_names FROM read_parquet('{inputs["company_meta"]}')
        WHERE cik IN (SELECT cik FROM tk_ciks)
        """).df()
    names: list[tuple] = []
    for r in cm.itertuples():
        names.append((int(r.cik), r.name, None, None))
        if r.former_names:
            try:
                formers = json.loads(r.former_names)
            except ValueError:
                formers = []
            for f in formers:
                names.append(
                    (
                        int(r.cik),
                        f.get("name"),
                        (f.get("from") or "")[:10] or None,
                        (f.get("to") or "")[:10] or None,
                    )
                )
    con.execute("CREATE OR REPLACE TABLE cikname (cik BIGINT, name VARCHAR, f VARCHAR, t VARCHAR)")
    if names:
        con.executemany("INSERT INTO cikname VALUES (?, ?, ?, ?)", names)

    # SIC: 분기 sub.txt 의 filed 날짜 기준. t 전날까지 공시된 최근 값
    con.execute(f"""
        CREATE OR REPLACE TABLE sic_hist AS
        SELECT cik, filed, sic FROM read_parquet('{inputs["filings_sub"]}')
        WHERE sic IS NOT NULL AND cik IN (SELECT cik FROM tk_ciks) ORDER BY cik, filed
        """)

    # FTD 설명: 결제일 반월 끝 + LAG_FTD 달력일 뒤 다음 거래일부터. 심볼·사용일마다 마지막 하나
    lag = int(rules.lag_ftd_days)
    con.execute(f"""
        CREATE OR REPLACE TABLE ftd_use AS
        WITH r AS (
            SELECT symbol, settlement_date AS sd, cusip, description AS d, quantity AS q,
                   (CASE WHEN day(settlement_date) <= 15
                         THEN make_date(year(settlement_date), month(settlement_date), 15)
                         ELSE last_day(settlement_date) END + INTERVAL {lag} DAY)::DATE AS pub
            FROM read_parquet('{inputs["ftd_fails"]}')
            WHERE settlement_date >= DATE '2017-06-01' AND description IS NOT NULL
              AND symbol IN (SELECT DISTINCT symbol FROM px)
        ),
        j AS (
            SELECT r.symbol, n.n1 AS use, r.sd, r.cusip, r.d,
                   row_number() OVER (
                       PARTITION BY r.symbol, n.n1 ORDER BY r.sd DESC, r.q DESC
                   ) AS rn
            FROM r JOIN nxt n ON n.d = r.pub
        )
        SELECT symbol, use, sd, cusip, d FROM j WHERE rn = 1 ORDER BY symbol, use
        """)

    # 상장 목록: 한 as_of 에 한 심볼이 두 갈래로 오면 하나만(드물다)
    con.execute("""
        CREATE OR REPLACE TABLE lf AS
        SELECT symbol, as_of, kind, security_name, is_etf, test_issue FROM (
            SELECT *, row_number() OVER (PARTITION BY symbol, as_of ORDER BY kind) AS rn FROM lst
        ) WHERE rn = 1 ORDER BY symbol, as_of
        """)
    con.execute("CREATE OR REPLACE TABLE lk AS SELECT DISTINCT kind, as_of FROM lst ORDER BY 1, 2")


def gather_listing(con, cand: pd.DataFrame) -> pd.DataFrame:
    """후보 심볼마다 ``t`` **앞의**(as_of < t) 가장 가까운 상장 목록 행과 낡음 여부.

    ``cand`` 열: ``symbol``, ``t``. 낡음(``stale``)은 그 갈래의 최신 목록(as_of < t)에
    그 심볼이 없다는 뜻이다(목록에서 빠진 뒤에도 옛 행이 끌려온다).
    """
    con.register("cm_df", cand[["symbol", "t"]])
    con.execute("CREATE OR REPLACE TEMP TABLE cm AS SELECT * FROM cm_df")
    con.unregister("cm_df")
    out = con.execute("""
        WITH a AS (
            SELECT c.symbol, c.t, l.as_of AS l_asof, l.kind AS l_kind,
                   l.security_name, l.is_etf AS l_etf, l.test_issue AS l_test
            FROM cm c ASOF LEFT JOIN lf l ON l.symbol = c.symbol AND c.t > l.as_of
        )
        SELECT a.symbol, a.l_asof, a.l_kind, a.security_name, a.l_etf, a.l_test,
               (a.l_asof IS NOT NULL AND a.l_asof < k.as_of) AS stale
        FROM a ASOF LEFT JOIN lk k ON k.kind = a.l_kind AND a.t > k.as_of
        """).df()
    return out


def gather_evidence(con, frame: pd.DataFrame, t, rules: V2Rules) -> pd.DataFrame:
    """``cik_e`` 가 있는 행에 SEC 근거·발행사 이름·SIC·5.06 을 붙인다 (시점 ``t``).

    ``frame`` 열: ``symbol``, ``cik_e``(nullable). 반환은 ``frame`` 과 같은 행 순서에 열만 더한다.
    """
    ciks = pd.DataFrame({"cik": frame.cik_e.dropna().astype("int64").unique()})
    con.register("q_df", ciks)
    con.execute("CREATE OR REPLACE TEMP TABLE q AS SELECT * FROM q_df")
    con.unregister("q_df")
    tstr = str(t)
    months = int(rules.periodic_months)
    evf = con.execute(f"""
        SELECT q.cik,
               bool_or(e.kind = 'fund') AS s_fund_any,
               max(e.ts) FILTER (WHERE e.kind = 'bdc') AS bdc_ts,
               max(e.ts) FILTER (WHERE e.kind = 'n54c') AS n54c_ts,
               bool_or(e.kind = 'op' AND e.filing_date >= DATE '{tstr}' - INTERVAL {months} MONTH)
                   AS op18,
               bool_or(e.kind = 'k6' AND e.filing_date >= DATE '{tstr}' - INTERVAL {months} MONTH)
                   AS k6_any
        FROM q JOIN ev e ON e.cik = q.cik WHERE e.usable <= DATE '{tstr}' GROUP BY q.cik
        """).df()
    cname = con.execute(f"""
        WITH cur AS (
            SELECT cik, any_value(name) AS name FROM cikname WHERE f IS NULL GROUP BY cik
        ),
        old AS (
            SELECT n.cik, n.name FROM cikname n JOIN q ON q.cik = n.cik
            WHERE n.f IS NOT NULL AND n.f <= '{tstr}' AND '{tstr}' < coalesce(n.t, '9999-99-99')
            QUALIFY row_number() OVER (PARTITION BY n.cik ORDER BY n.f DESC) = 1
        )
        SELECT q.cik, coalesce(o.name, c.name) AS cname
        FROM q LEFT JOIN old o ON o.cik = q.cik LEFT JOIN cur c ON c.cik = q.cik
        """).df()
    sic = con.execute(f"""
        SELECT q.cik, s.sic AS sic_e
        FROM (SELECT cik, DATE '{tstr}' AS t FROM q) q
        ASOF LEFT JOIN sic_hist s ON s.cik = q.cik AND q.t > s.filed
        """).df()
    u506 = con.execute(
        "SELECT cik, usable AS u506 FROM ev506 WHERE cik IN (SELECT cik FROM q)"
    ).df()
    out = frame[["symbol", "cik_e"]].copy()
    out["cik_key"] = out.cik_e.astype("Int64")
    for d in (evf, cname, sic, u506):
        d["cik_key"] = d.cik.astype("int64").astype("Int64")
    out = (
        out.merge(evf.drop(columns="cik"), on="cik_key", how="left")
        .merge(cname.drop(columns="cik"), on="cik_key", how="left")
        .merge(sic.drop(columns="cik"), on="cik_key", how="left")
        .merge(u506.drop(columns="cik"), on="cik_key", how="left")
    )
    out["s_fund"] = _truthy(out.s_fund_any)
    bdc = out.bdc_ts.notna() & (out.n54c_ts.isna() | (out.bdc_ts > out.n54c_ts))
    out["s_bdc"] = bdc.astype(bool)
    out["op18"] = _truthy(out.op18)
    out["k6_18"] = _truthy(out.k6_any) & ~out.op18
    out["rel506"] = out.u506.notna() & (pd.to_datetime(out.u506) <= pd.Timestamp(t))
    out["sic_e"] = out.sic_e.where(out.cik_e.notna(), None)
    return out.drop(
        columns=["cik_key", "s_fund_any", "bdc_ts", "n54c_ts", "k6_any", "u506"]
    ).reset_index(drop=True)


def gather_dormant(con, ciks: pd.Series, t, rules: V2Rules) -> set[int]:
    """t 에 마지막 공시가 ``cik_dormant_days`` 보다 오래됐거나 하나도 없는 cik."""
    ciks = ciks.dropna().astype("int64").unique()
    if not len(ciks):
        return set()
    con.register("dq_df", pd.DataFrame({"cik": ciks}))
    got = con.execute(f"""
        SELECT d.cik, max(a.usable) AS last_use
        FROM dq_df d LEFT JOIN fa a ON a.cik = d.cik AND a.usable <= DATE '{t}' GROUP BY d.cik
        """).df()
    con.unregister("dq_df")
    limit = pd.Timestamp(t) - pd.Timedelta(days=rules.cik_dormant_days)
    dead = got[got.last_use.isna() | (pd.to_datetime(got.last_use) < limit)]
    return {int(c) for c in dead.cik}


def gather_ftd(con, cand: pd.DataFrame) -> pd.DataFrame:
    """심볼마다 ``t`` 에 공개돼 있던 가장 최근 FTD 설명.

    열: ``symbol``, ``ftd_desc``, ``ftd_sd``.
    """
    con.register("cm_df", cand[["symbol", "t"]])
    out = con.execute("""
        SELECT c.symbol, f.d AS ftd_desc, f.sd AS ftd_sd
        FROM cm_df c ASOF LEFT JOIN ftd_use f ON f.symbol = c.symbol AND c.t >= f.use
        """).df()
    con.unregister("cm_df")
    return out


# --- 판정 (순수 pandas) ----------------------------------------------------------------------

_CLASSIFY_COLUMNS = (
    "symbol",
    "security_name",
    "l_etf",
    "l_test",
    "stale",
    "cik_e",
    "sic_e",
    "s_fund",
    "s_bdc",
    "op18",
    "k6_18",
    "cname",
    "rel506",
    "ftd_desc",
)


def classify(df: pd.DataFrame, rules: V2Rules) -> pd.DataFrame:
    """증권 종류를 우선순위대로 가른다.

    입력 열은 ``_CLASSIFY_COLUMNS`` + 선택 ``name_to_operating`` 이다.

    ``security_name`` 이 ``None`` 이면 상장 목록 이름이 없거나 분리로 무효가 된 행이다.
    ``ftd_desc`` 는 **이미 구간·나이 조건을 거친** 값이다(못 쓰는 설명은 ``None``).

    반환: ``reason_code``, ``source_code``, ``include``, ``issuer_kind``, ``security_type``,
    ``ftd_kind`` 를 더한 사본.
    """
    missing = [c for c in _CLASSIFY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"판정에 필요한 열이 없다: {missing}")
    r = df.copy().reset_index(drop=True)
    for c in ("stale", "l_etf", "l_test", "s_fund", "s_bdc", "op18", "k6_18", "rel506"):
        r[c] = _truthy(r[c])
    for c in ("security_name", "cname", "ftd_desc", "sic_e"):
        r[c] = r[c].astype(object).where(r[c].notna(), None)
    if "name_to_operating" not in r.columns:
        r["name_to_operating"] = False
    names = r.security_name
    hasname = names.notna() & (names != "")

    def name_kind(symbol: str, name: str | None, stale: bool) -> str:
        if stale:
            return KIND_PREFERRED if "$" in symbol else "stale"
        return classify_name(symbol, name)

    pairs = r[["symbol", "security_name", "stale"]].drop_duplicates()
    kind_of = {
        (p.symbol, p.security_name, p.stale): name_kind(p.symbol, p.security_name, p.stale)
        for p in pairs.itertuples()
    }
    key = list(zip(r.symbol, r.security_name, r.stale, strict=True))
    nk = pd.Series([kind_of[k] for k in key], index=r.index)
    tok = pd.Series(
        [None if s else token_other(n) for n, s in zip(r.security_name, r.stale, strict=True)],
        index=r.index,
    )
    fund_strong = pd.Series(
        [bool(isinstance(n, str) and FUND_RE.search(n)) for n in r.security_name], index=r.index
    )
    cname_nc = pd.Series(
        [issuer_name_kind(s, n) for s, n in zip(r.symbol, r.cname, strict=True)], index=r.index
    )
    cname_trust = r.cname.fillna("").map(lambda n: bool(COMMODITY_RE.search(n)))
    jac = pd.Series(
        [jaccard(a, b) for a, b in zip(r.security_name, r.cname, strict=True)], index=r.index
    ).astype(float)
    sic6770 = r.sic_e == "6770"
    sic6221 = r.sic_e == "6221"
    ftd_k = pd.Series([ftd_kind(d) for d in r.ftd_desc], index=r.index)
    io_base = r.op18 & ~hasname
    io_ftd = io_base & ftd_k.isin(FTD_EXCLUDE_KINDS)
    io_suffix = (
        io_base & ~io_ftd & pd.Series([suffix_noncommon(s) for s in r.symbol], index=r.index)
    )
    release_name = bool(rules.spac_release_name_change)
    spac_sic_active = sic6770 & ~r.rel506 & ~(r.name_to_operating & release_name)
    weak_ok = (nk == KIND_FUND) & ~fund_strong & r.op18 & (jac >= rules.weak_fund_jaccard)
    noncommon_issuer = cname_nc.isin([KIND_FUND, KIND_BOND, KIND_PREFERRED, KIND_SPAC])

    conds = [
        r.s_fund,
        r.s_bdc,
        r.l_etf,
        r.l_test,
        nk == KIND_BOND,
        nk == KIND_PREFERRED,
        (nk == KIND_SPAC) & ~r.rel506,
        spac_sic_active,
        tok.notna(),
        sic6221 & cname_trust,
        (nk == KIND_FUND) & fund_strong,
        weak_ok,
        (nk == KIND_FUND) & ~fund_strong,
        hasname & ~r.stale,
        hasname & r.stale,
        noncommon_issuer & ~hasname,
        io_ftd,
        io_suffix,
        io_base,
        r.k6_18 & ~hasname,
    ]
    labels = list(REASON_ORDER[:-1])
    r["reason_code"] = np.select(conds, labels, default="unknown")
    r["source_code"] = r.reason_code.map(SOURCE_CODE)
    # B3: 5.06 으로 SPAC 표시를 풀었으면 근거에 남긴다
    released = r.rel506 & (
        ((nk == KIND_SPAC) | sic6770) & ~r.reason_code.isin(["name_spac", "spac_sic"])
    )
    r.loc[released, "source_code"] = r.loc[released, "source_code"] + "+8k_506"
    r["include"] = r.reason_code.isin(INCLUDED)
    r["ftd_kind"] = ftd_k

    spac_reason = r.reason_code.isin(["name_spac", "spac_sic"])
    issuer_kind = np.select(
        [
            r.s_fund,
            r.s_bdc,
            spac_reason,
            r.reason_code == "commodity_trust",
            r.op18,
        ],
        ["registered_fund", "bdc", "spac", "registered_fund", "operating"],
        default="unknown",
    )
    r["issuer_kind"] = issuer_kind

    def sec_type(row, nk_, tok_, cnc, fk) -> str:
        reason = row.reason_code
        name = row.security_name
        if reason == "fund_sec":
            return "etf" if row.l_etf else "fund"
        if reason == "etf_list":
            return "etf"
        if reason == "test_list":
            return "test"
        if reason == "name_bond":
            return "etn" if isinstance(name, str) and _ETN_RE.search(name) else "bond"
        if reason == "name_pref":
            return "preferred"
        if reason in ("name_spac", "spac_sic"):
            return tok_ or "common"
        if reason == "token_other":
            return tok_
        if reason == "commodity_trust":
            return "trust"
        if reason in ("name_fund_strong", "name_fund_weak"):
            return "fund"
        if reason in ("common_weak_issuer", "common_name", "common_name_stale"):
            return common_subtype(name)
        if reason == "issuer_name_noncommon":
            return {KIND_BOND: "bond", KIND_PREFERRED: "preferred", KIND_FUND: "fund"}.get(
                cnc, "common"
            )
        if reason == "issuer_only_ftd":
            return {"bond": "bond", "preferred": "preferred"}.get(fk, "warrant_right_unit")
        if reason == "issuer_only":
            return "common"
        if reason == "bdc_sec":
            return common_subtype(name)
        return "unknown"

    r["security_type"] = [
        sec_type(row, a, b, c, d)
        for row, a, b, c, d in zip(r.itertuples(), nk, tok, cname_nc, ftd_k, strict=True)
    ]
    return r


def unknown_breakdown(frame: pd.DataFrame) -> dict[str, int]:
    """``unknown`` 판정의 원인을 가른다 (설계 7장: 목록 행 없음·분리로 이름 무효·cik 없음)."""
    u = frame[frame.reason_code == "unknown"]
    if not len(u):
        return {}
    out = {
        "no_listing_row": int(u.l_asof.isna().sum()),
        "name_invalidated": int((u.l_asof.notna() & u.security_name.isna()).sum()),
        "no_cik": int(u.cik_e.isna().sum()),
    }
    out["total"] = len(u)
    return out


__all__ = [
    "INCLUDED",
    "REASON_ORDER",
    "SOURCE_CODE",
    "classify",
    "gather_dormant",
    "gather_evidence",
    "gather_ftd",
    "gather_listing",
    "prepare_evidence",
    "unknown_breakdown",
    "KIND_COMMON",
    "KIND_UNKNOWN",
]

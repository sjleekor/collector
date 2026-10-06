"""유니버스 v2 조립 — 합성 레이크로 식별 표·마스터·멤버십을 끝까지 돌린다 (설계 02 §6).

T1·T2(재사용 사례)·T3(정상 보통주 보존)·T4(확인 안 됨 감시)·T7(PIT cutoff)·T8(issuer_only)·
T11(사후 보기를 안 읽음)과 5.06·N-54C 해제, 집단 재개일, 증분이 전체 재빌드와 같은지를 본다.
**원문은 하나도 없다.** 이름·심볼·cik 는 지어낸 값이다.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json

import duckdb
import pandas as pd
import pytest

from collector.us.sources import wayback
from collector.us.universe.v2 import build as v2
from collector.us.universe.v2.config import DEFAULT_RULES
from tests.helpers.us_v2_lake import D, SyntheticLake

FIRST, LAST = D(2019, 1, 2), D(2021, 12, 31)
START = "2020-03-02"


def q(path, sql: str):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{path}', hive_partitioning = false)")
    return con.execute(sql).fetchall()


def members(path, month: str) -> set[str]:
    """그 달 첫 거래일의 멤버 security id."""
    rows = q(
        path,
        f"""SELECT security_id FROM t WHERE in_universe AND date =
            (SELECT min(date) FROM t WHERE date >= DATE '{month}-01')""",
    )
    return {r[0] for r in rows}


def reasons(path, month: str) -> dict[str, str | None]:
    rows = q(
        path,
        f"""SELECT security_id, exclusion_reason FROM t WHERE date =
            (SELECT min(date) FROM t WHERE date >= DATE '{month}-01')""",
    )
    return dict(rows)


def build_lake(tmp_path) -> SyntheticLake:
    """재사용 사례·정상 보통주·SEC 시점 사례·issuer_only 를 한 레이크에 모은다."""
    lake = SyntheticLake(tmp_path, first=FIRST, last=LAST)
    static: dict[str, int] = {}  # 심볼 -> cik (한 번도 안 바뀌는 것)
    names: list[tuple] = []
    nxt = [1000]

    def common(symbol: str, name: str | None, *, periodic=True, listed=True, **kw) -> int:
        nxt[0] += 1
        c = nxt[0]
        lake.series(symbol, FIRST, LAST, **kw)
        static[symbol] = c
        lake.cik_of[symbol] = c
        if listed:
            names.append((symbol, name))
        lake.company(c, (name or symbol).split(" - ")[0], "3711")
        if periodic:
            lake.periodic(c, D(2018, 10, 1), LAST)
        return c

    # --- T3: 정상 보통주 (ADR·REIT·보통 지분·Unity·United 포함)
    for i in range(40):
        common(f"NC{i:02d}", f"Alpha{i} Zero Inc. - Common Stock")
    common(
        "ADR1", "Foo Holdings - American Depositary Shares, each representing one ordinary share"
    )
    common("RLTY", "Bar Realty Trust - Common Shares of Beneficial Interest")
    common("MLPX", "Barry Energy Partners L.P. - Common Units")
    common("UNTY", "Unity Software Inc. - Common Stock")
    common("UNTD", "United Rentals, Inc. - Common Stock")
    # 빼야 하는 것
    lake.series("ETFX", FIRST, LAST)
    names.append(("ETFX", "Shares of ETFX", True))
    lake.series("TSTX", FIRST, LAST)
    names.append(("TSTX", "Nasdaq test stock", False, True))
    for sym, nm in [
        ("BNDX", "Bondco 6.5% Notes due 2030"),
        ("PREF", "Prefco - 6.5% Preferred Shares"),
        ("WARR", "Warrco Corp - Warrant"),
        ("UNIT", "Unitco Corp - Unit"),
        ("SPCX", "Spacx Acquisition Corp - Class A Ordinary Shares"),
    ]:
        lake.series(sym, FIRST, LAST)
        names.append((sym, nm))

    # --- SEC 시점 (t = 2020-06-01 월요일)
    for sym, accepted in [
        ("FNDA", dt.datetime(2020, 5, 29, 15, 30)),  # 금 15:30 -> 6/1 부터
        ("FNDB", dt.datetime(2020, 5, 29, 17, 0)),  # 금 17:00 -> 6/2 부터
        ("FNDC", dt.datetime(2020, 5, 30, 10, 0)),  # 토 -> 6/2 부터
        ("FNDD", None),  # 접수 시각 없음, filing_date 가 금 -> 6/1 부터
    ]:
        c = common(sym, f"{sym} Corp - Common Stock")
        lake.filing(c, "N-CSR", D(2020, 5, 29), accepted=accepted)
    # 상장 목록: as_of 다음 거래일부터
    for sym, as_of in [("LSTA", D(2020, 5, 29)), ("LSTB", D(2020, 6, 1))]:
        common(sym, None, listed=False)
        lake.listing_monthly(
            [(sym, f"{sym} Corp - Common Stock")], first=D(2019, 1, 1), last=D(2020, 4, 30)
        )
        lake.listing(as_of, [(sym, f"{sym} Corp - Common Stock", True)])
    # B3: 8-K item 5.06 -> 다음 거래일부터 SPAC 표시를 푼다
    for sym, accepted in [
        ("SPCN", dt.datetime(2020, 8, 28, 15, 0)),
        ("SPCM", dt.datetime(2020, 8, 31, 16, 30)),
    ]:
        c = common(sym, f"{sym} Acquisition Corp - Class A Ordinary Shares")
        lake.filing(c, "8-K", accepted.date(), accepted=accepted, items="5.02,5.06,9.01")
    # N-54C: BDC 철회 뒤 풀린다 (2020-10-30 금 12:00 -> 11/2 부터)
    c = common("BDCA", "Bdca Capital Corp - Common Stock", periodic=False)
    lake.filing(
        c, "10-K", D(2019, 3, 1), accepted=dt.datetime(2019, 3, 1, 9, 0), file_number="814-12345"
    )
    lake.filing(c, "N-54C", D(2020, 10, 30), accepted=dt.datetime(2020, 10, 30, 12, 0))

    # --- T8·B1: 상장 목록 이름이 없는 증권 (issuer_only)
    for i in range(10):
        common(f"IOC{i}", None, listed=False)
        lake.ftd(D(2020, 1, 14), f"IOC{i}XXX101", f"IOC{i}", "XYZ CORP COM")
    for sym, desc in [
        ("IOB1", "AT&T INC 5.350% GLOBAL NTS"),
        ("IOW1", "ACME CORP WT"),
        ("IOP1", "SUPER MICRO COMP INC DEP SHS"),
        ("IOF1", "ISHARES TRUST"),
    ]:
        common(sym, None, listed=False)
        lake.ftd(D(2020, 1, 14), f"{sym}XXX101", sym, desc)
    common("IOSXU", None, listed=False)  # 설명 없음, 5글자 U 접미
    common("UNK1", None, listed=False, periodic=False)  # 근거가 하나도 없다 -> 확인 안 됨

    # --- T1: 재사용 사례 (심볼은 같고 다른 증권). 이름·cik 가 시점별로 바뀐다
    dyn: dict[str, dict[dt.date, int]] = {}  # 심볼 -> {지도 스냅샷일: cik}
    old, mid = D(2018, 11, 1), D(2020, 6, 30)
    # (a) 공백 253 + 보강 신호(K·C·N) — G_dorm
    lake.series("REUSE", FIRST, D(2019, 12, 31))
    lake.series("REUSE", D(2021, 1, 4), LAST)
    dyn["REUSE"] = {old: 10, D(2021, 1, 4): 11}
    lake.cusip("RRRRRR101", "REUSE", D(2019, 1, 2), D(2019, 12, 15))
    lake.cusip("SSSSSS101", "REUSE", D(2021, 1, 5), D(2021, 6, 30))
    # (b) 공백 63거래일 + CUSIP 앞 6자리 변경 뿐 — G_corr, 인지는 FTD 공개 지연 뒤 (BNY·P 모양)
    lake.series("GCOR", FIRST, D(2020, 3, 31))
    lake.series("GCOR", D(2020, 7, 1), LAST)
    dyn["GCOR"] = {old: 20}
    lake.cusip("GGGGGG101", "GCOR", D(2019, 1, 2), D(2020, 3, 15))
    lake.cusip("HHHHHH101", "GCOR", D(2020, 6, 25), D(2021, 6, 30))
    # (c) 옛 가격이 없는 새 상장 + 새 CUSIP — S_dorm (BXDC·HAWK 모양)
    lake.series("HAWK", D(2020, 6, 12), LAST)
    dyn["HAWK"] = {old: 30}
    lake.cusip("OLDOLD101", "HAWK", D(2019, 2, 1), D(2019, 6, 1))
    lake.cusip("NEWNEW101", "HAWK", D(2020, 6, 10), D(2021, 6, 30))
    # (d) 신호 없는 252 이상 공백 — G_gap252 (B4). 집단 재개일이 아니다
    lake.series("QUIET", FIRST, D(2019, 12, 31))
    lake.series("QUIET", D(2021, 2, 1), LAST)
    dyn["QUIET"] = {old: 40}
    # (d') 252 이상 공백, 보강 신호는 CUSIP 뿐이고 공개가 늦다 — G_dorm
    #      (설계 그대로면 인지가 늦어진다)
    lake.series("LATE", FIRST, D(2019, 12, 31))
    lake.series("LATE", D(2021, 3, 1), LAST)
    dyn["LATE"] = {old: 45}
    lake.cusip("LLLLLL101", "LATE", D(2019, 1, 2), D(2019, 12, 15))
    lake.cusip("MMMMMM101", "LATE", D(2021, 3, 2), D(2021, 6, 30))
    # (d'') 이월 방지: 옛 구간이 멤버였어도 새 구간은 진입 문턱($1M)이다. 대조군 KEEP 은 이어진다
    lake.series("HYST", FIRST, D(2020, 4, 30))
    lake.series("HYST", D(2021, 3, 1), LAST, volume=40_000)  # 거래대금 $0.8M
    dyn["HYST"] = {old: 90, D(2021, 3, 1): 91}
    lake.series("KEEP", FIRST, D(2020, 12, 31))
    lake.series("KEEP", D(2021, 1, 4), LAST, volume=40_000)
    static["KEEP"] = 92
    lake.company(92, "Keep Inc", "3711")
    lake.periodic(92, D(2018, 10, 1), LAST)
    # (d-3) 옛 ETF 표시: 심볼이 ETF 였다가 보통주 회사가 이어받았는데 목록은 안 갱신됐다
    lake.series("ETFR", FIRST, D(2019, 12, 31))
    lake.series("ETFR", D(2021, 1, 4), LAST)
    dyn["ETFR"] = {old: 80, D(2021, 1, 4): 81}
    lake.listing_monthly(
        [("ETFR", "Etfr Index Shares", True)], first=D(2019, 1, 1), last=D(2019, 12, 31)
    )
    # (e) 같은 날 20개가 재개 — 가격 결손으로 보고 안 끊는다
    for i in range(20):
        lake.series(f"MS{i:02d}", FIRST, D(2019, 12, 31))
        lake.series(f"MS{i:02d}", D(2021, 1, 5), LAST)
        dyn[f"MS{i:02d}"] = {old: 200 + i}
    # (e') 집단 재개일에 같이 재개했지만 보강 신호(CUSIP)가 나중에 공개된다 — 그때 끊는다
    lake.series("MSL", FIRST, D(2019, 12, 31))
    lake.series("MSL", D(2021, 1, 5), LAST)
    dyn["MSL"] = {old: 300}
    lake.cusip("QQQQQQ101", "MSL", D(2019, 1, 2), D(2019, 12, 15))
    lake.cusip("RRRQQQ101", "MSL", D(2021, 3, 2), D(2021, 6, 30))
    # (f) 오분리 방지: 가격이 이어지는 개명·cik 변경(PDD·AHG), 발행사 접두 변경(USLV)
    lake.series("PDDX", FIRST, LAST)
    dyn["PDDX"] = {old: 50, mid: 51}
    lake.series("USLX", FIRST, LAST)
    dyn["USLX"] = {old: 60}
    # (g) 같은 9자리 CUSIP 이 경계를 가로지름: PIT 는 끊고 사후 보기는 취소
    lake.series("SPAN", FIRST, D(2020, 4, 30))
    lake.series("SPAN", D(2020, 8, 3), LAST)
    dyn["SPAN"] = {old: 70, D(2020, 8, 3): 71}
    lake.cusip("SPNSPN101", "SPAN", D(2019, 1, 2), D(2021, 6, 30))
    # (h) 같은 날 cik 52건이 바뀌는 지도 일괄 갱신 + 가격 공백 (DIS·AA 모양)
    mass_day = D(2020, 9, 15)
    for i in range(52):
        sym = f"BK{i:02d}"
        lake.series(sym, D(2020, 7, 1), D(2020, 7, 31), volume=1_000)
        lake.series(sym, mass_day, D(2020, 12, 31), volume=1_000)
    lake.tickers(D(2020, 6, 1), [(f"BK{i:02d}", 3000 + i) for i in range(52)])
    lake.tickers(mass_day, [(f"BK{i:02d}", 4000 + i) for i in range(52)])

    # 상장 목록 이름. 재사용 심볼은 2021 년에 이름이 바뀐다
    renamed = {
        "REUSE": ("Reuse Old Inc. - Common Stock", "Brand New Tech Holdings - Common Stock"),
        "PDDX": ("Pinduoduo Inc. - Common Stock", "PDD Holdings Ltd - Common Stock"),
        "USLX": ("Credit Suisse AG - VelocityShares Silver Fund", "VelocityShares Silver Fund"),
        "SPAN": ("Span Old Corp - Common Stock", "Span New Holdings - Common Stock"),
    }
    plain = ["GCOR", "QUIET", "LATE", "MSL", "HYST", "KEEP"] + [f"MS{i:02d}" for i in range(20)]
    rows_old = [(s, renamed[s][0]) for s in renamed] + [
        (s, f"{s} Inc. - Common Stock") for s in plain
    ]
    rows_new = [(s, renamed[s][1]) for s in renamed] + [
        (s, f"{s} Inc. - Common Stock") for s in plain
    ]
    lake.listing_monthly(names + rows_old, first=D(2019, 1, 1), last=D(2020, 12, 31))
    lake.listing_monthly(
        names + rows_new + [("HAWK", "Hawk Inc. - Common Stock")],
        first=D(2021, 1, 1),
        last=D(2021, 12, 31),
    )
    lake.listing_monthly(
        [("HAWK", "Hawk Inc. - Common Stock")], first=D(2020, 6, 1), last=D(2020, 12, 31)
    )
    for cik_ in {c for m in dyn.values() for c in m.values()}:
        lake.periodic(cik_, D(2018, 10, 1), LAST)
        lake.company(cik_, f"Co{cik_}", "3711")
    # Wayback company_tickers 스냅샷: 정적 심볼은 전부 싣고, 재사용 심볼은 시점별 cik
    for as_of in sorted({d for m in dyn.values() for d in m} | {old, mid}):
        pairs = list(static.items())
        for sym, m in dyn.items():
            known = [d for d in m if d <= as_of]
            if known:
                pairs.append((sym, m[max(known)]))
        lake.tickers(as_of, pairs)
    return lake


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v2lake")
    lake = build_lake(tmp)
    sd = lake.flush()
    result = v2.build_universe_v2(lake.root, snapshot_date=sd, mode="rebuild", start=START)
    return lake, sd, result


@pytest.fixture(scope="module")
def paths(built):
    _, _, result = built
    return result["path"], result["security_segments"], result["security_master"]


def split_rows(seg_path, symbol: str, view: str = "pit"):
    return q(
        seg_path,
        f"""SELECT segment_no, split_reason, event_date, known_at, usable_from, identified
            FROM t WHERE symbol = '{symbol}' AND view = '{view}' AND segment_no > 1
            ORDER BY segment_no""",
    )


# --- T1 · T2: 재사용 사례 -------------------------------------------------------------------


def test_t1_reuse_patterns_are_split_with_the_documented_boundary(paths):
    _, seg, _ = paths
    reuse = split_rows(seg, "REUSE")
    assert [(r[0], r[1], r[2], r[4]) for r in reuse] == [
        (2, "G_dorm", D(2021, 1, 4), D(2021, 1, 5))
    ]
    gcor = split_rows(seg, "GCOR")
    assert [(r[1], r[2]) for r in gcor] == [("G_corr", D(2020, 7, 1))]
    assert gcor[0][3] > gcor[0][2] and gcor[0][4] > gcor[0][3]  # FTD 공개 지연 뒤에 안다
    hawk = split_rows(seg, "HAWK")
    assert [(r[1], r[2]) for r in hawk] == [("S_dorm", D(2020, 6, 12))]
    quiet = split_rows(seg, "QUIET")
    assert [(r[1], r[2], r[5]) for r in quiet] == [("G_gap252", D(2021, 2, 1), False)]
    late = split_rows(seg, "LATE")
    # 보강 신호(CUSIP FTD)는 4월에야 공개되지만 끊는 시점은 재개일이다. 옛 구간 무효화만 늦춘다
    assert [(r[1], r[2], r[4], r[5]) for r in late] == [
        ("G_dorm", D(2021, 3, 1), D(2021, 3, 2), True)
    ]
    msl = split_rows(seg, "MSL")
    # 집단 재개일(21개)이라 재개일에는 안 끊고, 보강 신호가 공개된 4/4 의 다음 거래일부터 끊는다
    assert [(r[1], r[2], r[3], r[4]) for r in msl] == [
        ("G_dorm", D(2021, 1, 5), D(2021, 4, 4), D(2021, 4, 5))
    ]
    # 경계 날짜는 사후 보기와 같다(사건일). 사후 보기는 사건일부터 쓴다
    post = split_rows(seg, "REUSE", "post")
    assert post[0][2] == D(2021, 1, 4) and post[0][4] == D(2021, 1, 4)


def test_t2_renames_and_mass_updates_are_not_reuse(paths):
    _, seg, _ = paths
    assert split_rows(seg, "PDDX") == []  # 이름·cik 가 바뀌어도 가격이 이어짐
    assert split_rows(seg, "USLX") == []  # 발행사 접두만 바뀜
    assert all(split_rows(seg, f"BK{i:02d}") == [] for i in range(52))  # cik 일괄 갱신(52건)
    # 같은 날 20개가 재개한 심볼은 가격 결손으로 본다
    assert all(split_rows(seg, f"MS{i:02d}") == [] for i in range(20))
    # 같은 CUSIP 이 경계를 가로지른 오분리는 PIT 에 하나, 사후 보기는 0 이다 (27건 상한 이내)
    assert [r[1] for r in split_rows(seg, "SPAN")] == ["G_corr"]
    assert split_rows(seg, "SPAN", "post") == []
    spans = q(
        seg, "SELECT count(*) FROM t WHERE view = 'pit' AND segment_no > 1 AND symbol = 'SPAN'"
    )
    assert spans[0][0] <= 27


def test_security_ids_follow_the_pit_label_by_date(paths):
    ud, _, _ = paths
    rows = q(
        ud,
        """SELECT security_id, min(date), max(date) FROM t WHERE symbol = 'REUSE'
           GROUP BY 1 ORDER BY 1""",
    )
    assert rows[0][0] == "REUSE#1" and rows[0][2] == D(2021, 1, 4)  # 인지 전(재개일 당일)까지
    assert rows[1][0] == "REUSE#2" and rows[1][1] == D(2021, 1, 5)


def test_new_segment_is_warmed_up_and_judged_at_the_entry_threshold(paths):
    ud, _, _ = paths
    # 2021-02-01 판정: 새 구간(REUSE#2)은 20번째 행이 2/1 이라 1월에 판정할 행이 없다 (예열)
    assert reasons(ud, "2021-02")["REUSE#2"] == "warmup"
    # 2021-01 첫 거래일(1/4)은 분리를 알기 전이라 REUSE#1 로 불린다. 앞 달 통계가 없어 판정 못 한다
    assert reasons(ud, "2021-01")["REUSE#1"] == "no_prev_month_stat"
    # 새 구간도 3월부터는 보통주로 들어온다. cik 는 새 회사의 것이다
    assert "REUSE#2" in members(ud, "2021-03")
    ciks = q(ud, "SELECT DISTINCT cik FROM t WHERE security_id = 'REUSE#2' AND cik IS NOT NULL")
    assert ciks == [(11,)]


def test_hysteresis_is_per_security_not_per_symbol(paths):
    """옛 구간이 멤버여도 새 구간은 진입 문턱 $1M 이다. 구간이 이어지는 심볼은 $0.7M 로 버틴다."""
    ud, _, _ = paths
    assert "KEEP#1" in members(ud, "2021-04")  # 거래대금 $0.8M, 이어달리기로 유지
    assert reasons(ud, "2021-04")["HYST#2"] == "below_entry_adv"
    assert "HYST#1" in members(ud, "2020-04")  # 옛 구간은 멤버였다


def test_old_etf_marker_is_invalidated_for_the_new_segment(paths):
    """심볼 재사용 직후 옛 ETF 표시가 새 증권에 붙지 않는다 (인지된 뒤부터)."""
    ud, _, master = paths
    # 목록은 2019-12 에서 멈췄고 거기에는 ETF 표시가 있다. 새 구간에서는 그 행을 무효로 둔다
    assert "ETFR#2" in members(ud, "2021-04")
    got = q(master, "SELECT reason_code FROM t WHERE security_id = 'ETFR#2' ORDER BY valid_from")
    assert got[-1] == ("issuer_only",)


# --- T3: 정상 보통주 보존 ---------------------------------------------------------------------


def test_t3_normal_common_stock_is_not_excluded(paths):
    ud, _, _ = paths
    normal = [f"NC{i:02d}" for i in range(40)] + ["ADR1", "RLTY", "MLPX", "UNTY", "UNTD"]
    months = ["2020-05", "2020-09", "2021-03", "2021-09", "2021-12"]
    wrong = total = 0
    for m in months:
        got = members(ud, m)
        for s in normal:
            total += 1
            wrong += f"{s}#1" not in got
    assert total == 45 * len(months)
    assert wrong / total <= 0.009  # 측정치(0.9%)를 넘지 않는다. 합성 데이터는 0 이다
    assert wrong == 0


def test_excluded_kinds_are_out_with_their_reason(paths):
    ud, _, _ = paths
    r = reasons(ud, "2021-03")
    assert r["ETFX#1"] == "etf_list"
    assert r["TSTX#1"] == "test_list"
    assert r["BNDX#1"] == "name_bond"
    assert r["PREF#1"] == "name_pref"
    assert r["WARR#1"] == "token_other"
    assert r["UNIT#1"] == "token_other"
    assert r["SPCX#1"] == "name_spac"


# --- T4: 확인 안 됨 감시 ---------------------------------------------------------------------


def test_t4_unknown_is_monitored_and_excluded(built, paths):
    ud, _, _ = paths
    _, _, result = built
    assert reasons(ud, "2021-03")["UNK1#1"] == "unknown"
    manifest = json.loads(result["completion_path"].read_text())
    ratios = [s["unknown_ratio"] for s in manifest["month_stats"]]
    assert max(ratios) <= DEFAULT_RULES.unknown_warn_ratio
    assert manifest["unknown_warnings"] == []
    assert all("unknown_breakdown" in s for s in manifest["month_stats"])
    last = manifest["month_stats"][-1]["unknown_breakdown"]
    assert last["total"] >= 1 and last["no_listing_row"] >= 1


def test_t4_unknown_warning_is_written_to_completion_without_failing(tmp_path):
    lake = build_lake(tmp_path)
    sd = lake.flush()
    strict = dataclasses.replace(DEFAULT_RULES, unknown_warn_ratio=0.0)
    result = v2.build_universe_v2(
        lake.root, snapshot_date=sd, mode="rebuild", start=START, rules=strict
    )
    assert result["unknown_warnings"]  # 경고만 하고 끝낸다 (설계 7장)
    assert members(result["path"], "2021-03")


# --- T8: issuer_only 구성 ---------------------------------------------------------------------


def test_t8_issuer_only_composition_and_b1_filter(paths):
    ud, _, master = paths
    r = reasons(ud, "2020-09")
    got = members(ud, "2020-09")
    for i in range(10):
        assert f"IOC{i}#1" in got  # 보통주 꼴 설명은 넣는다
    assert "IOF1#1" in got  # 펀드·ETF 꼴은 거르지 않는다
    for sym in ("IOB1", "IOW1", "IOP1"):
        assert r[f"{sym}#1"] == "issuer_only_ftd"
    assert r["IOSXU#1"] == "issuer_only_suffix"
    kinds = dict(
        q(
            master,
            "SELECT security_id, reason_code FROM t WHERE valid_from <= DATE '2020-09-01' "
            "AND (valid_to IS NULL OR valid_to > DATE '2020-09-01') AND security_id LIKE 'IO%'",
        )
    )
    kept = [k for k, v in kinds.items() if v == "issuer_only"]
    assert len(kept) == 11  # IOC0~9 와 IOF1
    # 남는 issuer_only 에 비보통주가 없다 (측정치 2.1% 이하)
    noncommon = [k for k in kept if k[:3] in ("IOB", "IOW", "IOP")]
    assert len(noncommon) / len(kept) <= 0.021


def test_ftd_description_older_than_400_days_is_not_used(paths):
    ud, _, _ = paths
    # 설명은 2020-01-14 결제 -> 2020-02 부터. 400일 뒤인 2021-03 에는 못 쓴다.
    # 그러면 이름 없는 증권은 issuer_only 로 그대로 들어온다
    assert reasons(ud, "2021-09")["IOB1#1"] is None


# --- SEC·상장 목록 시점 (T7 의 한 면) ----------------------------------------------------------


def test_sec_filings_are_used_from_the_previous_trading_day_1600_cutoff(paths):
    ud, _, _ = paths
    june = reasons(ud, "2020-06")  # t = 2020-06-01
    assert june["FNDA#1"] == "fund_sec"  # 금 15:30 접수 -> 6/1 부터
    assert june["FNDD#1"] == "fund_sec"  # 접수 시각 없음 -> filing_date 다음 거래일 6/1
    assert june["FNDB#1"] is None  # 금 17:00 접수 -> 6/2 부터라 6/1 판정에는 아직
    assert june["FNDC#1"] is None  # 토요일 접수 -> 6/2
    july = reasons(ud, "2020-07")
    assert july["FNDB#1"] == "fund_sec" and july["FNDC#1"] == "fund_sec"


def test_listing_rows_are_used_from_the_next_trading_day(paths):
    ud, _, _ = paths
    june = reasons(ud, "2020-06")
    assert june["LSTA#1"] == "etf_list"  # as_of 5/29 -> 6/1 부터
    assert june["LSTB#1"] is None  # as_of 6/1 은 같은 날이라 아직 못 쓴다
    assert reasons(ud, "2020-07")["LSTB#1"] == "etf_list"


# --- B3: 8-K 5.06 · N-54C ------------------------------------------------------------------


def test_item_506_releases_spac_from_the_next_trading_day(paths):
    ud, _, _ = paths
    assert reasons(ud, "2020-08")["SPCN#1"] == "name_spac"
    # 8/28(금) 15:00 접수 -> 8/31 부터 -> 9/1 판정에서 풀린다
    sep = reasons(ud, "2020-09")
    assert sep["SPCN#1"] is None
    # 8/31(월) 16:30 접수(장 마감 뒤) -> 9/2 부터 -> 9/1 판정에는 아직, 10월에 풀린다
    assert sep["SPCM#1"] == "name_spac"
    assert reasons(ud, "2020-10")["SPCM#1"] is None


def test_n54c_withdrawal_releases_bdc(paths):
    ud, _, _ = paths
    assert reasons(ud, "2020-10")["BDCA#1"] == "bdc_sec"
    # 10/30(금) 12:00 접수 -> 11/2 부터 -> 11/2 판정에서 풀린다
    assert reasons(ud, "2020-11")["BDCA#1"] is None


def test_mass_resume_day_symbols_keep_one_security(paths):
    ud, _, _ = paths
    ids = q(ud, "SELECT DISTINCT security_id FROM t WHERE symbol = 'MS00'")
    assert ids == [("MS00#1",)]
    # 신호 없는 252 이상 공백이지만 집단 재개일이 아닌 심볼은 끊는다
    got = {r[0] for r in q(ud, "SELECT DISTINCT security_id FROM t WHERE symbol = 'QUIET'")}
    assert got == {"QUIET#1", "QUIET#2"}


# --- T7: PIT cutoff (입력을 t 에서 잘라 다시 만든 분리 집합이 전체와 같다) ------------------------


def _splits(lake, *, known_cutoff=None, rules=DEFAULT_RULES):
    inputs = v2.v2_inputs.resolve_inputs(lake.root)
    rows = wayback.ticker_cik_map(lake.root)
    con = duckdb.connect()
    cal = lake.cal
    v2._load_base_tables(
        con,
        root=lake.root,
        inputs=inputs,
        cal=cal,
        rules=rules,
        ticker_rows=rows,
        ticker_parquet=None,
        known_cutoff=known_cutoff,
    )
    seg = v2.compute_segments(con, cal=cal, rules=rules)
    con.close()
    return seg.pit


def _known_by(pit: pd.DataFrame, cutoff: dt.date) -> set[tuple]:
    sub = pit[pit.usable_from <= pd.Timestamp(cutoff)]
    return {(r.symbol, r.event.date(), r.usable_from.date()) for r in sub.itertuples()}


CUTOFFS = [
    D(2020, 7, 15),
    D(2020, 7, 31),
    D(2020, 12, 31),
    D(2021, 1, 29),
    D(2021, 2, 26),
    D(2021, 3, 15),
    D(2021, 6, 30),
]


def test_t7_truncating_inputs_at_t_reproduces_the_splits_known_at_t(built):
    lake, _, _ = built
    full = _splits(lake)
    assert len(full) >= 4  # 시험이 빈 집합끼리 비교하지 않는다
    for cutoff in CUTOFFS:
        cut = _splits(lake, known_cutoff=cutoff)
        assert _known_by(cut, cutoff) == _known_by(full, cutoff), cutoff


def test_t7_the_literal_dorm_rule_would_fail_the_cutoff_check(built):
    """설계 그대로(G_dorm 을 보강 신호 중 늦은 쪽에 인지)는 G_gap252 와 시점이 어긋난다.

    입력을 잘라 다시 만들면 보강 신호가 안 보여 재개일에 이미 끊기 때문이다(LATE).
    그래서 기본은 재개일 인지다(``V2Rules.dorm_known_at_resume``).
    """
    lake, _, _ = built
    literal = dataclasses.replace(DEFAULT_RULES, dorm_known_at_resume=False)
    full = _splits(lake, rules=literal)
    cutoff = D(2021, 3, 15)  # LATE 재개 3/1, 보강(CUSIP FTD) 공개 4/4 사이
    cut = _splits(lake, known_cutoff=cutoff, rules=literal)
    assert _known_by(cut, cutoff) != _known_by(full, cutoff)


def test_t7_master_decisions_do_not_use_later_inputs(tmp_path):
    """t 이후에 접수된 공시·목록·FTD·5.06 은 t 의 판정에 안 들어간다.

    같은 레이크에 2021-06-30 뒤의 입력을 더해도 그날까지 굳은 판정이 안 바뀐다.
    """
    base = build_lake(tmp_path / "a")
    sd = base.flush()
    first = v2.build_universe_v2(
        base.root, snapshot_date=sd, mode="rebuild", start=START, end="2021-06-30"
    )
    more = build_lake(tmp_path / "b")
    more.filing(
        more.cik_of["NC00"], "N-CSR", D(2021, 7, 2), accepted=dt.datetime(2021, 7, 2, 10, 0)
    )
    more.filing(
        more.cik_of["SPCN"],
        "8-K",
        D(2021, 7, 6),
        accepted=dt.datetime(2021, 7, 6, 9, 0),
        items="5.06",
    )
    more.listing(D(2021, 7, 2), [("NC01", "Nc01 Fund", True)])
    more.ftd(D(2021, 7, 2), "NC02XXX101", "NC02", "NC02 CORP 5.35% NTS")
    sd2 = more.flush()
    second = v2.build_universe_v2(
        more.root, snapshot_date=sd2, mode="rebuild", start=START, end="2021-06-30"
    )
    cols = "date, symbol, security_id, cik, security_type, in_universe, exclusion_reason"
    assert q(first["path"], f"SELECT {cols} FROM t ORDER BY 1, 3") == q(
        second["path"], f"SELECT {cols} FROM t ORDER BY 1, 3"
    )
    mcols = "security_id, valid_from, valid_to, reason_code"
    assert q(first["security_master"], f"SELECT {mcols} FROM t ORDER BY 1, 2") == q(
        second["security_master"], f"SELECT {mcols} FROM t ORDER BY 1, 2"
    )


# --- T11: 판정 경로는 사후 보기를 읽지 않는다 ---------------------------------------------------


def test_t11_decisions_do_not_depend_on_the_post_view(tmp_path):
    lake = build_lake(tmp_path)
    sd = lake.flush()
    with_post = v2.build_universe_v2(lake.root, snapshot_date=sd, mode="rebuild", start=START)
    without = v2.build_universe_v2(
        lake.root, snapshot_date=f"{sd}x", mode="rebuild", start=START, write_post_view=False
    )
    cols = "date, symbol, security_id, cik, security_type, in_universe, exclusion_reason"
    assert q(with_post["path"], f"SELECT {cols} FROM t ORDER BY 1, 3") == q(
        without["path"], f"SELECT {cols} FROM t ORDER BY 1, 3"
    )
    mcols = "security_id, valid_from, valid_to, reason_code, include"
    assert q(with_post["security_master"], f"SELECT {mcols} FROM t ORDER BY 1, 2") == q(
        without["security_master"], f"SELECT {mcols} FROM t ORDER BY 1, 2"
    )
    assert q(with_post["security_segments"], "SELECT count(*) FROM t WHERE view = 'post'")[0][0] > 0
    assert q(without["security_segments"], "SELECT count(*) FROM t WHERE view = 'post'")[0][0] == 0
    # 판정 맥락은 PIT 번호 매김만 받는다
    assert "post" not in {f.name for f in dataclasses.fields(v2.JudgeContext)}


# --- 증분: 전체 재빌드와 같다 ---------------------------------------------------------------


def test_incremental_equals_full_rebuild_and_keeps_the_frozen_month(tmp_path):
    full_lake = build_lake(tmp_path / "full")
    full_lake.flush()
    full = v2.build_universe_v2(
        full_lake.root, snapshot_date="2022-01-31", mode="rebuild", start=START
    )
    # 같은 입력으로 9월 중순에서 끊은 스냅샷을 만들고 잇는다 (현재 달 = 9월이 굳은 달이다)
    inc_lake = build_lake(tmp_path / "inc")
    inc_lake.flush()
    part = v2.build_universe_v2(
        inc_lake.root, snapshot_date="2021-09-20", mode="rebuild", start=START, end="2021-09-15"
    )
    assert part["end"] == "2021-09-15"
    inc = v2.build_universe_v2(inc_lake.root, snapshot_date="2022-01-31", mode="incremental")
    assert inc["frozen_month"] == "2021-09-01"
    manifest = json.loads(inc["completion_path"].read_text())
    assert manifest["frozen_month_mismatch"] == 0
    assert manifest["previous_snapshot"].endswith("snapshot_date=2021-09-20/part.parquet")
    cols = (
        "date, symbol, security_id, cik, security_type, in_universe, exclusion_reason, rule_version"
    )
    assert q(inc["path"], f"SELECT {cols} FROM t ORDER BY 1, 3") == q(
        full["path"], f"SELECT {cols} FROM t ORDER BY 1, 3"
    )
    mcols = (
        "security_id, valid_from, valid_to, issuer_kind, security_type, include, "
        "reason_code, source_code"
    )
    assert q(inc["security_master"], f"SELECT {mcols} FROM t ORDER BY 1, 2") == q(
        full["security_master"], f"SELECT {mcols} FROM t ORDER BY 1, 2"
    )


def test_incremental_skips_when_nothing_is_new(tmp_path):
    lake = build_lake(tmp_path)
    lake.flush()
    v2.build_universe_v2(lake.root, snapshot_date="2022-01-31", mode="rebuild", start=START)
    skipped = v2.build_universe_v2(
        lake.root, snapshot_date="2022-02-01", mode="incremental", if_new=True
    )
    assert skipped == {"skipped": True, "reason": "no new price session"}
    with pytest.raises(ValueError, match="new completed price session"):
        v2.build_universe_v2(lake.root, snapshot_date="2022-02-01", mode="incremental")
    dry = v2.build_universe_v2(lake.root, snapshot_date="2022-02-02", mode="rebuild", dry_run=True)
    assert dry["dry_run"] is True


def test_incremental_without_a_prior_snapshot_asks_for_a_rebuild(tmp_path):
    lake = build_lake(tmp_path)
    lake.flush()
    with pytest.raises(FileNotFoundError, match="rebuild"):
        v2.build_universe_v2(lake.root, snapshot_date="2022-02-01", mode="incremental")


def test_completion_records_inputs_hashes_and_versions(built):
    _, _, result = built
    manifest = json.loads(result["completion_path"].read_text())
    assert manifest["table"] == "universe_daily_v2"
    assert set(manifest["input_snapshots"]) == {
        "prices_daily",
        "listing_snapshots_v2",
        "filings_index",
        "company_meta",
        "filings_sub",
        "cusip_symbol_pit",
        "ftd_fails",
    }
    assert all(len(h) == 64 for h in manifest["input_snapshot_sha256"].values())
    assert len(manifest["snapshot_sha256"]) == 64
    assert manifest["rule_version"].startswith("seg-r3c")
    assert "dolt_stocks" in manifest and manifest["rules"]["gap_dorm"] == 252
    assert manifest["ticker_source_sha256"]  # Wayback company_tickers 스냅샷의 해시


def test_universe_rows_have_unique_keys_and_the_documented_columns(paths):
    ud, _, _ = paths
    cols = [r[0] for r in q(ud, "SELECT column_name FROM (DESCRIBE SELECT * FROM t)")]
    assert cols == [
        "date",
        "symbol",
        "security_id",
        "cik",
        "security_type",
        "in_universe",
        "exclusion_reason",
        "rule_version",
        "observed_at",
    ]
    dup = q(
        ud,
        "SELECT count(*) FROM (SELECT date, security_id FROM t GROUP BY 1, 2 HAVING count(*) > 1)",
    )
    assert dup == [(0,)]
    bad = q(ud, "SELECT count(*) FROM t WHERE in_universe AND exclusion_reason IS NOT NULL")
    assert bad == [(0,)]

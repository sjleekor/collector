"""종목 식별 — 심볼 재사용을 가른 구간(``security_segments``, 설계 02 §2.1).

같은 심볼이 다른 증권을 가리키기 시작한 날을 **신호 넷**으로 찾는다.

| 신호 | 내용 | 인지 시점 |
|---|---|---|
| G | 같은 심볼의 연속 가격 행 사이 거래일 21 이상 | 재개일 |
| C | CUSIP 앞 6자리가 옛 것과 30일 이상 떨어짐 | 결제일 반월 구간 끝 + ``LAG_FTD`` |
| N | 상장 목록 이름이 다른 회사 이름으로 바뀜 | 목록 as_of |
| K | cik 전환(같은 날 50건 이상이면 지도 일괄 갱신이라 뺀다) | 전환일 |

**PIT 가 기준이다.** 사건일(event)과 인지 시점(known)을 따로 두고, 인지 시점 **다음
거래일부터** 쓴다. 사후 정정 보기(``post``)는 진단 전용이고 판정 경로가 읽지 않는다 (T11).

설계와 달라진 점 하나(``V2Rules.dorm_known_at_resume`` 로 되돌릴 수 있다).
``G_dorm``(공백 252거래일 이상 + 보강 신호)의 인지 시점을 **재개일로** 둔다
(집단 재개일은 빼고 — 그날은 안 끊으므로 보강 신호가 알려진 때 끊는다).
설계는 "보강 신호 중 늦은 쪽"이었는데, 그러면 입력을 t에서 잘라 다시 만들 때
``G_gap252``(보강 신호 없음, 재개일에 인지)와 시점이 어긋나 T7이 안 맞는다.
옛 구간의 cik·이름을 무효로 두는 것(``identified``)만 보강 신호가 인지된 뒤로 미룬다.
"""

from __future__ import annotations

import datetime as dt
import itertools
from collections.abc import Callable
from functools import lru_cache

import pandas as pd

from collector.us.universe.v2.config import V2Rules
from collector.us.universe.v2.names import big_name_change, norm_full

#: 사유 코드 (표 ``split_reason``)
G_CORR = "G_corr"
G_DORM = "G_dorm"
G_GAP252 = "G_gap252"
S_DORM = "S_dorm"
#: 내부 표시. 보강 신호가 **되돌림 K 뿐인** G_corr — PIT 는 G_corr 로 쓰고 사후 보기는 뺀다.
_G_CORR_REVK = "G_corr_revK"


# --- 신호 재료를 DuckDB 에서 뽑는다 ---------------------------------------------------


def make_signal_tables(con, rules: V2Rules) -> None:
    """``px``·``ticker_pit``·``cu``·``lst`` 에서 신호 재료 표를 만든다.

    전제 표: ``px(symbol, date, close, volume, idx)``(거래일만), ``ticker_pit(symbol, cik, as_of)``,
    ``cu(cusip, symbol, first_seen, last_seen, n)``, ``lst(symbol, as_of, kind, security_name)``.
    """
    # 가격 공백
    con.execute(f"""
        CREATE OR REPLACE TABLE pgap AS
        WITH g AS (
            SELECT symbol, date, idx,
                   lag(date) OVER w AS pd, idx - lag(idx) OVER w - 1 AS gap
            FROM px WINDOW w AS (PARTITION BY symbol ORDER BY date)
        )
        SELECT symbol, pd AS prev_price_date, date AS resume_date, gap
        FROM g WHERE gap >= {rules.gap_min}
        """)
    # 그날 cik 를 붙인다 (ticker_pit 이 PIT 다: date >= as_of 중 가장 최근 행)
    con.execute("""
        CREATE OR REPLACE TABLE px_cik AS
        SELECT p.symbol, p.date, ct.cik, ct.as_of AS cik_as_of
        FROM px p ASOF JOIN ticker_pit ct ON ct.symbol = p.symbol AND p.date >= ct.as_of
        """)
    con.execute("""
        CREATE OR REPLACE TABLE cik_changes AS
        WITH s AS (
            SELECT symbol, date, cik, lag(cik) OVER w AS pc, lag(date) OVER w AS pd
            FROM px_cik WINDOW w AS (PARTITION BY symbol ORDER BY date)
        )
        SELECT symbol, pd AS prev_date, date AS new_date, pc AS prev_cik, cik AS new_cik
        FROM s WHERE pc IS NOT NULL AND pc <> cik
        """)
    # (심볼, cik) 가 처음 가격에 붙은 날 — 분리 뒤 "경계 전에 붙은 cik" 판정에 쓴다
    con.execute("""
        CREATE OR REPLACE TABLE cik_first AS
        SELECT symbol, cik, min(date) AS first_date FROM px_cik GROUP BY 1, 2
        """)
    # CUSIP 앞 6자리 순차 사건: 새 앞 6자리마다 바로 앞서 끝난 다른 앞 6자리를 붙인다
    con.execute("""
        CREATE OR REPLACE TABLE cu6 AS
        SELECT symbol, left(cusip, 6) AS c6, min(first_seen) AS f, max(last_seen) AS l, sum(n) AS n
        FROM cu GROUP BY 1, 2
        """)
    con.execute("""
        CREATE OR REPLACE TABLE sig_cusip AS
        SELECT b.symbol, a.c6 AS c6_before, b.c6 AS c6_after, a.l AS old_last, b.f AS new_first,
               b.f - a.l AS sep_days
        FROM cu6 b JOIN cu6 a ON a.symbol = b.symbol AND a.c6 <> b.c6 AND a.l < b.f
        QUALIFY row_number() OVER (PARTITION BY b.symbol, b.c6 ORDER BY a.l DESC) = 1
        """)
    con.execute("""
        CREATE OR REPLACE TABLE lst2 AS
        SELECT symbol, as_of, min(security_name) AS nm FROM lst GROUP BY 1, 2
        """)


@lru_cache(maxsize=200_000)
def _norm_cached(name: str | None) -> tuple[str, ...]:
    return norm_full(name)


def name_events(rows) -> pd.DataFrame:
    """상장 목록 이름이 **다른 회사 이름으로** 바뀐 사건(신호 N).

    ``rows`` 는 ``(symbol, as_of, name)`` 을 심볼·날짜순으로. 같은 이름 구간(run)으로
    묶고, 한 번만 나왔다가 되돌아오는 구간(A→B→A)은 합친다.
    """
    events = []
    for sym, group in itertools.groupby(rows, key=lambda r: r[0]):
        runs: list[list] = []  # [토큰, 이름, 시작 as_of, 끝 as_of, 횟수]
        for _, as_of, nm in group:
            toks = _norm_cached(nm)
            if runs and not big_name_change(runs[-1][0], toks):
                runs[-1][3] = as_of
                runs[-1][4] += 1
                if toks and not runs[-1][0]:
                    runs[-1][0] = toks
            else:
                runs.append([toks, nm, as_of, as_of, 1])
        changed = True
        while changed:
            changed = False
            for i in range(1, len(runs) - 1):
                if runs[i][4] == 1 and not big_name_change(runs[i - 1][0], runs[i + 1][0]):
                    runs[i - 1][3] = runs[i + 1][3]
                    runs[i - 1][4] += runs[i + 1][4] + 1
                    del runs[i : i + 2]
                    changed = True
                    break
        for i in range(1, len(runs)):
            events.append((sym, runs[i - 1][3], runs[i][2], runs[i - 1][1], runs[i][1]))
    return pd.DataFrame(
        events, columns=["symbol", "prev_as_of", "new_as_of", "name_before", "name_after"]
    )


# --- 분리 사건 ------------------------------------------------------------------------


def derive_splits(
    *,
    gaps: pd.DataFrame,
    cik_changes: pd.DataFrame,
    cusip_events: pd.DataFrame,
    name_events_df: pd.DataFrame,
    first_price: dict[str, dt.date],
    cusip_ranges: pd.DataFrame,
    rules: V2Rules,
    pub_fn: Callable[[dt.date], dt.date],
) -> pd.DataFrame:
    """분리 사건 표. 한 행이 한 사건이다.

    열: ``symbol, event, kind, known_pit, ident_pit, old_end, gap, cusip_span`` 그리고 사후 보기용
    ``ident_post``. 날짜는 ``Timestamp`` 다. ``ident_pit`` 가 비면 보강 신호가 없다(``G_gap252``).
    """
    window = pd.Timedelta(days=rules.corroboration_days)

    # --- 신호 목록 (심볼, 형, 사건일, 인지일, 되돌림 K 표시)
    ck = cik_changes.copy()
    ck["rev"] = False
    if len(ck):
        counts = ck.groupby("new_date").size()
        mass = set(counts[counts >= rules.mass_cik_day].index)
        for _sym, x in ck.sort_values("new_date").groupby("symbol"):
            idx = list(x.index)
            for a, b in itertools.combinations(range(len(idx)), 2):
                i, j = idx[a], idx[b]
                if (
                    ck.at[j, "prev_cik"] == ck.at[i, "new_cik"]
                    and ck.at[j, "new_cik"] == ck.at[i, "prev_cik"]
                ):
                    ck.at[i, "rev"] = True
                    ck.at[j, "rev"] = True
        ck["mass"] = ck.new_date.isin(mass)
    else:
        ck["mass"] = False
    sig: list[tuple] = []
    for r in ck.itertuples():
        if not r.mass:
            sig.append((r.symbol, "K", r.new_date, r.new_date, bool(r.rev)))
    for r in cusip_events.itertuples():
        if r.sep_days >= rules.cusip_sep_days:
            sig.append((r.symbol, "C", r.new_first, pub_fn(r.new_first), False))
    for r in name_events_df.itertuples():
        sig.append((r.symbol, "N", r.new_as_of, r.new_as_of, False))
    sg = pd.DataFrame(sig, columns=["symbol", "typ", "event", "known", "rev"])
    sg["event"] = pd.to_datetime(sg["event"])
    sg["known"] = pd.to_datetime(sg["known"])
    by_symbol = {s: x for s, x in sg.groupby("symbol")}

    # --- 가격 공백 + 보강 신호
    splits: list[dict] = []
    unconfirmed: list[dict] = []
    resume_counts = gaps.groupby("resume_date").size() if len(gaps) else pd.Series(dtype=int)
    for r in gaps.itertuples():
        r0 = pd.Timestamp(r.resume_date)
        x = by_symbol.get(r.symbol)
        w = x[(x.event >= r0 - window) & (x.event <= r0 + window)] if x is not None else None
        if w is None or len(w) == 0:
            unconfirmed.append(
                {
                    "symbol": r.symbol,
                    "event": r0,
                    "gap": r.gap,
                    "old_end": pd.Timestamp(r.prev_price_date),
                    "mass_day": int(resume_counts.get(r.resume_date, 0))
                    >= rules.mass_resume_symbols,
                }
            )
            continue
        first_known = w.known.min()
        post_w = w[~((w.typ == "K") & w.rev)]
        ident_post = len(post_w) > 0
        old_end = pd.Timestamp(r.prev_price_date)
        if r.gap >= rules.gap_dorm:
            # 재개일에 이미 끊는다. 보강 신호는 옛 구간의 cik·이름을 무효로 두는 때만 늦춘다.
            # 단 집단 재개일(가격 결손 의심)은 재개일에 안 끊으므로(G_gap252 와 같다) 보강 신호가
            # 알려진 때 끊는다. 안 그러면 입력을 자른 재실행과 시점이 어긋난다(T7).
            # (``dorm_known_at_resume=False`` 면 설계 그대로 늘 보강 신호 중 늦은 쪽이다.)
            mass_day = int(resume_counts.get(r.resume_date, 0)) >= rules.mass_resume_symbols
            at_resume = rules.dorm_known_at_resume and not mass_day
            splits.append(
                {
                    "symbol": r.symbol,
                    "event": r0,
                    "kind": G_DORM,
                    "known_pit": r0 if at_resume else max(r0, first_known),
                    "ident_pit": first_known,
                    "ident_post": True,
                    "old_end": old_end,
                    "gap": r.gap,
                }
            )
        else:
            splits.append(
                {
                    "symbol": r.symbol,
                    "event": r0,
                    "kind": G_CORR if ident_post else _G_CORR_REVK,
                    "known_pit": max(r0, first_known),
                    "ident_pit": first_known,
                    "ident_post": ident_post,
                    "old_end": old_end,
                    "gap": r.gap,
                }
            )

    # --- 새 CUSIP 앞 6자리가 옛 것과 252일 이상 떨어진 새 상장
    for r in cusip_events.itertuples():
        if r.sep_days < rules.cusip_dorm_days:
            continue
        f = first_price.get(r.symbol)
        if f is None:
            continue
        f = pd.Timestamp(f)
        nf = pd.Timestamp(r.new_first)
        if abs((f - nf).days) <= rules.s_dorm_first_days:
            kp = max(f, pd.Timestamp(pub_fn(r.new_first)))
            splits.append(
                {
                    "symbol": r.symbol,
                    "event": f,
                    "kind": S_DORM,
                    "known_pit": kp,
                    "ident_pit": kp,
                    "ident_post": True,
                    # 옛 가격이 레이크에 없다. 옛 이름·표시를 무효로 둘 기준일을 120일 앞에 둔다.
                    "old_end": f - pd.Timedelta(days=120),
                    "gap": r.sep_days,
                }
            )

    sp = pd.DataFrame(splits)
    columns = [
        "symbol",
        "event",
        "kind",
        "known_pit",
        "ident_pit",
        "ident_post",
        "old_end",
        "gap",
    ]
    if len(sp):
        sp = _merge_close_events(sp, rules.merge_days)
    else:
        sp = pd.DataFrame(columns=columns)

    # --- B4: 보강 신호 없는 252거래일 이상 공백 (집단 재개일 제외)
    extra = [
        {
            "symbol": u["symbol"],
            "event": u["event"],
            "kind": G_GAP252,
            "known_pit": u["event"],
            "ident_pit": pd.NaT,
            "ident_post": False,
            "old_end": u["old_end"],
            "gap": u["gap"],
        }
        for u in unconfirmed
        if u["gap"] >= rules.gap_dorm and not u["mass_day"]
    ]
    if extra:
        sp = (
            pd.concat([sp, pd.DataFrame(extra)], ignore_index=True)
            if len(sp)
            else pd.DataFrame(extra)
        )
    if not len(sp):
        sp["cusip_span"] = pd.Series(dtype=bool)
        return sp

    # 사후 보기: 같은 9자리 CUSIP 이 경계를 가로지르면 분리를 취소한다
    ranges = {
        s: (g.first_seen.to_numpy(), g.last_seen.to_numpy())
        for s, g in cusip_ranges.assign(
            first_seen=pd.to_datetime(cusip_ranges.first_seen),
            last_seen=pd.to_datetime(cusip_ranges.last_seen),
        ).groupby("symbol")
    }

    def span(row) -> bool:
        if row.kind == S_DORM:
            return False
        got = ranges.get(row.symbol)
        if got is None:
            return False
        first, last = got
        return bool(
            ((first <= row.old_end.to_datetime64()) & (last >= row.event.to_datetime64())).any()
        )

    sp["cusip_span"] = [span(r) for r in sp.itertuples()]
    sp["pit_lag_days"] = (sp.known_pit - sp.event).dt.days
    return sp.sort_values(["symbol", "event"]).reset_index(drop=True)


def _merge_close_events(sp: pd.DataFrame, merge_days: int) -> pd.DataFrame:
    """같은 심볼에서 ``merge_days`` 안의 사건은 하나로 합친다 (인지는 빠른 쪽)."""
    sp = sp.sort_values(["symbol", "event"]).reset_index(drop=True)
    keep: list[dict] = []
    for _s, x in sp.groupby("symbol"):
        last: dict | None = None
        for r in x.to_dict("records"):
            if last is not None and (r["event"] - last["event"]).days <= merge_days:
                last["known_pit"] = min(last["known_pit"], r["known_pit"])
                if pd.isna(last["ident_pit"]) or (
                    not pd.isna(r["ident_pit"]) and r["ident_pit"] < last["ident_pit"]
                ):
                    last["ident_pit"] = r["ident_pit"]
                last["ident_post"] = bool(last["ident_post"]) or bool(r["ident_post"])
                continue
            last = dict(r)
            keep.append(last)
    return pd.DataFrame(keep)


# --- PIT·사후 보기로 구간 번호를 매긴다 --------------------------------------------------


def number_segments(splits: pd.DataFrame, cal, *, view: str) -> pd.DataFrame:
    """분리 사건에 구간 번호와 쓰기 시작하는 날을 붙인다.

    * ``pit``: 전부. ``usable_from`` 은 ``known_pit`` 다음 거래일.
    * ``post``: ``G_corr_revK``(되돌림 K 뿐)와 같은 CUSIP 이 경계를 가로지른 것을 뺀다.
      사건일부터 쓴다.

    구간 번호는 심볼별로 사건일 순서다(첫 구간은 1이라 사건이 2부터).
    """
    if view not in ("pit", "post"):
        raise ValueError(view)
    sp = splits.copy()
    if view == "post" and len(sp):
        sp = sp[(sp.kind != _G_CORR_REVK) & (~sp.cusip_span)]
    if not len(sp):
        return sp.assign(segment_no=pd.Series(dtype="int64"), usable_from=pd.NaT)
    sp = sp.sort_values(["symbol", "event"]).reset_index(drop=True)
    sp["segment_no"] = sp.groupby("symbol").cumcount() + 2
    if view == "pit":
        sp["usable_from"] = [pd.Timestamp(cal.n1(k.date())) for k in sp.known_pit]
        sp["ident_usable"] = [
            pd.Timestamp(cal.n1(k.date())) if not pd.isna(k) else pd.NaT for k in sp.ident_pit
        ]
        sp["identified"] = sp.ident_pit.notna()
    else:
        sp["usable_from"] = sp.event
        sp["ident_usable"] = sp.event.where(sp.ident_post, pd.NaT)
        sp["identified"] = sp.ident_post.astype(bool)
    return sp


def label_timeline(pit: pd.DataFrame) -> list[tuple[str, dt.date, int]]:
    """``(심볼, 적용 시작일, 구간 번호)`` — 날짜별 PIT 구간 번호의 변화 시점.

    t 에 알려진 사건 가운데 **사건일이 가장 늦은 것**의 구간이 그 날의 구간이다
    (``judge_month`` 와 같은 규칙). 인지가 사건 순서와 뒤바뀌어도 번호가 거꾸로 가지 않는다.
    """
    rows: list[tuple[str, dt.date, int]] = []
    if not len(pit):
        return rows
    for sym, x in pit.groupby("symbol"):
        best = None
        out: dict[dt.date, int] = {}
        for r in x.sort_values(["usable_from", "event"]).itertuples():
            if best is None or r.event > best.event:
                best = r
            out[r.usable_from.date()] = int(best.segment_no)
        rows.extend((sym, d, no) for d, no in sorted(out.items()))
    return rows


def split_reason(kind: str) -> str:
    return G_CORR if kind == _G_CORR_REVK else kind


def segment_rows(
    symbols: list[str],
    numbered: pd.DataFrame,
    *,
    view: str,
    first_dates: dict[str, dt.date],
    rule_version: str,
) -> pd.DataFrame:
    """``security_segments`` 행. 첫 구간 행은 모든 심볼에, 그 뒤는 분리 사건마다."""
    rows: list[dict] = []
    by_symbol = {s: x for s, x in numbered.groupby("symbol")} if len(numbered) else {}
    for sym in symbols:
        events = by_symbol.get(sym)
        starts = [] if events is None else list(events.itertuples())
        nxt_event = starts[0].event if starts else None
        rows.append(
            {
                "symbol": sym,
                "segment_no": 1,
                "security_id": f"{sym}#1",
                "view": view,
                "seg_start": None,
                "seg_end": (nxt_event - pd.Timedelta(days=1)).date()
                if nxt_event is not None
                else None,
                "event_date": None,
                "known_at": None,
                "usable_from": None,
                "split_reason": None,
                "identified": None,
                "rule_version": rule_version,
                "_first_date": first_dates.get(sym),
            }
        )
        for i, e in enumerate(starts):
            nxt = starts[i + 1].event if i + 1 < len(starts) else None
            rows.append(
                {
                    "symbol": sym,
                    "segment_no": int(e.segment_no),
                    "security_id": f"{sym}#{int(e.segment_no)}",
                    "view": view,
                    "seg_start": e.event.date(),
                    "seg_end": (nxt - pd.Timedelta(days=1)).date() if nxt is not None else None,
                    "event_date": e.event.date(),
                    "known_at": (e.known_pit if view == "pit" else e.event).date(),
                    "usable_from": e.usable_from.date(),
                    "split_reason": split_reason(e.kind),
                    "identified": bool(e.identified),
                    "rule_version": rule_version,
                    "_first_date": e.event.date(),
                }
            )
    return pd.DataFrame(rows)

"""``universe_daily_v2`` 조립 — 식별 표 → 증권 마스터 → 멤버십 (설계 02 §2.1~§2.3).

v1(``collector.us.universe.build``)과 **같은 것**: 거래대금(원시 종가 × 원시 거래량) 20거래일
롤링, 진입 $1M·유지 $0.7M, 20일 중 거래일 10 이상, 달 M 은 달 M-1 통계, 월 1회 판정.

**v1과 다른 것** 셋.

1. 롤링·월 중앙값·유지 문턱 이어달리기를 **security id 단위**로 한다. 새 구간은 20번째 행부터
   판정(예열)하고, 구간이 바뀌면 전달 멤버 이력을 잇지 않는다(진입 문턱).
2. 후보는 증권 마스터가 ``include`` 로 표시한 것만 둔다.
3. 키는 ``(date, security_id)`` 다.

**t 시점 PIT.** 달 M 의 판정 시점 t 는 M 의 첫 거래일이다. 분리 사건은 인지 시점 다음 거래일
(``usable_from``) 부터 t 에 적용하고, 마스터 근거는 ``master`` 모듈의 시점 규칙을 따른다.
**판정 경로는 ``view=post`` 를 읽지 않는다** (T11) — ``pit`` 번호 매김만 받는다.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from collector.us.universe.v2 import (
    MASTER_RULE_VERSION,
    RULE_VERSION,
    SEGMENT_RULE_VERSION,
    UNIVERSE_RULE_VERSION,
    master,
    segments,
)
from collector.us.universe.v2 import (
    inputs as v2_inputs,
)
from collector.us.universe.v2.cal import Calendar, ftd_publication
from collector.us.universe.v2.config import (
    DEFAULT_RULES,
    ENTRY_ADV_USD,
    JUDGE_LAG_MONTHS,
    MAINTAIN_ADV_USD,
    MIN_TRADED_DAYS_20,
    USABLE_FROM,
    V2Rules,
)
from collector.us.universe.v2.names import KIND_SPAC, classify_name

_CALENDAR_FROM = _dt.date(2010, 1, 1)

#: 달 판정에서 멤버가 아닌 이유 코드 (마스터가 뺀 것은 ``reason_code`` 를 그대로 쓴다)
REASON_WARMUP = "warmup"
REASON_NO_STAT = "no_stat"
REASON_MIN_TRADED = "min_traded_days"
REASON_BELOW_ENTRY = "below_entry_adv"
REASON_BELOW_MAINTAIN = "below_maintain_adv"
REASON_NOT_JUDGED = "segment_not_judged"
REASON_NO_PREV_STAT = "no_prev_month_stat"


def _shift_months(ym: _dt.date, months: int) -> _dt.date:
    total = ym.year * 12 + (ym.month - 1) + months
    return _dt.date(total // 12, total % 12 + 1, 1)


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dolt_commit(root) -> dict[str, str | None]:
    """dolt ``stocks`` 레포의 지금 HEAD 커밋. 없거나 못 읽으면 이유를 적는다."""
    from collector.us.sources import dolt

    repo = dolt.repo_dir(root, "stocks")
    if not (repo / ".dolt").is_dir():
        return {"commit": None, "note": "dolt stocks 레포가 없다"}
    try:
        return {"commit": dolt.head_commit(repo), "note": None}
    except Exception as exc:  # noqa: BLE001 — 기록이 빌드를 막지 않는다
        return {"commit": None, "note": f"{type(exc).__name__}: {exc}"}


# --- 준비: 가격·지도·신호 재료 ----------------------------------------------------------------


def _load_base_tables(
    con,
    *,
    root,
    inputs: dict[str, Path],
    cal: Calendar,
    rules: V2Rules,
    ticker_rows,
    ticker_parquet: Path | None,
    known_cutoff: _dt.date | None,
) -> None:
    """``px``·``ticker_pit``·``cu``·``lst`` 를 만든다.

    ``known_cutoff`` 를 주면 그날까지 알던 것만 쓴다.
    """
    import pyarrow as pa

    cal.register(con, from_date=_CALENDAR_FROM)
    cut_px = f"AND p.date <= DATE '{known_cutoff}'" if known_cutoff else ""
    con.execute(f"""
        CREATE OR REPLACE TABLE px AS
        SELECT p.symbol, p.date, CAST(p.close AS DOUBLE) AS close, p.volume, x.idx
        FROM read_parquet('{inputs["prices_daily"]}') p JOIN xcal x ON x.date = p.date
        WHERE TRUE {cut_px} ORDER BY p.symbol, p.date
        """)
    cut_t = f"WHERE as_of <= DATE '{known_cutoff}'" if known_cutoff else ""
    if ticker_parquet is not None:
        con.execute(f"""
            CREATE OR REPLACE TABLE ticker_pit AS
            SELECT symbol, CAST(cik AS BIGINT) AS cik, CAST(as_of AS DATE) AS as_of
            FROM read_parquet('{ticker_parquet}') {cut_t} ORDER BY symbol, as_of
            """)
    else:
        table = pa.table(
            {
                "symbol": pa.array([r[0] for r in ticker_rows], type=pa.string()),
                "cik": pa.array([r[1] for r in ticker_rows], type=pa.int64()),
                "as_of": pa.array([r[2] for r in ticker_rows], type=pa.date32()),
            }
        )
        con.register("ticker_arrow", table)
        con.execute(f"""
            CREATE OR REPLACE TABLE ticker_pit AS
            SELECT symbol, cik, as_of FROM ticker_arrow {cut_t} ORDER BY symbol, as_of
            """)
        con.unregister("ticker_arrow")
    cu = con.execute(f"""
        SELECT cusip, symbol, first_seen, last_seen, n_settlement_dates AS n
        FROM read_parquet('{inputs["cusip_symbol_pit"]}')
        """).df()
    if known_cutoff is not None and len(cu):
        pub = [ftd_publication(pd.Timestamp(d).date(), rules.lag_ftd_days) for d in cu.first_seen]
        cu = cu[[p <= known_cutoff for p in pub]]
    con.register("cu_df", cu)
    con.execute(
        "CREATE OR REPLACE TABLE cu AS SELECT cusip, symbol, "
        "CAST(first_seen AS DATE) AS first_seen, CAST(last_seen AS DATE) AS last_seen, n "
        "FROM cu_df"
    )
    con.unregister("cu_df")
    cut_l = f"WHERE as_of <= DATE '{known_cutoff}'" if known_cutoff else ""
    con.execute(f"""
        CREATE OR REPLACE TABLE lst AS
        SELECT symbol, as_of, kind, security_name, is_etf, test_issue
        FROM read_parquet('{inputs["listing_snapshots_v2"]}') {cut_l}
        """)


@dataclasses.dataclass
class SegmentResult:
    splits: pd.DataFrame
    pit: pd.DataFrame
    post: pd.DataFrame
    name_flips: pd.DataFrame


def compute_segments(con, *, cal: Calendar, rules: V2Rules) -> SegmentResult:
    """분리 사건과 PIT·사후 번호 매김. 전제 표는 ``_load_base_tables`` 가 만든다."""
    segments.make_signal_tables(con, rules)
    gaps = con.execute("SELECT * FROM pgap").df()
    cik_changes = con.execute("SELECT * FROM cik_changes").df()
    cusip_events = con.execute("SELECT * FROM sig_cusip").df()
    rows = con.execute("SELECT symbol, as_of, nm FROM lst2 ORDER BY symbol, as_of").fetchall()
    name_ev = segments.name_events(rows)
    first_price = dict(con.execute("SELECT symbol, min(date) FROM px GROUP BY 1").fetchall())
    ranges = con.execute("SELECT symbol, first_seen, last_seen FROM cu").df()

    def pub(d) -> _dt.date:
        return ftd_publication(pd.Timestamp(d).date(), rules.lag_ftd_days)

    splits = segments.derive_splits(
        gaps=gaps,
        cik_changes=cik_changes,
        cusip_events=cusip_events,
        name_events_df=name_ev,
        first_price=first_price,
        cusip_ranges=ranges,
        rules=rules,
        pub_fn=pub,
    )
    pit = segments.number_segments(splits, cal, view="pit")
    post = segments.number_segments(splits, cal, view="post")
    # 이름이 SPAC 꼴에서 운영회사 꼴로 바뀐 사건 — 설계 7장 보조 규칙(기본 끔)의 재료
    flips = []
    for r in name_ev.itertuples():
        if (
            classify_name(r.symbol, r.name_before) == KIND_SPAC
            and classify_name(r.symbol, r.name_after) != KIND_SPAC
        ):
            u = cal.n1(pd.Timestamp(r.new_as_of).date())
            if u is not None:
                flips.append((r.symbol, pd.Timestamp(u)))
    flip_df = pd.DataFrame(flips, columns=["symbol", "usable"])
    return SegmentResult(splits, pit, post, flip_df)


def _segment_table(
    con,
    seg: SegmentResult,
    *,
    observed_at,
    write_post_view: bool,
):
    """``security_segments`` 의 arrow 표."""
    import pyarrow as pa

    from collector.us.store.schema import SECURITY_SEGMENTS_ARROW

    symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM px ORDER BY 1").fetchall()]
    first_dates = dict(con.execute("SELECT symbol, min(date) FROM px GROUP BY 1").fetchall())
    frames = [
        segments.segment_rows(
            symbols, seg.pit, view="pit", first_dates=first_dates, rule_version=SEGMENT_RULE_VERSION
        )
    ]
    if write_post_view:
        frames.append(
            segments.segment_rows(
                symbols,
                seg.post,
                view="post",
                first_dates=first_dates,
                rule_version=SEGMENT_RULE_VERSION,
            )
        )
    rows = pd.concat(frames, ignore_index=True)
    # 구간 첫 가격일 시점의 cik 와 CUSIP 앞 6자리 (참고용)
    q = rows[["symbol", "_first_date"]].copy()
    q["rid"] = np.arange(len(q))
    q = q.dropna(subset=["_first_date"])
    q["d0"] = pd.to_datetime(q._first_date).dt.date
    q["d30"] = (pd.to_datetime(q._first_date) + pd.Timedelta(days=30)).dt.date
    con.register("sq_df", q[["rid", "symbol", "d0", "d30"]])
    ck = con.execute("""
        SELECT q.rid, ct.cik, ct.as_of AS cik_known_at
        FROM sq_df q ASOF LEFT JOIN ticker_pit ct ON ct.symbol = q.symbol AND q.d0 >= ct.as_of
        """).df()
    c6 = con.execute("""
        SELECT q.rid, u.c6 AS cusip6
        FROM sq_df q ASOF LEFT JOIN cu6 u ON u.symbol = q.symbol AND q.d30 >= u.f
        """).df()
    early = con.execute("""
        SELECT symbol, arg_min(c6, f) AS c6_first FROM cu6 GROUP BY symbol
        """).df()
    con.unregister("sq_df")
    rows["rid"] = np.arange(len(rows))
    rows = rows.merge(ck, on="rid", how="left").merge(c6, on="rid", how="left")
    rows = rows.merge(early, on="symbol", how="left")
    rows["cusip6"] = rows.cusip6.where(rows.cusip6.notna(), rows.c6_first)
    obs = observed_at

    def col(name, typ):
        values = rows[name].astype(object).where(rows[name].notna(), None).tolist()
        if name in (
            "seg_start",
            "seg_end",
            "event_date",
            "known_at",
            "usable_from",
            "cik_known_at",
        ):
            values = [pd.Timestamp(v).date() if v is not None else None for v in values]
        return pa.array(values, type=typ)

    schema = SECURITY_SEGMENTS_ARROW
    arrays = {
        "symbol": col("symbol", pa.string()),
        "segment_no": col("segment_no", pa.int32()),
        "security_id": col("security_id", pa.string()),
        "view": col("view", pa.string()),
        "seg_start": col("seg_start", pa.date32()),
        "seg_end": col("seg_end", pa.date32()),
        "event_date": col("event_date", pa.date32()),
        "known_at": col("known_at", pa.date32()),
        "usable_from": col("usable_from", pa.date32()),
        "split_reason": col("split_reason", pa.string()),
        "identified": col("identified", pa.bool_()),
        "cik": pa.array([None if pd.isna(v) else int(v) for v in rows.cik], type=pa.int64()),
        "cik_known_at": col("cik_known_at", pa.date32()),
        "cusip6": col("cusip6", pa.string()),
        "rule_version": col("rule_version", pa.string()),
        "observed_at": pa.array([obs] * len(rows), type=pa.timestamp("us", tz="UTC")),
    }
    return pa.table({n: arrays[n] for n in schema.names})


# --- 월 통계 (v1 방식 + 구간 방식) ------------------------------------------------------------


def _make_stat_tables(con, *, roll_from: _dt.date, pit: pd.DataFrame, rules: V2Rules) -> None:
    """``v1_month``(심볼 단위, v1 과 같다)와 ``seg_month``(구간 단위, 예열 뒤 행만).

    ``adv`` 는 20거래일 평균 거래대금(원시 종가 × 원시 거래량), ``td`` 는 20거래일 중 거래량이 있는
    날 수다. 월 값은 그 달 행의 **중앙값**이다.
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE v1_month AS
        WITH r AS (
            SELECT symbol, date,
                   avg(close * volume) OVER w AS adv,
                   count(*) FILTER (WHERE volume > 0) OVER w AS td
            FROM px WHERE date >= DATE '{roll_from}'
            WINDOW w AS (
                PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        )
        SELECT CAST(date_trunc('month', date) AS DATE) AS ym, symbol,
               median(adv) AS adv, median(td) AS td
        FROM r GROUP BY 1, 2
        """)
    con.execute("CREATE OR REPLACE TABLE seg_pairs (symbol VARCHAR, seg_start DATE)")
    if len(pit):
        pairs = (
            pit[["symbol", "event"]].drop_duplicates().assign(seg_start=lambda d: d.event.dt.date)
        )
        con.executemany(
            "INSERT INTO seg_pairs VALUES (?, ?)",
            list(zip(pairs.symbol, pairs.seg_start, strict=True)),
        )
    con.execute(f"""
        CREATE OR REPLACE TABLE seg_month AS
        WITH r AS (
            SELECT s.symbol, s.seg_start, p.date,
                   avg(p.close * p.volume) OVER w AS adv,
                   count(*) FILTER (WHERE p.volume > 0) OVER w AS td,
                   row_number() OVER (PARTITION BY s.symbol, s.seg_start ORDER BY p.date) AS rn
            FROM seg_pairs s JOIN px p ON p.symbol = s.symbol AND p.date >= s.seg_start
            WINDOW w AS (
                PARTITION BY s.symbol, s.seg_start ORDER BY p.date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        )
        SELECT symbol, seg_start, CAST(date_trunc('month', date) AS DATE) AS ym,
               median(adv) AS adv_s, median(td) AS td_s
        FROM r WHERE rn >= {int(rules.warmup_rows)} GROUP BY 1, 2, 3
        """)


# --- 증권 마스터 이어 쓰기 -----------------------------------------------------------------------


class MasterBuilder:
    """달마다 판정한 상태를 같은 상태가 이어지는 동안 한 행으로 묶는다."""

    def __init__(self, prior: pd.DataFrame | None = None):
        self.closed: list[tuple] = []
        self.open: dict[str, list] = {}
        if prior is not None:
            for r in prior.itertuples():
                state = (
                    r.issuer_kind,
                    r.security_type,
                    bool(r.include),
                    r.reason_code,
                    r.source_code,
                )
                valid_from = pd.Timestamp(r.valid_from).date()
                if pd.isna(r.valid_to):
                    self.open[r.security_id] = [valid_from, state, r.rule_version]
                else:
                    valid_to = pd.Timestamp(r.valid_to).date()
                    self.closed.append(
                        (r.security_id, valid_from, valid_to, *state, r.rule_version)
                    )

    def add_month(self, t: _dt.date, states: dict[str, tuple]) -> None:
        for sid, state in states.items():
            cur = self.open.get(sid)
            if cur is None:
                self.open[sid] = [t, state, RULE_VERSION]
            elif cur[1] != state:
                self.closed.append((sid, cur[0], t, *cur[1], cur[2]))
                self.open[sid] = [t, state, RULE_VERSION]
        for sid in [s for s in self.open if s not in states]:
            cur = self.open.pop(sid)
            self.closed.append((sid, cur[0], t, *cur[1], cur[2]))

    def table(self, observed_at):
        import pyarrow as pa

        from collector.us.store.schema import SECURITY_MASTER_ARROW

        rows = list(self.closed)
        rows += [(sid, v[0], None, *v[1], v[2]) for sid, v in self.open.items()]
        rows.sort(key=lambda r: (r[0], r[1]))
        cols = list(zip(*rows, strict=True)) if rows else [()] * 9
        names = (
            "security_id",
            "valid_from",
            "valid_to",
            "issuer_kind",
            "security_type",
            "include",
            "reason_code",
            "source_code",
            "rule_version",
        )
        types = (
            pa.string(),
            pa.date32(),
            pa.date32(),
            pa.string(),
            pa.string(),
            pa.bool_(),
            pa.string(),
            pa.string(),
            pa.string(),
        )
        data = {n: pa.array(list(c), type=t) for n, c, t in zip(names, cols, types, strict=True)}
        data["observed_at"] = pa.array([observed_at] * len(rows), type=pa.timestamp("us", tz="UTC"))
        return pa.table({n: data[n] for n in SECURITY_MASTER_ARROW.names})


# --- 달 하나를 판정한다 --------------------------------------------------------------------------


@dataclasses.dataclass
class JudgeContext:
    con: object
    rules: V2Rules
    cal: Calendar
    #: PIT 번호 매김만 받는다. **사후 보기는 여기 못 들어온다** (T11)
    pit: pd.DataFrame
    flips: pd.DataFrame
    cik_first: dict[tuple[str, int], pd.Timestamp]
    seg_month: pd.DataFrame


def judge_month(ctx: JudgeContext, M: _dt.date, t: _dt.date, prev_members: set[str]):
    """달 ``M`` 을 시점 ``t`` 에서 판정한다. ``(frame, stats)`` — 근거가 없으면 ``(None, {})``."""
    con, rules = ctx.con, ctx.rules
    src = _shift_months(M, -JUDGE_LAG_MONTHS)
    cand = con.execute("SELECT symbol, adv, td FROM v1_month WHERE ym = ?", [src]).df()
    if cand.empty:
        return None, {}
    tt = pd.Timestamp(t)
    cand["t"] = tt

    # 1. t 에 알려진 분리 (인지 시점 다음 거래일 <= t). 가장 최근 사건의 구간이 그 심볼의 구간이다.
    pit = ctx.pit
    app = pit[pit.usable_from <= tt] if len(pit) else pit
    if len(app):
        last_any = (
            app.sort_values("event")
            .groupby("symbol")
            .tail(1)[["symbol", "segment_no", "event"]]
            .rename(columns={"event": "seg_start"})
        )
        ident = (
            app[app.identified & (app.ident_usable <= tt)]
            .sort_values("event")
            .groupby("symbol")
            .tail(1)[["symbol", "event", "old_end"]]
            .rename(columns={"event": "id_event", "old_end": "id_old_end"})
        )
    else:
        last_any = pd.DataFrame(
            {
                "symbol": pd.Series(dtype=object),
                "segment_no": pd.Series(dtype="int64"),
                "seg_start": pd.Series(dtype="datetime64[ns]"),
            }
        )
        ident = pd.DataFrame(
            {
                "symbol": pd.Series(dtype=object),
                "id_event": pd.Series(dtype="datetime64[ns]"),
                "id_old_end": pd.Series(dtype="datetime64[ns]"),
            }
        )
    cand = cand.merge(last_any, on="symbol", how="left").merge(ident, on="symbol", how="left")
    cand["segment_no"] = pd.to_numeric(cand.segment_no).fillna(1).astype(int)
    cand["security_id"] = cand.symbol + "#" + cand.segment_no.astype(str)

    # 2. 구간이 있는 심볼은 구간 안의 행으로 다시 잰 월 통계를 쓴다 (예열 뒤 행만)
    has_seg = cand.seg_start.notna()
    if has_seg.any():
        sm = ctx.seg_month
        sm = sm[sm.ym == pd.Timestamp(src)][["symbol", "seg_start", "adv_s", "td_s"]]
        m = cand.loc[has_seg, ["symbol", "seg_start"]].merge(
            sm, on=["symbol", "seg_start"], how="left"
        )
        cand.loc[has_seg, "adv"] = m.adv_s.to_numpy()
        cand.loc[has_seg, "td"] = m.td_s.to_numpy()

    # 3. 가격 마지막 날(120일 안)의 cik. 그날 가격이 있으면 그날 값이다
    t_s = str(t)
    cm = con.execute(
        f"""
        WITH c AS (SELECT unnest(?::VARCHAR[]) AS symbol),
        l AS (
            SELECT p.symbol, max(p.date) AS lp FROM px p JOIN c ON c.symbol = p.symbol
            WHERE p.date <= DATE '{t_s}' AND p.date > DATE '{t_s}' - INTERVAL 120 DAY GROUP BY 1
        )
        SELECT c.symbol, l.lp, k.cik AS cik0
        FROM c LEFT JOIN l ON l.symbol = c.symbol
        LEFT JOIN px_cik k ON k.symbol = c.symbol AND k.date = l.lp
        """,
        [cand.symbol.tolist()],
    ).df()
    cand = cand.merge(cm[["symbol", "cik0"]], on="symbol", how="left")
    cand["cik_e"] = cand.cik0.astype("float64")

    # 4. 상장 목록 (as_of < t)
    lst = master.gather_listing(con, cand[["symbol", "t"]])
    cand = cand.merge(lst, on="symbol", how="left")

    # 5. 분리 뒤 구간에서 옛 구간의 흔적을 무효로 둔다 (보강 신호가 인지된 뒤부터)
    name_valid = pd.Series(True, index=cand.index)
    hit = cand.id_event.notna()
    for i in cand.index[hit]:
        sym = cand.at[i, "symbol"]
        ck = cand.at[i, "cik_e"]
        if pd.notna(ck):
            first = ctx.cik_first.get((sym, int(ck)))
            if first is not None and first < cand.at[i, "id_event"]:
                cand.at[i, "cik_e"] = np.nan
        la = cand.at[i, "l_asof"]
        if pd.notna(la) and pd.Timestamp(la) <= cand.at[i, "id_old_end"]:
            name_valid.at[i] = False
    if hit.any():
        sub = cand.loc[hit & cand.cik_e.notna(), "cik_e"]
        dead = master.gather_dormant(con, sub, t, rules)
        mask = hit & cand.cik_e.isin(dead)
        cand.loc[mask, "cik_e"] = np.nan
    nv = ~name_valid
    cand.loc[nv, ["security_name"]] = None
    cand.loc[nv, ["l_etf", "l_test", "stale"]] = False

    # 6. SEC 근거·발행사 이름·SIC·5.06
    ev = master.gather_evidence(con, cand, t, rules)
    for c in ("s_fund", "s_bdc", "op18", "k6_18", "rel506", "cname", "sic_e"):
        cand[c] = ev[c].to_numpy()

    # 7. FTD 설명 — 구간 시작 앞의 설명과 400일 넘은 설명은 쓰지 않는다
    cand = cand.merge(master.gather_ftd(con, cand[["symbol", "t"]]), on="symbol", how="left")
    age = (tt - pd.to_datetime(cand.ftd_sd)).dt.days
    stale_ftd = (age > rules.ftd_max_age_days) | (
        cand.seg_start.notna() & (pd.to_datetime(cand.ftd_sd) < cand.seg_start)
    )
    cand.loc[stale_ftd, "ftd_desc"] = None

    # 8. 설계 7장 보조 규칙의 재료: 이름이 이미 운영회사 꼴로 바뀐 심볼
    flipped = set(ctx.flips[ctx.flips.usable <= tt].symbol) if len(ctx.flips) else set()
    cand["name_to_operating"] = cand.symbol.isin(flipped)

    judged = master.classify(cand, rules)
    judged["cik_e"] = cand.cik_e.to_numpy()

    # 9. 멤버십 (v1 과 같은 문턱, 단위만 security id)
    adv = judged.adv
    td = judged.td.fillna(0)
    stat_ok = adv.notna() & (td >= MIN_TRADED_DAYS_20)
    was = judged.security_id.isin(prev_members)
    threshold = np.where(was, MAINTAIN_ADV_USD, ENTRY_ADV_USD)
    eligible = judged.include & stat_ok
    member = eligible & (adv >= threshold)
    judged["member"] = member
    seg_gt1 = judged.segment_no > 1
    reasons = np.select(
        [
            ~judged.include,
            adv.isna() & seg_gt1,
            adv.isna(),
            td < MIN_TRADED_DAYS_20,
            adv < threshold,
        ],
        [
            judged.reason_code,
            REASON_WARMUP,
            REASON_NO_STAT,
            REASON_MIN_TRADED,
            np.where(was, REASON_BELOW_MAINTAIN, REASON_BELOW_ENTRY),
        ],
        default="",
    )
    judged["exclusion_reason"] = np.where(member, None, reasons)

    candidates = (adv >= ENTRY_ADV_USD) & (td >= MIN_TRADED_DAYS_20)
    n_cand = int(candidates.sum())
    n_unknown = int((candidates & (judged.reason_code == "unknown")).sum())
    stats = {
        "month": str(M),
        "t": str(t),
        "candidates": int(len(judged)),
        "liquid_candidates": n_cand,
        "unknown": n_unknown,
        "unknown_ratio": (n_unknown / n_cand) if n_cand else 0.0,
        "members": int(member.sum()),
        "unknown_breakdown": master.unknown_breakdown(judged[candidates]),
    }
    return judged, stats


# --- 전체 -------------------------------------------------------------------------------------


def _members_of_month(con, parquet: Path, month: _dt.date) -> set[str]:
    nxt = _shift_months(month, 1)
    rows = con.execute(
        "SELECT DISTINCT security_id FROM read_parquet(?) "
        "WHERE date >= ? AND date < ? AND in_universe",
        [str(parquet), month, nxt],
    ).fetchall()
    return {r[0] for r in rows}


def _membership_frame(judged: pd.DataFrame, M: _dt.date) -> pd.DataFrame:
    """달 ``M`` 의 판정 결과를 ``membership`` 표 모양으로."""
    return pd.DataFrame(
        {
            "ym": pd.Timestamp(M),
            "symbol": judged.symbol,
            "security_id": judged.security_id,
            "in_universe": judged.member.astype(bool),
            "exclusion_reason": judged.exclusion_reason,
            "cik": judged.cik_e.astype("float64"),
            "security_type": judged.security_type,
        }
    )


def build_universe_v2(
    root,
    *,
    snapshot_date,
    mode: str = "rebuild",
    start: str | None = None,
    end: str | None = None,
    observed_at=None,
    if_new: bool = False,
    dry_run: bool = False,
    rules: V2Rules | None = None,
    ticker_map_parquet: Path | None = None,
    bounded: bool | None = None,
    write_post_view: bool = True,
) -> dict[str, object]:
    """``security_segments``·``security_master``·``universe_daily_v2`` 를 한 번에 굳힌다.

    ``mode="rebuild"`` 는 처음부터 다시, ``"incremental"`` 은 직전 완료 스냅샷 뒤의
    새 세션만 잇는다.
    증분은 **이미 굳은 달의 멤버십을 안 바꾼다**(현재 달은 직전 스냅샷 값을 그대로 쓴다).
    ``if_new`` 는 새 세션이 없거나 같은 ``snapshot_date`` 파티션이 있을 때 건너뛰기로 끝낸다.
    """
    if mode not in ("rebuild", "incremental"):
        raise ValueError(mode)
    from collector.us.store.writer import (
        latest_snapshot,
        snapshot_path,
        verify_snapshot,
        write_snapshot_arrow,
    )
    from collector.us.universe.build import _bounded_duckdb

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    inputs = v2_inputs.resolve_inputs(root)
    dest = snapshot_path(root, "universe_daily_v2", snapshot_date)
    if bounded is None:
        bounded = mode == "incremental"
    if dest.parent.exists():
        if if_new:
            return {
                "skipped": True,
                "reason": f"universe_daily_v2 파티션이 있다: {dest.parent.name}",
            }
        raise FileExistsError(f"universe_daily_v2 partition already exists: {dest.parent}")

    prior = latest_snapshot(root, "universe_daily_v2") if mode == "incremental" else None
    if mode == "incremental" and prior is None:
        raise FileNotFoundError("증분에는 직전 universe_daily_v2 스냅샷이 필요하다 — 먼저 rebuild")

    con = _bounded_duckdb(root, bounded=bounded)
    try:
        prices_end = con.execute(
            "SELECT max(date) FROM read_parquet(?)", [str(inputs["prices_daily"])]
        ).fetchone()[0]
        end_date = _dt.date.fromisoformat(end) if end else prices_end
        cal = Calendar.xnys(_CALENDAR_FROM, end_date + _dt.timedelta(days=420))

        base_start = _dt.date.fromisoformat(start) if start else USABLE_FROM
        prior_end = None
        frozen_month: _dt.date | None = None
        if mode == "incremental":
            prior_manifest = prior.parent / "completion.json"
            if prior_manifest.is_file():
                recorded = json.loads(prior_manifest.read_text())
                base_start = _dt.date.fromisoformat(recorded.get("base_start", str(USABLE_FROM)))
                if rules is None and recorded.get("rules"):
                    # 직전 빌드와 같은 규칙으로 잇는다. 규칙을 바꾸려면 rebuild 한다.
                    known = {f.name for f in dataclasses.fields(V2Rules)}
                    rules = V2Rules(**{k: v for k, v in recorded["rules"].items() if k in known})
            prior_end = con.execute(
                "SELECT max(date) FROM read_parquet(?)", [str(prior)]
            ).fetchone()[0]
            if prior_end is None or prices_end <= prior_end:
                if if_new:
                    return {"skipped": True, "reason": "no new price session"}
                raise ValueError("new completed price session is required")
            new_sessions = [d for d in cal.sessions if prior_end < d <= end_date]
            if not new_sessions:
                if if_new:
                    return {"skipped": True, "reason": "no new XNYS session"}
                raise ValueError("no new XNYS session is present")
            recorded = {
                r[0]
                for r in con.execute(
                    "SELECT DISTINCT date FROM read_parquet(?) WHERE date > ? AND date <= ?",
                    [str(inputs["prices_daily"]), prior_end, end_date],
                ).fetchall()
            }
            missing = sorted(set(new_sessions) - recorded)
            if missing:
                raise ValueError(f"prices snapshot has missing new XNYS sessions: {missing}")
            start_date = new_sessions[0]
            prev_month = _shift_months(start_date.replace(day=1), -1)
            prior_month_sessions = [
                d for d in cal.sessions if d.year == prev_month.year and d.month == prev_month.month
            ]
            if not prior_month_sessions:
                raise ValueError("previous month has no confirmed XNYS session")
            if prior_end < prior_month_sessions[-1]:
                raise ValueError(
                    "prior universe_daily_v2 snapshot does not cover the previous month"
                )
            if prior_end >= start_date.replace(day=1):
                frozen_month = start_date.replace(day=1)
            if dry_run:
                return {
                    "dry_run": True,
                    "would_build": True,
                    "added_start": start_date.isoformat(),
                    "added_end": end_date.isoformat(),
                    "sessions": len(new_sessions),
                }
        else:
            start_date = max(base_start, cal.sessions[0])
            if dry_run:
                return {"dry_run": True, "would_build": True, "start": str(start_date)}

        rules = rules or DEFAULT_RULES
        roll_from = base_start - _dt.timedelta(days=90)
        sessions_out = [d for d in cal.sessions if start_date <= d <= end_date]
        months = sorted({d.replace(day=1) for d in sessions_out})
        target_months = [m for m in months if m != frozen_month]

        # 입력 해시를 먼저 적는다 — 만드는 동안 입력이 바뀌면 멈춘다
        ticker_rows, ticker_files = v2_inputs.load_ticker_map(root, parquet=ticker_map_parquet)
        hashes_before = {n: _sha(p) for n, p in inputs.items()}
        ticker_hashes = {str(p): _sha(p) for p in ticker_files}
        prior_hash = _sha(prior) if prior is not None else None

        _load_base_tables(
            con,
            root=root,
            inputs=inputs,
            cal=cal,
            rules=rules,
            ticker_rows=ticker_rows,
            ticker_parquet=ticker_map_parquet,
            known_cutoff=None,
        )
        del ticker_rows
        seg = compute_segments(con, cal=cal, rules=rules)
        seg_table = _segment_table(
            con, seg, observed_at=observed_at, write_post_view=write_post_view
        )
        master.prepare_evidence(con, rules=rules, inputs=inputs)
        _make_stat_tables(con, roll_from=roll_from, pit=seg.pit, rules=rules)

        cik_first = {
            (s, int(c)): pd.Timestamp(d)
            for s, c, d in con.execute(
                "SELECT symbol, cik, first_date FROM cik_first "
                "WHERE symbol IN (SELECT unnest(?::VARCHAR[]))",
                [sorted(set(seg.pit.symbol)) if len(seg.pit) else []],
            ).fetchall()
        }
        ctx = JudgeContext(
            con=con,
            rules=rules,
            cal=cal,
            pit=seg.pit,
            flips=seg.name_flips,
            cik_first=cik_first,
            seg_month=con.execute("SELECT * FROM seg_month")
            .df()
            .assign(
                seg_start=lambda d: pd.to_datetime(d.seg_start), ym=lambda d: pd.to_datetime(d.ym)
            ),
        )

        # --- 이어달리기 시작 상태
        prior_master = None
        if prior is not None:
            prior_master_path = latest_snapshot(root, "security_master")
            if prior_master_path is not None:
                prior_master = con.execute(
                    "SELECT * FROM read_parquet(?)", [str(prior_master_path)]
                ).df()
        builder = MasterBuilder(prior_master)
        con.execute("""
            CREATE OR REPLACE TABLE membership (
                ym DATE, symbol VARCHAR, security_id VARCHAR, in_universe BOOLEAN,
                exclusion_reason VARCHAR, cik BIGINT, security_type VARCHAR
            )
            """)
        prev_members: set[str] = set()
        frozen_mismatch = 0
        if prior is not None:
            if frozen_month is not None:
                prev_members = _members_of_month(con, prior, frozen_month)
                con.execute(
                    """
                    INSERT INTO membership
                    SELECT CAST(date_trunc('month', date) AS DATE), symbol, security_id,
                           bool_or(in_universe), any_value(exclusion_reason),
                           any_value(cik), any_value(security_type)
                    FROM read_parquet(?)
                    WHERE date >= ?
                      -- 판정 결과가 아니라 행에서 붙인 사유는 가져오지 않는다
                      AND (exclusion_reason IS NULL OR exclusion_reason NOT IN (?, ?))
                    GROUP BY 1, 2, 3
                    """,
                    [str(prior), frozen_month, REASON_NOT_JUDGED, REASON_NO_PREV_STAT],
                )
                # 직전 스냅샷에 아직 행이 없는 후보(달 후반에 처음 거래되는 심볼 등)를 채운다.
                # 이미 굳은 (심볼, 구간)은 직전 값이 이기고, 다른 값이 나오면 개수를 남긴다.
                t_frozen = cal.first_in_month(
                    frozen_month.year, frozen_month.month, floor=base_start
                )
                before = _members_of_month(con, prior, _shift_months(frozen_month, -1))
                judged_f, _ = judge_month(ctx, frozen_month, t_frozen, before)
                if judged_f is not None:
                    con.register("fm_df", _membership_frame(judged_f, frozen_month))
                    frozen_mismatch = con.execute(
                        """
                        SELECT count(*) FROM fm_df f JOIN membership m
                          ON m.ym = CAST(f.ym AS DATE) AND m.symbol = f.symbol
                         AND m.security_id = f.security_id
                        WHERE m.in_universe IS DISTINCT FROM f.in_universe
                        """
                    ).fetchone()[0]
                    con.execute(
                        """
                        INSERT INTO membership
                        SELECT CAST(f.ym AS DATE), f.symbol, f.security_id, f.in_universe,
                               f.exclusion_reason, CAST(f.cik AS BIGINT), f.security_type
                        FROM fm_df f WHERE NOT EXISTS (
                            SELECT 1 FROM membership m WHERE m.ym = CAST(f.ym AS DATE)
                              AND m.symbol = f.symbol AND m.security_id = f.security_id)
                        """
                    )
                    con.unregister("fm_df")
            else:
                prev_members = _members_of_month(
                    con, prior, _shift_months(start_date.replace(day=1), -1)
                )
            if not prev_members:
                raise ValueError("previous month membership seed is missing")
            _check_labels_unchanged(con, prior, prior_end, seg.pit)

        month_stats: list[dict] = []
        unjudged: list[str] = []
        for M in target_months:
            t = cal.first_in_month(M.year, M.month, floor=start_date)
            judged, stats = judge_month(ctx, M, t, prev_members)
            if judged is None:
                unjudged.append(str(M))
                prev_members = set()
                continue
            month_stats.append(stats)
            states = {
                r.security_id: (
                    r.issuer_kind,
                    r.security_type,
                    bool(r.include),
                    r.reason_code,
                    r.source_code,
                )
                for r in judged.itertuples()
            }
            builder.add_month(t, states)
            mm = _membership_frame(judged, M)
            con.register("mm_df", mm)
            con.execute(
                "INSERT INTO membership SELECT CAST(ym AS DATE), symbol, security_id, in_universe, "
                "exclusion_reason, CAST(cik AS BIGINT), security_type FROM mm_df"
            )
            con.unregister("mm_df")
            prev_members = {r.security_id for r in judged.itertuples() if r.member}
        if mode == "incremental" and unjudged:
            raise ValueError(f"incremental universe has unjudged months: {unjudged}")

        # --- 일 단위 행: 그날 가격이 있는 (심볼, 구간)
        con.execute(
            "CREATE OR REPLACE TABLE seg_apply "
            "(symbol VARCHAR, usable_from DATE, segment_no INTEGER)"
        )
        timeline = segments.label_timeline(seg.pit)
        if timeline:
            con.executemany("INSERT INTO seg_apply VALUES (?, ?, ?)", timeline)
        stage_parent = dest.parent.parent
        stage_parent.mkdir(parents=True, exist_ok=True)
        staged_dir = Path(tempfile.mkdtemp(prefix=".universe-v2-", dir=stage_parent))
        staged_dir.chmod(0o755)
        try:
            part = staged_dir / "part.parquet"
            prefix = (
                "SELECT date, symbol, security_id, cik, security_type, in_universe, "
                "exclusion_reason, rule_version, observed_at "
                f"FROM read_parquet('{prior}') WHERE date <= DATE '{prior_end}' UNION ALL "
                if prior is not None
                else ""
            )
            con.execute(
                f"""
                COPY (
                    WITH d AS (
                        SELECT p.date, p.symbol, CAST(date_trunc('month', p.date) AS DATE) AS ym,
                               p.symbol || '#' || CAST(coalesce(a.segment_no, 1) AS VARCHAR) AS sid
                        FROM (
                            SELECT * FROM px
                            WHERE date >= DATE '{start_date}' AND date <= DATE '{end_date}'
                        ) p
                        ASOF LEFT JOIN seg_apply a
                          ON a.symbol = p.symbol AND p.date >= a.usable_from
                    ),
                    cur AS (
                        SELECT d.date, d.symbol, d.sid AS security_id,
                               CASE WHEN m.security_id IS NOT NULL THEN m.cik END AS cik,
                               m.security_type AS security_type,
                               coalesce(m.in_universe, FALSE) AS in_universe,
                               CASE WHEN m.security_id IS NOT NULL
                                         THEN (CASE WHEN m.in_universe THEN NULL
                                                    ELSE m.exclusion_reason END)
                                    WHEN s.symbol IS NOT NULL THEN '{REASON_NOT_JUDGED}'
                                    ELSE '{REASON_NO_PREV_STAT}' END AS exclusion_reason,
                               '{RULE_VERSION}' AS rule_version,
                               CAST(? AS TIMESTAMP WITH TIME ZONE) AS observed_at
                        FROM d
                        LEFT JOIN membership m
                               ON m.ym = d.ym AND m.symbol = d.symbol AND m.security_id = d.sid
                        LEFT JOIN (SELECT DISTINCT ym, symbol FROM membership) s
                               ON s.ym = d.ym AND s.symbol = d.symbol
                    )
                    {prefix}
                    SELECT * FROM cur
                ) TO '{part}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [observed_at],
            )
            stats_file = verify_snapshot(
                part, "universe_daily_v2", unique_on=("date", "security_id"), connection=con
            )

            hashes_after = {n: _sha(p) for n, p in inputs.items()}
            ticker_after = {str(p): _sha(p) for p in ticker_files}
            if hashes_after != hashes_before or ticker_after != ticker_hashes:
                raise ValueError("universe v2 input snapshot changed during the build")
            if prior is not None and _sha(prior) != prior_hash:
                raise ValueError("prior universe_daily_v2 snapshot changed during the build")

            master_table = builder.table(observed_at)
            seg_dest = snapshot_path(root, "security_segments", snapshot_date)
            write_snapshot_arrow(
                seg_table, "security_segments", seg_dest, unique_on=("symbol", "segment_no", "view")
            )
            master_dest = snapshot_path(root, "security_master", snapshot_date)
            write_snapshot_arrow(
                master_table,
                "security_master",
                master_dest,
                unique_on=("security_id", "valid_from"),
            )

            warnings = [
                {"month": s["month"], "unknown_ratio": round(s["unknown_ratio"], 4)}
                for s in month_stats
                if s["unknown_ratio"] > rules.unknown_warn_ratio
            ]
            kinds = (
                seg.splits.kind.map(segments.split_reason).value_counts().to_dict()
                if len(seg.splits)
                else {}
            )
            manifest = {
                "schema_version": 1,
                "table": "universe_daily_v2",
                "snapshot_date": str(snapshot_date),
                "mode": mode,
                "base_start": str(base_start),
                "start": str(start_date),
                "end": str(end_date),
                "rule_version": RULE_VERSION,
                "rule_versions": {
                    "segments": SEGMENT_RULE_VERSION,
                    "master": MASTER_RULE_VERSION,
                    "universe": UNIVERSE_RULE_VERSION,
                },
                "rules": dataclasses.asdict(rules),
                "snapshot_sha256": _sha(part),
                "previous_snapshot": str(prior) if prior else None,
                "previous_snapshot_sha256": prior_hash,
                "previous_end": prior_end.isoformat() if prior_end else None,
                "input_snapshots": {
                    n: p.parent.name.removeprefix("snapshot_date=") for n, p in inputs.items()
                },
                "input_snapshot_sha256": hashes_before,
                "ticker_source_sha256": {
                    (str(Path(k).relative_to(root.base)) if str(root.base) in k else k): v
                    for k, v in ticker_hashes.items()
                },
                "dolt_stocks": _dolt_commit(root),
                "judge_lag_months": JUDGE_LAG_MONTHS,
                "months_judged": [s["month"] for s in month_stats],
                "frozen_month": str(frozen_month) if frozen_month else None,
                "frozen_month_mismatch": frozen_mismatch,
                "unjudged_months": unjudged,
                "unknown_warn_ratio": rules.unknown_warn_ratio,
                "unknown_warnings": warnings,
                "month_stats": month_stats,
                "segment_splits": kinds,
                "security_segments_snapshot": str(seg_dest),
                "security_master_snapshot": str(master_dest),
            }
            (staged_dir / "completion.json").write_text(
                json.dumps(manifest, sort_keys=True, indent=2, default=str) + "\n"
            )
            os.rename(staged_dir, dest.parent)
        except BaseException:
            shutil.rmtree(staged_dir, ignore_errors=True)
            raise
        return {
            "path": dest,
            "completion_path": dest.parent / "completion.json",
            "mode": mode,
            "start": str(start_date),
            "end": str(end_date),
            "months_judged": len(month_stats),
            "frozen_month": str(frozen_month) if frozen_month else None,
            "frozen_month_mismatch": frozen_mismatch,
            "unknown_warnings": warnings,
            "segment_splits": kinds,
            "rows": stats_file["rows"],
            "bytes": stats_file["bytes"],
            "security_segments": seg_dest,
            "security_master": master_dest,
            "input_snapshots": manifest["input_snapshots"],
        }
    finally:
        con.close()


def _check_labels_unchanged(con, prior: Path, prior_end: _dt.date, pit: pd.DataFrame) -> None:
    """직전 스냅샷 마지막 날의 구간 번호가 지금 계산과 같은지 본다. 다르면 입력이 고쳐진 것이다."""
    got = con.execute(
        "SELECT symbol, security_id FROM read_parquet(?) WHERE date = ?", [str(prior), prior_end]
    ).df()
    if got.empty:
        return
    tt = pd.Timestamp(prior_end)
    app = pit[pit.usable_from <= tt] if len(pit) else pit
    seg_no = (
        app.sort_values("event").groupby("symbol").tail(1).set_index("symbol").segment_no
        if len(app)
        else pd.Series(dtype=int)
    )
    want = got.symbol + "#" + got.symbol.map(seg_no).fillna(1).astype(int).astype(str)
    changed = int((want != got.security_id).sum())
    if changed:
        raise ValueError(
            f"직전 스냅샷의 구간 번호 {changed}개가 지금 계산과 다르다 — "
            "입력이 고쳐졌다. rebuild 한다."
        )

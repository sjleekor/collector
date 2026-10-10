"""기준선 저장소 검사 — 계획 04 §8을 **저장소만 보고** 할 수 있는 만큼.

기존 ``sdc_daily_freshness``에는 넣지 않는다. 실패가 하나라도 있으면 호출자가 0이
아닌 종료 코드를 낸다. 관측 표는 연도 파일 단위로 필요한 열만 읽는다(ETF 180만 행).

못 하는 것: KIND 분배금 교차, 2차 관측 대조 보고, 추적 대조(합성 3년 대 KTB 월 상관)는
외부 자료나 백필 뒤 비교가 필요해서 여기에 없다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from collector.kr.adapters.krx_baseline_openapi.bond_index import EXPECTED_GROUPS
from collector.kr.adapters.krx_baseline_openapi.derivative_index import KOSPI200_TR_NAME
from collector.kr.baseline import BaselineStore, TableSpec
from collector.kr.baseline.pending import fill_plan
from collector.kr.service import krx_baseline as kb
from collector.kr.service import seibro_dist as sd

#: 상장폐지 포함 재현 — (비교 날짜, 2026-10-08 목록에 없는 비율 %, 허용 오차 %p).
#: 05a §1.4 실측. 기준 집합을 **2026-10-08로 고정**한다(오늘 목록이면 새 폐지가 생길 때
#: 비율이 달라진다, R09).
DELISTING_REFERENCE_DAY = date(2026, 10, 8)
DELISTING_CHECKS: tuple[tuple[date, float], ...] = (
    (date(2010, 1, 4), 38.0),
    (date(2015, 1, 2), 31.0),
    (date(2020, 1, 2), 22.0),
    (date(2025, 1, 2), 7.7),
)
DELISTING_TOLERANCE_PP = 1.0
ROW_COUNT_JUMP = 0.20
TR_JUMP = 0.05
_YEAR = re.compile(r"/year=(\d{4})/")


@dataclass
class VerifyReport:
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures

    def render(self) -> str:
        verdict = "통과" if self.ok else "실패"
        lines = [f"결과: {verdict} (실패 {len(self.failures)}, 경고 {len(self.warnings)})"]
        lines += [f"  실패: {m}" for m in self.failures]
        lines += [f"  경고: {m}" for m in self.warnings]
        lines += [f"  참고: {m}" for m in self.notes]
        return "\n".join(lines)


def _years(store: BaselineStore, table: str) -> list[str]:
    found = set()
    for path in store.committed_paths("observation", table):
        match = _YEAR.search(path.as_posix())
        if match:
            found.add(match[1])
    return sorted(found)


def _read(
    store: BaselineStore, spec: TableSpec, year: str, extra: list[str] | None = None
) -> pd.DataFrame:
    cols = [*spec.key_columns, "obs_seq", "obs_kind", "row_hash", *(extra or [])]
    return store.read_observations(spec, "all", year=year, columns=list(dict.fromkeys(cols)))


def _latest(frame: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    if frame.empty:
        return frame
    top = frame.sort_values("obs_seq").drop_duplicates(list(spec.key_columns), keep="last")
    return top[top["obs_kind"] != "absent"]


def _check_table_integrity(store: BaselineStore, spec: TableSpec, report: VerifyReport) -> None:
    keys = list(spec.key_columns)
    rows = dup_seq = dup_first = same_hash = 0
    for year in _years(store, spec.name):
        frame = _read(store, spec, year)
        rows += len(frame)
        dup_seq += int(frame.duplicated([*keys, "obs_seq"]).sum())
        dup_first += int(frame[frame["obs_seq"] == 1].duplicated(keys).sum())
        ordered = frame.sort_values([*keys, "obs_seq"])
        prev_hash = ordered.groupby(keys)["row_hash"].shift()
        prev_kind = ordered.groupby(keys)["obs_kind"].shift()
        same_hash += int(
            (
                (ordered["row_hash"] == prev_hash)
                & (ordered["obs_kind"] == "value")
                & (prev_kind == "value")
            ).sum()
        )
    report.notes.append(f"{spec.name}: 관측 {rows:,}행")
    if dup_seq:
        report.failures.append(f"{spec.name}: (키, obs_seq) 중복 {dup_seq}")
    if dup_first:
        report.failures.append(f"{spec.name}: 최초 뷰 업무 키 중복 {dup_first}")
    if same_hash:
        report.failures.append(f"{spec.name}: 직전 관측과 row_hash가 같은 새 관측 {same_hash}")


def _check_dates(
    store: BaselineStore,
    svc: kb.KrxBaselineService,
    start: date,
    end: date,
    today: date,
    window_weekdays: int,
    report: VerifyReport,
) -> None:
    """평일마다 완료 요청 기록이 있어야 하고, 창보다 오래된 "대기"가 없어야 한다."""
    missing, _ = fill_plan(store, svc.service, max(start, svc.history_start), end)
    if missing:
        report.failures.append(
            f"{svc.service}: 완료 요청 기록이 없는 평일 {len(missing)}일 "
            f"({missing[0]}~{missing[-1]})"
        )
    window_start, _ = kb.sync_window(kb.last_available_date(today), window_weekdays)
    _, pending = fill_plan(store, svc.service, svc.history_start, kb.last_available_date(today))
    stale = [d for d in pending if d < window_start]
    if stale:
        report.failures.append(
            f"{svc.service}: 창({window_start}~)보다 오래된 대기 날짜 {len(stale)}일 "
            f"(첫 {stale[0]})"
        )


def _trading_dates(store: BaselineStore, svc: kb.KrxBaselineService) -> dict[str, tuple[str, int]]:
    """요청 키별 성공한 마지막 기록의 ``(day_kind, 순자산 0 행 수)``."""
    log = store.read_fetch_log(svc.service)
    if log.empty:
        return {}
    ok = log[log["result"] != "failed"].sort_values("fetched_at")
    ok = ok.drop_duplicates("request_key", keep="last")
    return {
        k: (str(d), 0 if pd.isna(z) else int(z))
        for k, d, z in zip(ok["request_key"], ok["day_kind"], ok["netasst_zero_rows"], strict=True)
    }


def _check_etf(store: BaselineStore, start: date, end: date, report: VerifyReport) -> None:
    spec = kb.ETF_SPEC
    status = _trading_dates(store, kb.SERVICES["etf_bydd_trd"])
    counts: dict[str, int] = {}
    zero_all: list[str] = []
    lo, hi = f"{start:%Y%m%d}", f"{end:%Y%m%d}"
    for year in _years(store, spec.name):
        if not (lo[:4] <= year <= hi[:4]):
            continue
        latest = _latest(_read(store, spec, year, ["invstasst_netasst_totamt_num"]), spec)
        latest = latest[(latest["BAS_DD"] >= lo) & (latest["BAS_DD"] <= hi)]
        for day, grp in latest.groupby("BAS_DD"):
            counts[day] = len(grp)
            kind, zeros = status.get(f"bas_dd={day}", ("", 0))
            if (
                kind == "trading"
                and zeros == 0
                and (grp["invstasst_netasst_totamt_num"] == 0).all()
            ):
                zero_all.append(day)
    prev: tuple[str, int] | None = None
    for day in sorted(counts):
        if status.get(f"bas_dd={day}", ("",))[0] != "trading":
            continue
        if prev and prev[1] and abs(counts[day] - prev[1]) / prev[1] > ROW_COUNT_JUMP:
            report.warnings.append(
                f"etf_daily: {day} 행 수 {counts[day]} (직전 거래일 {prev[0]} {prev[1]}, ±20% 넘음)"
            )
        prev = (day, counts[day])
    if zero_all:
        report.failures.append(
            f"etf_daily: 확정된 거래일인데 순자산이 전부 0인 날짜 {zero_all[:3]}"
        )


def _check_delisting(store: BaselineStore, report: VerifyReport) -> None:
    spec = kb.ETF_SPEC

    def codes(day: date) -> set[str] | None:
        year = f"{day:%Y}"
        if year not in _years(store, spec.name):
            return None
        frame = _read(store, spec, year)
        first = frame[(frame["obs_seq"] == 1) & (frame["BAS_DD"] == f"{day:%Y%m%d}")]
        return set(first["ISU_CD"]) if len(first) else None

    reference = codes(DELISTING_REFERENCE_DAY)
    if reference is None:
        report.notes.append(f"상폐 재현: 기준일 {DELISTING_REFERENCE_DAY} 목록이 없어 건너뜀")
        return
    for day, expected in DELISTING_CHECKS:
        listed = codes(day)
        if listed is None:
            report.notes.append(f"상폐 재현: {day} 목록이 아직 없어 건너뜀")
            continue
        ratio = 100.0 * len(listed - reference) / len(listed)
        text = f"상폐 재현 {day}: {ratio:.1f}% (기대 {expected}%)"
        if abs(ratio - expected) > DELISTING_TOLERANCE_PP:
            report.failures.append(text)
        else:
            report.notes.append(text)


def _check_bond(store: BaselineStore, start: date, end: date, report: VerifyReport) -> None:
    spec = kb.BOND_SPEC
    svc = kb.SERVICES["bon_dd_trd"]
    status = _trading_dates(store, svc)
    lo, hi = f"{start:%Y%m%d}", f"{end:%Y%m%d}"
    series: list[pd.DataFrame] = []
    for year in _years(store, spec.name):
        latest = _latest(_read(store, spec, year, ["TOT_EARNG_IDX", "tot_earng_idx_num"]), spec)
        series.append(latest)
        sub = latest[(latest["BAS_DD"] >= lo) & (latest["BAS_DD"] <= hi)]
        for day, grp in sub.groupby("BAS_DD"):
            if status.get(f"bas_dd={day}", ("",))[0] != "trading":
                continue
            have = set(grp.loc[grp["TOT_EARNG_IDX"].fillna("") != "", "BND_IDX_GRP_NM"])
            if len(grp) != 3 or not EXPECTED_GROUPS <= have:
                report.failures.append(
                    f"bon_dd_trd: {day} trading인데 3그룹 값이 아님 ({len(grp)}행)"
                )
    if series:
        _check_continuity(
            pd.concat(series), "BND_IDX_GRP_NM", "tot_earng_idx_num", "bon_dd_trd", report
        )


def _check_derivative(store: BaselineStore, report: VerifyReport) -> None:
    spec = kb.DERIVATIVE_SPEC
    series = []
    for year in _years(store, spec.name):
        latest = _latest(_read(store, spec, year, ["clsprc_idx_num"]), spec)
        series.append(latest[latest["IDX_NM"] == KOSPI200_TR_NAME])
    if series:
        _check_continuity(
            pd.concat(series), "IDX_NM", "clsprc_idx_num", "drvprod_dd_trd 코스피200 TR", report
        )


def _check_continuity(
    frame: pd.DataFrame, group_col: str, value_col: str, label: str, report: VerifyReport
) -> None:
    """0·음수는 실패, 일별 ±5% 넘는 점프는 경고."""
    for name, grp in frame.groupby(group_col):
        values = grp.sort_values("BAS_DD")[["BAS_DD", value_col]].dropna()
        bad = values[values[value_col] <= 0]
        if len(bad):
            report.failures.append(
                f"{label} {name}: 0 이하 값 {len(bad)}건 (첫 {bad.iloc[0]['BAS_DD']})"
            )
        change = values[value_col].pct_change().abs()
        jumps = values[change > TR_JUMP]
        if len(jumps):
            report.warnings.append(
                f"{label} {name}: 일별 ±5% 넘는 점프 {len(jumps)}건 (첫 {jumps.iloc[0]['BAS_DD']})"
            )


def _check_seibro(store: BaselineStore, report: VerifyReport) -> None:
    spec = sd.SPEC
    if not _years(store, spec.name):
        return
    frames = [store.read_observations(spec, "all", year=y) for y in _years(store, spec.name)]
    frame = pd.concat(frames)
    keys = list(spec.key_columns)
    if frame.duplicated([*keys, "obs_seq"]).any():
        report.failures.append(f"{spec.name}: (키, obs_seq) 중복")
    latest = _latest(frame, spec)
    if latest.duplicated(keys).any():
        report.failures.append(f"{spec.name}: 최신 뷰 업무 키 중복")
    pairs = {
        "ESTM_STDPRC": "per_share_amount",
        "BUNBE": "dist_rate_pct",
        "RGT_STD_DT": "rgt_std_date",
        "TH1_PAY_TERM_BEGIN_DT": "pay_begin_date",
    }
    for source, parsed in pairs.items():
        blank = latest[source].isna() | latest[source].str.strip().isin(["", "-"])
        failed = int((~blank & latest[parsed].isna()).sum())
        if failed:
            report.failures.append(f"{spec.name}: {source} → {parsed} 변환 실패 {failed}건")
    report.notes.append(
        f"{spec.name}: 최신 {len(latest):,}행, TAXSTD 0(결측 표시) "
        f"{int(latest['tax_std_is_zero'].fillna(False).sum()):,}행"
    )
    window = store.read_fetch_log(sd.SERVICE, columns=["request_key", "day_kind"])
    incomplete = int((window["day_kind"] == "incomplete").sum()) if not window.empty else 0
    if incomplete:
        report.notes.append(f"{sd.SERVICE}: 미완료 창 기록 {incomplete}건 (다음 창이 다시 받음)")


def verify(
    store: BaselineStore,
    *,
    today: date,
    start: date | None = None,
    end: date | None = None,
    services: list[str] | None = None,
    window_weekdays: int = kb.SYNC_WINDOW_WEEKDAYS,
) -> VerifyReport:
    """검사 전체. 기본 범위는 최근 평일 20일, ``start``를 주면 그 이후 전부."""
    report = VerifyReport()
    last = min(end, kb.last_available_date(today)) if end else kb.last_available_date(today)
    first = start or kb.sync_window(last, window_weekdays)[0]
    wanted = set(services) if services else set(kb.SERVICES) | {sd.SERVICE}

    bad = store.verify_committed()
    for item in bad[:10]:
        report.failures.append(f"manifest 대조: {item}")
    if len(bad) > 10:
        report.failures.append(f"manifest 대조: 그 밖에 {len(bad) - 10}건")
    loose = store.unreferenced_files()
    report.notes.append(
        f"manifest 없는 파일: parquet {len(loose['parquet'])}개, 원문 {len(loose['raw'])}개"
    )

    def guarded(label: str, check) -> None:  # noqa: ANN001
        """파일이 깨져 읽기 자체가 실패해도 검사 전체가 죽지 않고 실패로 적는다."""
        try:
            check()
        except Exception as exc:  # noqa: BLE001
            report.failures.append(f"{label}: 읽기 실패 — {type(exc).__name__}: {exc}"[:300])

    for name, svc in kb.SERVICES.items():
        if name not in wanted:
            continue
        guarded(f"{name} 관측 표", lambda svc=svc: _check_table_integrity(store, svc.spec, report))
        guarded(
            f"{name} 날짜",
            lambda svc=svc: _check_dates(store, svc, first, last, today, window_weekdays, report),
        )
    if "etf_bydd_trd" in wanted:
        guarded("etf 행 수·순자산", lambda: _check_etf(store, first, last, report))
        guarded("etf 상폐 재현", lambda: _check_delisting(store, report))
    if "bon_dd_trd" in wanted:
        guarded("채권지수", lambda: _check_bond(store, first, last, report))
    if "drvprod_dd_trd" in wanted:
        guarded("코스피200 TR", lambda: _check_derivative(store, report))
    if sd.SERVICE in wanted:
        guarded("분배금", lambda: _check_seibro(store, report))
    return report


__all__ = ["VerifyReport", "verify"]

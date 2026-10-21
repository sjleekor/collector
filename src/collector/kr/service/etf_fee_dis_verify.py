"""ETF 총보수(DIS) 저장소 검사 — 계획 04 §8처럼 **저장소만 보고** 할 수 있는 만큼.

**실패**(종료 코드 1): ① 완료 manifest가 가리키는 파일의 sha256·크기가 다르다 ② 월이 중간에
빠졌다(저장된 첫 달~마지막 달 사이 모든 달에 값이 든 기준일이 있어야 한다) ③ 기준일의 행 수가
요청 기록의 ``dbio_total_count_``와 다르다 ④ 원문 본문의 sha256이 요청 기록과 다르거나
``</root>``로 끝나지 않는다 ⑤ ``(키, obs_seq)``가 겹친다 ⑥ 직전 관측과 해시가 같은 새 관측이 있다.

**경고**: 기준일이 월말 후보(말일 이하 마지막 평일부터 3평일 앞) 밖이다 · 합계가 네 항목 합과
0.011 넘게 다르다 · TER이 합계+기타비용과 0.011 넘게 다르다 · 마지막 저장 달이 70일보다
오래됐다(공개 지연이 보통보다 길거나 정기 수집이 멈췄다는 신호).
"""

from __future__ import annotations

import hashlib
from datetime import date

import pandas as pd

from collector.kr.adapters.kofia_dis_fee import (
    is_plausible_month_end,
    iter_months,
    month_key,
)
from collector.kr.baseline import BaselineStore
from collector.kr.service import etf_fee_dis as ed
from collector.kr.service.krx_baseline_verify import VerifyReport

STALE_DAYS = 70
SUM_TOLERANCE = 0.011


def _years(store: BaselineStore) -> list[str]:
    found = set()
    for path in store.committed_paths("observation", ed.SPEC.name):
        part = next((p for p in path.parts if p.startswith("year=")), None)
        if part:
            found.add(part[5:])
    return sorted(found)


def verify(store: BaselineStore, *, today: date) -> VerifyReport:
    report = VerifyReport()
    bad = store.verify_committed()
    for item in bad[:10]:
        report.failures.append(f"manifest 대조: {item}")
    if len(bad) > 10:
        report.failures.append(f"manifest 대조: 그 밖에 {len(bad) - 10}건")
    loose = store.unreferenced_files()
    raw_loose = [p for p in loose["raw"] if f"/{ed.SERVICE}/" in p]
    table_loose = [p for p in loose["parquet"] if p.startswith(f"{ed.SPEC.name}/")]
    report.notes.append(
        f"manifest 없는 파일: parquet {len(table_loose)}개, 원문 {len(raw_loose)}개"
    )

    dates = ed.completed_std_dates(store)
    if not dates:
        report.failures.append(f"{ed.SERVICE}: 값이 든 기준일이 하나도 없습니다")
        return report
    months = sorted({(int(d[:4]), int(d[4:6])) for d in dates})
    missing = [m for m in iter_months(months[0], months[-1]) if m not in set(months)]
    if missing:
        report.failures.append(
            f"{ed.SERVICE}: 중간에 빠진 달 {len(missing)}개 "
            f"({month_key(missing[0])}~{month_key(missing[-1])}, 첫 {month_key(missing[0])})"
        )
    report.notes.append(
        f"{ed.SERVICE}: {month_key(months[0])}~{month_key(months[-1])}, 기준일 {len(dates)}개"
    )
    odd = [d for d in sorted(dates) if not is_plausible_month_end(_to_date(d))]
    if odd:
        report.warnings.append(f"월말 후보 밖의 기준일 {len(odd)}개 (첫 {odd[0]})")

    # 파일이 깨져 읽기 자체가 실패해도 검사 전체가 죽지 않고 실패로 적는다.
    for label, check in (("관측 표", _check_rows_and_integrity), ("원문", _check_raw)):
        try:
            check(store, dates, report)
        except Exception as exc:  # noqa: BLE001
            report.failures.append(
                f"{label}을 읽지 못했습니다: {type(exc).__name__}: {str(exc)[:120]}"
            )

    latest_day = _to_date(max(dates))
    age = (today - latest_day).days
    report.notes.append(f"마지막 기준일 {latest_day} (오늘 기준 {age}일 전)")
    if age > STALE_DAYS:
        report.warnings.append(
            f"마지막 기준일이 {age}일 전입니다(>{STALE_DAYS}). 정기 수집이 멈췄거나 공개가 늦습니다"
        )
    return report


def _to_date(ymd: str) -> date:
    return date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))


def _check_rows_and_integrity(store: BaselineStore, dates: dict[str, int], report: VerifyReport):
    spec = ed.SPEC
    keys = list(spec.key_columns)
    wanted = [
        *keys, "obs_seq", "obs_kind", "row_hash",
        "fee_mgmt_num", "fee_sell_num", "fee_trust_num", "fee_admin_num",
        "fee_total_num", "cost_other_num", "ter_num",
    ]  # fmt: skip
    total = dup_seq = same_hash = bad_sum = bad_ter = 0
    counts: dict[str, int] = {}
    for year in _years(store):
        frame = store.read_observations(spec, "all", year=year, columns=wanted)
        total += len(frame)
        dup_seq += int(frame.duplicated([*keys, "obs_seq"]).sum())
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
        top = frame.sort_values("obs_seq").drop_duplicates(keys, keep="last")
        top = top[top["obs_kind"] != "absent"]
        for ymd, n in top.groupby("STD_YMD").size().items():
            counts[ymd] = int(n)
        parts = top[["fee_mgmt_num", "fee_sell_num", "fee_trust_num", "fee_admin_num"]].sum(
            axis=1, min_count=4
        )
        bad_sum += int(((parts - top["fee_total_num"]).abs() > SUM_TOLERANCE).sum())
        ter = top["fee_total_num"] + top["cost_other_num"]
        bad_ter += int(((ter - top["ter_num"]).abs() > SUM_TOLERANCE).sum())
    report.notes.append(f"etf_fee_snapshot: 관측 {total:,}행")
    if dup_seq:
        report.failures.append(f"etf_fee_snapshot: (키, obs_seq) 중복 {dup_seq}")
    if same_hash:
        report.failures.append(f"etf_fee_snapshot: 직전 관측과 row_hash가 같은 새 관측 {same_hash}")
    wrong = [
        f"{d}({counts.get(d, 0)}≠{n})" for d, n in sorted(dates.items()) if counts.get(d, 0) != n
    ]
    if wrong:
        report.failures.append(
            f"기준일 {len(wrong)}개의 행 수가 요청 기록(dbio_total_count_)과 다릅니다: {wrong[:3]}"
        )
    if bad_sum:
        report.warnings.append(f"합계가 운용+판매+수탁+사무관리와 다른 행 {bad_sum}개")
    if bad_ter:
        report.warnings.append(f"TER이 합계+기타비용과 다른 행 {bad_ter}개")


def _check_raw(store: BaselineStore, dates: dict[str, int], report: VerifyReport) -> None:
    """값이 든 요청의 원문 본문이 요청 기록의 sha256과 같고 ``</root>``로 끝나는가."""
    log = store.read_fetch_log(
        ed.SERVICE, columns=["request_key", "result", "day_kind", "raw_path", "raw_sha256"]
    )
    if log.empty:
        return
    rows = log[(log["result"] != "failed") & (log["day_kind"] == ed.KIND_COMPLETE)]
    bad_hash: list[str] = []
    truncated: list[str] = []
    for _, row in rows.iterrows():
        if pd.isna(row["raw_path"]):
            bad_hash.append(f"{row['request_key']}: 원문 경로 없음")
            continue
        try:
            body = store.read_raw(row["raw_path"])
        except OSError as exc:
            bad_hash.append(f"{row['request_key']}: 원문을 못 읽음 ({type(exc).__name__})")
            continue
        if hashlib.sha256(body).hexdigest() != row["raw_sha256"]:
            bad_hash.append(row["request_key"])
        if not body.rstrip().endswith(b"</root>"):
            truncated.append(row["request_key"])
    if bad_hash:
        report.failures.append(f"원문 sha256이 요청 기록과 다름 {len(bad_hash)}건: {bad_hash[:3]}")
    if truncated:
        report.failures.append(f"</root>로 끝나지 않는 원문 {len(truncated)}건: {truncated[:3]}")

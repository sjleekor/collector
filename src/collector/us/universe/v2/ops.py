"""v1 이 끝난 뒤 v2 를 잇는 운영 연결 (설계 02 §2.5).

**v1 빌더(``collector.us.universe.build``)는 바꾸지 않는다.** v1 이 끝난 뒤 CLI 핸들러가 이 모듈을
부른다. 둘은 서로 다른 일을 한다.

* :func:`record_v1_dolt_commit` — v1 은 dolt ``symbol`` **현재값**으로 ETF·시험 표시를 메운다
  (``build.py`` 의 ``symbol_current`` fallback). 그 값은 얼릴 수 없어서, 빌드마다 그때의 dolt
  커밋을 **별도 기록 파일**에 남긴다. 4차 보조 판정이 v1 을 재현할 때 이 커밋으로 돌린다.
* :func:`run_v2_after_v1` — v2 증분을 돈다. **절대 예외를 던지지 않는다.** v2 가 실패해도 v1 결과와
  종료 코드는 그대로고, 실패는 기록 파일과 stderr 에 남는다.
"""

from __future__ import annotations

import datetime as _dt
import json
import sys
from pathlib import Path

#: v1 빌드마다 한 줄. ``output/`` 아래라 미러(``us-mirror.sh``)의 ``--delete`` 가 안 지운다.
V1_PROVENANCE = ("universe_v1_provenance", "dolt_commits.jsonl")
#: v2 실행마다 한 줄 (성공·실패·건너뜀 모두).
V2_RUNS = ("universe_v2", "runs.jsonl")


def _append(root, parts: tuple[str, str], record: dict) -> Path:
    path = root.output.joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True, default=str) + "\n")
    return path


def record_v1_dolt_commit(
    root, *, snapshot_date, kind: str, v1_result: dict | None = None
) -> dict[str, object]:
    """v1 빌드 한 번에 대한 dolt ``stocks`` 커밋을 적는다. 실패해도 예외를 던지지 않는다."""
    from collector.us.universe.v2.build import _dolt_commit

    record: dict[str, object] = {
        "recorded_at": _dt.datetime.now(_dt.UTC).isoformat(),
        "snapshot_date": str(snapshot_date),
        "kind": kind,
        "dolt_stocks": _dolt_commit(root),
    }
    if v1_result:
        for key in ("path", "completion_path", "start", "end", "added_start", "added_end"):
            if v1_result.get(key) is not None:
                record[f"v1_{key}"] = str(v1_result[key])
    try:
        record["journal"] = str(_append(root, V1_PROVENANCE, record))
    except OSError as exc:
        record["journal_error"] = f"{type(exc).__name__}: {exc}"
        print(f"warning: v1 dolt commit record failed: {exc}", file=sys.stderr)
    return record


def run_v2_after_v1(
    root, *, snapshot_date, observed_at=None, bounded: bool | None = None
) -> dict[str, object]:
    """v2 증분을 돌린다. 상태는 ``ok``(성공·건너뜀)·``needs_rebuild``·``failed`` 중 하나다.

    ``listing_snapshots_v2`` 는 raw 가 새로 들어왔으면 먼저 다시 굳힌다 (derive 와 같은 규칙).
    직전 ``universe_daily_v2`` 가 없으면 증분을 못 하므로 ``needs_rebuild`` 로 끝낸다 —
    첫 한 번은 ``us-universe-v2 rebuild`` 를 손으로 돌린다.
    """
    started = _dt.datetime.now(_dt.UTC)
    out: dict[str, object] = {"status": "failed", "snapshot_date": str(snapshot_date)}
    try:
        from collector.us.ops import derive
        from collector.us.store.writer import latest_snapshot
        from collector.us.universe.v2.build import build_universe_v2

        derived = derive.run_derive(
            root, snapshot_date=snapshot_date, tables=("listing-snapshots-v2",), budget_seconds=None
        )
        out["listing_snapshots_v2"] = derived["tables"][0]
        if not derived["ok"]:
            raise RuntimeError(f"listing_snapshots_v2 를 굳히지 못했다: {derived['tables'][0]}")
        if latest_snapshot(root, "universe_daily_v2") is None:
            out["status"] = "needs_rebuild"
            out["reason"] = (
                "직전 universe_daily_v2 가 없다 — us-universe-v2 rebuild 를 한 번 돌린다"
            )
        else:
            result = build_universe_v2(
                root,
                snapshot_date=snapshot_date,
                mode="incremental",
                if_new=True,
                observed_at=observed_at,
                bounded=bounded,
            )
            out["status"] = "ok"
            out["result"] = result
    except Exception as exc:  # noqa: BLE001 — v2 의 실패가 v1 의 종료 코드를 바꾸면 안 된다
        out["error"] = f"{type(exc).__name__}: {exc}"
        print(f"warning: universe v2 failed (v1 is unaffected): {out['error']}", file=sys.stderr)
    out["seconds"] = round((_dt.datetime.now(_dt.UTC) - started).total_seconds(), 1)
    try:
        record = {k: v for k, v in out.items() if k != "result"}
        result = out.get("result")
        if isinstance(result, dict):
            record["result"] = {
                k: result.get(k)
                for k in ("skipped", "reason", "path", "start", "end", "months_judged", "rows")
            }
        record["recorded_at"] = started.isoformat()
        out["journal"] = str(_append(root, V2_RUNS, record))
    except OSError as exc:
        print(f"warning: universe v2 run record failed: {exc}", file=sys.stderr)
    return out

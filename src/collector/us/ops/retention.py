"""스냅샷 보존 — **바뀐 것만 남긴다** (04 C8 · 05 §5.1).

매일 전량을 굳히면 낭비다. 그런데 파일을 바이트로 비교할 수가 없다 —
``observed_at``이 실행마다 달라 **내용이 같아도 파일이 다르다.**

그래서 ``observed_at``을 뺀 나머지 컬럼으로 지문을 만든다. 행 순서에 안 흔들리게
행별 해시를 더한다. **증명이 아니라 변경 감지기다** — 충돌 확률이 0은 아니지만
같은 표의 이웃 스냅샷을 가르는 데는 충분하다.

지우는 것은 **뒤쪽**이다. 앞 스냅샷이 그 내용을 처음 본 시점이라 그쪽을 남긴다.
**가장 최근 스냅샷은 절대 안 지운다** — 다음 실행이 비교 대상으로 쓴다.
"""

from __future__ import annotations

from pathlib import Path

from collector.lake import DataRoot
from collector.us.store.schema import ARROW_SCHEMAS
from collector.us.store.writer import PROVENANCE_REQUIRED


def snapshot_paths(root: DataRoot, table: str) -> list[Path]:
    """그 표의 스냅샷을 ``snapshot_date`` 오름차순으로."""
    base = root.derived / "snapshots" / table
    if not base.is_dir():
        return []
    return sorted(base.glob("snapshot_date=*/part.parquet"), key=lambda p: p.parent.name)


def fingerprint(path: Path, table: str) -> tuple[int, int]:
    """``(행 수, 내용 지문)``. ``observed_at``을 뺀 컬럼만 본다."""
    import duckdb

    columns = [c for c in ARROW_SCHEMAS[table].names if c not in PROVENANCE_REQUIRED]
    if not columns:
        raise ValueError(f"{table}: 비교할 컬럼이 없다")
    expr = ", ".join(f'"{c}"' for c in columns)
    rows, digest = duckdb.connect().execute(
        f"SELECT count(*), sum(hash({expr})::HUGEINT) FROM read_parquet('{path}')"
    ).fetchone()
    return int(rows), int(digest or 0)


def prune_unchanged(
    root: DataRoot, table: str, *, dry_run: bool = True
) -> dict[str, object]:
    """내용이 앞 스냅샷과 같은 것을 지운다. **기본이 ``dry_run``이다.**"""
    paths = snapshot_paths(root, table)
    if len(paths) < 2:
        return {"table": table, "snapshots": len(paths), "removed": [], "kept": len(paths)}

    removed: list[str] = []
    previous = fingerprint(paths[0], table)
    for path in paths[1:-1]:  # 마지막은 건드리지 않는다
        current = fingerprint(path, table)
        if current == previous:
            removed.append(path.parent.name)
            if not dry_run:
                path.unlink()
                if not any(path.parent.iterdir()):
                    path.parent.rmdir()
        else:
            previous = current
    return {
        "table": table,
        "snapshots": len(paths),
        "removed": removed,
        "kept": len(paths) - len(removed),
        "dry_run": dry_run,
    }

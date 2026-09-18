"""DoltHub 로컬 clone에서 읽는다.

**원격 SQL API를 쓰지 않는 이유.** 한 번에 1,000행이고 페이징 토큰이 없다
(2026-09-18 실측: 1,500행을 요청하면 ``RowLimit`` 상태로 1,000행만 온다).
``volatility_history``는 **하루치가 1,535행**이라 날짜로 쪼개도 한 번에 안
들어온다. 그리고 API로 뽑으면 commit 이력이 없어 [`06` §2.3]의
``as of <commit>`` PIT 감사가 안 된다. clone은 둘 다 해결한다.

**clone은 테이블을 고르지 못한다.** ``options``를 받으면 ``option_chain``
1.18억 행이 같이 온다. **받되 적재하지 않는다** — 미결제약정도 거래량도 없어
지금 설계된 피쳐가 없고, 개인이 유지하는 레포라 멈추면 시점 데이터를 다시 못
받으므로 clone 자체는 보험이다 (03 §4.6).
"""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from datetime import date as _date
from pathlib import Path

import pyarrow as pyar
import pyarrow.parquet as pq

from collector.lake import DataRoot
from collector.us.store.writer import snapshot_path, write_snapshot_arrow

#: 쓰는 레포. clone 위치는 ``<root>/raw/dolt/<repo>``다.
REPOS: tuple[str, ...] = ("stocks", "options", "earnings")


class DoltError(RuntimeError):
    """dolt 명령이 실패했다."""


def repo_dir(root: DataRoot, repo: str) -> Path:
    return root.raw / "dolt" / repo


def _run(repo: Path, *args: str) -> str:
    if not (repo / ".dolt").is_dir():
        raise DoltError(f"{repo} 가 dolt 레포가 아니다. clone이 끝났는지 본다.")
    proc = subprocess.run(["dolt", *args], cwd=repo, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise DoltError(f"dolt {' '.join(args)} 실패: {proc.stderr.strip()[:300]}")
    return proc.stdout


def head_commit(repo: Path) -> str:
    """``source_rev``로 쓸 커밋 해시. 스냅샷이 어느 시점 원천인지를 이게 잡는다."""
    out = _run(repo, "sql", "-q", "select commit_hash from dolt_log limit 1", "-r", "csv")
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    if len(lines) < 2:
        raise DoltError(f"{repo}: dolt_log 를 못 읽었다")
    return lines[1]


def export_table(repo: Path, table: str, dest: Path) -> Path:
    """``dolt table export`` — parquet을 그대로 낸다. 원천 타입이 보존된다."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    _run(repo, "table", "export", "-f", table, str(dest))
    return dest


def load_volatility_daily(
    root: DataRoot,
    *,
    snapshot_date: _date | str,
    observed_at: datetime | None = None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """``options/volatility_history`` 한 벌을 레이크 스냅샷으로 굳힌다 (03 §4.6).

    ``option_chain``은 건드리지 않는다.
    """
    observed_at = observed_at or datetime.now(UTC)
    repo = repo_dir(root, "options")
    rev = head_commit(repo)

    work_dir = work_dir or (root.output / "_tmp")
    staged = export_table(repo, "volatility_history", work_dir / "volatility_history.parquet")

    tbl = pq.read_table(staged)
    tbl = tbl.rename_columns(
        ["symbol" if name == "act_symbol" else name for name in tbl.column_names]
    )
    n = tbl.num_rows
    tbl = tbl.append_column(
        "observed_at", pyar.array([observed_at] * n, type=pyar.timestamp("us", tz="UTC"))
    )
    tbl = tbl.append_column("source_rev", pyar.array([rev] * n, type=pyar.string()))

    dest = snapshot_path(root, "volatility_daily", snapshot_date)
    write_snapshot_arrow(tbl, "volatility_daily", dest, unique_on=("date", "symbol"))
    staged.unlink(missing_ok=True)

    return {"path": dest, "rows": n, "source_rev": rev, "observed_at": observed_at}

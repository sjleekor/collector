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
from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

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


#: 로컬 미러의 merge 커밋에만 붙는 신원. **어디로도 푸시하지 않는다.**
#: ``dolt pull``이 ff-only여도 신원을 먼저 요구해서(dolt 2.3.5) 없으면 못 돈다 —
#: C8 첫 실행에서 레포 셋이 다 그렇게 멈췄다 (2026-09-20).
LOCAL_AUTHOR_NAME = "collector"
LOCAL_AUTHOR_EMAIL = "collector@localhost"


def ensure_identity(repo: Path) -> bool:
    """레포 **안에만** 신원을 둔다. 전역 설정(``~/.dolt``)을 건드리지 않는다.

    사람의 이름·메일을 쓰지 않는다. 이 커밋은 읽기 전용 미러를 앞으로 감는
    merge 커밋뿐이고 원격으로 나가지 않는다. 이미 있으면 아무것도 안 한다.
    """
    current = _run(repo, "config", "--local", "--list")
    have = {
        line.split(" = ", 1)[0].strip()
        for line in current.splitlines()
        if " = " in line
    }
    changed = False
    for key, value in (
        ("user.name", LOCAL_AUTHOR_NAME),
        ("user.email", LOCAL_AUTHOR_EMAIL),
    ):
        if key not in have:
            _run(repo, "config", "--local", "--add", key, value)
            changed = True
    return changed


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


def pull(root: DataRoot, repo: str) -> dict[str, object]:
    """``dolt pull``. 커밋 해시가 바뀌었는지 돌려준다.

    **해시가 같으면 새로 굳힐 것이 없다** (05 §5 — 가격은 바뀐 것만 남긴다).
    """
    path = repo_dir(root, repo)
    ensure_identity(path)  # 없으면 ff-only 여도 pull이 거부된다
    before = head_commit(path)
    _run(path, "pull", "--silent")
    after = head_commit(path)
    return {"repo": repo, "before": before, "after": after, "changed": before != after}


def load_prices_daily(
    root: DataRoot,
    *,
    snapshot_date: _date | str,
    observed_at: datetime | None = None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """``stocks/ohlcv`` 한 벌을 ``prices_daily`` 스냅샷으로 굳힌다 (03 §4.1).

    2,900만 행이라 메모리에 올리지 않는다 — DuckDB가 곧장 parquet으로 흘리고,
    계약 확인은 쓴 뒤에 :func:`verify_snapshot`이 한다.

    **조정하지 않는다.** 원시값과 이벤트만 저장하고 조정은 읽을 때 계산한다
    (:mod:`collector.us.adjust`).
    """
    import duckdb

    observed_at = observed_at or datetime.now(UTC)
    repo = repo_dir(root, "stocks")
    rev = head_commit(repo)
    work_dir = work_dir or (root.output / "_tmp")
    staged = export_table(repo, "ohlcv", work_dir / "ohlcv.parquet")

    dest = snapshot_path(root, "prices_daily", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT CAST(date AS DATE) AS date,
                   act_symbol AS symbol,
                   open, high, low, close,
                   CAST(volume AS BIGINT) AS volume,
                   CAST(? AS TIMESTAMP WITH TIME ZONE) AS observed_at,
                   CAST(? AS VARCHAR) AS source_rev
            FROM read_parquet('{staged}')
        ) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [observed_at, rev],
    )
    staged.unlink(missing_ok=True)
    stats = verify_snapshot(dest, "prices_daily", unique_on=("date", "symbol"))
    return {"path": dest, "source_rev": rev, "observed_at": observed_at, **stats}


def load_corp_actions(
    root: DataRoot,
    *,
    snapshot_date: _date | str,
    observed_at: datetime | None = None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """분할 세 곳 + 배당을 ``corp_actions`` 한 장으로 합친다 (03 §2.1, §4.2).

    **DoltHub `split`만 쓰면 안 된다.** `GOOGL` 2014-04-03 2:1이 거기 없다.
    겹치면 DoltHub를 우선하고 보충분에만 있는 것을 더한다.
    """
    import duckdb

    observed_at = observed_at or datetime.now(UTC)
    repo = repo_dir(root, "stocks")
    rev = head_commit(repo)
    work_dir = work_dir or (root.output / "_tmp")
    splits = export_table(repo, "split", work_dir / "split.parquet")
    dividends = export_table(repo, "dividend", work_dir / "dividend.parquet")

    backfill = root.derived / "splits" / "us_splits_2011_2014.csv"
    missing = root.derived / "splits" / "us_splits_dolt_missing_2014.csv"
    for extra in (backfill, missing):
        if not extra.is_file():
            raise FileNotFoundError(f"보충 분할표가 없다: {extra} (03 §2.1)")

    dest = snapshot_path(root, "corp_actions", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            -- dolt table export 가 DATE 를 timestamp 로 쓴다. 되돌린다.
            WITH dolt_split AS (
                SELECT act_symbol AS symbol, CAST(ex_date AS DATE) AS ex_date,
                       to_factor, for_factor,
                       'dolt' AS source, 'dolt' AS tier
                FROM read_parquet('{splits}')
            ),
            extra AS (
                SELECT act_symbol AS symbol, CAST(ex_date AS DATE) AS ex_date,
                       CAST(to_factor AS DECIMAL(10,5)) AS to_factor,
                       CAST(for_factor AS DECIMAL(10,5)) AS for_factor,
                       source, tier
                FROM read_csv_auto('{backfill}')
                UNION ALL
                SELECT act_symbol, CAST(ex_date AS DATE),
                       CAST(to_factor AS DECIMAL(10,5)),
                       CAST(for_factor AS DECIMAL(10,5)), source, tier
                FROM read_csv_auto('{missing}')
            ),
            -- 겹치면 DoltHub가 이긴다: 보충분에서 (symbol, ex_date)가 같은 것을 뺀다
            extra_only AS (
                SELECT e.* FROM extra e
                WHERE NOT EXISTS (
                    SELECT 1 FROM dolt_split d
                    WHERE d.symbol = e.symbol AND d.ex_date = e.ex_date
                )
            ),
            all_splits AS (
                SELECT * FROM dolt_split UNION ALL SELECT * FROM extra_only
            )
            SELECT symbol,
                   ex_date,
                   'split' AS kind,
                   to_factor, for_factor,
                   CAST(NULL AS DECIMAL(10,5)) AS amount,
                   CAST(NULL AS DATE) AS declaration_date,
                   CAST(NULL AS DATE) AS record_date,
                   CAST(NULL AS DATE) AS payment_date,
                   source, tier,
                   CAST(? AS TIMESTAMP WITH TIME ZONE) AS observed_at,
                   CAST(? AS VARCHAR) AS source_rev
            FROM all_splits
            UNION ALL
            SELECT act_symbol, CAST(ex_date AS DATE), 'dividend',
                   CAST(NULL AS DECIMAL(10,5)), CAST(NULL AS DECIMAL(10,5)),
                   amount,
                   CAST(NULL AS DATE), CAST(NULL AS DATE), CAST(NULL AS DATE),
                   'dolt', 'dolt',
                   CAST(? AS TIMESTAMP WITH TIME ZONE), CAST(? AS VARCHAR)
            FROM read_parquet('{dividends}')
        ) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [observed_at, rev, observed_at, rev],
    )
    for f in (splits, dividends):
        f.unlink(missing_ok=True)
    stats = verify_snapshot(dest, "corp_actions", unique_on=("symbol", "ex_date", "kind"))
    return {"path": dest, "source_rev": rev, "observed_at": observed_at, **stats}

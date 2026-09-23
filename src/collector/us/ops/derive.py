"""``raw/`` 에 받아 둔 것을 ``derived/`` 스냅샷으로 굳히는 한 번의 실행
(미국 계획 04 C8 · 05 §4.1·§5).

**하루 실행(``us-daily run``)은 raw 만 받는다.** 굳히는 단계가 따로 없어서
``dolt pull`` 은 매일 도는데 ``prices_daily`` 스냅샷은 2026-09-09 에서 멈춰
있었다 (2026-09-21 확인). 이 모듈이 그 자리를 메운다.

**언제 다시 굳힐지를 일정으로 안 판단한다.** 하루 실행과 같은 규칙이다 —
입력이 바뀌었나만 본다. 그래서 분기짜리 SEC 표는 주 1회 돌려도 분기에 한 번만
새로 굳고, 안 바뀐 주에는 아무것도 안 쌓인다.

dolt 셋(``prices_daily``·``corp_actions``·``volatility_daily``)은 **스냅샷에
적힌 ``source_rev`` 와 지금 레포 HEAD 를 비교한다** — 05 §5 가 "가격은 커밋
해시가 같으면 안 받는다"고 적어 두고 코드로는 안 옮긴 규칙이다. 나머지는
``raw/`` 쪽 입력의 가장 늦은 mtime 이 스냅샷보다 뒤면 다시 굳힌다.

``universe_daily`` 는 여기 없다. **월 1회 재판정이고 명령이 따로 있다**
(``us-universe rebuild`` · 03 §5.3). 매일·매주 다시 판정하면 하루짜리 거래량
급증에 유니버스가 흔들린다.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from collector.lake import DataRoot

#: 하루 실행과 같은 기본 예산. 표 하나가 오래 걸려도 다음 표를 굶기지 않는다.
DEFAULT_BUDGET_SECONDS = 3_600.0


@dataclass(frozen=True)
class Recipe:
    """표를 굳히는 법 하나.

    ``tables`` 가 여럿인 것은 **한 로더가 표 둘을 같이 쓰기 때문**이다 —
    ``submissions`` 가 ``filings_index`` 와 ``company_meta`` 를 함께 낸다.
    """

    loader: str
    tables: tuple[str, ...]
    #: dolt 레포 이름. 있으면 ``source_rev`` 로 판단한다
    dolt_repo: str | None = None
    #: ``raw/`` 아래 상대 경로들. ``dolt_repo`` 가 없을 때 mtime 으로 판단한다
    raw_inputs: tuple[str, ...] = ()
    #: **앞을 내다보는 표.** 표의 마지막 날이 오늘로부터 이만큼 안으로
    #: 들어오면 다시 굳힌다. 원천이 아니라 지평이 판단 근거다.
    horizon_days: int | None = None


#: ``us-load <key>`` 와 ``us-derive run`` 이 같이 쓰는 표. **여기가 정본이다** —
#: CLI 의 ``_LOADERS`` 는 이걸 보고 만든다.
RECIPES: dict[str, Recipe] = {
    "prices-daily": Recipe(
        "collector.us.sources.dolt:load_prices_daily",
        ("prices_daily",),
        dolt_repo="stocks",
    ),
    "corp-actions": Recipe(
        "collector.us.sources.dolt:load_corp_actions",
        ("corp_actions",),
        dolt_repo="stocks",
    ),
    "volatility-daily": Recipe(
        "collector.us.sources.dolt:load_volatility_daily",
        ("volatility_daily",),
        dolt_repo="options",
    ),
    "short-interest": Recipe(
        "collector.us.sources.finra:load_short_interest",
        ("short_interest",),
        raw_inputs=("finra/short_interest",),
    ),
    "short-volume": Recipe(
        "collector.us.sources.finra:load_short_volume",
        ("short_volume",),
        raw_inputs=("finra/regsho",),
    ),
    "earnings-calendar": Recipe(
        "collector.us.sources.nasdaq:load_earnings_calendar",
        ("earnings_calendar",),
        raw_inputs=("nasdaq/earnings_calendar",),
    ),
    "nasdaq-analyst-estimates": Recipe(
        "collector.us.sources.nasdaq_analyst:load_nasdaq_analyst_estimates",
        ("nasdaq_analyst_estimates",),
        raw_inputs=("nasdaq/analyst_earnings_forecast",),
    ),
    "fundamentals": Recipe(
        "collector.us.sources.sec_bulk:load_companyfacts",
        ("fundamentals",),
        raw_inputs=("sec/bulk/companyfacts.zip",),
    ),
    "submissions": Recipe(
        "collector.us.sources.sec_bulk:load_submissions",
        ("filings_index", "company_meta"),
        raw_inputs=("sec/bulk/submissions.zip",),
    ),
    "insider": Recipe(
        "collector.us.sources.sec:extract_insider",
        ("insider_trans", "insider_owners"),
        raw_inputs=("sec/quarterly/insider",),
    ),
    "filings-sub": Recipe(
        "collector.us.sources.sec:extract_filings_sub",
        ("filings_sub",),
        raw_inputs=("sec/quarterly/financial",),
    ),
    "midas": Recipe(
        "collector.us.sources.sec:extract_midas",
        ("midas_security_daily",),
        raw_inputs=("sec/quarterly/midas",),
    ),
    "listing-snapshots": Recipe(
        "collector.us.sources.wayback:build_listing_snapshots",
        ("listing_snapshots",),
        raw_inputs=("wayback/symdir",),
    ),
    # **원천이 없는 표다.** `exchange_calendars` 가 주는데, 그 라이브러리가
    # 대략 오늘+1년까지만 세션을 만든다. 한 번 굳히고 두면 그 날짜에 하루
    # 실행이 조용히 멈춘다 — 옛 기본값 `end="2026-12-31"` 이 2027-01-01 에
    # 그럴 참이었다 (2026-09-21). 남은 날로 판단해 미리 늘려 둔다.
    "trading-calendar": Recipe(
        "collector.us.calendars:load_trading_calendar",
        ("trading_calendar",),
        horizon_days=270,
    ),
}

#: ``us-derive`` 를 안 거치고 굳는 표. **왜 안 거치는지**를 같이 적는다.
NOT_DERIVED: dict[str, str] = {
    "universe_daily": "us-universe rebuild — 월 1회 재판정 (03 §5.3)",
    "macro_series": "us-daily 의 weekly_macro — FRED, 주 1회",
    "index_constituents": "us-daily 의 weekly_macro — Wikipedia, 주 1회",
}


@dataclass
class TableRun:
    """표 하나를 본 결과. **안 굳힌 것도 왜 안 굳혔는지 적는다.**"""

    name: str
    rebuilt: bool = False
    reason: str = ""
    seconds: float = 0.0
    rows: int | None = None
    ok: bool = True
    error: str = ""
    tables: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "rebuilt": self.rebuilt,
            "reason": self.reason,
            "seconds": round(self.seconds, 1),
            "rows": self.rows,
            "ok": self.ok,
            "error": self.error,
            "tables": self.tables,
        }


class _Budget:
    def __init__(self, seconds: float | None):
        self.seconds = seconds
        self.started = time.monotonic()

    def spent(self) -> bool:
        return self.seconds is not None and time.monotonic() - self.started > self.seconds


def _snapshot_rev(path: Path) -> str | None:
    """스냅샷에 적힌 ``source_rev``. 행 그룹 하나만 읽는다 — 4억 행을 안 연다."""
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    if "source_rev" not in pf.schema_arrow.names or pf.num_row_groups == 0:
        return None
    col = pf.read_row_group(0, columns=["source_rev"])["source_rev"]
    return col[0].as_py() if col.length() else None


def _newest_mtime(path: Path) -> float | None:
    """파일이면 제 mtime, 디렉터리면 그 아래 가장 늦은 mtime."""
    if path.is_file():
        return path.stat().st_mtime
    if not path.is_dir():
        return None
    times = [p.stat().st_mtime for p in path.rglob("*") if p.is_file()]
    return max(times) if times else None


def needs_rebuild(root: DataRoot, name: str, recipe: Recipe) -> tuple[bool, str]:
    """다시 굳혀야 하나, 그리고 그 이유.

    **판단할 수 없으면 굳히는 쪽으로 간다.** 안 굳혀서 조용히 낡는 것이
    한 번 더 굳히는 것보다 나쁘다 — 이 버그가 그렇게 생겼다.
    """
    from collector.us.store.writer import latest_snapshot

    snap = latest_snapshot(root, recipe.tables[0])
    if snap is None:
        return True, "스냅샷이 없다"

    if recipe.horizon_days is not None:
        import datetime as _dt

        import duckdb

        last = duckdb.connect().execute(
            f"SELECT max(date) FROM read_parquet('{snap}')"
        ).fetchone()[0]
        if last is None:
            return True, "표가 비었다"
        left = (last - _dt.date.today()).days
        if left <= recipe.horizon_days:
            return True, f"{left}일 뒤 끝난다 (지평 {recipe.horizon_days}일)"
        return False, f"{left}일 남았다"

    if recipe.dolt_repo:
        from collector.us.sources import dolt

        repo = dolt.repo_dir(root, recipe.dolt_repo)
        if not (repo / ".dolt").is_dir():
            return False, f"dolt 레포가 없다: {repo}"
        head = dolt.head_commit(repo)
        have = _snapshot_rev(snap)
        if have == head:
            return False, f"source_rev 그대로 ({head[:8]})"
        return True, f"source_rev {(have or '없음')[:8]} → {head[:8]}"

    snap_mtime = snap.stat().st_mtime
    newest, newest_src = None, ""
    for rel in recipe.raw_inputs:
        got = _newest_mtime(root.raw / rel)
        if got is not None and (newest is None or got > newest):
            newest, newest_src = got, rel
    if newest is None:
        return False, f"입력이 없다: {', '.join(recipe.raw_inputs)}"
    if newest <= snap_mtime:
        return False, "raw 가 스냅샷보다 오래됐다"
    return True, f"raw 가 새로 들어왔다 ({newest_src})"


def run_derive(
    root: DataRoot,
    *,
    snapshot_date,
    tables: tuple[str, ...] | None = None,
    force: bool = False,
    budget_seconds: float | None = DEFAULT_BUDGET_SECONDS,
    dry_run: bool = False,
) -> dict[str, object]:
    """굳힐 것을 굳힌다. **예산이 다하면 남은 것은 다음 실행이 한다.**"""
    import importlib

    wanted = tables or tuple(RECIPES)
    unknown = [t for t in wanted if t not in RECIPES]
    if unknown:
        raise ValueError(f"모르는 표: {unknown} (있는 것: {sorted(RECIPES)})")

    budget = _Budget(budget_seconds)
    runs: list[TableRun] = []
    pending = 0

    for name in wanted:
        recipe = RECIPES[name]
        run = TableRun(name, tables=list(recipe.tables))

        if budget.spent():
            run.reason = "예산이 다했다 — 다음 실행이 한다"
            pending += 1
            runs.append(run)
            continue

        try:
            should, why = (True, "force") if force else needs_rebuild(root, name, recipe)
        except Exception as exc:  # noqa: BLE001 — 표 하나가 죽어도 나머지는 본다
            run.ok, run.error, run.reason = False, f"{type(exc).__name__}: {exc}", "판단 실패"
            runs.append(run)
            continue

        run.reason = why
        if not should:
            runs.append(run)
            continue
        if dry_run:
            run.rebuilt = True
            pending += 1
            runs.append(run)
            continue

        started = time.monotonic()
        try:
            module_name, _, func_name = recipe.loader.partition(":")
            func = getattr(importlib.import_module(module_name), func_name)
            result = func(root, snapshot_date=snapshot_date)
            run.rebuilt = True
            if isinstance(result, dict):
                run.rows = result.get("rows")
        except Exception as exc:  # noqa: BLE001
            run.ok, run.error = False, f"{type(exc).__name__}: {exc}"
        run.seconds = time.monotonic() - started
        runs.append(run)

    return {
        "snapshot_date": str(snapshot_date),
        "dry_run": dry_run,
        "force": force,
        "rebuilt": sum(1 for r in runs if r.rebuilt and r.ok),
        "skipped": sum(1 for r in runs if not r.rebuilt and r.ok),
        "pending": pending,
        "budget_spent": budget.spent(),
        "ok": all(r.ok for r in runs),
        "tables": [r.as_dict() for r in runs],
    }

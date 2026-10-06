"""하루치를 받는 한 번의 실행 (04 C8 · 05 §3·§4).

**놓친 날짜를 따로 메꾸지 않는다.** 맥이 꺼져 있던 날이 생기는 것이 로컬 운영의
전제라(D15) 할 일 목록을 **일정이 아니라 `raw/`에 무엇이 있나로 만든다** —
마지막으로 받은 것부터 어제까지가 저절로 대상이 된다 (04 §2.4). backfill과
상시 운영이 **같은 함수**를 쓰는 이유도 이것이다.

**시간 예산을 받는다.** 며칠 꺼져 있었으면 할 일이 수백 건이 되는데, 한 번에
다 하려다 중간에 죽으면 어디까지 했는지 모른다. 예산 안에서 하고 남은 수를
돌려준다 — 다음 실행이 이어서 한다.

원천별 성공 판정은 06 §1이다. **"200이면 성공"이 아니다** — 받은 것을 열어
보고, 빈 응답과 실패를 가른다.
"""

from __future__ import annotations

import datetime as dt
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

from collector.lake import DataRoot

#: 주 단위로 도는 것의 기준. 파일이 이보다 오래되면 다시 받는다 (05 §4).
WEEKLY_MAX_AGE_DAYS = 7

class CalendarExhaustedError(RuntimeError):
    """거래일 캘린더가 필요한 날짜까지 안 간다. **조용히 멈추면 안 된다.**"""


#: 캘린더 끝이 이 안으로 들어오면 결과에 남긴다. 사람이 볼 수 있게.
CALENDAR_WARN_DAYS = 180

#: 하루 실행의 기본 예산. 05 §3.1이 말하는 "하루 약 35요청·3분"의 열 배다 —
#: 며칠 꺼져 있었을 때 따라잡을 여지를 둔다.
DEFAULT_BUDGET_SECONDS = 1_800.0


#: 분기 ZIP이 **마감 뒤 이만큼 안에는 아직 안 올라와도 정상**인 날수. 안에서 받은
#: 404는 "발표 전"이고 실패가 아니다. 넘은 뒤의 404는 경로가 바뀐 것일 수 있어
#: 실패로 센다. 근거는 2026-10-05 sj2-server에서 잰 ``Last-Modified`` 마감 대비 지연이다.
#:
#: * ``financial``(FSDS) 8~16일, 단 2025q3 **62일**·2026q2 50일 → 90일
#: * ``insider`` 7~9일, 단 2025q3 **49일** → 90일
#: * ``midas`` 2025q1부터 **128~289일**(37일인 분기도 있다) → 300일
#:   (``my/milestones/us/plan/20260927_us4_flow_features/02_lag_constants.md`` §3)
#:
#: 여기 없는 갈래는 유예가 없다 — 404가 곧 실패다.
QUARTERLY_PUBLISH_GRACE_DAYS: dict[str, int] = {
    "financial": 90,
    "insider": 90,
    "midas": 300,
}


@dataclass
class SourceRun:
    """원천 하나의 실행 결과. **실패를 성공으로 세지 않는다.**"""

    name: str
    fetched: int = 0
    skipped: int = 0
    missing: list[str] = field(default_factory=list)
    pending: int = 0
    ok: bool = True
    note: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "fetched": self.fetched,
            "skipped": self.skipped,
            "missing": self.missing,
            "pending": self.pending,
            "ok": self.ok,
            "note": self.note,
        }


class _Budget:
    def __init__(self, seconds: float | None):
        self.seconds = seconds
        self.started = time.monotonic()

    def spent(self) -> bool:
        return self.seconds is not None and time.monotonic() - self.started > self.seconds


def sessions_through(root: DataRoot, *, until: dt.date) -> list:
    """굳혀 둔 거래일 캘린더에서 ``until``까지의 세션.

    **오늘 날짜가 아니라 가장 최근 스냅샷을 쓴다.** 캘린더는 천천히 바뀌어
    매일 다시 굳히지 않는다.
    """
    import duckdb

    from collector.us.store.writer import latest_snapshot

    path = latest_snapshot(root, "trading_calendar")
    if path is None:
        raise FileNotFoundError(
            "거래일 캘린더 스냅샷이 없다. 먼저 굳힌다 — collector us-calendar build"
        )
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT date FROM read_parquet('{path}')"
        f" WHERE date <= DATE '{until}' ORDER BY date"
    ).fetchall()
    # **캘린더가 모자라면 조용히 적게 돌려주지 않는다.** 그러면 "받을 것이
    # 없다"와 "캘린더가 끝났다"가 구별이 안 되고 수집이 exit 0 으로 멈춘다 —
    # 옛 기본값 `end="2026-12-31"` 이 2027-01-01 에 그렇게 만들 뻔했다
    # (2026-09-21 확인).
    calendar_end = con.execute(f"SELECT max(date) FROM read_parquet('{path}')").fetchone()[0]
    if calendar_end is not None and calendar_end < until:
        raise CalendarExhaustedError(
            f"거래일 캘린더가 {calendar_end} 에서 끝나는데 {until} 까지가 필요하다. "
            "다시 굳힌다 — collector us-calendar build"
        )
    return [r[0] for r in rows]


def calendar_end(root: DataRoot) -> dt.date | None:
    """굳혀 둔 캘린더의 마지막 날. 없으면 ``None``."""
    import duckdb

    from collector.us.store.writer import latest_snapshot

    path = latest_snapshot(root, "trading_calendar")
    if path is None:
        return None
    return duckdb.connect().execute(
        f"SELECT max(date) FROM read_parquet('{path}')"
    ).fetchone()[0]


def _age_days(path: Path, today: dt.date) -> float:
    return (today - dt.date.fromtimestamp(path.stat().st_mtime)).days


# --- 원천별 한 걸음 ----------------------------------------------------------


def run_dolt(root: DataRoot, *, dry_run: bool = False) -> SourceRun:
    """레포 셋을 당긴다. 커밋이 안 바뀌어도 실패가 아니다 — 쉬는 날이 있다."""
    from collector.us.sources import dolt

    run = SourceRun("dolt")
    changed = []
    for repo in dolt.REPOS:
        if not (dolt.repo_dir(root, repo) / ".dolt").is_dir():
            run.missing.append(repo)
            run.ok = False
            continue
        if dry_run:
            run.pending += 1
            continue
        try:
            result = dolt.pull(root, repo)
        except dolt.DoltError as exc:
            run.missing.append(f"{repo}: {exc}")
            run.ok = False
            continue
        run.fetched += 1
        if result.get("changed"):
            changed.append(repo)
    run.note = "바뀐 레포: " + (", ".join(changed) if changed else "없음")
    return run


def run_nasdaq_earnings(
    root: DataRoot, sessions: list, *, budget: _Budget, dry_run: bool = False
) -> SourceRun:
    from collector.us.sources import nasdaq

    run = SourceRun("nasdaq_earnings")
    # **원천마다 시작일이 다르다.** 캘린더는 2011년부터인데 실적 캘린더는
    # 검정 구간(2018-07)부터다 — 안 자르면 못 쓰는 1,886일을 받으러 간다
    candidates = [d for d in sessions if d >= nasdaq.EARNINGS_START]
    todo = [d for d in candidates if not nasdaq.earnings_path(root, d).is_file()]
    run.skipped = len(candidates) - len(todo)
    run.pending = len(todo)
    if dry_run or not todo:
        return run
    client = nasdaq.NasdaqClient()
    for day in todo:
        if budget.spent():
            break
        try:
            nasdaq.fetch_earnings(client, root, day, skip_existing=True)
        except nasdaq.NasdaqError as exc:
            run.missing.append(f"{day}: {exc}")
            run.ok = False
            break
        run.fetched += 1
    run.pending = len(todo) - run.fetched
    return run


def run_finra_regsho(
    root: DataRoot, sessions: list, *, budget: _Budget, dry_run: bool = False
) -> SourceRun:
    from collector.us.sources import finra

    run = SourceRun("finra_regsho")
    candidates = [d for d in sessions if d >= finra.REGSHO_START]
    todo = [d for d in candidates if not finra.regsho_path(root, d).is_file()]
    run.skipped = len(candidates) - len(todo)
    run.pending = len(todo)
    if dry_run or not todo:
        return run
    client = finra.FinraClient()
    for day in todo:
        if budget.spent():
            break
        result = finra.fetch_regsho(client, root, day)
        if result["missing"]:
            # 파일이 아직 안 올라온 날이 있다. 실패가 아니라 다음 실행이 다시 본다
            run.missing.append(str(day))
        else:
            run.fetched += 1
    run.pending = len(todo) - run.fetched - len(run.missing)
    return run


def run_finra_short_interest(
    root: DataRoot, sessions: list, *, until: dt.date, budget: _Budget, dry_run: bool = False
) -> SourceRun:
    from collector.us.sources import finra

    run = SourceRun("finra_short_interest")
    dates = finra.settlement_dates(
        sessions, start=finra.SHORT_INTEREST_START, end=until
    )
    todo = [d for d in dates if not finra.short_interest_path(root, d).is_file()]
    run.skipped = len(dates) - len(todo)
    run.pending = len(todo)
    if dry_run or not todo:
        return run
    client = finra.FinraClient()
    for day in todo:
        if budget.spent():
            break
        result = finra.fetch_short_interest(client, root, day)
        if result["rows"] == 0:
            # 발표 전이다. 204로 온다 — 실패가 아니고 다음 실행이 다시 본다
            run.missing.append(str(day))
        else:
            run.fetched += 1
    run.pending = len(todo) - run.fetched - len(run.missing)
    run.note = "발표 전인 결제일: " + (", ".join(run.missing) if run.missing else "없음")
    return run


def run_sec_bulk(root: DataRoot, *, today: dt.date, dry_run: bool = False) -> SourceRun:
    """주 1회. **3GB라 매일 받지 않는다** (05 §4)."""
    from collector.us.sources import sec

    run = SourceRun("sec_bulk")
    client = None
    for kind in sec.BULK_URLS:
        dest = sec.bulk_path(root, kind)
        if dest.is_file() and _age_days(dest, today) < WEEKLY_MAX_AGE_DAYS:
            run.skipped += 1
            continue
        run.pending += 1
        if dry_run:
            continue
        if client is None:
            client = sec.SecClient(user_agent=sec.user_agent_from_env())
        try:
            sec.download_bulk(client, root, kind, skip_existing=False)
        except sec.SecAccessError as exc:
            run.missing.append(f"{kind}: {exc}")
            run.ok = False
            continue
        run.fetched += 1
    run.pending = max(0, run.pending - run.fetched)
    return run


def _quarter_end(year: int, quarter: int) -> dt.date:
    month = quarter * 3
    return (dt.date(year + month // 12, month % 12 + 1, 1) - dt.timedelta(days=1))


def run_sec_quarterly(root: DataRoot, *, today: dt.date, dry_run: bool = False) -> SourceRun:
    """분기 ZIP 셋. **마감된 분기만 본다** — 진행 중인 분기는 아직 안 나온다.

    **마감 직후 분기는 404가 정상이다.** SEC가 분기가 끝난 뒤 며칠~몇 달 걸려
    올리기 때문이다. 갈래별 유예(:data:`QUARTERLY_PUBLISH_GRACE_DAYS`) 안의 404는
    ``note``에만 적고 ``missing``·``ok``에는 안 센다 — 실패가 아니고 다음 실행이
    다시 본다. 유예를 넘긴 404와 404가 아닌 실패는 그대로 실패다.
    """
    from collector.us.sources import sec

    run = SourceRun("sec_quarterly")
    unpublished: list[str] = []
    last_closed = _last_closed_quarter(today)
    quarters = sec.quarters((2018, 3), last_closed)
    client = None
    for kind in sec.QUARTERLY_KINDS:
        for year, quarter in quarters:
            if sec.quarterly_path(root, kind, year, quarter).is_file():
                run.skipped += 1
                continue
            run.pending += 1
            if dry_run:
                continue
            if client is None:
                client = sec.SecClient(user_agent=sec.user_agent_from_env())
            try:
                sec.download_quarterly(client, root, kind, year, quarter)
            except sec.SecAccessError as exc:
                age = (today - _quarter_end(year, quarter)).days
                grace = QUARTERLY_PUBLISH_GRACE_DAYS.get(kind, 0)
                if exc.status_code == 404 and age <= grace:
                    # 발표 전이다 — 실패가 아니고 다음 실행이 다시 본다
                    unpublished.append(f"{kind} {year}q{quarter} (마감 {age}일, 유예 {grace}일)")
                    continue
                # 유예를 넘긴 404는 경로가 바뀌었을 수 있다 — 목록 페이지를 본다
                run.missing.append(f"{kind} {year}q{quarter}: {exc}")
                continue
            run.fetched += 1
    # 발표 전인 것은 ``pending``에서 뺀다 — ``pending``은 "예산이 모자라 남았다"는
    # 신호라, 몇 달씩 못 받는 MIDAS가 영영 0이 안 되면 그 뜻이 흐려진다
    run.pending = max(0, run.pending - run.fetched - len(unpublished))
    run.ok = not run.missing
    if unpublished:
        run.note = "발표 전인 분기: " + ", ".join(unpublished)
    return run


def _last_closed_quarter(today: dt.date) -> tuple[int, int]:
    q = (today.month - 1) // 3 + 1
    return (today.year - 1, 4) if q == 1 else (today.year, q - 1)


def run_sec_ftd(root: DataRoot, *, budget: _Budget, dry_run: bool = False) -> SourceRun:
    """SEC 반월 Fails-to-Deliver. **목록 페이지를 매번 다시 읽는다** — 분기 ZIP과
    달리 URL을 규칙으로 못 만든다(``sec_ftd.py`` 모듈 docstring, 경로가 네 가지).
    새 반월이 올라왔는지는 목록 자체를 봐야 안다. 목록 페이지 요청 하나는
    매일 더해도 무시할 만하다 — 새 원천 요청은 반월당 하나뿐이다
    (연구 01_sec_ftd.md §8, 유지 비용 월 2요청).
    """
    from collector.us.sources import sec, sec_ftd

    run = SourceRun("sec_ftd")
    client = sec.SecClient(user_agent=sec.user_agent_from_env())
    try:
        listing = sec_ftd.list_ftd_files(client)
    except (sec.SecAccessError, sec_ftd.FtdError) as exc:
        run.missing.append(f"목록 페이지: {exc}")
        run.ok = False
        return run

    have = sec_ftd.raw_periods(root)
    todo = sorted(p for p in listing.files if p not in have)
    run.skipped = len(listing.files) - len(todo)
    run.pending = len(todo)
    if listing.duplicates:
        run.note = f"같은 반월에 링크가 둘 이상: {sorted(listing.duplicates)}"
    if dry_run or not todo:
        return run

    for period in todo:
        if budget.spent():
            break
        try:
            sec_ftd.download_ftd(client, root, period, listing.files[period])
        except sec.SecAccessError as exc:
            run.missing.append(f"{period}: {exc}")
            continue
        run.fetched += 1
    run.pending = len(todo) - run.fetched
    run.ok = not run.missing
    return run


def run_sec_13f(root: DataRoot, *, budget: _Budget, dry_run: bool = False) -> SourceRun:
    """SEC Form 13F(기관 보유). **목록 페이지를 매번 다시 읽는다** — ``sec_ftd``와
    같은 이유다(``sec_13f.py`` 모듈 docstring). 새 기간이 올라왔는지는 목록
    자체를 봐야 안다. 유지 비용은 연구 02 §8 기준 분기 1요청이다.
    """
    from collector.us.sources import sec, sec_13f

    run = SourceRun("sec_13f")
    client = sec.SecClient(user_agent=sec.user_agent_from_env())
    try:
        listing = sec_13f.list_13f_files(client)
    except (sec.SecAccessError, sec_13f.ThirteenFError) as exc:
        run.missing.append(f"목록 페이지: {exc}")
        run.ok = False
        return run

    have = sec_13f.raw_periods(root)
    todo = sorted(p for p in listing.files if p not in have)
    run.skipped = len(listing.files) - len(todo)
    run.pending = len(todo)
    if listing.duplicates:
        run.note = f"같은 기간에 링크가 둘 이상: {sorted(listing.duplicates)}"
    if dry_run or not todo:
        return run

    for period in todo:
        if budget.spent():
            break
        try:
            sec_13f.download_13f(client, root, period, listing.files[period])
        except sec.SecAccessError as exc:
            run.missing.append(f"{period}: {exc}")
            continue
        run.fetched += 1
    run.pending = len(todo) - run.fetched
    run.ok = not run.missing
    return run


def run_weekly_macro(
    root: DataRoot,
    *,
    today: dt.date,
    snapshot_date: dt.date | str,
    dry_run: bool = False,
    force: bool = False,
) -> SourceRun:
    """FRED·Wikipedia. 주 1회다 (05 §4). **FRED vintage는 매번 새로 받는다.**

    ``force``는 나이 문턱을 건너뛰고 두 원천을 다 받는다. 새 시리즈를 바로 채울 때 쓴다.
    """
    from collector.us.store.writer import latest_snapshot

    run = SourceRun("weekly_macro")
    # **있는 것 중 가장 최근 스냅샷의 나이**로 판단한다. 오늘 날짜 경로로
    # 찾으면 매일 "없음"이 되어 주 1회가 매일 1회가 된다
    stale = [
        table
        for table in ("macro_series", "index_constituents")
        if force
        or (path := latest_snapshot(root, table)) is None
        or _age_days(path, today) >= WEEKLY_MAX_AGE_DAYS
    ]
    run.skipped = 2 - len(stale)
    run.pending = len(stale)
    if dry_run or not stale:
        return run

    from collector.us.sources import fred, sec
    from collector.us.sources import wikipedia as wp

    if "macro_series" in stale:
        fred.load_macro_series(
            root, fred.FredClient(api_key=fred.api_key_from_env()), snapshot_date=snapshot_date
        )
        run.fetched += 1
    if "index_constituents" in stale:
        # 연락처 UA를 하나만 둔다 (`.env` 키를 늘리면 X21이 다시 난다).
        # `user_agent_from_env`를 쓰는 이유는 **빈 문자열도 잡기 위해서**다 —
        # compose가 `${SEC_USER_AGENT:-}`로 넘기면 키가 없어도 빈 값이 온다
        wp.load_index_constituents(
            root,
            wp.WikipediaClient(user_agent=sec.user_agent_from_env()),
            snapshot_date=snapshot_date,
        )
        run.fetched += 1
    run.pending = 0
    return run


def run_nasdaqtrader_symdir(
    root: DataRoot, *, force: bool = False, dry_run: bool = False
) -> SourceRun:
    """nasdaqtrader 심볼 디렉터리 원문. **하루 한 번**(KST), 원문만 쌓는다.

    ``listing_snapshots``는 건드리지 않는다 (1단계). 같은 파일이 반복되는 날은
    ``skipped``로 센다. 한 kind가 실패해도 다른 kind는 계속한다.
    """
    from collector.us.sources import nasdaqtrader_symdir as sd

    run = SourceRun("nasdaqtrader_symdir")
    now = dt.datetime.now(dt.UTC)
    todo = [k for k in sd.KINDS if force or not sd.fetched_today(root, k, now=now)]
    run.skipped = len(sd.KINDS) - len(todo)
    run.pending = len(todo)
    if dry_run or not todo:
        return run

    client = sd.SymdirClient()
    notes = []
    for kind in todo:
        try:
            result = sd.fetch_kind(client, root, kind, now=now, force=force)
        except (sd.SymdirError, OSError) as exc:
            run.missing.append(f"{kind}: {exc}")
            run.ok = False
            continue
        if result["status"] == "written":
            run.fetched += 1
            notes.append(f"{kind} 새 파일({result['as_of']})")
        else:
            run.skipped += 1
            notes.append(f"{kind} 같은 파일")
    run.pending = len(run.missing)
    run.note = "; ".join(notes)
    return run


#: 하루 실행이 도는 원천. 이름으로 골라 돌릴 수 있다.
SOURCES: tuple[str, ...] = (
    "dolt",
    "nasdaq_earnings",
    "finra_regsho",
    "finra_short_interest",
    "sec_bulk",
    "sec_quarterly",
    "sec_ftd",
    "sec_13f",
    "weekly_macro",
    "nasdaqtrader_symdir",
)


def run_daily(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    today: dt.date | None = None,
    budget_seconds: float | None = DEFAULT_BUDGET_SECONDS,
    sources: tuple[str, ...] | None = None,
    dry_run: bool = False,
    force_weekly: bool = False,
    force_symdir: bool = False,
) -> dict[str, object]:
    """하루치를 받는다. **어제까지가 대상이다** — 오늘 것은 아직 안 나온다.

    돌려주는 것에 ``ok``와 ``pending``이 있다. ``pending``이 0이 아니면 예산이
    모자랐다는 뜻이고 **다음 실행이 이어서 한다.**
    """
    unknown = [n for n in (sources or ()) if n not in SOURCES]
    if unknown:
        raise ValueError(f"모르는 원천: {unknown[0]!r} (있는 것: {sorted(SOURCES)})")
    today = today or dt.date.today()
    until = today - dt.timedelta(days=1)
    wanted = sources or SOURCES
    budget = _Budget(budget_seconds)
    sessions = sessions_through(root, until=until)

    def _run(name: str) -> SourceRun:
        if name == "dolt":
            return run_dolt(root, dry_run=dry_run)
        if name == "nasdaq_earnings":
            return run_nasdaq_earnings(root, sessions, budget=budget, dry_run=dry_run)
        if name == "finra_regsho":
            return run_finra_regsho(root, sessions, budget=budget, dry_run=dry_run)
        if name == "finra_short_interest":
            return run_finra_short_interest(
                root, sessions, until=until, budget=budget, dry_run=dry_run
            )
        if name == "sec_bulk":
            return run_sec_bulk(root, today=today, dry_run=dry_run)
        if name == "sec_quarterly":
            return run_sec_quarterly(root, today=today, dry_run=dry_run)
        if name == "sec_ftd":
            return run_sec_ftd(root, budget=budget, dry_run=dry_run)
        if name == "sec_13f":
            return run_sec_13f(root, budget=budget, dry_run=dry_run)
        if name == "weekly_macro":
            return run_weekly_macro(
                root,
                today=today,
                snapshot_date=snapshot_date,
                dry_run=dry_run,
                force=force_weekly,
            )
        if name == "nasdaqtrader_symdir":
            return run_nasdaqtrader_symdir(root, force=force_symdir, dry_run=dry_run)
        raise ValueError(f"모르는 원천: {name!r} (있는 것: {sorted(SOURCES)})")

    # **원천 하나가 죽어도 다음 원천은 돈다.** 예외는 그 원천의 실패로 남기고
    # 종료 코드는 ``ok``가 정하니 실패가 묻히지 않는다. 원인은 stderr에 남긴다.
    runs: list[SourceRun] = []
    for name in wanted:
        try:
            runs.append(_run(name))
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            runs.append(
                SourceRun(
                    name=name,
                    ok=False,
                    missing=[f"{type(exc).__name__}: {exc}"],
                    note="예외로 중단",
                )
            )

    # **캘린더가 언제 끝나는지 매번 남긴다.** 끝나고 나서 알면 늦다.
    cal_end = calendar_end(root)
    days_left = (cal_end - today).days if cal_end else None
    return {
        "until": until,
        "sessions": len(sessions),
        "budget_spent": budget.spent(),
        "ok": all(r.ok for r in runs),
        "pending": sum(r.pending for r in runs),
        "calendar_end": str(cal_end) if cal_end else None,
        "calendar_days_left": days_left,
        "calendar_warning": (
            f"거래일 캘린더가 {days_left}일 뒤 끝난다 — collector us-calendar build"
            if days_left is not None and days_left <= CALENDAR_WARN_DAYS
            else None
        ),
        "sources": [r.as_dict() for r in runs],
    }

"""``us-`` 서브커맨드 (02 §2.2 · D10).

**접두어는 미국에만 붙는다.** 한국은 접두어 없이 들어와 있고
``prices``·``universe``·``validate``가 이름이 겹친다. 한국 쪽을 건드리면
prod 래퍼 35개와 Cronicle 이벤트를 같이 고쳐야 한다.

**import 시점에 아무것도 하지 않는다** (08 L5). ``DataRoot.resolve()``는 핸들러
안에서 부른다 — 모듈 최상위에서 부르면 환경변수가 없는 한국 prod 컨테이너가
기동에서 죽는다.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json


def _root(args: argparse.Namespace):
    from collector.lake import DataRoot

    if getattr(args, "lake_root", None):
        from pathlib import Path

        return DataRoot(Path(args.lake_root))
    return DataRoot.resolve(market="us")


def _snapshot_date(args: argparse.Namespace) -> str:
    return args.snapshot_date or dt.date.today().isoformat()


def _print(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _handle_daily_run(args: argparse.Namespace) -> None:
    from collector.us.ops import daily

    sources = tuple(s.strip() for s in args.sources.split(",")) if args.sources else None
    result = daily.run_daily(
        _root(args),
        snapshot_date=_snapshot_date(args),
        today=dt.date.fromisoformat(args.today) if args.today else None,
        budget_seconds=args.budget_seconds,
        sources=sources,
        dry_run=args.dry_run,
        force_weekly=args.force_weekly,
        force_symdir=args.force_symdir,
    )
    _print(result)
    if not result["ok"]:
        raise SystemExit(1)


def _handle_calendar_build(args: argparse.Namespace) -> None:
    from collector.us import calendars

    _print(
        calendars.load_trading_calendar(
            _root(args),
            snapshot_date=_snapshot_date(args),
            start=args.start,
            end=args.end,
            exchange=args.exchange,
        )
    )


def _loaders() -> dict[str, str]:
    """``us-load``가 아는 표.

    **정본은 :mod:`collector.us.ops.derive` 의 ``RECIPES``** 다 — 굳히는 법과
    "언제 다시 굳히나"를 한자리에 둔다. 여기서 또 적으면 둘이 벌어진다.

    전부 **네트워크를 안 탄다** — ``raw/``에 받아 둔 것만 읽는다.
    ``corp-actions``만 ``derived/splits/``의 보충 분할표도 읽는데 그것도 이미
    레이크에 있는 파일이다 (03 §2.1).
    """
    from collector.us.ops.derive import RECIPES

    return {name: recipe.loader for name, recipe in RECIPES.items()}


def _handle_load(args: argparse.Namespace) -> None:
    import importlib

    module_name, _, func_name = _loaders()[args.table].partition(":")
    func = getattr(importlib.import_module(module_name), func_name)
    _print(func(_root(args), snapshot_date=_snapshot_date(args)))


def _handle_tickers_sync(args: argparse.Namespace) -> None:
    from collector.us.sources import sec, wayback

    client = wayback.WaybackClient(
        user_agent=sec.user_agent_from_env(),
        interval_seconds=args.interval_seconds or wayback.WAYBACK_INTERVAL_SECONDS,
    )
    stamps = wayback.cdx_timestamps(client)
    if args.since:
        stamps = [t for t in stamps if t >= args.since]
    if args.limit:
        stamps = stamps[: args.limit]
    if args.dry_run:
        _print({"available": len(stamps), "first": stamps[:1], "last": stamps[-1:]})
        return
    root = _root(args)
    result = wayback.download_company_tickers(client, root, timestamps=stamps)
    # **아카이브만으로는 앞으로가 빈다.** 오늘 SEC 가 뭐라고 하는지를 같이
    # 굳혀 두면 우리 스스로 PIT 계열을 쌓게 된다.
    if not args.no_live:
        result["live"] = wayback.fetch_live_company_tickers(
            root, user_agent=sec.user_agent_from_env()
        )
    _print(result)
    if result["failed"]:
        raise SystemExit(1)


def _handle_derive_run(args: argparse.Namespace) -> None:
    from collector.us.ops import derive

    tables = tuple(t.strip() for t in args.tables.split(",")) if args.tables else None
    result = derive.run_derive(
        _root(args),
        snapshot_date=_snapshot_date(args),
        tables=tables,
        force=args.force,
        budget_seconds=args.budget_seconds,
        dry_run=args.dry_run,
    )
    _print(result)
    if not result["ok"]:
        raise SystemExit(1)


def _handle_universe_rebuild(args: argparse.Namespace) -> None:
    from collector.us.universe import build
    from collector.us.universe.v2 import ops as v2_ops

    root = _root(args)
    snapshot_date = _snapshot_date(args)
    kwargs = {"start": args.start}
    if args.end:
        kwargs["end"] = args.end
    result = build.build_universe_daily(root, snapshot_date=snapshot_date, **kwargs)
    # v1 은 dolt symbol 현재값을 쓴다. 그 커밋을 빌드마다 따로 남긴다 (v1 빌더는 안 바꾼다).
    v2_ops.record_v1_dolt_commit(
        root, snapshot_date=snapshot_date, kind="rebuild", v1_result=result
    )
    _print(result)


def _handle_universe_incremental(args: argparse.Namespace) -> None:
    from collector.us.universe import build
    from collector.us.universe.v2 import ops as v2_ops

    root = _root(args)
    snapshot_date = _snapshot_date(args)
    result = build.build_universe_incremental(
        root, snapshot_date=snapshot_date, if_new=args.if_new, dry_run=args.dry_run
    )
    built = not (args.dry_run or result.get("skipped"))
    if built:
        v2_ops.record_v1_dolt_commit(
            root, snapshot_date=snapshot_date, kind="incremental", v1_result=result
        )
    payload = dict(result)
    # v2 는 v1 이 끝난 **뒤에** 잇는다. v2 가 실패해도 v1 결과와 종료 코드는 그대로다.
    if args.v2 and not args.dry_run:
        payload["v2"] = v2_ops.run_v2_after_v1(root, snapshot_date=snapshot_date)
    _print(payload)


def _handle_universe_v2(args: argparse.Namespace) -> None:
    import dataclasses
    from pathlib import Path

    from collector.us.universe.v2 import build as v2_build
    from collector.us.universe.v2.config import DEFAULT_RULES

    # 플래그가 없으면 rebuild 는 기본 규칙, incremental 은 직전 빌드의 규칙을 따른다.
    rules = None
    if args.spac_release_name_change or args.dorm_known_by_corroboration:
        rules = dataclasses.replace(
            DEFAULT_RULES,
            spac_release_name_change=args.spac_release_name_change,
            dorm_known_at_resume=not args.dorm_known_by_corroboration,
        )
    kwargs: dict[str, object] = {}
    if args.mode == "rebuild":
        if args.start:
            kwargs["start"] = args.start
    else:
        kwargs["if_new"] = args.if_new
    if args.end:
        kwargs["end"] = args.end
    if args.ticker_map_parquet:
        kwargs["ticker_map_parquet"] = Path(args.ticker_map_parquet)
    _print(
        v2_build.build_universe_v2(
            _root(args),
            snapshot_date=_snapshot_date(args),
            mode=args.mode,
            dry_run=args.dry_run,
            rules=rules,
            **kwargs,
        )
    )


def _handle_nasdaq_analyst_run(args: argparse.Namespace) -> None:
    from collector.us.ops import nasdaq_analyst

    symbols = (
        [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    )
    result = nasdaq_analyst.run_weekly(
        _root(args),
        today=dt.date.fromisoformat(args.today) if args.today else None,
        symbols=symbols,
        budget_seconds=args.budget_seconds,
        interval_seconds=args.interval_seconds,
        max_retries=args.max_retries,
        dry_run=args.dry_run,
    )
    _print(result)
    if not result["ok"]:
        raise SystemExit(1)


def _handle_prune(args: argparse.Namespace) -> None:
    from collector.us.ops import retention
    from collector.us.store.schema import ARROW_SCHEMAS

    tables = [args.table] if args.table else sorted(ARROW_SCHEMAS)
    _print(
        {
            "apply": args.apply,
            "tables": [
                retention.prune_unchanged(_root(args), table, dry_run=not args.apply)
                for table in tables
            ],
        }
    )


def register(subparsers: argparse._SubParsersAction) -> None:
    """최상위 파서에 ``us-*`` 서브커맨드를 단다."""

    def _common(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        parser.add_argument(
            "--snapshot-date",
            default=None,
            help="스냅샷 파티션 키 (기본: 오늘).",
        )
        parser.add_argument(
            "--lake-root",
            default=None,
            help="변형 lake 경로. 기본은 $STOCK_DATA_ROOT/us 다 (D9).",
        )
        return parser

    daily_parser = subparsers.add_parser(
        "us-daily", help="미국 상시 운영 — 하루치를 받는다 (04 C8)."
    )
    daily_sub = daily_parser.add_subparsers(dest="us_daily_command", required=True)
    daily_run = _common(
        daily_sub.add_parser("run", help="마지막으로 받은 것부터 어제까지 받는다.")
    )
    daily_run.add_argument("--today", default=None, help="오늘 날짜를 고정한다 (시험용).")
    daily_run.add_argument(
        "--budget-seconds",
        type=float,
        default=None,
        help="이 시간이 지나면 멈춘다. 남은 것은 다음 실행이 한다.",
    )
    daily_run.add_argument("--sources", default=None, help="쉼표로 고른다.")
    daily_run.add_argument("--dry-run", action="store_true", help="할 일만 센다.")
    daily_run.add_argument(
        "--force-weekly",
        action="store_true",
        help="주간 원천(FRED·Wikipedia)의 7일 문턱을 건너뛰고 지금 받는다.",
    )
    daily_run.add_argument(
        "--force-symdir",
        action="store_true",
        help="nasdaqtrader 심볼 디렉터리의 하루 한 번 제한을 건너뛰고 지금 받는다.",
    )
    daily_run.set_defaults(handler=_handle_daily_run)

    cal_parser = subparsers.add_parser("us-calendar", help="거래일 캘린더 (04 C5).")
    cal_sub = cal_parser.add_subparsers(dest="us_calendar_command", required=True)
    cal_build = _common(cal_sub.add_parser("build", help="exchange_calendars 를 굳힌다."))
    cal_build.add_argument("--start", default="2011-01-01")
    cal_build.add_argument(
        "--end",
        default=None,
        help="기본은 오늘 + HORIZON_YEARS 년 말. **고정 날짜를 두지 않는다** — "
        "그 날짜가 지나면 수집이 조용히 멈춘다.",
    )
    cal_build.add_argument("--exchange", default="XNYS")
    cal_build.set_defaults(handler=_handle_calendar_build)

    load_parser = _common(
        subparsers.add_parser("us-load", help="raw/ 에 받아 둔 것을 스냅샷으로 굳힌다.")
    )
    load_parser.add_argument("table", choices=sorted(_loaders()))
    load_parser.set_defaults(handler=_handle_load)

    tick_parser = subparsers.add_parser(
        "us-tickers", help="과거 티커→CIK 맵 (Wayback). `universe`의 PIT join 재료."
    )
    tick_sub = tick_parser.add_subparsers(dest="us_tickers_command", required=True)
    tick_sync = _common(
        tick_sub.add_parser(
            "sync",
            help="Wayback 의 company_tickers.json 스냅샷을 받는다. "
            "**있는 것은 다시 안 받는다.**",
        )
    )
    tick_sync.add_argument(
        "--since", default=None, help="이 timestamp(YYYYMMDDhhmmss) 이후만."
    )
    tick_sync.add_argument("--limit", type=int, default=None, help="앞에서 N개만.")
    tick_sync.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help=f"요청 간격 (기본 {1.0}s). Wayback 은 공표된 한도가 없다.",
    )
    tick_sync.add_argument(
        "--no-live",
        action="store_true",
        help="SEC 의 지금 맵을 as_of=오늘 로 굳히는 것을 건너뛴다.",
    )
    tick_sync.add_argument("--dry-run", action="store_true", help="몇 개인지만 본다.")
    tick_sync.set_defaults(handler=_handle_tickers_sync)

    derive_parser = subparsers.add_parser(
        "us-derive", help="raw/ 를 derived/ 스냅샷으로 굳힌다 — 바뀐 것만 (05 §4.1)."
    )
    derive_sub = derive_parser.add_subparsers(dest="us_derive_command", required=True)
    derive_run = _common(
        derive_sub.add_parser(
            "run",
            help="입력이 바뀐 표만 다시 굳힌다. **유니버스는 여기 없다** — "
            "월 1회라 us-universe rebuild 가 따로 한다.",
        )
    )
    derive_run.add_argument(
        "--tables", default=None, help="쉼표로 고른다. 기본은 전부 본다."
    )
    derive_run.add_argument(
        "--force", action="store_true", help="안 바뀌었어도 다시 굳힌다."
    )
    derive_run.add_argument(
        "--budget-seconds",
        type=float,
        default=None,
        help="이 시간이 지나면 멈춘다. 남은 것은 다음 실행이 한다.",
    )
    derive_run.add_argument(
        "--dry-run", action="store_true", help="무엇을 굳힐지만 본다."
    )
    derive_run.set_defaults(handler=_handle_derive_run)

    uni_parser = subparsers.add_parser("us-universe", help="PIT 유니버스 (04 C4).")
    uni_sub = uni_parser.add_subparsers(dest="us_universe_command", required=True)
    uni_build = _common(
        uni_sub.add_parser(
            "rebuild",
            help="유니버스를 다시 판정한다. **월 1회다** — 매일 하면 하루짜리 "
            "거래량 급증에 흔들린다 (03 §5.3).",
        )
    )
    uni_build.add_argument("--start", default="2018-09-07")
    uni_build.add_argument("--end", default=None)
    uni_build.set_defaults(handler=_handle_universe_rebuild)
    uni_extend = _common(
        uni_sub.add_parser(
            "incremental",
            help="이전 완료 snapshot과 월 membership을 seed로 새 US 세션만 확장한다.",
        )
    )
    uni_extend.add_argument(
        "--if-new",
        action="store_true",
        help="새 XNYS 세션이 없거나 같은 snapshot_date 파티션이 있으면 skipped로 끝낸다.",
    )
    uni_extend.add_argument(
        "--dry-run",
        action="store_true",
        help="검사만 하고 snapshot을 쓰지 않는다.",
    )
    uni_extend.add_argument(
        "--v2",
        action="store_true",
        help="v1 이 끝난 뒤 universe_daily_v2 증분을 잇는다. v2 가 실패해도 v1 결과와 "
        "종료 코드는 그대로다 (실패는 출력의 v2 항목과 output/universe_v2/runs.jsonl 에 남는다).",
    )
    uni_extend.set_defaults(handler=_handle_universe_incremental)

    v2_parser = subparsers.add_parser(
        "us-universe-v2",
        help="유니버스 v2 — 종목 구간·증권 마스터·universe_daily_v2 (설계 20261006_universe_v2).",
    )
    v2_sub = v2_parser.add_subparsers(dest="us_universe_v2_command", required=True)
    for mode, help_text in (
        ("rebuild", "처음부터 다시 판정한다. 첫 한 번은 이걸로 만든다."),
        ("incremental", "직전 완료 snapshot 뒤의 새 세션만 잇는다. 이미 굳은 달은 안 바꾼다."),
    ):
        v2_cmd = _common(v2_sub.add_parser(mode, help=help_text))
        v2_cmd.add_argument("--end", default=None, help="기본은 prices_daily 마지막 날.")
        v2_cmd.add_argument("--dry-run", action="store_true", help="검사만 하고 쓰지 않는다.")
        v2_cmd.add_argument(
            "--ticker-map-parquet",
            default=None,
            help="(symbol, cik, as_of) parquet. Wayback company_tickers 스냅샷이 없는 레이크에서 "
            "검증 빌드를 돌릴 때만 쓴다.",
        )
        v2_cmd.add_argument(
            "--spac-release-name-change",
            action="store_true",
            help="설계 7장 보조 규칙(5.06 없는 합병). 측정 전이라 기본은 끈다.",
        )
        v2_cmd.add_argument(
            "--dorm-known-by-corroboration",
            action="store_true",
            help="G_dorm 을 설계 그대로 보강 신호가 알려진 때 인지한다. 기본은 재개일(T7 일치).",
        )
        if mode == "rebuild":
            v2_cmd.add_argument("--start", default=None, help="기본은 2018-09-07.")
        else:
            v2_cmd.add_argument(
                "--if-new",
                action="store_true",
                help="새 XNYS 세션이 없거나 같은 snapshot_date 파티션이 있으면 skipped 로 끝낸다.",
            )
        v2_cmd.set_defaults(handler=_handle_universe_v2, mode=mode)

    prune_parser = _common(
        subparsers.add_parser(
            "us-prune", help="내용이 안 바뀐 스냅샷을 지운다 (04 C8 · 05 §5.1)."
        )
    )
    prune_parser.add_argument("--table", default=None, help="한 표만. 기본은 전부.")
    prune_parser.add_argument(
        "--apply", action="store_true", help="실제로 지운다. 기본은 세기만 한다."
    )
    prune_parser.set_defaults(handler=_handle_prune)

    nasdaq_parser = subparsers.add_parser(
        "us-nasdaq-analyst",
        help="Nasdaq 애널리스트 추정치 — 주 1회 전진 축적 전용 (source_expansion 04).",
    )
    nasdaq_sub = nasdaq_parser.add_subparsers(dest="us_nasdaq_analyst_command", required=True)
    nasdaq_run = nasdaq_sub.add_parser(
        "run",
        help="이번 주 유니버스 중 아직 못 받은 심볼을 받는다. 같은 주에 다시 돌려도 안전하다.",
    )
    nasdaq_run.add_argument(
        "--lake-root",
        default=None,
        help="변형 lake 경로. 기본은 $STOCK_DATA_ROOT/us 다 (D9).",
    )
    nasdaq_run.add_argument(
        "--today",
        default=None,
        help="오늘 날짜를 고정한다 (시험용). 이 날짜가 속한 ISO 주가 대상이다.",
    )
    nasdaq_run.add_argument(
        "--symbols",
        default=None,
        help="쉼표로 심볼을 직접 준다. 기본은 universe_daily 최신 멤버 — "
        "스모크 테스트나 특정 종목 재수집에 쓴다.",
    )
    nasdaq_run.add_argument(
        "--budget-seconds",
        type=float,
        default=None,
        help="이 시간이 지나면 멈춘다. 남은 것은 다음 실행이 한다.",
    )
    nasdaq_run.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="심볼 하나가 실패할 때(타임아웃 등) 다시 시도하는 횟수.",
    )
    nasdaq_run.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="요청 간격 (기본 5초 — 04 §4의 비용 어림).",
    )
    nasdaq_run.add_argument("--dry-run", action="store_true", help="할 일만 센다.")
    nasdaq_run.set_defaults(handler=_handle_nasdaq_analyst_run)

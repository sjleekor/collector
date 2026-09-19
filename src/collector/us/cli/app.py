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


#: ``us-load``가 아는 표. 전부 **``raw/``만 읽는다** — 네트워크를 안 탄다.
_LOADERS: dict[str, str] = {
    "short-interest": "collector.us.sources.finra:load_short_interest",
    "short-volume": "collector.us.sources.finra:load_short_volume",
    "earnings-calendar": "collector.us.sources.nasdaq:load_earnings_calendar",
    "insider": "collector.us.sources.sec:extract_insider",
    "filings-sub": "collector.us.sources.sec:extract_filings_sub",
    "midas": "collector.us.sources.sec:extract_midas",
    "fundamentals": "collector.us.sources.sec_bulk:load_companyfacts",
    "submissions": "collector.us.sources.sec_bulk:load_submissions",
}


def _handle_load(args: argparse.Namespace) -> None:
    import importlib

    module_name, _, func_name = _LOADERS[args.table].partition(":")
    func = getattr(importlib.import_module(module_name), func_name)
    _print(func(_root(args), snapshot_date=_snapshot_date(args)))


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
    daily_run.set_defaults(handler=_handle_daily_run)

    cal_parser = subparsers.add_parser("us-calendar", help="거래일 캘린더 (04 C5).")
    cal_sub = cal_parser.add_subparsers(dest="us_calendar_command", required=True)
    cal_build = _common(cal_sub.add_parser("build", help="exchange_calendars 를 굳힌다."))
    cal_build.add_argument("--start", default="2011-01-01")
    cal_build.add_argument("--end", default="2026-12-31")
    cal_build.add_argument("--exchange", default="XNYS")
    cal_build.set_defaults(handler=_handle_calendar_build)

    load_parser = _common(
        subparsers.add_parser("us-load", help="raw/ 에 받아 둔 것을 스냅샷으로 굳힌다.")
    )
    load_parser.add_argument("table", choices=sorted(_LOADERS))
    load_parser.set_defaults(handler=_handle_load)

"""CLI 진입점.

서브명령 등록은 ``collector.kr.cli.app``의 파서를 그대로 쓴다 (시장 구분이
하나뿐이라 ``collector kr universe sync`` 같은 시장 세그먼트를 두지 않는다).
여기서는 ``--version``만 더하고 실행 흐름(설정 로드, 로깅, 핸들러 디스패치)은
kr 쪽과 동일하게 맞춘다.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import logging
import sys

from collector.kr.cli.app import build_parser as _build_kr_parser
from collector.kr.infra.config.settings import get_settings
from collector.kr.infra.logging.setup import setup_logging
from collector.us.cli.app import register as _register_us

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """최상위 파서. kr CLI의 서브파서 등록 결과에 ``us-*``와 ``--version``을 더한다."""
    parser = _build_kr_parser()
    # 한국 파서가 만든 서브파서를 찾아 미국 명령을 얹는다. **한국 쪽 정의를
    # 안 건드린다** — prod 래퍼 35개와 Cronicle 이벤트가 그 이름을 쓴다 (D10).
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            _register_us(action)
            break
    parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("collector"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    setup_logging(
        level=settings.log_level,
        fmt=settings.log_format.value,
        log_dir=settings.log_dir,
    )

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        args.handler(args)
    except NotImplementedError as exc:
        logger.warning("Command not yet implemented: %s", exc)
        print(f"⚠  Not implemented yet: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

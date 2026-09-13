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

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """최상위 파서. kr CLI의 서브파서 등록 결과에 ``--version``만 더한다."""
    parser = _build_kr_parser()
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

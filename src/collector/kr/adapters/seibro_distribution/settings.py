"""SEIBro 수집 스위치."""

from __future__ import annotations

from collections.abc import Mapping

ENV_ENABLED = "SDC_SEIBRO_ENABLED"


def seibro_enabled(env: Mapping[str, str]) -> bool:
    """``SDC_SEIBRO_ENABLED``가 "0"이면 끕니다. 기본은 켜짐("1")."""
    return env.get(ENV_ENABLED, "1").strip() != "0"

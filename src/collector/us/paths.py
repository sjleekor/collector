"""``stock_data/us/`` 경로 조립 — 계층 이름을 아는 유일한 곳.

**자체 ``DataRoot``를 만들지 않는다.** 정본은 ``modeler``의
``modeler.etl.config.DataRoot``고, ``collector`` 안에도 사본이 하나 있다
(``collector.kr.analysis.n7_kis_cross_section._DataRoot``). 셋째 사본을 만들면
같은 것이 세 군데서 따로 늙는다 — 미국 계획 Q10이 그 정리를 다루고, 그때까지
여기서는 **경로를 인자로 받는 함수만** 둔다.

**import 시점에 환경변수를 읽지 않는다.** 모듈 최상위에서 읽으면 미국 코드를
한 줄도 안 돌렸는데 ``collector`` 진입점이 죽는다 — 진입점이 하나라 한국 prod
컨테이너가 같이 죽는다 (미국 계획 X16). 그래서 ``resolve_lake_root``는 함수다.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

MARKET = "us"

#: 한 lake가 갖춰야 하는 계층. ``--lake-root``로 변형 lake를 받을 때 이 넷이
#: 다 있어야 한다 (미국 계획 02 §1.2.1 — 한국이 kr/derived/_e5 진입로를 잃은 뒤
#: 세운 규칙).
LAYERS: tuple[str, ...] = ("raw", "derived", "datasets", "output")


def resolve_lake_root(
    override: Path | str | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    """``stock_data/us`` 경로 하나를 돌려준다.

    ``override``(CLI의 ``--lake-root``)가 있으면 그대로 쓴다 — 고정 스냅샷에
    다시 돌려 대조하거나 별도 구간을 따로 쌓을 때 쓰는 진입로다. 없으면
    ``$STOCK_DATA_ROOT/us``다.

    환경변수는 **이 함수를 부를 때** 읽는다. import 시점이 아니다.
    """
    if override is not None:
        return Path(override)
    env = os.environ if env is None else env
    root = env.get("STOCK_DATA_ROOT")
    if not root:
        raise RuntimeError("STOCK_DATA_ROOT가 없습니다. .envrc를 확인하십시오 (direnv allow).")
    return Path(root) / MARKET


def raw_dir(root: Path) -> Path:
    """원천에서 받은 원문. 원천이 막히면 다시 못 만든다."""
    return root / "raw"


def derived_dir(root: Path) -> Path:
    """받은 것으로 만든 것. 코드만 있으면 다시 만든다."""
    return root / "derived"


def datasets_dir(root: Path) -> Path:
    """모델 학습 입력. modeler/ 몫이다 — 수집은 안 쓴다."""
    return root / "datasets"


def output_dir(root: Path) -> Path:
    """실행 산출물 (리포트·체크포인트)."""
    return root / "output"


def snapshots_dir(root: Path, table: str) -> Path:
    """일자별 스냅샷이 쌓이는 곳 — ``derived/snapshots/<table>/``."""
    return derived_dir(root) / "snapshots" / table


def missing_layers(root: Path) -> tuple[str, ...]:
    """``root``에 없는 계층. 변형 lake를 받을 때 모양을 확인하는 데 쓴다."""
    return tuple(name for name in LAYERS if not (root / name).is_dir())

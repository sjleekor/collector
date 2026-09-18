"""``stock_data/<market>/`` 경로 계약 — ``collector``와 ``modeler``가 같이 쓴다.

**왜 여기 사나.** ``modeler``가 ``collector``를 경로 의존한다
(``modeler/pyproject.toml``의 ``{ path = "../collector" }``). 반대로 두면 순환이고,
``collector``의 prod 이미지에 모델링 의존성(scikit-learn·polars)이 딸려 들어간다.
그래서 두 저장소가 공유하는 타입은 ``collector`` 쪽에 산다. 옛 모노레포에서
``research/``가 이 코드를 갖고 있었던 것은 설계가 아니라 이력이다.

``kr/``·``us/`` 위의 최상위 모듈인 것에 뜻이 있다 — **수집 전용이 아니라 이
저장소가 아는 레이크 계약이다.**

계층 이름이 "다시 만들 수 있나"를 드러낸다.

* ``raw``      원천에서 받은 원문. 원천이 막히면 다시 못 만든다
* ``derived``  받은 것으로 만든 것. 코드만 있으면 만든다
* ``datasets`` 모델 학습 입력. ``modeler``가 쓴다
* ``output``   실행 산출물 (리포트·예측·스캔)
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

#: 한 lake가 갖춰야 하는 계층. 변형 lake를 ``DataRoot(base=...)``로 받을 때도
#: 이 넷이 있어야 한다.
LAYERS: tuple[str, ...] = ("raw", "derived", "datasets", "output")


@dataclass(frozen=True)
class DataRoot:
    """stock_data/<market>/ 하나. 계층 조립은 여기서만 한다."""

    base: Path

    @classmethod
    def resolve(cls, market: str = "kr", *, env: Mapping[str, str] | None = None) -> DataRoot:
        """``$STOCK_DATA_ROOT/<market>``.

        환경변수는 **이 메서드를 부를 때** 읽는다. 모듈 최상위에서 부르면
        import만으로 죽고, 진입점이 하나라 다른 시장 명령까지 같이 죽는다.
        """
        env = os.environ if env is None else env
        root = env.get("STOCK_DATA_ROOT")
        if not root:
            raise RuntimeError("STOCK_DATA_ROOT가 없습니다. .envrc를 확인하십시오 (direnv allow).")
        return cls(Path(root) / market)

    @property
    def raw(self) -> Path:
        return self.base / "raw"

    @property
    def derived(self) -> Path:
        return self.base / "derived"

    @property
    def datasets(self) -> Path:
        return self.base / "datasets"

    @property
    def output(self) -> Path:
        return self.base / "output"

    def missing_layers(self) -> tuple[str, ...]:
        """없는 계층. 변형 lake를 받을 때 모양이 맞는지 본다.

        ``DataRoot(base=...)``는 평소 경로가 아닌 lake를 가리키는 진입로다 —
        고정 스냅샷에 다시 돌려 대조하거나 한 실험이 자기 lake를 쓸 때.
        ``modeler``가 ``kr/derived/_e5``를 그렇게 읽는다.
        """
        return tuple(name for name in LAYERS if not (self.base / name).is_dir())

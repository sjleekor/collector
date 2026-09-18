"""스냅샷 쓰기·읽기 — ``observed_at`` 없이는 쓰지 못한다.

미국 계획 03 §3의 규칙을 코드로 강제하는 자리다. 주석이나 문서로는 안 막힌다 —
한 번 ``observed_at`` 없이 쌓으면 그 스냅샷이 어느 시점 기준인지 영영 모른다.

파티션 키를 ``snapshot_date``로 둔다. ``date``가 아니다 — 경로에 들어간 값이
같은 이름의 데이터 컬럼을 덮어쓰는 사고가 한국에서 있었다(``source``가 경로에도
컬럼에도 있어 KRX 우선 dedup이 무력화됐다). **경로 키와 컬럼 이름을 겹치지
않게 둔다.**
"""

from __future__ import annotations

from datetime import date as _date
from pathlib import Path

import pandas as pd
import pyarrow as pyar
import pyarrow.parquet as pq

from collector.lake import DataRoot
from collector.us.store.schema import (
    ARROW_SCHEMAS,
    FRAME_SCHEMAS,
    PROVENANCE_REQUIRED,
)


class MissingProvenanceError(ValueError):
    """``observed_at`` 같은 출처 컬럼이 빠진 채로 쓰려고 했다."""


class UnknownTableError(KeyError):
    """schema.py에 계약이 없는 테이블이다."""


def snapshot_path(root: DataRoot, table: str, snapshot_date: _date | str) -> Path:
    """``<root>/derived/snapshots/<table>/snapshot_date=<d>/part.parquet``."""
    day = snapshot_date.isoformat() if isinstance(snapshot_date, _date) else str(snapshot_date)
    return root.derived / "snapshots" / table / f"snapshot_date={day}" / "part.parquet"


def write_snapshot(
    frame: pd.DataFrame,
    table: str,
    path: Path,
    *,
    validate: bool = True,
) -> Path:
    """한 스냅샷을 parquet으로 굳힌다.

    ``observed_at``이 없으면 :class:`MissingProvenanceError`를 낸다. 검사를
    끄고 싶어도 이것만은 못 끈다 — ``validate=False``는 값 검사만 건너뛴다.
    """
    try:
        arrow_schema = ARROW_SCHEMAS[table]
    except KeyError as exc:
        raise UnknownTableError(f"{table!r}의 계약이 schema.py에 없다. 먼저 정의한다.") from exc

    missing = [c for c in PROVENANCE_REQUIRED if c not in frame.columns]
    if missing:
        raise MissingProvenanceError(
            f"{table}: {', '.join(missing)} 없이 쓸 수 없다 (미국 계획 03 §3). "
            "원천이 과거를 고치므로 이것 없이는 어느 시점 기준인지 되돌릴 수 없다."
        )

    if validate and (frame_schema := FRAME_SCHEMAS.get(table)) is not None:
        frame = frame_schema.validate(frame)

    table_arrow = pyar.Table.from_pandas(frame, schema=arrow_schema, preserve_index=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table_arrow, path, compression="zstd")
    return path


def read_snapshot(path: Path) -> pd.DataFrame:
    """굳힌 스냅샷을 돌려 읽는다. 왕복 대조에 쓴다."""
    return pq.read_table(path).to_pandas()

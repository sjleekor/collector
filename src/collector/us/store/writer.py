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


def latest_snapshot(root: DataRoot, table: str) -> Path | None:
    """그 표의 **가장 최근 스냅샷**. 없으면 ``None``.

    상시 운영이 이걸 쓴다. 캘린더·거시처럼 천천히 바뀌는 표는 매일 다시 굳히지
    않으므로 **오늘 날짜로 찾으면 늘 없다** — 실제로 C8 첫 실행이 그렇게 죽었다
    (2026-09-20).
    """
    base = root.derived / "snapshots" / table
    if not base.is_dir():
        return None
    parts = sorted(
        (p for p in base.glob("snapshot_date=*/part.parquet")),
        key=lambda p: p.parent.name,
    )
    return parts[-1] if parts else None


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


def write_snapshot_arrow(
    table_arrow: pyar.Table,
    table: str,
    path: Path,
    *,
    unique_on: tuple[str, ...] | None = None,
) -> Path:
    """벌크 적재용 — pandas를 거치지 않는다.

    :func:`write_snapshot`은 pandera로 값을 보지만 그러려면 pandas를 거쳐야 하고,
    decimal 컬럼이 object dtype이 되어 수백만 행에서는 무겁다. 원천에서 통째로
    받아 굳히는 경로는 arrow 그대로 간다.

    **``observed_at`` 강제는 여기서도 같다.** 값 검사 대신 구조 검사를 한다 —
    출처 컬럼, 스키마 캐스팅(``safe=True``라 정밀도가 깎이면 실패), 그리고
    ``unique_on``을 주면 파일 안의 유일성까지.
    """
    try:
        schema = ARROW_SCHEMAS[table]
    except KeyError as exc:
        raise UnknownTableError(f"{table!r}의 계약이 schema.py에 없다. 먼저 정의한다.") from exc

    missing = [c for c in PROVENANCE_REQUIRED if c not in table_arrow.column_names]
    if missing:
        raise MissingProvenanceError(
            f"{table}: {', '.join(missing)} 없이 쓸 수 없다 (미국 계획 03 §3)."
        )

    table_arrow = table_arrow.select(schema.names).cast(schema)

    if unique_on:
        keys = table_arrow.select(list(unique_on))
        if keys.num_rows != keys.group_by(list(unique_on)).aggregate([]).num_rows:
            raise ValueError(f"{table}: 파일 안에서 {unique_on} 가 유일하지 않다 (03 §3.1).")

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table_arrow, path, compression="zstd")
    return path


def verify_snapshot(
    path: Path,
    table: str,
    *,
    unique_on: tuple[str, ...] | None = None,
) -> dict[str, object]:
    """이미 쓴 파일이 계약에 맞는지 본다 — 흘려 쓴 경로용.

    수천만 행은 메모리에 올리지 않고 DuckDB가 곧장 parquet으로 흘리는 편이
    낫다. 그러면 :func:`write_snapshot_arrow`를 못 거치므로 **쓴 뒤에** 같은
    것을 확인한다: 컬럼·타입이 계약과 같은가, 출처 컬럼에 결측이 없는가,
    파일 안에서 키가 유일한가.
    """
    import duckdb

    try:
        schema = ARROW_SCHEMAS[table]
    except KeyError as exc:
        raise UnknownTableError(f"{table!r}의 계약이 schema.py에 없다.") from exc

    got = pq.ParquetFile(path).schema_arrow
    if got.names != schema.names:
        raise ValueError(
            f"{table}: 컬럼이 계약과 다르다.\n  계약: {schema.names}\n  파일: {got.names}"
        )
    mismatched = [
        f"{f.name}: 계약 {f.type} / 파일 {got.field(f.name).type}"
        for f in schema
        if not f.type.equals(got.field(f.name).type)
    ]
    if mismatched:
        raise ValueError(f"{table}: 타입이 계약과 다르다 — " + "; ".join(mismatched))

    con = duckdb.connect()
    src = f"read_parquet('{path}')"
    nulls = con.execute(
        f"SELECT count(*) FROM {src} WHERE "
        + " OR ".join(f"{c} IS NULL" for c in PROVENANCE_REQUIRED)
    ).fetchone()[0]
    if nulls:
        raise MissingProvenanceError(f"{table}: 출처 컬럼이 비어 있는 행 {nulls:,}개")

    rows = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    if unique_on:
        keys = ", ".join(unique_on)
        distinct = con.execute(f"SELECT count(DISTINCT ({keys})) FROM {src}").fetchone()[0]
        if distinct != rows:
            raise ValueError(
                f"{table}: 파일 안에서 ({keys}) 가 유일하지 않다 — "
                f"{rows:,}행 중 {distinct:,}개 (03 §3.1)"
            )
    return {"rows": rows, "bytes": path.stat().st_size}


def read_snapshot(path: Path) -> pd.DataFrame:
    """굳힌 스냅샷을 돌려 읽는다. 왕복 대조에 쓴다."""
    return pq.read_table(path).to_pandas()

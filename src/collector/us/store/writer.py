"""스냅샷 쓰기·읽기 — ``observed_at`` 없이는 쓰지 못한다.

미국 계획 03 §3의 규칙을 코드로 강제하는 자리다. 주석이나 문서로는 안 막힌다 —
한 번 ``observed_at`` 없이 쌓으면 그 스냅샷이 어느 시점 기준인지 영영 모른다.

파티션 키를 ``snapshot_date``로 둔다. ``date``가 아니다 — 경로에 들어간 값이
같은 이름의 데이터 컬럼을 덮어쓰는 사고가 한국에서 있었다(``source``가 경로에도
컬럼에도 있어 KRX 우선 dedup이 무력화됐다). **경로 키와 컬럼 이름을 겹치지
않게 둔다.**
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
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


@contextmanager
def staged_snapshot(dest: Path) -> Iterator[Path]:
    """표 디렉터리의 임시 파일에 쓰게 하고, **with 블록이 끝까지 가야만** 옮긴다.

    흘려 쓰는 경로(DuckDB ``COPY ... TO``, ``ParquetWriter``)는 쓰고 나서야
    계약을 확인할 수 있다. 최종 경로에 곧장 쓰면 확인이 실패해도 파일이 남고,
    ``us-derive``의 "raw가 스냅샷보다 오래됐다" 판단이 그걸 최신으로 본다
    (2026-10-03 ``filings_index`` 사고). 그래서 이렇게 한다.

    * 블록 안에서 임시 경로에 쓰고 ``verify_snapshot``까지 마친다.
    * 예외 없이 나오면 그때 ``snapshot_date=`` 디렉터리를 만들고 ``os.replace``로
      옮긴다. 같은 파일시스템이라 원자적이다.
    * 예외가 나면 임시 파일을 지운다. **이전 스냅샷은 건드리지 않는다.**

    임시 파일은 ``snapshot_date=`` 디렉터리 **밖**(표 디렉터리 바로 아래, 점으로
    시작하는 이름)에 둔다. 쓰는 동안 빈 ``snapshot_date=`` 디렉터리가 있으면
    디렉터리 날짜로 최신을 고르는 쪽(modeler ``UsLake.latest_snapshot``)이 그걸
    집고 파일이 없다고 깨진다. 강제 종료로 ``finally``가 못 돌아도 남는 것은 이
    임시 파일뿐이고 어느 쪽 glob에도 안 잡힌다.
    """
    table_dir = dest.parent.parent
    table_dir.mkdir(parents=True, exist_ok=True)
    tmp = table_dir / f".{dest.parent.name}.{dest.name}.tmp"
    tmp.unlink(missing_ok=True)
    try:
        yield tmp
        if not tmp.is_file():
            raise FileNotFoundError(f"{tmp}: 블록이 임시 파일을 만들지 않았다")
        made_dir = not dest.parent.exists()
        dest.parent.mkdir(exist_ok=True)
        try:
            os.replace(tmp, dest)
        except BaseException:
            if made_dir:
                try:
                    dest.parent.rmdir()
                except OSError:
                    pass
            raise
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


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
    with staged_snapshot(path) as tmp:
        pq.write_table(table_arrow, tmp, compression="zstd")
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

    # 임시 경로에 쓰고 계약까지 확인한 뒤에 옮긴다 — 실패한 파일이 남지 않는다
    with staged_snapshot(path) as tmp:
        pq.write_table(table_arrow, tmp, compression="zstd")
        verify_snapshot(tmp, table, unique_on=unique_on)
    return path


def verify_snapshot(
    path: Path,
    table: str,
    *,
    unique_on: tuple[str, ...] | None = None,
    connection=None,
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

    con = connection if connection is not None else duckdb.connect()
    try:
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
    finally:
        if connection is None:
            con.close()
    return {"rows": rows, "bytes": path.stat().st_size}


def read_snapshot(path: Path) -> pd.DataFrame:
    """굳힌 스냅샷을 돌려 읽는다. 왕복 대조에 쓴다."""
    return pq.read_table(path).to_pandas()

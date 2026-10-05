"""``staged_snapshot`` — 검증을 통과해야만 최종 경로에 자리를 잡는다."""

from __future__ import annotations

import pyarrow as pyar
import pytest

from collector.lake import DataRoot
from collector.us.store.schema import ARROW_SCHEMAS
from collector.us.store.writer import (
    MissingProvenanceError,
    latest_snapshot,
    snapshot_path,
    staged_snapshot,
    write_snapshot_arrow,
)


def _root(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


def test_commit_replaces_atomically_and_leaves_no_temp(tmp_path):
    dest = tmp_path / "snapshot_date=2026-01-01" / "part.parquet"
    with staged_snapshot(dest) as tmp:
        # 임시 파일은 snapshot_date= 디렉터리 밖(표 디렉터리)에 있고, 쓰는 동안
        # snapshot_date= 디렉터리 자체가 없다 — 디렉터리 날짜로 최신을 고르는
        # modeler UsLake.latest_snapshot이 빈 디렉터리를 집지 않는다
        assert tmp != dest and tmp.parent == dest.parent.parent
        assert tmp.name.startswith(".")
        tmp.write_bytes(b"new")
        assert not dest.parent.exists()
    assert dest.read_bytes() == b"new"
    assert [p.name for p in dest.parent.iterdir()] == ["part.parquet"]
    assert [p.name for p in tmp_path.iterdir()] == ["snapshot_date=2026-01-01"]


def test_failure_removes_temp_and_keeps_the_previous_file(tmp_path):
    dest = tmp_path / "snapshot_date=2026-01-01" / "part.parquet"
    dest.parent.mkdir()
    dest.write_bytes(b"old")
    with pytest.raises(ValueError), staged_snapshot(dest) as tmp:
        tmp.write_bytes(b"partial")
        raise ValueError("검증 실패")
    assert dest.read_bytes() == b"old"
    assert [p.name for p in dest.parent.iterdir()] == ["part.parquet"]
    assert [p.name for p in tmp_path.iterdir()] == ["snapshot_date=2026-01-01"]


def test_failure_on_a_new_date_leaves_no_directory(tmp_path):
    dest = tmp_path / "snapshot_date=2026-01-02" / "part.parquet"
    with pytest.raises(RuntimeError), staged_snapshot(dest) as tmp:
        tmp.write_bytes(b"partial")
        raise RuntimeError("죽었다")
    assert not dest.parent.exists()
    assert list(tmp_path.iterdir()) == []


def test_temp_name_is_invisible_to_latest_snapshot(tmp_path):
    root = _root(tmp_path)
    dest = snapshot_path(root, "trading_calendar", "2026-01-02")
    with staged_snapshot(dest) as tmp:
        tmp.write_bytes(b"x")
        assert latest_snapshot(root, "trading_calendar") is None


def test_write_snapshot_arrow_validates_before_it_lands(tmp_path):
    """출처 컬럼이 빈 표는 검증에서 막히고 최종 경로에 아무것도 안 남는다."""
    root = _root(tmp_path)
    schema = ARROW_SCHEMAS["trading_calendar"]
    bad = pyar.table({f.name: pyar.nulls(1, f.type) for f in schema}, schema=schema)
    dest = snapshot_path(root, "trading_calendar", "2026-01-03")
    with pytest.raises(MissingProvenanceError):
        write_snapshot_arrow(bad, "trading_calendar", dest)
    assert not dest.parent.exists()
    assert latest_snapshot(root, "trading_calendar") is None

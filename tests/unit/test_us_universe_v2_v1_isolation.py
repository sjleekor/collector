"""T5 — v2 를 만들어도 v1 출력이 안 바뀐다 (설계 02 §2.5 · §6).

R3c 의 5번 기준과 같은 검사인데 이번에는 **실제 collector 공유 표 경로**로 돌린다.
v1 빌더 ``build_universe_daily`` 를 v2 입력·식별 표·마스터가 생기기 전과 뒤에 각각 돌려
중간 표 넷(``daily_base``·``monthly``·``listing_daily``·``membership``)과 ``in_universe`` 가
같은지, 공유 표 디렉터리에 v2 가 아무것도 안 썼는지 본다.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import duckdb
import pytest

from collector.us.universe import build as v1
from collector.us.universe.v2 import build as v2
from tests.unit.test_us_universe_v2_build import START, build_lake

V1_TABLES = ("daily_base", "monthly", "listing_daily", "membership")
SHARED = (
    "listing_snapshots",
    "universe_daily",
    "prices_daily",
    "filings_sub",
    "midas_security_daily",
)


class _Recorder:
    """v1 의 DuckDB 연결을 감싸 닫을 때 중간 표를 꺼낸다."""

    def __init__(self, con, sink: dict):
        self._con = con
        self._sink = sink

    def __getattr__(self, name):
        return getattr(self._con, name)

    def close(self):
        for table in V1_TABLES:
            self._sink[table] = self._con.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
        self._con.close()


def run_v1(root, snapshot_date, monkeypatch) -> dict:
    sink: dict = {}
    real = v1._bounded_duckdb
    monkeypatch.setattr(
        v1, "_bounded_duckdb", lambda r, bounded=True: _Recorder(real(r, bounded=bounded), sink)
    )
    result = v1.build_universe_daily(root, snapshot_date=snapshot_date, start=START)
    monkeypatch.setattr(v1, "_bounded_duckdb", real)
    sink["in_universe"] = (
        duckdb.connect()
        .execute(
            "SELECT date, symbol, cik, in_universe FROM "
            "read_parquet(?, hive_partitioning = false) ORDER BY 1, 2",
            [str(result["path"])],
        )
        .fetchall()
    )
    return sink


def tree(root) -> dict[str, str]:
    """공유 표 디렉터리의 파일과 내용 해시."""
    out = {}
    for name in SHARED:
        base = root.derived / "snapshots" / name
        for path in sorted(base.rglob("*")):
            if path.is_file():
                out[str(path.relative_to(root.base))] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
    return out


def test_t5_v1_output_is_unchanged_by_v2_inputs_tables_and_builds(tmp_path, monkeypatch):
    lake = build_lake(tmp_path)
    lake.flush("2021-12-31")
    root = lake.root
    inputs_before = {k: str(p) for k, p in v1.resolve_inputs(root).items()}

    first = run_v1(root, "2022-01-10", monkeypatch)
    shared_before = tree(root)
    assert (
        first["membership"] and first["daily_base"] and first["listing_daily"] and first["monthly"]
    )
    assert any(row[3] for row in first["in_universe"])  # 멤버가 하나는 있다

    # v2 입력(상장 목록 v2)·식별 표·마스터·멤버십을 만든다
    result = v2.build_universe_v2(root, snapshot_date="2022-01-20", mode="rebuild", start=START)
    assert result["rows"] > 0
    assert (root.derived / "snapshots" / "security_segments").is_dir()
    assert (root.derived / "snapshots" / "security_master").is_dir()

    # 공유 표에는 한 글자도 안 썼다
    assert tree(root) == shared_before
    assert {k: str(p) for k, p in v1.resolve_inputs(root).items()} == inputs_before

    second = run_v1(root, "2022-01-30", monkeypatch)
    for table in (*V1_TABLES, "in_universe"):
        assert second[table] == first[table], table


def test_v1_builder_source_is_untouched_and_v2_writes_only_its_own_tables():
    """v1 빌더를 안 바꿨다는 것과 v2 가 쓰는 표의 목록."""
    source = Path(v1.__file__).read_text()
    assert "universe.v2" not in source and "security_segments" not in source
    own = {"listing_snapshots_v2", "security_segments", "security_master", "universe_daily_v2"}
    from collector.us.store.schema import ARROW_SCHEMAS

    assert own <= set(ARROW_SCHEMAS)
    # v2 쓰기 경로는 이 이름들만 안다
    text = (Path(v2.__file__)).read_text()
    for shared in ('"universe_daily"', '"listing_snapshots"'):
        assert shared not in text.replace('"universe_daily_v2"', "").replace(
            '"listing_snapshots_v2"', ""
        )


def test_v1_build_py_has_no_diff_against_main():
    """작업 트리의 build.py 가 main 의 것과 같다 (v1 은 건드리지 않는다)."""
    repo = Path(v1.__file__).resolve().parents[4]
    run = subprocess.run(
        ["git", "diff", "--quiet", "main", "--", "src/collector/us/universe/build.py"],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    if run.returncode not in (0, 1):  # git 이 없거나 main 이 없는 환경
        pytest.skip("git diff 를 못 돌린다")
    assert run.returncode == 0, "src/collector/us/universe/build.py 가 바뀌었다"

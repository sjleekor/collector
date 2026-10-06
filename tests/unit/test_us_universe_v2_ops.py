"""v1 뒤에 v2 를 잇는 운영 연결 (설계 02 §2.5): 실패 격리, dolt 커밋 기록, CLI 배선."""

from __future__ import annotations

import json

import pytest

from collector.cli.app import build_parser
from collector.lake import DataRoot
from collector.us.cli import app as us_app
from collector.us.universe import build as v1
from collector.us.universe.v2 import build as v2_build
from collector.us.universe.v2 import ops
from tests.unit.test_us_universe_v2_build import START, build_lake


def _root(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir(exist_ok=True)
    return DataRoot(tmp_path)


def _lines(root, parts) -> list[dict]:
    path = root.output.joinpath(*parts)
    return [json.loads(line) for line in path.read_text().splitlines()]


# --- dolt 커밋 기록 ---------------------------------------------------------------------------


def test_v1_dolt_commit_is_recorded_outside_the_v1_builder(tmp_path):
    root = _root(tmp_path)
    record = ops.record_v1_dolt_commit(
        root, snapshot_date="2026-10-06", kind="incremental", v1_result={"path": "/x/part.parquet"}
    )
    assert record["dolt_stocks"]["commit"] is None  # 합성 레이크에는 dolt 레포가 없다
    assert "dolt" in record["dolt_stocks"]["note"]
    ops.record_v1_dolt_commit(root, snapshot_date="2026-10-07", kind="rebuild")
    rows = _lines(root, ops.V1_PROVENANCE)
    assert [r["kind"] for r in rows] == ["incremental", "rebuild"]  # 빌드마다 한 줄씩 쌓인다
    assert rows[0]["v1_path"] == "/x/part.parquet"


def test_dolt_commit_is_read_from_the_stocks_repo(tmp_path, monkeypatch):
    root = _root(tmp_path)
    (root.raw / "dolt" / "stocks" / ".dolt").mkdir(parents=True)
    from collector.us.sources import dolt

    monkeypatch.setattr(dolt, "head_commit", lambda repo: "abc123")
    record = ops.record_v1_dolt_commit(root, snapshot_date="2026-10-06", kind="rebuild")
    assert record["dolt_stocks"] == {"commit": "abc123", "note": None}


# --- v2 실패가 v1 을 안 깬다 -------------------------------------------------------------------


def test_run_v2_after_v1_never_raises_and_records_the_failure(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)

    def boom(*a, **k):
        raise RuntimeError("v2 가 죽었다")

    monkeypatch.setattr(v2_build, "build_universe_v2", boom)
    # 직전 v2 스냅샷이 있어야 증분을 부른다
    (root.derived / "snapshots" / "universe_daily_v2" / "snapshot_date=2026-10-01").mkdir(
        parents=True
    )
    (
        root.derived
        / "snapshots"
        / "universe_daily_v2"
        / "snapshot_date=2026-10-01"
        / "part.parquet"
    ).write_bytes(b"x")
    from collector.us.ops import derive

    monkeypatch.setattr(
        derive, "run_derive", lambda *a, **k: {"ok": True, "tables": [{"name": "x"}]}
    )
    out = ops.run_v2_after_v1(root, snapshot_date="2026-10-06")
    assert out["status"] == "failed" and "RuntimeError" in out["error"]
    assert "v1 is unaffected" in capsys.readouterr().err
    rows = _lines(root, ops.V2_RUNS)
    assert rows[-1]["status"] == "failed" and "죽었다" in rows[-1]["error"]


def test_run_v2_after_v1_without_a_prior_snapshot_asks_for_a_manual_rebuild(tmp_path, monkeypatch):
    root = _root(tmp_path)
    from collector.us.ops import derive

    monkeypatch.setattr(
        derive, "run_derive", lambda *a, **k: {"ok": True, "tables": [{"name": "x"}]}
    )
    out = ops.run_v2_after_v1(root, snapshot_date="2026-10-06")
    assert out["status"] == "needs_rebuild" and "rebuild" in out["reason"]
    assert _lines(root, ops.V2_RUNS)[-1]["status"] == "needs_rebuild"


def test_run_v2_after_v1_extends_an_existing_snapshot(tmp_path):
    lake = build_lake(tmp_path)
    lake.flush()
    v2_build.build_universe_v2(
        lake.root, snapshot_date="2021-09-20", mode="rebuild", start=START, end="2021-09-15"
    )
    out = ops.run_v2_after_v1(lake.root, snapshot_date="2022-01-31")
    assert out["status"] == "ok", out
    assert out["result"]["mode"] == "incremental" and out["result"]["end"] == "2021-12-31"
    again = ops.run_v2_after_v1(lake.root, snapshot_date="2022-02-01")
    assert again["status"] == "ok" and again["result"]["skipped"] is True  # 새 세션이 없다
    assert len(_lines(lake.root, ops.V2_RUNS)) == 2


# --- CLI 배선 ---------------------------------------------------------------------------------


def _parse(*argv: str):
    return build_parser().parse_args(argv)


def test_cli_parses_v2_flags_and_commands():
    args = _parse("us-universe", "incremental", "--if-new", "--v2")
    assert args.handler is us_app._handle_universe_incremental and args.v2 is True
    assert _parse("us-universe", "incremental").v2 is False
    rebuild = _parse("us-universe-v2", "rebuild", "--start", "2020-03-02", "--dry-run")
    assert rebuild.handler is us_app._handle_universe_v2 and rebuild.mode == "rebuild"
    assert rebuild.start == "2020-03-02" and rebuild.dry_run is True
    inc = _parse("us-universe-v2", "incremental", "--if-new", "--spac-release-name-change")
    assert inc.mode == "incremental" and inc.if_new and inc.spac_release_name_change


def test_incremental_handler_keeps_v1_result_and_exit_code_when_v2_fails(
    tmp_path, monkeypatch, capsys
):
    root = _root(tmp_path)
    monkeypatch.setattr(
        v1, "build_universe_incremental", lambda r, **k: {"path": "/x/part.parquet", "rows": 3}
    )

    def boom(*a, **k):
        raise RuntimeError("v2 가 죽었다")

    monkeypatch.setattr(v2_build, "build_universe_v2", boom)
    from collector.us.ops import derive

    monkeypatch.setattr(
        derive, "run_derive", lambda *a, **k: {"ok": True, "tables": [{"name": "x"}]}
    )
    snap = root.derived / "snapshots" / "universe_daily_v2" / "snapshot_date=2026-10-01"
    snap.mkdir(parents=True)
    (snap / "part.parquet").write_bytes(b"x")
    args = _parse(
        "us-universe",
        "incremental",
        "--if-new",
        "--v2",
        "--lake-root",
        str(tmp_path),
        "--snapshot-date",
        "2026-10-06",
    )
    args.handler(args)  # SystemExit 이 안 난다 = 종료 코드 0
    out = json.loads(capsys.readouterr().out)
    assert out["path"] == "/x/part.parquet" and out["rows"] == 3  # v1 결과 그대로
    assert out["v2"]["status"] == "failed"
    # v1 빌드마다 dolt 커밋이 기록된다
    assert _lines(root, ops.V1_PROVENANCE)[-1]["kind"] == "incremental"


def test_incremental_handler_without_v2_flag_does_not_touch_v2(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)
    monkeypatch.setattr(v1, "build_universe_incremental", lambda r, **k: {"skipped": True})
    called = []
    monkeypatch.setattr(ops, "run_v2_after_v1", lambda *a, **k: called.append(1))
    args = _parse("us-universe", "incremental", "--if-new", "--lake-root", str(tmp_path))
    args.handler(args)
    assert not called
    assert json.loads(capsys.readouterr().out) == {"skipped": True}
    # 건너뛴 날에는 dolt 커밋도 안 적는다(빌드가 없었다)
    assert not root.output.joinpath(*ops.V1_PROVENANCE).exists()


def test_incremental_dry_run_does_not_run_v2(tmp_path, monkeypatch, capsys):
    _root(tmp_path)
    monkeypatch.setattr(v1, "build_universe_incremental", lambda r, **k: {"dry_run": True})
    monkeypatch.setattr(ops, "run_v2_after_v1", lambda *a, **k: pytest.fail("v2 를 부르면 안 된다"))
    args = _parse("us-universe", "incremental", "--v2", "--dry-run", "--lake-root", str(tmp_path))
    args.handler(args)
    assert json.loads(capsys.readouterr().out) == {"dry_run": True}


def test_rebuild_handler_records_the_dolt_commit(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)
    monkeypatch.setattr(v1, "build_universe_daily", lambda r, **k: {"path": "/x/part.parquet"})
    args = _parse(
        "us-universe", "rebuild", "--lake-root", str(tmp_path), "--snapshot-date", "2026-10-06"
    )
    args.handler(args)
    assert _lines(root, ops.V1_PROVENANCE)[-1]["kind"] == "rebuild"
    assert json.loads(capsys.readouterr().out)["path"] == "/x/part.parquet"


def test_v2_command_calls_the_builder(tmp_path, monkeypatch, capsys):
    _root(tmp_path)
    seen = {}

    def fake(root, **kwargs):
        seen.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(v2_build, "build_universe_v2", fake)
    args = _parse(
        "us-universe-v2",
        "incremental",
        "--if-new",
        "--lake-root",
        str(tmp_path),
        "--snapshot-date",
        "2026-10-06",
        "--dorm-known-by-corroboration",
    )
    args.handler(args)
    assert seen["mode"] == "incremental" and seen["if_new"] is True
    assert (
        seen["rules"].dorm_known_at_resume is False
        and seen["rules"].spac_release_name_change is False
    )
    plain = _parse("us-universe-v2", "rebuild", "--lake-root", str(tmp_path))
    plain.handler(plain)
    assert seen["rules"] is None  # 플래그가 없으면 기본(rebuild)·직전 빌드(incremental) 규칙
    capsys.readouterr()

"""krx-baseline-sync.sh·seibro-dist-sync.sh — 환경값이 명령 인자가 되는지, 락·uid·import 볼륨."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

BIN = Path(__file__).resolve().parents[2] / "deploy/prod/bin"


def _run(tmp_path: Path, wrapper: str, env: dict[str, str]) -> tuple[int, str, str]:
    compose = tmp_path / "compose"
    compose.write_text('#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" >> "$CAPTURE_ARGS"\n')
    compose.chmod(0o755)
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    docker = fake_bin / "docker"
    docker.write_text('#!/usr/bin/env bash\ncase "$1" in inspect) exit 1 ;; *) exit 99 ;; esac\n')
    docker.chmod(0o755)
    (tmp_path / "stock_data/kr/raw/krx_baseline").mkdir(parents=True, exist_ok=True)
    capture = tmp_path / "args.txt"
    capture.unlink(missing_ok=True)  # 실행마다 새로 본다
    environment = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_LOCK_WAIT_SECONDS": "1",
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        **env,
    }
    for key in ("SDC_RUN_AS_ROOT", "KRX_BASELINE_MODE", "SDC_SEIBRO_ENABLED", "SEIBRO_DIST_FULL"):
        if key not in env:
            environment.pop(key, None)
    done = subprocess.run([str(BIN / wrapper)], env=environment, text=True, capture_output=True)
    text = capture.read_text() if capture.exists() else ""
    return done.returncode, text, done.stdout + done.stderr


def test_default_mode_is_the_scheduled_sync_with_a_60_call_budget(tmp_path: Path) -> None:
    code, captured, out = _run(tmp_path, "krx-baseline-sync.sh", {})
    assert code == 0, out
    assert "krx-baseline sync --max-calls 60" in captured
    assert f"--user {os.getuid()}:{os.getgid()}" in captured
    assert "lock acquired: domain=krx_baseline" in out


def test_backfill_needs_one_service_and_reobserve_needs_a_run_id(tmp_path: Path) -> None:
    assert _run(tmp_path, "krx-baseline-sync.sh", {"KRX_BASELINE_MODE": "backfill"})[0] == 2
    both = {"KRX_BASELINE_MODE": "backfill", "KRX_BASELINE_SERVICES": "etf_bydd_trd,bon_dd_trd"}
    assert _run(tmp_path, "krx-baseline-sync.sh", both)[0] == 2
    reobserve = {
        "KRX_BASELINE_MODE": "backfill",
        "KRX_BASELINE_SERVICES": "etf_bydd_trd",
        "KRX_BASELINE_FILL_MODE": "reobserve",
    }
    assert _run(tmp_path, "krx-baseline-sync.sh", reobserve)[0] == 2
    code, captured, out = _run(
        tmp_path,
        "krx-baseline-sync.sh",
        {
            **reobserve,
            "KRX_BASELINE_RUN_ID": "reobs_1",
            "KRX_BASELINE_START": "2010-01-04",
            "KRX_BASELINE_MAX_CALLS": "5000",
        },
    )
    assert code == 0, out
    assert (
        "krx-baseline backfill --service etf_bydd_trd --mode reobserve --start 2010-01-04 "
        "--run-id reobs_1 --max-calls 5000"
    ) in captured


def test_import_path_outside_the_volume_is_mounted_read_only(tmp_path: Path) -> None:
    env = {
        "KRX_BASELINE_MODE": "import-research",
        "KRX_BASELINE_IMPORT_PATH": "/home/whi/data/research_refs/x/raw",
        "KRX_BASELINE_EXPECT_SHA256": "ab" * 32,
    }
    code, captured, out = _run(tmp_path, "krx-baseline-sync.sh", env)
    assert code == 0, out
    assert "-v /home/whi/data/research_refs/x/raw:/import:ro" in captured
    assert "import-research --path /import --expect-manifest-sha256" in captured
    inside = {**env, "KRX_BASELINE_IMPORT_PATH": f"{tmp_path}/stock_data/kr/output/copy"}
    code, captured, out = _run(tmp_path, "krx-baseline-sync.sh", inside)
    assert code == 0, out
    assert "--path /stock_data/kr/output/copy" in captured and ":/import" not in captured
    assert _run(tmp_path, "krx-baseline-sync.sh", {"KRX_BASELINE_MODE": "import-research"})[0] == 2


def test_verify_does_not_take_the_lock(tmp_path: Path) -> None:
    code, captured, out = _run(tmp_path, "krx-baseline-sync.sh", {"KRX_BASELINE_MODE": "verify"})
    assert code == 0, out
    assert "krx-baseline verify" in captured and "lock acquired" not in out


def test_unknown_mode_exits_2(tmp_path: Path) -> None:
    assert _run(tmp_path, "krx-baseline-sync.sh", {"KRX_BASELINE_MODE": "nope"})[0] == 2


def test_seibro_wrapper_uses_its_own_lock_and_full_switch(tmp_path: Path) -> None:
    code, captured, out = _run(tmp_path, "seibro-dist-sync.sh", {})
    assert code == 0, out
    assert "seibro-dist sync" in captured and "--full" not in captured
    assert "lock acquired: domain=seibro" in out
    code, captured, _ = _run(tmp_path, "seibro-dist-sync.sh", {"SEIBRO_DIST_FULL": "1"})
    assert code == 0 and "seibro-dist sync --full" in captured


def test_seibro_wrapper_exits_without_a_container_when_disabled(tmp_path: Path) -> None:
    code, captured, out = _run(tmp_path, "seibro-dist-sync.sh", {"SDC_SEIBRO_ENABLED": "0"})
    assert code == 0 and captured == "" and "꺼져" in out


@pytest.mark.skipif(os.geteuid() == 0, reason="root는 권한 검사를 통과한다")
def test_unwritable_lake_stops_with_73_before_the_container(tmp_path: Path) -> None:
    (tmp_path / "stock_data/kr/raw/krx_baseline").mkdir(parents=True, exist_ok=True)
    (tmp_path / "stock_data/kr/raw").chmod(0o555)
    (tmp_path / "stock_data/kr/raw/krx_baseline").chmod(0o555)
    try:
        code, captured, _ = _run(tmp_path, "krx-baseline-sync.sh", {})
    finally:
        (tmp_path / "stock_data/kr/raw/krx_baseline").chmod(0o755)
        (tmp_path / "stock_data/kr/raw").chmod(0o755)
    assert code == 73 and captured == ""

"""flows-sync-kis.sh(19:00 수급 잡): 호출자 uid 실행, 토큰 캐시 사전 검사(73), 상태 플래그 옵션."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/flows-sync-kis.sh"
NOT_ROOT = pytest.mark.skipif(os.geteuid() == 0, reason="root는 권한 검사를 통과한다")


def _run(
    tmp_path: Path, args: list[str] | None = None, extra_env: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], Path]:
    fake_compose = tmp_path / "compose"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    capture = tmp_path / "args.txt"
    fake_compose.write_text('#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" >> "$CAPTURE_ARGS"\n')
    fake_compose.chmod(0o755)
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        '#!/usr/bin/env bash\ncase "$1" in inspect) exit 1 ;; *) exit 99 ;; esac\n'
    )
    fake_docker.chmod(0o755)
    environment = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        **(extra_env or {}),
    }
    for key in (
        "SDC_RUN_AS_ROOT",
        "SDC_LOCK_WAIT_SECONDS",
        "KIS_FLOW_NO_STATUS",
        "KIS_FLOW_TICKERS",
        "KIS_FLOW_EXCLUDE_GROUPS",
    ):
        if key not in (extra_env or {}):
            environment.pop(key, None)
    result = subprocess.run(
        [str(WRAPPER), *(args or [])], env=environment, text=True, capture_output=True
    )
    return result, capture


def test_default_runs_sync_kis_as_invoking_user_in_the_kis_lock_domain(tmp_path: Path) -> None:
    (tmp_path / "state").mkdir()
    result, capture = _run(tmp_path)

    assert result.returncode == 0, result.stdout + result.stderr
    line = capture.read_text().splitlines()[-1]
    assert "flows sync-kis --lookback-days 14" in line
    assert f"--user {os.getuid()}:{os.getgid()} -e HOME=/tmp" in line
    assert "--no-status" not in line  # 상태 플래그는 기본으로 같이 쌓는다
    assert "lock acquired: domain=kis" in result.stdout


def test_status_capture_can_be_switched_off(tmp_path: Path) -> None:
    (tmp_path / "state").mkdir()
    result, capture = _run(tmp_path, extra_env={"KIS_FLOW_NO_STATUS": "1"})

    assert result.returncode == 0, result.stdout + result.stderr
    assert "--no-status" in capture.read_text()


def test_root_escape_hatch_runs_as_root_without_the_token_check(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    state.chmod(0o555)
    try:
        result, capture = _run(tmp_path, extra_env={"SDC_RUN_AS_ROOT": "1"})
    finally:
        state.chmod(0o755)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "--user" not in capture.read_text()


def test_invalid_run_as_root_value_exits_2(tmp_path: Path) -> None:
    (tmp_path / "state").mkdir()
    result, capture = _run(tmp_path, extra_env={"SDC_RUN_AS_ROOT": "yes"})

    assert result.returncode == 2
    assert not capture.exists()


@NOT_ROOT
def test_unreadable_token_stops_with_73_before_the_container_and_issues_no_token(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir()
    token = state / "kis_token.json"
    token.write_text("{}")
    token.chmod(0o000)  # root 0600을 whi가 읽지 못하는 상황
    try:
        result, capture = _run(tmp_path)
    finally:
        token.chmod(0o600)

    assert result.returncode == 73
    assert "kis_token.json" in result.stdout
    assert "카카오톡" in result.stdout
    assert not capture.exists()


@NOT_ROOT
def test_unwritable_state_dir_stops_with_73(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "kis_token.json").write_text("{}")  # 읽을 수는 있지만 갱신(rename)은 못 한다
    state.chmod(0o555)
    try:
        result, capture = _run(tmp_path)
    finally:
        state.chmod(0o755)

    assert result.returncode == 73
    assert not capture.exists()


@NOT_ROOT
def test_missing_token_file_with_writable_state_dir_runs(tmp_path: Path) -> None:
    (tmp_path / "state").mkdir()  # 첫 실행: 토큰이 아직 없다
    result, capture = _run(tmp_path)

    assert result.returncode == 0, result.stdout + result.stderr
    assert capture.exists()

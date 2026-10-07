"""flows-sync-kis-status.sh: 락 도메인, 컨테이너 사용자, 토큰 캐시 사전 검사."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/flows-sync-kis-status.sh"
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
    for key in ("SDC_RUN_AS_ROOT", "SDC_LOCK_WAIT_SECONDS"):
        if key not in (extra_env or {}):
            environment.pop(key, None)
    result = subprocess.run(
        [str(WRAPPER), *(args or [])], env=environment, text=True, capture_output=True
    )
    return result, capture


def test_default_runs_the_status_command_as_root_in_the_kis_lock_domain(tmp_path: Path) -> None:
    result, capture = _run(tmp_path)

    assert result.returncode == 0, result.stdout + result.stderr
    line = capture.read_text().splitlines()[-1]
    assert "flows sync-kis-status" in line
    assert "--user" not in line  # 토큰 캐시가 root 0600이라 기본은 root
    assert "lock acquired: domain=kis" in result.stdout
    assert "sdc-collector-kis" in result.stdout  # flows-sync-kis.sh와 같은 컨테이너 이름


def test_extra_arguments_are_passed_through(tmp_path: Path) -> None:
    result, capture = _run(tmp_path, ["--plan-only", "--tickers", "005930"])

    assert result.returncode == 0, result.stdout + result.stderr
    assert "flows sync-kis-status --plan-only --tickers 005930" in capture.read_text()


def test_default_lock_wait_is_long_enough_to_outlast_the_19h_flows_job(tmp_path: Path) -> None:
    result, _ = _run(tmp_path)

    assert "wait=3600s" in result.stdout


def test_lock_wait_can_be_overridden(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, extra_env={"SDC_LOCK_WAIT_SECONDS": "5"})

    assert "wait=5s" in result.stdout


@NOT_ROOT
def test_as_invoking_user_runs_when_the_token_is_readable(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "kis_token.json").write_text("{}")
    result, capture = _run(tmp_path, extra_env={"SDC_RUN_AS_ROOT": "0"})

    assert result.returncode == 0, result.stdout + result.stderr
    assert f"--user {os.getuid()}:{os.getgid()} -e HOME=/tmp" in capture.read_text()


@NOT_ROOT
def test_as_invoking_user_refuses_an_unreadable_token_before_starting_the_container(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir()
    token = state / "kis_token.json"
    token.write_text("{}")
    token.chmod(0o000)  # root 0600을 whi가 읽지 못하는 상황을 흉내 낸다
    try:
        result, capture = _run(tmp_path, extra_env={"SDC_RUN_AS_ROOT": "0"})
    finally:
        token.chmod(0o600)

    assert result.returncode == 73
    assert "kis_token.json" in result.stdout
    assert not capture.exists()  # 컨테이너를 띄우지 않았다


@NOT_ROOT
def test_as_invoking_user_refuses_an_unwritable_state_dir(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    state.chmod(0o555)
    try:
        result, capture = _run(tmp_path, extra_env={"SDC_RUN_AS_ROOT": "0"})
    finally:
        state.chmod(0o755)

    assert result.returncode == 73
    assert not capture.exists()

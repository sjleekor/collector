"""수집 wrapper가 종료 코드와 중단 신호(Cronicle abort = TERM)를 컨테이너까지 전하는지 확인한다.

가짜 `docker`·`docker compose`를 PATH 앞에 둔다. 실제 docker는 쓰지 않는다.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/us-derive-daily.sh"


def _setup(tmp_path: Path, compose_body: str) -> dict[str, str]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    compose = tmp_path / "compose"
    compose.write_text("#!/usr/bin/env bash\n" + compose_body)
    compose.chmod(0o755)
    docker = fake_bin / "docker"
    docker.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$*" >> "$DOCKER_CAPTURE_ARGS"\n'
        'case "$1" in inspect) exit 1 ;; *) exit 0 ;; esac\n'
    )
    docker.chmod(0o755)
    (tmp_path / "stock_data/us").mkdir(parents=True)
    return {
        **os.environ,
        "DOCKER_CAPTURE_ARGS": str(tmp_path / "docker-args.txt"),
        "STARTED_MARKER": str(tmp_path / "started"),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_LOCK_WAIT_SECONDS": "1",
        "SDC_STOP_GRACE_SECONDS": "2",
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }


@pytest.mark.parametrize("code", [0, 1, 73, 75])
def test_compose_exit_code_is_passed_through(tmp_path: Path, code: int) -> None:
    env = _setup(tmp_path, f"exit {code}\n")
    result = subprocess.run(
        [str(WRAPPER), "--dry-run"], env=env, text=True, capture_output=True, timeout=30
    )
    assert result.returncode == code, result.stderr + result.stdout


def test_lock_conflict_still_returns_75(tmp_path: Path) -> None:
    env = _setup(tmp_path, "exit 0\n")
    env["SDC_LOCK_BACKEND"] = "mkdir"
    (tmp_path / "locks/us.lock.d").mkdir(parents=True)
    result = subprocess.run(
        [str(WRAPPER), "--dry-run"], env=env, text=True, capture_output=True, timeout=30
    )
    assert result.returncode == 75, result.stdout


def test_term_stops_container_and_wrapper_exits(tmp_path: Path) -> None:
    # 가짜 compose는 TERM을 받으면 바로 끝난다 (compose run이 컨테이너 정지로 끝나는 것과 같다).
    env = _setup(
        tmp_path,
        'trap "kill \\$! 2>/dev/null; exit 143" TERM\n'
        'touch "$STARTED_MARKER"\n'
        "sleep 30 &\n"
        "wait\n",
    )
    proc = subprocess.Popen(
        [str(WRAPPER), "--dry-run"], env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    marker = Path(env["STARTED_MARKER"])
    deadline = time.time() + 10
    while not marker.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert marker.exists(), "fake compose did not start"
    started = time.time()
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=15)
    elapsed = time.time() - started
    assert proc.returncode == 143, out
    assert elapsed < 10, f"wrapper took {elapsed:.1f}s to stop"
    calls = Path(env["DOCKER_CAPTURE_ARGS"]).read_text().splitlines()
    assert "stop -t 2 sdc-collector-us" in calls
    assert "rm -f sdc-collector-us" in calls
    # 정상 경로와 달리 신호 경로의 정리가 순서대로 일어난다: 수거(inspect) -> stop -> rm
    assert calls.index("stop -t 2 sdc-collector-us") < calls.index("rm -f sdc-collector-us")

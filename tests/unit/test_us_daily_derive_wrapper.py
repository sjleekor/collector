"""US 일일 준비 wrapper가 필수 derived만 같은 `us` lock으로 실행하는지 확인한다."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/us-derive-daily.sh"


def test_daily_derive_uses_us_lock_and_keeps_monthly_universe_separate(tmp_path: Path) -> None:
    fake_compose = tmp_path / "compose"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    capture = tmp_path / "args.txt"
    docker_capture = tmp_path / "docker-args.txt"
    fake_compose.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" > \"$CAPTURE_ARGS\"\n"
    )
    fake_compose.chmod(0o755)
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$DOCKER_CAPTURE_ARGS\"\n"
        "case \"$1\" in inspect) exit 1 ;; *) exit 99 ;; esac\n"
    )
    fake_docker.chmod(0o755)
    environment = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "DOCKER_CAPTURE_ARGS": str(docker_capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_LOCK_WAIT_SECONDS": "1",
        "SDC_US_DERIVE_DAILY_BUDGET_SECONDS": "321",
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }
    result = subprocess.run([str(WRAPPER), "--dry-run"], env=environment, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert "--name sdc-collector-us" in args
    assert "us-derive run --tables" in args
    assert "--budget-seconds 321 --dry-run" in args
    assert "prices-daily" in args and "thirteenf" in args and "trading-calendar" in args
    assert "us-universe" not in args
    assert docker_capture.read_text().splitlines() == ["inspect sdc-collector-us"]

"""US universe incremental wrapper가 같은 `us` lock으로 `--if-new`를 기본 전달하는지 확인한다."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/us-universe-incremental.sh"


def _run(tmp_path: Path, *extra: str) -> tuple[subprocess.CompletedProcess[str], str, str]:
    fake_compose = tmp_path / "compose"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    capture = tmp_path / "args.txt"
    docker_capture = tmp_path / "docker-args.txt"
    fake_compose.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"$CAPTURE_ARGS\"\n")
    fake_compose.chmod(0o755)
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$DOCKER_CAPTURE_ARGS\"\n"
        "case \"$1\" in inspect) exit 1 ;; *) exit 99 ;; esac\n"
    )
    fake_docker.chmod(0o755)
    (tmp_path / "stock_data/us").mkdir(parents=True)
    environment = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "DOCKER_CAPTURE_ARGS": str(docker_capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_LOCK_WAIT_SECONDS": "1",
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }
    result = subprocess.run(
        [str(WRAPPER), *extra], env=environment, text=True, capture_output=True
    )
    return result, capture.read_text(), docker_capture.read_text()


# The `us.lock` file only exists under the flock backend; without flock(1) the wrapper
# falls back to a mkdir lock (sdc-wrapper.sh sdc_with_source_lock), e.g. on macOS.
@pytest.mark.skipif(shutil.which("flock") is None, reason="flock(1) not installed")
def test_incremental_wrapper_uses_us_lock_and_if_new(tmp_path: Path) -> None:
    result, args, docker_args = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "--name sdc-collector-us" in args
    assert "us-universe incremental --if-new" in args
    assert "--dry-run" not in args
    assert "rebuild" not in args
    assert (tmp_path / "locks" / "us.lock").exists()
    assert docker_args.splitlines() == ["inspect sdc-collector-us"]


def test_incremental_wrapper_passes_dry_run_through(tmp_path: Path) -> None:
    result, args, _docker_args = _run(tmp_path, "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "us-universe incremental --if-new --dry-run" in args


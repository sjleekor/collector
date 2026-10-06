"""US universe v2 rebuild wrapper 가 같은 `us` lock 으로 rebuild 만 전달하는지 본다."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/us-universe-v2-rebuild.sh"


def _run(tmp_path: Path, *extra: str) -> tuple[subprocess.CompletedProcess[str], str]:
    fake_compose = tmp_path / "compose"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    capture = tmp_path / "args.txt"
    fake_compose.write_text('#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" > "$CAPTURE_ARGS"\n')
    fake_compose.chmod(0o755)
    fake_docker.write_text(
        '#!/usr/bin/env bash\ncase "$1" in inspect) exit 1 ;; *) exit 99 ;; esac\n'
    )
    fake_docker.chmod(0o755)
    (tmp_path / "stock_data/us").mkdir(parents=True)
    environment = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_LOCK_WAIT_SECONDS": "1",
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }
    result = subprocess.run([str(WRAPPER), *extra], env=environment, text=True, capture_output=True)
    return result, capture.read_text()


def test_rebuild_wrapper_runs_v2_rebuild_in_the_us_lock_domain(tmp_path: Path) -> None:
    result, args = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "--name sdc-collector-us" in args
    assert "us-universe-v2 rebuild" in args
    assert "incremental" not in args and "--if-new" not in args


def test_rebuild_wrapper_passes_dry_run_and_start_through(tmp_path: Path) -> None:
    result, args = _run(tmp_path, "--dry-run", "--start", "2018-09-07")
    assert result.returncode == 0, result.stderr
    assert "us-universe-v2 rebuild --dry-run --start 2018-09-07" in args

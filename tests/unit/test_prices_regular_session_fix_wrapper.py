"""prices-regular-session-fix.sh: 20:00 시총 뒤에 도는 정규장 보정 wrapper (fake compose)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

WRAPPER = Path(__file__).resolve().parents[2] / "deploy/prod/bin/prices-regular-session-fix.sh"


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
    env = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_KRX_MARKETDATA_MIN_INTERVAL_SECONDS": "0",
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        **(extra_env or {}),
    }
    for key in ("REGULAR_FIX_LAST_TRADING_DAYS", "REGULAR_FIX_DRY_RUN", "SDC_LOCK_WAIT_SECONDS"):
        if key not in (extra_env or {}):
            env.pop(key, None)
    result = subprocess.run([str(WRAPPER), *(args or [])], env=env, text=True, capture_output=True)
    return result, capture


def test_default_covers_last_ten_trading_days(tmp_path: Path) -> None:
    result, capture = _run(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "prices fix-regular-session --last-trading-days 10" in capture.read_text()


def test_env_overrides_window_and_dry_run(tmp_path: Path) -> None:
    result, capture = _run(
        tmp_path, extra_env={"REGULAR_FIX_LAST_TRADING_DAYS": "5", "REGULAR_FIX_DRY_RUN": "1"}
    )
    assert result.returncode == 0, result.stdout + result.stderr
    line = capture.read_text().splitlines()[-1]
    assert "--last-trading-days 5" in line and line.rstrip().endswith("--dry-run")


def test_explicit_args_replace_the_default_window(tmp_path: Path) -> None:
    result, capture = _run(tmp_path, ["--start", "2026-09-14", "--end", "2026-09-18"])
    assert result.returncode == 0, result.stdout + result.stderr
    line = capture.read_text().splitlines()[-1]
    assert "--start 2026-09-14 --end 2026-09-18" in line and "--last-trading-days" not in line


def test_source_defaults_to_combined_and_is_overridable(tmp_path: Path) -> None:
    _, capture = _run(tmp_path)
    assert "--source combined" in capture.read_text()
    (tmp_path / "args.txt").unlink()
    _, capture = _run(
        tmp_path, extra_env={"REGULAR_FIX_SOURCE": "market-cap", "REGULAR_FIX_ALLOW_EARLY_MDC": "1"}
    )
    line = capture.read_text().splitlines()[-1]
    assert "--source market-cap" in line and "--allow-early-mdc" in line

"""``kr-price-vintage-trigger.sh``: read-only list job; runs as the calling user (fake compose)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

WRAPPER = Path(__file__).resolve().parents[2] / "deploy/prod/bin/kr-price-vintage-trigger.sh"


def _run(
    tmp_path: Path, args: list[str], extra_env: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], Path]:
    fake_compose = tmp_path / "compose"
    capture = tmp_path / "args.txt"
    fake_compose.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$CAPTURE"\nexit 0\n')
    fake_compose.chmod(0o755)
    base = {
        k: v for k, v in os.environ.items() if k not in ("SDC_RUN_AS_ROOT", "SDC_RUN_EXTRA_ARGS")
    }
    env = {
        **base,
        "CAPTURE": str(capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
        **(extra_env or {}),
    }
    result = subprocess.run([str(WRAPPER), *args], env=env, text=True, capture_output=True)
    return result, capture


def test_runs_ops_command_with_output_dir_and_passes_args_through(tmp_path: Path) -> None:
    result, capture = _run(tmp_path, ["--lookback-days", "30"])
    assert result.returncode == 0, result.stdout + result.stderr
    out_dir = tmp_path / "stock_data/kr/output/price_vintage_trigger"
    assert out_dir.is_dir()
    line = capture.read_text()
    assert f"ops price-vintage-trigger --output-dir {out_dir} --lookback-days 30" in line
    assert f"-v {out_dir}:{out_dir}" in line


def test_container_runs_as_invoking_user(tmp_path: Path) -> None:
    _, capture = _run(tmp_path, [])
    assert f"--user {os.getuid()}:{os.getgid()}" in capture.read_text()


def test_root_escape_hatch_drops_user_flag(tmp_path: Path) -> None:
    _, capture = _run(tmp_path, [], {"SDC_RUN_AS_ROOT": "1"})
    assert "--user" not in capture.read_text()


def test_does_not_start_a_refetch(tmp_path: Path) -> None:
    _, capture = _run(tmp_path, [])
    text = capture.read_text()
    assert "prices" not in text and "--refetch" not in text


def test_exits_73_when_output_dir_is_not_writable(tmp_path: Path) -> None:
    out_dir = tmp_path / "ro"
    out_dir.mkdir()
    out_dir.chmod(0o500)
    try:
        if os.access(out_dir, os.W_OK):  # root 로 돌면 막을 수 없다
            return
        result, capture = _run(tmp_path, [], {"KR_VINTAGE_TRIGGER_DIR": str(out_dir)})
        assert result.returncode == 73
        assert not capture.exists()
    finally:
        out_dir.chmod(0o700)

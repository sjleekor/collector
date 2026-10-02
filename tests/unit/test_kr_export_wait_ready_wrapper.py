"""Polling wrapper around ``collector ops kr-export-readiness`` (fake compose)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

WRAPPER = Path(__file__).resolve().parents[2] / "deploy/prod/bin/kr-export-wait-ready.sh"


def _run(tmp_path: Path, args: list[str], codes: list[int]):
    fake_compose = tmp_path / "compose"
    counter = tmp_path / "count"
    capture = tmp_path / "args.txt"
    fake_compose.write_text(
        "#!/usr/bin/env bash\n"
        'n=$(cat "$COUNTER" 2>/dev/null || echo 0)\n'
        'echo $((n + 1)) > "$COUNTER"\n'
        'printf "%s\\n" "$*" >> "$CAPTURE"\n'
        'IFS=, read -r -a codes <<< "$CODES"\n'
        "idx=$n; ((idx >= ${#codes[@]})) && idx=$((${#codes[@]} - 1))\n"
        'exit "${codes[$idx]}"\n'
    )
    fake_compose.chmod(0o755)
    env = {
        **os.environ,
        "COUNTER": str(counter),
        "CAPTURE": str(capture),
        "CODES": ",".join(map(str, codes)),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "KR_EXPORT_READY_EVIDENCE_DIR": str(tmp_path / "evidence"),
    }
    result = subprocess.run([str(WRAPPER), *args], env=env, text=True, capture_output=True)
    calls = int(counter.read_text()) if counter.exists() else 0
    return result, calls, capture


K_ARGS = ["--feature-asof-date", "2026-10-01"]


def test_ready_first_try(tmp_path: Path) -> None:
    result, calls, capture = _run(tmp_path, K_ARGS, [0])
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls == 1
    line = capture.read_text()
    assert "ops kr-export-readiness --feature-asof-date 2026-10-01 --output" in line
    assert "K=2026-10-01.json" in line


def test_polls_until_ready(tmp_path: Path) -> None:
    result, calls, _ = _run(
        tmp_path, [*K_ARGS, "--interval-seconds", "1", "--deadline-seconds", "10"], [75, 75, 0]
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls == 3


def test_deadline_returns_not_yet(tmp_path: Path) -> None:
    result, calls, _ = _run(
        tmp_path, [*K_ARGS, "--interval-seconds", "1", "--deadline-seconds", "0"], [75]
    )
    assert result.returncode == 75
    assert calls == 1


def test_blocked_stops_immediately(tmp_path: Path) -> None:
    result, calls, _ = _run(
        tmp_path, [*K_ARGS, "--interval-seconds", "1", "--deadline-seconds", "10"], [1, 0]
    )
    assert result.returncode == 1
    assert calls == 1


def test_poll_through_blocked(tmp_path: Path) -> None:
    result, calls, _ = _run(
        tmp_path,
        [*K_ARGS, "--interval-seconds", "1", "--deadline-seconds", "10", "--poll-through-blocked"],
        [1, 0],
    )
    assert result.returncode == 0
    assert calls == 2


def test_passthrough_args_and_usage_errors(tmp_path: Path) -> None:
    result, _, capture = _run(tmp_path, [*K_ARGS, "--", "--min-ticker-ratio", "0.9"], [0])
    assert result.returncode == 0
    assert capture.read_text().strip().endswith("--min-ticker-ratio 0.9")
    other = tmp_path / "other"
    other.mkdir()
    bad, calls, _ = _run(other, [], [0])
    assert bad.returncode == 2 and calls == 0

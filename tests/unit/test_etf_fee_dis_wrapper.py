"""etf-fee-dis-sync.sh — 환경값이 명령 인자가 되는지, 자기 락 도메인, import 볼륨."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

BIN = Path(__file__).resolve().parents[2] / "deploy/prod/bin"
W = "etf-fee-dis-sync.sh"


def _run(tmp_path: Path, env: dict[str, str]) -> tuple[int, str, str]:
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
    capture.unlink(missing_ok=True)
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
    for key in os.environ:
        if key.startswith("ETF_FEE_DIS_") and key not in env:
            environment.pop(key, None)
    environment.pop("SDC_RUN_AS_ROOT", None)
    done = subprocess.run([str(BIN / W)], env=environment, text=True, capture_output=True)
    text = capture.read_text() if capture.exists() else ""
    return done.returncode, text, done.stdout + done.stderr


def test_default_mode_is_the_light_sync_with_its_own_lock(tmp_path: Path) -> None:
    code, captured, out = _run(tmp_path, {})
    assert code == 0, out
    assert "etf-fee-dis sync --discovery latest --max-calls 30" in captured
    assert f"--user {os.getuid()}:{os.getgid()}" in captured
    assert "lock acquired: domain=kofia_dis" in out
    assert "domain=krx_baseline" not in out and "domain=seibro" not in out


def test_discovery_switch_and_validation(tmp_path: Path) -> None:
    code, captured, _ = _run(tmp_path, {"ETF_FEE_DIS_DISCOVERY": "direct"})
    assert code == 0 and "--discovery direct" in captured
    assert _run(tmp_path, {"ETF_FEE_DIS_DISCOVERY": "nope"})[0] == 2


def test_backfill_needs_a_month_range_and_reobserve_needs_a_run_id(tmp_path: Path) -> None:
    assert _run(tmp_path, {"ETF_FEE_DIS_MODE": "backfill"})[0] == 2
    base = {
        "ETF_FEE_DIS_MODE": "backfill",
        "ETF_FEE_DIS_START": "2026-05",
        "ETF_FEE_DIS_END": "2026-08",
    }
    code, captured, out = _run(tmp_path, base)
    assert code == 0, out
    assert "etf-fee-dis backfill --from 2026-05 --to 2026-08 --mode fill" in captured
    assert _run(tmp_path, {**base, "ETF_FEE_DIS_FILL_MODE": "reobserve"})[0] == 2
    code, captured, _ = _run(
        tmp_path,
        {
            **base,
            "ETF_FEE_DIS_FILL_MODE": "reobserve",
            "ETF_FEE_DIS_RUN_ID": "r1",
            "ETF_FEE_DIS_MAX_CALLS": "50",
        },
    )
    assert code == 0 and "--mode reobserve --run-id r1 --max-calls 50" in captured


def test_import_path_volume_handling(tmp_path: Path) -> None:
    env = {
        "ETF_FEE_DIS_MODE": "import-research",
        "ETF_FEE_DIS_IMPORT_PATH": "/home/whi/data/research_refs/x/dis_fee",
        "ETF_FEE_DIS_EXPECT_SHA256": "ab" * 32,
    }
    code, captured, out = _run(tmp_path, env)
    assert code == 0, out
    assert "-v /home/whi/data/research_refs/x/dis_fee:/import:ro" in captured
    assert "import-research --path /import --expect-manifest-sha256" in captured
    inside = {**env, "ETF_FEE_DIS_IMPORT_PATH": f"{tmp_path}/stock_data/kr/output/copy"}
    code, captured, _ = _run(tmp_path, inside)
    assert code == 0 and "--path /stock_data/kr/output/copy" in captured
    assert _run(tmp_path, {"ETF_FEE_DIS_MODE": "import-research"})[0] == 2


def test_verify_takes_no_lock_and_unknown_mode_exits_2(tmp_path: Path) -> None:
    code, captured, out = _run(tmp_path, {"ETF_FEE_DIS_MODE": "verify"})
    assert code == 0 and "etf-fee-dis verify" in captured and "lock acquired" not in out
    assert _run(tmp_path, {"ETF_FEE_DIS_MODE": "nope"})[0] == 2

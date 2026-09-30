"""KR raw export wrapper, container 이미지, --direct-db 계약을 정적으로 확인한다."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = COLLECTOR_ROOT / "deploy/prod/bin/kr-raw-parquet-export.sh"
EXPORT_ALL = COLLECTOR_ROOT / "bin/raw-parquet-export-all.sh"
DOCKERFILE = COLLECTOR_ROOT / "Dockerfile"
FAKE_EXPORTER = COLLECTOR_ROOT / "tests/shell/fixtures/fake-raw-parquet-exporter.py"


def _run_wrapper(tmp_path: Path, args: list[str], extra_env: dict[str, str] | None = None):
    fake_compose = tmp_path / "compose"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    fake_docker = fake_bin / "docker"
    capture = tmp_path / "args.txt"
    docker_capture = tmp_path / "docker-args.txt"
    fake_compose.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"$CAPTURE_ARGS\"\n")
    fake_compose.chmod(0o755)
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$DOCKER_CAPTURE_ARGS\"\n"
        'case "$1" in inspect) exit 1 ;; *) exit 99 ;; esac\n'
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
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        **(extra_env or {}),
    }
    result = subprocess.run(
        [str(WRAPPER), *args], env=environment, text=True, capture_output=True
    )
    return result, capture, docker_capture


def test_wrapper_runs_direct_route_with_fixed_safe_options(tmp_path: Path) -> None:
    result, capture, docker_capture = _run_wrapper(
        tmp_path, ["--snapshot-date", "2026-09-30", "--dry-run"]
    )
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert "run --rm --name sdc-collector-kr_raw_export" in args
    assert "--entrypoint /app/bin/raw-parquet-export-all.sh" in args
    assert (
        "--route remote --direct-db --jobs 1 --no-build "
        "--snapshot-date 2026-09-30 --dry-run"
    ) in args
    assert "--force" not in args and "--no-validate" not in args
    assert docker_capture.read_text().splitlines() == ["inspect sdc-collector-kr_raw_export"]


def test_wrapper_defaults_snapshot_date_and_omits_dry_run(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(tmp_path, [])
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert re.search(r"--snapshot-date \d{4}-\d{2}-\d{2}\b", args)
    assert "--dry-run" not in args


def test_wrapper_defaults_to_host_path_mode(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(tmp_path, ["--snapshot-date", "2026-09-30"])
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert "-v /home/whi/data/stock_data:/home/whi/data/stock_data" in args
    assert (
        "-e SDC_RAW_PARQUET_OUTPUT_ROOT=/home/whi/data/stock_data/kr/raw/raw_postgres" in args
    )


def test_wrapper_container_path_mode_can_be_selected(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(
        tmp_path, ["--snapshot-date", "2026-09-30"], {"SDC_KR_EXPORT_HOST_PATHS": "0"}
    )
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert "SDC_RAW_PARQUET_OUTPUT_ROOT" not in args


def test_wrapper_host_path_mode_mounts_same_path(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(
        tmp_path,
        ["--snapshot-date", "2026-09-30"],
        {"SDC_KR_EXPORT_HOST_PATHS": "1", "STOCK_DATA_HOST_DIR": "/srv/sd"},
    )
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert "-v /srv/sd:/srv/sd" in args
    assert "-e SDC_RAW_PARQUET_OUTPUT_ROOT=/srv/sd/kr/raw/raw_postgres" in args


@pytest.mark.parametrize(
    "bad",
    [
        ["--force"],
        ["--force-table", "daily_ohlcv"],
        ["--route", "local"],
        ["--jobs", "3"],
        ["--no-validate"],
        ["--snapshot-date", "20260930"],
        ["--snapshot-date"],
    ],
)
def test_wrapper_rejects_unsafe_or_invalid_options(tmp_path: Path, bad: list[str]) -> None:
    result, capture, _ = _run_wrapper(tmp_path, bad)
    assert result.returncode == 2
    assert not capture.exists()


def _run_export_all(tmp_path: Path, extra: list[str], env: dict[str, str]):
    environment = {
        **os.environ,
        "SDC_APP_DIR": str(COLLECTOR_ROOT),
        "SDC_RAW_PARQUET_BIN": str(FAKE_EXPORTER),
        "SDC_RAW_PARQUET_OUTPUT_ROOT": str(tmp_path / "raw_postgres"),
        "STOCK_DATA_ROOT": str(tmp_path),
        "FAKE_EXPORTER_RUNTIME_CAPTURE": str(tmp_path / "runtime.toml"),
        # --no-build 는 cargo 를 찾지 않아야 한다. bash 4+ 가 앞서도록 둔다.
        "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
        **env,
    }
    return subprocess.run(
        [str(EXPORT_ALL), "--no-build", "--dry-run", "--snapshot-date", "2026-09-30", *extra],
        env=environment,
        text=True,
        capture_output=True,
    )


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq 없음: 운영 이미지는 jq 를 설치한다")
def test_direct_db_uses_db_env_and_keeps_remote_contract(tmp_path: Path) -> None:
    result = _run_export_all(
        tmp_path,
        ["--route", "remote", "--direct-db", "--jobs", "1"],
        {"DB_HOST": "db", "DB_PASSWORD": "x"},
    )
    assert result.returncode == 0, result.stderr + result.stdout
    runtime = (tmp_path / "runtime.toml").read_text()
    assert 'name = "sj2_remote"' in runtime
    assert 'dsn_env = ""' in runtime
    assert "DB_PASSWORD" not in result.stdout and "DB_PASSWORD" not in result.stderr


@pytest.mark.parametrize("host", ["", "localhost", "127.0.0.1"])
def test_direct_db_refuses_local_host(tmp_path: Path, host: str) -> None:
    result = _run_export_all(
        tmp_path, ["--route", "remote", "--direct-db"], {"DB_HOST": host, "DB_PASSWORD": "x"}
    )
    assert result.returncode == 2
    assert "sj2_remote" in result.stderr


def test_direct_db_requires_remote_route(tmp_path: Path) -> None:
    result = _run_export_all(
        tmp_path, ["--route", "local", "--direct-db"], {"DB_HOST": "db", "DB_PASSWORD": "x"}
    )
    assert result.returncode == 2


def test_dockerfile_builds_exporter_locked_and_ships_binary() -> None:
    text = DOCKERFILE.read_text()
    assert re.search(r"^FROM rust:\d+\.\d+(\.\d+)?-bookworm AS raw-parquet-exporter$", text, re.M)
    assert "cargo build --locked --release" in text
    assert re.search(r"^COPY [^\n]*tools/raw-parquet-exporter/Cargo\.lock", text, re.M)
    assert (
        "COPY --from=raw-parquet-exporter /usr/local/bin/raw-parquet-exporter "
        "/usr/local/bin/raw-parquet-exporter"
    ) in text
    assert "SDC_RAW_PARQUET_BIN=/usr/local/bin/raw-parquet-exporter" in text
    assert re.search(r"apt-get install[^\n]*\bjq\b", text)
    assert "COPY bin/raw-parquet-export-all.sh" in text
    assert "export_tables.toml" in text
    # builder(bookworm, glibc 2.36)는 최종 이미지(python:3.12-slim, trixie)보다
    # 오래된 glibc 여야 한다.
    assert "FROM python:3.12-slim" in text
    assert "tools/raw-parquet-exporter/target" in (COLLECTOR_ROOT / ".dockerignore").read_text()

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
        # 쓰기 가능성 사전 검사가 맥·CI의 실제 경로를 보지 않도록 격리한다.
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
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


def test_wrapper_consistent_snapshot_is_opt_in(tmp_path: Path) -> None:
    off = tmp_path / "off"
    off.mkdir()
    result, capture, _ = _run_wrapper(off, ["--snapshot-date", "2026-09-30"])
    assert result.returncode == 0, result.stderr
    assert "--consistent-snapshot" not in capture.read_text()

    on = tmp_path / "on"
    on.mkdir()
    result, capture, _ = _run_wrapper(
        on, ["--snapshot-date", "2026-09-30", "--consistent-snapshot"]
    )
    assert result.returncode == 0, result.stderr
    assert "--no-build --snapshot-date 2026-09-30 --consistent-snapshot" in capture.read_text()


def test_wrapper_consistent_snapshot_env_and_invalid_value(tmp_path: Path) -> None:
    env_on = tmp_path / "env_on"
    env_on.mkdir()
    result, capture, _ = _run_wrapper(env_on, [], {"SDC_KR_EXPORT_CONSISTENT_SNAPSHOT": "1"})
    assert result.returncode == 0, result.stderr
    assert "--consistent-snapshot" in capture.read_text()

    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    bad, capture2, _ = _run_wrapper(bad_dir, [], {"SDC_KR_EXPORT_CONSISTENT_SNAPSHOT": "yes"})
    assert bad.returncode == 2
    assert not capture2.exists()


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
    host = tmp_path / "stock_data"
    assert f"-v {host}:{host}" in args
    assert f"-e SDC_RAW_PARQUET_OUTPUT_ROOT={host}/kr/raw/raw_postgres" in args


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
        {"SDC_KR_EXPORT_HOST_PATHS": "1", "STOCK_DATA_HOST_DIR": str(tmp_path / "sd")},
    )
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    sd = tmp_path / "sd"
    assert f"-v {sd}:{sd}" in args
    assert f"-e SDC_RAW_PARQUET_OUTPUT_ROOT={sd}/kr/raw/raw_postgres" in args


def test_wrapper_runs_container_as_invoking_user(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(tmp_path, ["--snapshot-date", "2026-09-30"])
    assert result.returncode == 0, result.stderr
    args = capture.read_text()
    assert f"--user {os.getuid()}:{os.getgid()} -e HOME=/tmp" in args
    # 마운트·출력 루트 옵션 뒤에 붙고, 서비스 이름 앞에 온다
    assert args.index("-v ") < args.index("--user ") < args.index("collector --route")


def test_wrapper_run_as_root_escape_hatch(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(
        tmp_path, ["--snapshot-date", "2026-09-30"], {"SDC_KR_EXPORT_RUN_AS_ROOT": "1"}
    )
    assert result.returncode == 0, result.stderr
    assert "--user" not in capture.read_text()


def test_wrapper_rejects_invalid_run_as_root_value(tmp_path: Path) -> None:
    result, capture, _ = _run_wrapper(
        tmp_path, ["--snapshot-date", "2026-09-30"], {"SDC_KR_EXPORT_RUN_AS_ROOT": "yes"}
    )
    assert result.returncode == 2
    assert not capture.exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root는 권한 검사를 통과한다")
def test_wrapper_fails_early_on_unwritable_existing_snapshot_dir(tmp_path: Path) -> None:
    part = (
        tmp_path
        / "stock_data/kr/raw/raw_postgres/snapshot_date=2026-09-30/source=sj2_remote/daily_ohlcv"
    )
    part.mkdir(parents=True)
    part.chmod(0o555)  # root가 만든 디렉터리처럼 쓸 수 없다
    try:
        result, capture, _ = _run_wrapper(tmp_path, ["--snapshot-date", "2026-09-30"])
        assert result.returncode == 73
        assert "not writable" in result.stdout
        assert "daily_ohlcv" in result.stdout
        assert not capture.exists()  # 컨테이너를 띄우지 않았다
        # 탈출구는 검사 없이 통과한다
        ok, capture2, _ = _run_wrapper(
            tmp_path, ["--snapshot-date", "2026-09-30"], {"SDC_KR_EXPORT_RUN_AS_ROOT": "1"}
        )
        assert ok.returncode == 0, ok.stderr
        assert capture2.exists()
        # --dry-run은 아무것도 쓰지 않으므로 검사하지 않는다
        dry, _, _ = _run_wrapper(tmp_path, ["--snapshot-date", "2026-09-30", "--dry-run"])
        assert dry.returncode == 0, dry.stderr
    finally:
        part.chmod(0o755)


@pytest.mark.skipif(os.geteuid() == 0, reason="root는 권한 검사를 통과한다")
def test_wrapper_fails_early_when_raw_root_is_unwritable(tmp_path: Path) -> None:
    raw = tmp_path / "stock_data/kr/raw"
    raw.mkdir(parents=True)
    raw.chmod(0o555)  # snapshot 디렉터리를 새로 만들 수 없다
    try:
        result, capture, _ = _run_wrapper(tmp_path, ["--snapshot-date", "2026-09-30"])
        assert result.returncode == 73
        assert not capture.exists()
    finally:
        raw.chmod(0o755)


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

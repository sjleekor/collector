"""US wrapper가 컨테이너를 호출 사용자 uid:gid로 돌리는지, 쓸 수 없으면 73으로 끝나는지 본다."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
BIN = COLLECTOR_ROOT / "deploy/prod/bin"
US_WRAPPERS = [
    "us-daily.sh",
    "us-derive-daily.sh",
    "us-derive.sh",
    "us-nasdaq-analyst.sh",
    "us-universe-incremental.sh",
]
ROOT_ONLY = pytest.mark.skipif(os.geteuid() == 0, reason="root는 권한 검사를 통과한다")


def _run(
    tmp_path: Path,
    wrapper: str,
    args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    fake_compose = tmp_path / "compose"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    capture = tmp_path / "args.txt"
    fake_compose.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"$CAPTURE_ARGS\"\n")
    fake_compose.chmod(0o755)
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        '#!/usr/bin/env bash\ncase "$1" in inspect) exit 1 ;; *) exit 99 ;; esac\n'
    )
    fake_docker.chmod(0o755)
    environment = {
        **os.environ,
        "CAPTURE_ARGS": str(capture),
        "SDC_APP_DIR": str(tmp_path),
        "SDC_DOCKER_COMPOSE_CMD": str(fake_compose),
        "SDC_LOCK_DIR": str(tmp_path / "locks"),
        "SDC_THROTTLE_DIR": str(tmp_path / "throttle"),
        "SDC_LOCK_WAIT_SECONDS": "1",
        # 쓰기 가능성 사전 검사가 실제 레이크 경로를 보지 않게 격리한다.
        "STOCK_DATA_HOST_DIR": str(tmp_path / "stock_data"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        **(extra_env or {}),
    }
    for key in ("SDC_RUN_AS_ROOT", "SDC_KR_EXPORT_RUN_AS_ROOT"):
        if key not in (extra_env or {}):
            environment.pop(key, None)
    result = subprocess.run(
        [str(BIN / wrapper), *(args or [])], env=environment, text=True, capture_output=True
    )
    return result, capture


def _lake(tmp_path: Path) -> Path:
    lake = tmp_path / "stock_data/us"
    subs = ("raw/nasdaq/analyst_earnings_forecast", "derived/snapshots/prices_daily", "output/_tmp")
    for sub in subs:
        (lake / sub).mkdir(parents=True, exist_ok=True)
    return lake


@pytest.mark.parametrize("wrapper", US_WRAPPERS)
def test_container_runs_as_invoking_user(tmp_path: Path, wrapper: str) -> None:
    _lake(tmp_path)
    result, capture = _run(tmp_path, wrapper)
    assert result.returncode == 0, result.stdout + result.stderr
    # us-derive.sh는 컨테이너를 두 번 돌린다(derive, tickers sync). 모두 --user가 있어야 한다.
    lines = capture.read_text().splitlines()
    assert lines
    for line in lines:
        assert f"--user {os.getuid()}:{os.getgid()} -e HOME=/tmp" in line
        assert line.index("--user ") < line.index("collector ")


@pytest.mark.parametrize("wrapper", US_WRAPPERS)
def test_run_as_root_escape_hatch(tmp_path: Path, wrapper: str) -> None:
    _lake(tmp_path)
    result, capture = _run(tmp_path, wrapper, extra_env={"SDC_RUN_AS_ROOT": "1"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--user" not in capture.read_text()
    assert "image default (root)" in result.stdout


@pytest.mark.parametrize("wrapper", US_WRAPPERS)
def test_legacy_kr_variable_does_not_affect_us_wrappers(tmp_path: Path, wrapper: str) -> None:
    _lake(tmp_path)
    result, capture = _run(tmp_path, wrapper, extra_env={"SDC_KR_EXPORT_RUN_AS_ROOT": "1"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"--user {os.getuid()}:{os.getgid()}" in capture.read_text()


@pytest.mark.parametrize("wrapper", US_WRAPPERS)
def test_rejects_invalid_run_as_root_value(tmp_path: Path, wrapper: str) -> None:
    _lake(tmp_path)
    result, capture = _run(tmp_path, wrapper, extra_env={"SDC_RUN_AS_ROOT": "yes"})
    assert result.returncode == 2
    assert not capture.exists()


@ROOT_ONLY
@pytest.mark.parametrize(
    ("wrapper", "bad_dir"),
    [
        ("us-daily.sh", "raw/nasdaq/analyst_earnings_forecast"),
        ("us-derive-daily.sh", "derived/snapshots/prices_daily"),
        ("us-derive.sh", "output/_tmp"),
        ("us-nasdaq-analyst.sh", "raw/nasdaq/analyst_earnings_forecast"),
        ("us-universe-incremental.sh", "derived/snapshots/prices_daily"),
    ],
)
def test_unwritable_lake_directory_fails_early_with_73(
    tmp_path: Path, wrapper: str, bad_dir: str
) -> None:
    lake = _lake(tmp_path)
    target = lake / bad_dir
    target.chmod(0o555)  # root가 만든 디렉터리처럼 쓸 수 없다
    try:
        result, capture = _run(tmp_path, wrapper)
        assert result.returncode == 73, result.stdout + result.stderr
        assert "not writable" in result.stdout
        assert str(target) in result.stdout
        assert "SDC_RUN_AS_ROOT=1" in result.stdout
        assert not capture.exists()  # 컨테이너를 띄우지 않았다
        # 탈출구는 검사 없이 통과한다
        ok, capture2 = _run(tmp_path, wrapper, extra_env={"SDC_RUN_AS_ROOT": "1"})
        assert ok.returncode == 0, ok.stdout + ok.stderr
        assert capture2.exists()
        # --dry-run은 아무것도 쓰지 않으므로 검사하지 않는다
        dry, _ = _run(tmp_path, wrapper, ["--dry-run"])
        assert dry.returncode == 0, dry.stdout + dry.stderr
    finally:
        target.chmod(0o755)


@ROOT_ONLY
def test_files_owned_by_another_uid_are_checked_for_read_write(tmp_path: Path) -> None:
    # 실제 사고는 root 소유 0600 파일(dolt .dolt/noms)이다. 테스트는 root가 아니라서 가짜 `id`로
    # 호출 uid를 바꿔, 파일이 "남의 소유"로 보이게 한다. 남의 파일이라도 읽고 쓸 수 있으면 통과한다.
    lake = _lake(tmp_path)
    noms = lake / "raw/dolt/stocks/.dolt/noms"
    noms.mkdir(parents=True)
    manifest = noms / "manifest"
    manifest.write_text("x")
    fake_bin = tmp_path / "id-bin"
    fake_bin.mkdir()
    fake_id = fake_bin / "id"
    fake_id.write_text(
        '#!/bin/sh\ncase "$1" in -un) echo other ;; *) echo 54321 ;; esac\n'
    )
    fake_id.chmod(0o755)
    env = {"PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"}
    ok, first_capture = _run(tmp_path, "us-daily.sh", extra_env=env)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    first_capture.unlink()
    manifest.chmod(0o000)  # 소유자 전용 0600을 다른 uid가 만난 것과 같다 (읽을 수 없다)
    try:
        result, capture = _run(tmp_path, "us-daily.sh", extra_env=env)
        assert result.returncode == 73
        assert str(manifest) in result.stdout
        assert not capture.exists()
    finally:
        manifest.chmod(0o644)


@ROOT_ONLY
def test_owned_directory_without_write_permission_fails_early(tmp_path: Path) -> None:
    lake = _lake(tmp_path)
    nested = lake / "raw/sec/ftd/inner"
    nested.mkdir(parents=True)
    nested.chmod(0o555)
    try:
        result, capture = _run(tmp_path, "us-daily.sh")
        assert result.returncode == 73
        assert str(nested) in result.stdout
        assert not capture.exists()
    finally:
        nested.chmod(0o755)


@ROOT_ONLY
def test_missing_lake_checks_nearest_existing_parent(tmp_path: Path) -> None:
    # 레이크가 아직 없으면(새 호스트) 가장 가까운 상위 디렉터리가 쓸 수 있어야 한다.
    (tmp_path / "stock_data").mkdir()
    (tmp_path / "stock_data").chmod(0o555)
    try:
        result, capture = _run(tmp_path, "us-daily.sh")
        assert result.returncode == 73
        assert not capture.exists()
    finally:
        (tmp_path / "stock_data").chmod(0o755)


@pytest.mark.skipif(shutil.which("find") is None, reason="find not available")
def test_us_nasdaq_wrapper_checks_only_its_subtree(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        return
    lake = _lake(tmp_path)
    other = lake / "derived/snapshots/prices_daily"
    other.chmod(0o555)  # nasdaq 잡이 안 쓰는 곳이라 막으면 안 된다
    try:
        result, _ = _run(tmp_path, "us-nasdaq-analyst.sh")
        assert result.returncode == 0, result.stdout + result.stderr
    finally:
        other.chmod(0o755)

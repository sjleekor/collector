"""``bin/raw-parquet-export-all.sh --consistent-snapshot`` with the fake exporter.

The holder, the per-table ``--pg-snapshot`` hand-off, the same-snapshot resume
rule, and the ``_SUCCESS.json`` fields are exercised without PostgreSQL. The
real SET TRANSACTION SNAPSHOT path needs a database; see
``tests/integration/test_raw_export_pg_snapshot.py`` (skipped without
``RAW_EXPORT_TEST_DSN``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

COLLECTOR_ROOT = Path(__file__).resolve().parents[2]
EXPORT_ALL = COLLECTOR_ROOT / "bin/raw-parquet-export-all.sh"
FAKE_EXPORTER = COLLECTOR_ROOT / "tests/shell/fixtures/fake-raw-parquet-exporter.py"
SNAPSHOT_DATE = "2026-09-30"

pytestmark = pytest.mark.skipif(
    shutil.which("jq") is None, reason="jq 없음: 운영 이미지는 jq 를 설치한다"
)


def _run(tmp_path: Path, args: list[str], extra_env: dict[str, str] | None = None):
    env = {
        **os.environ,
        "SDC_APP_DIR": str(COLLECTOR_ROOT),
        "SDC_RAW_PARQUET_BIN": str(FAKE_EXPORTER),
        "SDC_RAW_PARQUET_OUTPUT_ROOT": str(tmp_path / "raw_postgres"),
        "STOCK_DATA_ROOT": str(tmp_path),
        "FAKE_EXPORTER_CALL_LOG": str(tmp_path / "calls.log"),
        "FAKE_EXPORTER_HOLDER_STATE": str(tmp_path / "holder.state"),
        "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
        **(extra_env or {}),
    }
    return subprocess.run(
        [
            str(EXPORT_ALL),
            "--no-build",
            "--snapshot-date",
            SNAPSHOT_DATE,
            "--route",
            "local",
            "--jobs",
            "2",
            *args,
        ],
        env=env,
        text=True,
        capture_output=True,
        timeout=120,
    )


def _manifests_dir(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "raw_postgres"
        / f"snapshot_date={SNAPSHOT_DATE}"
        / "source=local_mydb"
        / "_manifests"
    )


def _success(tmp_path: Path) -> dict:
    return json.loads((_manifests_dir(tmp_path) / "_SUCCESS.json").read_text())


def _calls(tmp_path: Path) -> list[str]:
    log = tmp_path / "calls.log"
    return log.read_text().splitlines() if log.exists() else []


def test_default_is_unchanged(tmp_path: Path) -> None:
    result = _run(tmp_path, [])
    assert result.returncode == 0, result.stderr + result.stdout
    marker = _success(tmp_path)
    assert marker["snapshot_policy"] == "read_committed_per_chunk"
    assert "pg_snapshot_id" not in marker
    assert not any(c.startswith("snapshot-hold") for c in _calls(tmp_path))
    manifest = json.loads(
        (_manifests_dir(tmp_path) / "table_manifests/daily_ohlcv.json").read_text()
    )
    assert manifest["source"]["snapshot_policy"] == "per_chunk_read_committed"


def test_consistent_snapshot_records_policy_and_id_everywhere(tmp_path: Path) -> None:
    result = _run(tmp_path, ["--consistent-snapshot"])
    assert result.returncode == 0, result.stderr + result.stdout
    marker = _success(tmp_path)
    assert marker["snapshot_policy"] == "repeatable_read_exported_snapshot"
    assert marker["pg_snapshot_id"] == "00000003-0000002A-1"
    for entry in marker["tables"].values():
        manifest = json.loads(Path(entry["manifest_path"]).read_text())
        assert manifest["source"]["pg_snapshot_id"] == "00000003-0000002A-1"
        assert manifest["source"]["snapshot_policy"] == "repeatable_read_exported_snapshot"
    assert [c for c in _calls(tmp_path) if c.startswith("snapshot-hold")] == [
        "snapshot-hold 00000003-0000002A-1"
    ]
    # Holder is stopped when the run ends: the snapshot must not outlive the export.
    assert not (tmp_path / "holder.state").exists()


def test_rerun_on_same_snapshot_skips_but_new_snapshot_reexports(tmp_path: Path) -> None:
    assert _run(tmp_path, ["--consistent-snapshot"]).returncode == 0

    same = _run(tmp_path, ["--consistent-snapshot"])
    assert same.returncode == 0, same.stderr + same.stdout
    # Same fake id again == same snapshot id: completed tables are reused.
    assert "Skipping daily_ohlcv (valid manifest on snapshot 00000003-0000002A-1)" in same.stdout

    other = _run(
        tmp_path,
        ["--consistent-snapshot"],
        {"FAKE_EXPORTER_SNAPSHOT_ID": "00000005-0000003B-1"},
    )
    assert other.returncode == 0, other.stderr + other.stdout
    assert "Skipping" not in other.stdout
    assert "Exporting daily_ohlcv (--force)" in other.stdout
    assert _success(tmp_path)["pg_snapshot_id"] == "00000005-0000003B-1"


def test_default_run_after_snapshot_run_keeps_old_skip_rule(tmp_path: Path) -> None:
    """Without the flag the old rule applies: any valid manifest is skipped."""
    assert _run(tmp_path, ["--consistent-snapshot"]).returncode == 0
    result = _run(tmp_path, [])
    assert result.returncode == 0
    assert "Skipping daily_ohlcv (valid manifest already present)" in result.stdout


def test_holder_death_stops_launching_and_no_marker(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        ["--consistent-snapshot", "--jobs", "1"],
        {
            "FAKE_EXPORTER_HOLDER_EXIT_AFTER": "1",
            "FAKE_EXPORTER_SLEEP_SECONDS": "2",
            "FAKE_EXPORTER_SLEEP_TABLES": "dart_xbrl_fact_raw",
        },
    )
    assert result.returncode == 1
    assert "snapshot holder is gone" in result.stdout
    assert not (_manifests_dir(tmp_path) / "_SUCCESS.json").exists()


def test_missing_snapshot_id_aborts_before_any_export(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        ["--consistent-snapshot"],
        {"FAKE_EXPORTER_SNAPSHOT_ID": "not-a-snapshot"},
    )
    assert result.returncode == 1
    assert "Could not obtain a PostgreSQL snapshot id" in result.stderr
    assert not any(c.startswith("export ") for c in _calls(tmp_path))


def test_dry_run_does_not_start_a_holder(tmp_path: Path) -> None:
    result = _run(tmp_path, ["--consistent-snapshot", "--dry-run"])
    assert result.returncode == 0, result.stderr + result.stdout
    assert not any(c.startswith("snapshot-hold") for c in _calls(tmp_path))
    assert "no holder is started" in result.stdout

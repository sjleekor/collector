"""Real-PostgreSQL check of ``raw-parquet-exporter --pg-snapshot`` / ``snapshot-hold``.

Skipped unless both are set:

* ``RAW_EXPORT_TEST_DSN``  libpq conninfo of a THROWAWAY database (this test
  creates and drops the table ``snap_probe`` in it; never point it at sj2 or the
  local ``mydb``).
* ``RAW_EXPORT_TEST_BIN``  path of a built ``raw-parquet-exporter`` binary.

Shows that a row committed after the holder opened its snapshot is not exported
when the export imports the snapshot, is exported without it, and that a dead
snapshot id fails loudly.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

DSN = os.environ.get("RAW_EXPORT_TEST_DSN")
BIN = os.environ.get("RAW_EXPORT_TEST_BIN")

pytestmark = pytest.mark.skipif(
    not (DSN and BIN), reason="RAW_EXPORT_TEST_DSN / RAW_EXPORT_TEST_BIN 이 없다 (임시 DB 필요)"
)

CONFIG = """
[defaults]
compression = "zstd"
row_group_rows = 1000
target_file_bytes = 536870912
db_read_connections = 1
writer_workers = 1

[[tables]]
name = "snap_probe"
priority = "P0"
extract_strategy = "full_table"
output_partitions = []
order_by = ["k"]
"""


def _runtime(root: Path) -> str:
    return f"""
[source]
name = "t"
dsn_env = "RAW_EXPORT_TEST_DSN"
schema = "public"
read_only = true

[output]
root = "{root}/out"
snapshot_date = "2026-10-01"
tmp_root = "{root}/tmp"
"""


def _export(tmp_path: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            BIN,
            "--log-level",
            "error",
            "export",
            "--config",
            str(tmp_path / "cfg.toml"),
            "--runtime",
            str(tmp_path / "rt.toml"),
            "--tables",
            "snap_probe",
            *extra,
        ],
        env=os.environ,
        text=True,
        capture_output=True,
    )


def _manifest(tmp_path: Path) -> dict:
    path = (
        tmp_path
        / "out/snapshot_date=2026-10-01/source=t/_manifests/table_manifests/snap_probe.json"
    )
    return json.loads(path.read_text())


def test_export_reads_the_held_snapshot(tmp_path: Path) -> None:
    import psycopg2

    (tmp_path / "cfg.toml").write_text(CONFIG)
    (tmp_path / "rt.toml").write_text(_runtime(tmp_path))
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS snap_probe")
    cur.execute("CREATE TABLE snap_probe (k int PRIMARY KEY, v text)")
    cur.execute("INSERT INTO snap_probe VALUES (1,'a'),(2,'b'),(3,'c')")
    try:
        holder = subprocess.Popen(
            [BIN, "--log-level", "error", "snapshot-hold", "--runtime", str(tmp_path / "rt.toml")],
            env=os.environ,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        snapshot_id = holder.stdout.readline().strip()
        assert snapshot_id, holder.stderr.read()
        cur.execute("INSERT INTO snap_probe VALUES (4,'d')")  # after the snapshot

        pinned = _export(tmp_path, "--pg-snapshot", snapshot_id)
        assert pinned.returncode == 0, pinned.stderr
        manifest = _manifest(tmp_path)
        assert manifest["table"]["rows_exported"] == 3
        assert manifest["source"]["snapshot_policy"] == "repeatable_read_exported_snapshot"
        assert manifest["source"]["pg_snapshot_id"] == snapshot_id

        default = _export(tmp_path, "--force")
        assert default.returncode == 0, default.stderr
        manifest = _manifest(tmp_path)
        assert manifest["table"]["rows_exported"] == 4
        assert "pg_snapshot_id" not in manifest["source"]

        holder.terminate()
        holder.wait(timeout=10)
        time.sleep(0.5)
        stale = _export(tmp_path, "--pg-snapshot", snapshot_id, "--force")
        assert stale.returncode != 0
        assert "could not import pg snapshot" in stale.stderr
    finally:
        cur.execute("DROP TABLE IF EXISTS snap_probe")
        conn.close()

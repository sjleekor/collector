#!/usr/bin/env python3
"""Fake ``raw-parquet-exporter`` binary for shell-level orchestration tests.

Stands in for the real Rust binary (``tools/raw-parquet-exporter``) via
``SDC_RAW_PARQUET_BIN`` so ``bin/raw-parquet-export-all.sh``'s route/TOML and
(P4) resume-state-machine logic can be exercised without a real Postgres/
Parquet round trip. Mirrors just the ``export`` / ``validate`` /
``validate-samples`` / ``resume`` CLI surface documented in
``tools/raw-parquet-exporter/src/cli.rs`` and the manifest/checkpoint JSON
shapes in ``tools/raw-parquet-exporter/src/manifest.rs``.

Controlled by env vars:

  FAKE_EXPORTER_FAIL_TABLES=t1,t2       ``export``/``resume`` exits 1 for these
                                        tables, writes nothing.
  FAKE_EXPORTER_LEAVE_CHECKPOINT=t1,t2  ``export`` writes an incomplete
                                        checkpoint (completed=false) then
                                        exits 1, simulating a mid-run crash.
  FAKE_EXPORTER_CALL_LOG=/path          Appended with "<subcommand> <table>"
                                        per invocation (order/replay
                                        assertions in tests).
  FAKE_EXPORTER_RUNTIME_CAPTURE=/path   The first ``--runtime`` TOML this
                                        process sees is copied here verbatim,
                                        before bin/raw-parquet-export-all.sh's
                                        own trap can delete the original.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
import tomllib
from pathlib import Path


def _opt(args: list[str], name: str, default: str | None = None) -> str | None:
    if name in args:
        return args[args.index(name) + 1]
    return default


def _flag(args: list[str], name: str) -> bool:
    return name in args


def _split_env_list(name: str) -> set[str]:
    raw = os.environ.get(name, "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _log_call(subcommand: str, key: str) -> None:
    log_path = os.environ.get("FAKE_EXPORTER_CALL_LOG")
    if not log_path:
        return
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{subcommand} {key}\n")


def _capture_runtime(runtime_path: str | None) -> None:
    capture_path = os.environ.get("FAKE_EXPORTER_RUNTIME_CAPTURE")
    if not capture_path or not runtime_path:
        return
    shutil.copyfile(runtime_path, capture_path)


def _load_runtime(runtime_path: str) -> dict:
    with open(runtime_path, "rb") as f:
        return tomllib.load(f)


def _table_output_root(runtime: dict) -> Path:
    root = Path(runtime["output"]["root"])
    snapshot_date = runtime["output"]["snapshot_date"]
    source_name = runtime["source"]["name"]
    return root / f"snapshot_date={snapshot_date}" / f"source={source_name}"


def _manifest_path(runtime: dict, table: str) -> Path:
    return _table_output_root(runtime) / "_manifests" / "table_manifests" / f"{table}.json"


def _checkpoint_path(runtime: dict, table: str, run_id: str) -> Path:
    return _table_output_root(runtime) / "_manifests" / "checkpoints" / f"{run_id}.json"


def _write_manifest(runtime: dict, table: str, run_id: str, extract_predicate: str) -> None:
    manifest_path = _manifest_path(runtime, table)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "created_at_unix_seconds": int(time.time()),
                "source": {
                    "name": runtime["source"]["name"],
                    "schema": runtime["source"].get("schema", "public"),
                    "snapshot_date": runtime["output"]["snapshot_date"],
                    "snapshot_policy": "per_chunk_read_committed",
                },
                "table": {
                    "name": table,
                    "schema": None,
                    "rows_exported": 0,
                    "files": [],
                    "extract_predicate": extract_predicate,
                    "min_raw_id": None,
                    "max_raw_id": None,
                },
            }
        ),
        encoding="utf-8",
    )


def cmd_export(args: list[str]) -> int:
    runtime_path = _opt(args, "--runtime")
    _capture_runtime(runtime_path)
    runtime = _load_runtime(runtime_path)
    tables = (_opt(args, "--tables", "") or "").split(",")
    dry_run = _flag(args, "--dry-run")

    fail_tables = _split_env_list("FAKE_EXPORTER_FAIL_TABLES")
    leave_checkpoint_tables = _split_env_list("FAKE_EXPORTER_LEAVE_CHECKPOINT")
    sleep_tables = _split_env_list("FAKE_EXPORTER_SLEEP_TABLES")
    sleep_seconds = float(os.environ.get("FAKE_EXPORTER_SLEEP_SECONDS", "0") or 0)

    exit_code = 0
    for table in tables:
        _log_call("export", table)
        if sleep_seconds > 0 and (not sleep_tables or table in sleep_tables):
            time.sleep(sleep_seconds)
        if dry_run:
            continue

        if table in leave_checkpoint_tables:
            run_id = f"{table}-fake-{int(time.time())}-{os.getpid()}"
            checkpoint_path = _checkpoint_path(runtime, table, run_id)
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "strategy": "raw_id_range",
                        "run_id": run_id,
                        "completed": False,
                        "table": table,
                        "source": {
                            "name": runtime["source"]["name"],
                            "schema": runtime["source"].get("schema", "public"),
                            "snapshot_date": runtime["output"]["snapshot_date"],
                            "snapshot_policy": "per_chunk_read_committed",
                        },
                        "extract_predicate": "fake",
                        "extract_start_raw_id": 0,
                        "final_exclusive_end": 1000,
                        "next_raw_id": 500,
                        "chunk_rows": 500,
                        "batch_rows": 65536,
                        "max_rows_per_file": 5000000,
                        "chunks_planned": 2,
                        "chunks_completed": 1,
                        "rows_exported": 0,
                        "files": [],
                        "schema": None,
                        "partitions": [],
                        "manifest_file": None,
                        "updated_at_unix_seconds": int(time.time()),
                    }
                ),
                encoding="utf-8",
            )
            print(f"fake-exporter: left incomplete checkpoint for {table}", file=sys.stderr)
            exit_code = 1
            continue

        if table in fail_tables:
            print(f"fake-exporter: forced failure for {table}", file=sys.stderr)
            exit_code = 1
            continue

        run_id = f"{table}-fake-{int(time.time())}-{os.getpid()}"
        _write_manifest(runtime, table, run_id, extract_predicate="fake")

    return exit_code


def cmd_resume(args: list[str]) -> int:
    runtime_path = _opt(args, "--runtime")
    _capture_runtime(runtime_path)
    runtime = _load_runtime(runtime_path)
    checkpoint_path = Path(_opt(args, "--checkpoint"))
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    table = checkpoint["table"]
    _log_call("resume", table)

    fail_tables = _split_env_list("FAKE_EXPORTER_FAIL_TABLES")
    if table in fail_tables:
        print(f"fake-exporter: forced resume failure for {table}", file=sys.stderr)
        return 1

    checkpoint["completed"] = True
    checkpoint["updated_at_unix_seconds"] = int(time.time())
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    _write_manifest(runtime, table, checkpoint["run_id"], checkpoint["extract_predicate"])
    return 0


def cmd_validate(args: list[str]) -> int:
    manifest_path = Path(_opt(args, "--manifest"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest["table"]["files"]
    parquet_rows = sum(f.get("rows", 0) for f in files)
    manifest_rows = manifest["table"]["rows_exported"]
    report = {
        "manifest": str(manifest_path),
        "table": manifest["table"]["name"],
        "manifest_rows": manifest_rows,
        "parquet_rows": parquet_rows,
        "files_checked": len(files),
        "passed": parquet_rows == manifest_rows,
    }
    print(json.dumps(report))
    return 0


def cmd_validate_samples(args: list[str]) -> int:
    runtime_path = _opt(args, "--runtime")
    _capture_runtime(runtime_path)
    print(json.dumps({"passed": True}))
    return 0


_SUBCOMMANDS = {
    "export": cmd_export,
    "validate": cmd_validate,
    "validate-samples": cmd_validate_samples,
    "resume": cmd_resume,
}


def main(argv: list[str]) -> int:
    subcommand = next((a for a in argv if a in _SUBCOMMANDS), None)
    if subcommand is None:
        print(f"fake-exporter: no known subcommand in {argv}", file=sys.stderr)
        return 2
    return _SUBCOMMANDS[subcommand](argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

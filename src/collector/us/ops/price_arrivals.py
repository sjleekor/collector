"""Forward-only evidence of when a US price session first appeared locally.

The first run is a baseline, not historical arrival evidence. Subsequent new
sessions receive a first-local-capture timestamp. Neither timestamp claims the
upstream Dolt commit time or proves availability before an earlier cutoff.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_records(journal_root: Path) -> list[dict[str, object]]:
    """Every observation, oldest capture first. Legacy one-file-per-date names load too."""
    records = [
        (json.loads(path.read_text()), path.name)
        for path in journal_root.glob("snapshot_date=*.json")
    ]
    records.sort(key=lambda item: (item[0]["captured_at"], item[1]))
    return [record for record, _name in records]


def _read_source_rev(snapshot_path: Path) -> str:
    import duckdb

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT DISTINCT source_rev FROM read_parquet(?) LIMIT 2", [str(snapshot_path)]
        ).fetchall()
    finally:
        con.close()
    if len(rows) != 1 or not rows[0][0]:
        raise ValueError("prices_daily source_rev is ambiguous")
    return str(rows[0][0])


def record_price_snapshot(
    *,
    snapshot_path: Path,
    journal_root: Path,
    source_rev: str | None = None,
    captured_at: datetime | None = None,
) -> dict[str, object]:
    """Record one completed snapshot as an append-only observation.

    Observations are keyed by the snapshot content sha256, not by the partition
    date. Recording the same content again returns the earlier record. The same
    ``snapshot_date`` rebuilt with different content is a new observation in a
    new file, and an earlier record is never rewritten. A session already seen
    keeps its original ``first_local_capture``; only newly appearing sessions
    are attributed to the new observation.

    The full-file sha256 is skipped when the newest journal record has the same
    partition, file size, mtime_ns and (if given) source_rev as the file now.
    ``source_rev=None`` reads it from the parquet after that check.
    """
    if not snapshot_path.is_file() or snapshot_path.name != "part.parquet":
        raise ValueError("completed prices_daily part.parquet is required")
    partition = snapshot_path.parent.name
    if not partition.startswith("snapshot_date="):
        raise ValueError("prices_daily snapshot partition is missing")
    journal_root.mkdir(parents=True, exist_ok=True)
    # The scheduler's US lock is the normal guard; this journal lock also
    # protects ad hoc runs and makes first-local-capture unique across writers.
    with (journal_root / ".write.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            captured_at = captured_at or datetime.now(UTC)
            if captured_at.tzinfo is None or captured_at.utcoffset() is None:
                raise ValueError("captured_at must have a timezone")
            captured_at = captured_at.astimezone(UTC)
            return _record_locked(snapshot_path, journal_root, source_rev, captured_at, partition)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _unchanged_since(
    record: dict[str, object], partition: str, stat: os.stat_result, source_rev: str | None
) -> bool:
    """True only when partition, size and mtime_ns (and source_rev, if known) all match."""
    return (
        record.get("snapshot_partition") == partition
        and record.get("snapshot_size") == stat.st_size
        and record.get("snapshot_mtime_ns") == stat.st_mtime_ns
        and (source_rev is None or record.get("source_rev") == source_rev)
    )


def _record_locked(
    snapshot_path: Path,
    journal_root: Path,
    source_rev: str | None,
    captured_at: datetime,
    partition: str,
) -> dict[str, object]:
    import duckdb

    records = _load_records(journal_root)
    stat = snapshot_path.stat()
    if records and _unchanged_since(records[-1], partition, stat, source_rev):
        return records[-1]
    snapshot_sha = _sha256(snapshot_path)
    for old in records:
        if old.get("snapshot_sha256") == snapshot_sha:
            return old
    if source_rev is None:
        source_rev = _read_source_rev(snapshot_path)

    seen: set[str] = set()
    for old in records:
        seen.update(old["all_sessions"])
    if records and captured_at <= datetime.fromisoformat(records[-1]["captured_at"]):
        raise ValueError("arrival captures must be recorded in time order")

    con = duckdb.connect()
    try:
        # Only the date column is read. No label or return data is accessed.
        sessions = [str(row[0]) for row in con.execute(
            "SELECT DISTINCT date FROM read_parquet(?) WHERE date IS NOT NULL ORDER BY date",
            [str(snapshot_path)],
        ).fetchall()]
    finally:
        con.close()
    if _sha256(snapshot_path) != snapshot_sha:
        raise ValueError("prices_daily snapshot changed while reading session dates")
    if not sessions:
        raise ValueError("prices_daily snapshot has no sessions")
    new_sessions = sorted(set(sessions) - seen)
    record = {
        "schema_version": 1,
        "source": "local_prices_daily_snapshot",
        "source_rev": source_rev,
        "snapshot_partition": partition,
        "snapshot_sha256": snapshot_sha,
        "snapshot_size": stat.st_size,
        "snapshot_mtime_ns": stat.st_mtime_ns,
        "captured_at": captured_at.isoformat(),
        "capture_kind": "first_local_capture" if records else "baseline_unknown_arrival",
        "all_sessions": sessions,
        "new_sessions": new_sessions,
        "latest_session": sessions[-1],
        "upstream_commit_at": None,
    }
    destination = journal_root / f"{partition}.{snapshot_sha[:16]}.json"
    temporary = journal_root / f".{partition}.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(record, sort_keys=True, indent=2) + "\n")
    try:
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return record


def first_local_capture(journal_root: Path, session: str) -> dict[str, object] | None:
    """Return qualified evidence, excluding the initial unknown-arrival baseline."""
    for record in _load_records(journal_root):
        if session in record["new_sessions"]:
            return record if record["capture_kind"] == "first_local_capture" else None
    return None

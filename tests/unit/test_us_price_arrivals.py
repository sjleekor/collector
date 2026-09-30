"""Forward arrivals never fabricate historical upstream availability."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.us.ops import price_arrivals
from collector.us.ops.price_arrivals import first_local_capture, record_price_snapshot


def _snapshot(tmp_path, partition, days, revision="revision-a"):
    path = tmp_path / "prices_daily" / f"snapshot_date={partition}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"date": pa.array(days, type=pa.date32()),
                             "source_rev": [revision] * len(days)}), path)
    return path


def test_baseline_is_unknown_and_new_session_has_bounded_local_capture(tmp_path):
    from datetime import date

    journal = tmp_path / "journal"
    initial = _snapshot(tmp_path, "2026-09-28", [date(2026, 9, 24), date(2026, 9, 25)])
    first = record_price_snapshot(
        snapshot_path=initial, journal_root=journal, source_rev="revision-a",
        captured_at=datetime(2026, 9, 28, 6, tzinfo=UTC),
    )
    assert first["capture_kind"] == "baseline_unknown_arrival"
    assert first_local_capture(journal, "2026-09-25") is None
    assert record_price_snapshot(
        snapshot_path=initial, journal_root=journal, source_rev="revision-a",
    ) == first

    later = _snapshot(
        tmp_path, "2026-09-29", [date(2026, 9, 24), date(2026, 9, 25), date(2026, 9, 28)]
    )
    second = record_price_snapshot(
        snapshot_path=later, journal_root=journal, source_rev="revision-b",
        captured_at=datetime(2026, 9, 29, 6, tzinfo=UTC),
    )
    assert second["new_sessions"] == ["2026-09-28"]
    assert first_local_capture(journal, "2026-09-28")["captured_at"] == "2026-09-29T06:00:00+00:00"
    assert second["upstream_commit_at"] is None

    # Same content again is idempotent, whatever source_rev the caller passes.
    assert record_price_snapshot(
        snapshot_path=later, journal_root=journal, source_rev="other"
    ) == second
    assert len(list(journal.glob("snapshot_date=*.json"))) == 2

    # The same snapshot_date rewritten with other content is a new observation.
    # Already seen sessions keep their first capture; only 2026-09-29 is new.
    _snapshot(tmp_path, "2026-09-29", [date(2026, 9, 24), date(2026, 9, 29)], "revision-c")
    third = record_price_snapshot(
        snapshot_path=later, journal_root=journal, source_rev="revision-c",
    )
    assert third["new_sessions"] == ["2026-09-29"]
    assert third["snapshot_sha256"] != second["snapshot_sha256"]
    assert len(list(journal.glob("snapshot_date=*.json"))) == 3
    assert first_local_capture(journal, "2026-09-28")["captured_at"] == "2026-09-29T06:00:00+00:00"
    assert first_local_capture(journal, "2026-09-29")["source_rev"] == "revision-c"


def test_unchanged_file_skips_the_full_hash(tmp_path, monkeypatch):
    from datetime import date

    journal = tmp_path / "journal"
    snapshot = _snapshot(tmp_path, "2026-09-28", [date(2026, 9, 25)])
    first = record_price_snapshot(snapshot_path=snapshot, journal_root=journal, source_rev=None)
    assert first["source_rev"] == "revision-a"

    def no_hash(_path):
        raise AssertionError("hash must be skipped for an unchanged file")

    monkeypatch.setattr(price_arrivals, "_sha256", no_hash)
    assert record_price_snapshot(
        snapshot_path=snapshot, journal_root=journal, source_rev=None
    ) == first
    assert record_price_snapshot(
        snapshot_path=snapshot, journal_root=journal, source_rev="revision-a"
    ) == first
    # A different source_rev is not the same observation, so the hash runs.
    with pytest.raises(AssertionError):
        record_price_snapshot(snapshot_path=snapshot, journal_root=journal, source_rev="other")


def test_concurrent_writers_assign_one_first_local_capture(tmp_path):
    from datetime import date

    journal = tmp_path / "journal"
    initial = _snapshot(tmp_path, "2026-09-28", [date(2026, 9, 25)])
    record_price_snapshot(snapshot_path=initial, journal_root=journal, source_rev="a",
                          captured_at=datetime(2026, 9, 28, 6, tzinfo=UTC))
    days = [date(2026, 9, 25), date(2026, 9, 28)]
    one = _snapshot(tmp_path, "2026-09-29", days, "b")
    two = _snapshot(tmp_path, "2026-09-30", days, "c")
    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(pool.map(lambda item: record_price_snapshot(
            snapshot_path=item[0], journal_root=journal, source_rev=item[1]), [
                (one, "b"), (two, "c"),
            ]))
    assert sum("2026-09-28" in row["new_sessions"] for row in records) == 1


def test_snapshot_change_between_hash_and_dates_rejected(tmp_path, monkeypatch):
    from datetime import date

    snapshot = _snapshot(tmp_path, "2026-09-28", [date(2026, 9, 25)])
    real_sha = price_arrivals._sha256
    calls = 0

    def changed(path):
        nonlocal calls
        calls += 1
        result = real_sha(path)
        return result if calls == 1 else "0" * 64

    monkeypatch.setattr(price_arrivals, "_sha256", changed)
    journal = tmp_path / "journal"
    with pytest.raises(ValueError, match="changed while reading"):
        record_price_snapshot(snapshot_path=snapshot, journal_root=journal, source_rev="a")
    assert not list(journal.glob("snapshot_date=*.json"))

"""``run_derive`` 의 가격 도착 journal hook은 derive 결과를 바꾸지 않는다."""

from __future__ import annotations

import importlib
import json
import sys
from datetime import date
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from collector.lake import DataRoot
from collector.us.ops import derive, price_arrivals


def _write_prices(root, partition, days, revision):
    path = (
        root.derived / "snapshots" / "prices_daily" / f"snapshot_date={partition}" / "part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "date": pa.array(days, type=pa.date32()),
        "source_rev": [revision] * len(days),
    })
    pq.write_table(table, path)
    return path


def _journal(root):
    return sorted((root.output / "us_price_arrivals_v1").glob("snapshot_date=*.json"))


def _patch_dolt(monkeypatch, root, head, loader=None):
    """Fake the dolt source and snapshot writer modules.

    Both import pandera/dolt at module load, which the test venv may lack. The
    hook under test only needs ``head_commit``, the loader and ``latest_snapshot``.
    """
    (root.raw / "dolt" / "stocks" / ".dolt").mkdir(parents=True, exist_ok=True)

    def latest_snapshot(r, table):
        parts = sorted((r.derived / "snapshots" / table).glob("snapshot_date=*/part.parquet"))
        return parts[-1] if parts else None

    def load_prices_daily(r, *, snapshot_date):
        return loader(r, snapshot_date=snapshot_date)

    fake_dolt = SimpleNamespace(
        repo_dir=lambda r, repo: r.raw / "dolt" / repo,
        head_commit=lambda _path: head["rev"],
        load_prices_daily=load_prices_daily,
    )
    monkeypatch.setitem(sys.modules, "collector.us.sources.dolt", fake_dolt)
    monkeypatch.setitem(
        sys.modules, "collector.us.store.writer", SimpleNamespace(latest_snapshot=latest_snapshot)
    )
    importlib.import_module("collector.us.sources")  # make sure the package is loaded
    monkeypatch.setattr(sys.modules["collector.us.sources"], "dolt", fake_dolt, raising=False)


def _run(root, **kwargs):
    return derive.run_derive(
        root, snapshot_date="2026-09-30", tables=("prices-daily",), **kwargs
    )


def test_rebuild_path_records_journal(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    head = {"rev": "rev-a"}

    def loader(r, *, snapshot_date):
        path = _write_prices(r, snapshot_date, [date(2026, 9, 25)], head["rev"])
        return {"path": path, "source_rev": head["rev"], "rows": 1}

    _patch_dolt(monkeypatch, root, head, loader)
    out = _run(root)
    table = out["tables"][0]
    assert out["ok"] and table["rebuilt"]
    assert table["arrival_journal"] == {"ok": True, "error": ""}
    records = [json.loads(p.read_text()) for p in _journal(root)]
    assert len(records) == 1 and records[0]["capture_kind"] == "baseline_unknown_arrival"


def test_skip_path_records_journal_and_avoids_rehash(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    _write_prices(root, "2026-09-29", [date(2026, 9, 25)], "rev-a")
    _patch_dolt(monkeypatch, root, {"rev": "rev-a"})

    first = _run(root)
    assert first["ok"] and not first["tables"][0]["rebuilt"]
    assert first["tables"][0]["arrival_journal"]["ok"] is True
    assert len(_journal(root)) == 1

    def no_hash(_path):
        raise AssertionError("unchanged snapshot must not be hashed again")

    monkeypatch.setattr(price_arrivals, "_sha256", no_hash)
    second = _run(root)
    assert second["ok"] and second["tables"][0]["arrival_journal"]["ok"] is True
    assert len(_journal(root)) == 1


def test_journal_failure_does_not_fail_derive(tmp_path, monkeypatch, capsys):
    root = DataRoot(tmp_path)
    _write_prices(root, "2026-09-29", [date(2026, 9, 25)], "rev-a")
    _patch_dolt(monkeypatch, root, {"rev": "rev-a"})

    def broken(**_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(price_arrivals, "record_price_snapshot", broken)
    out = _run(root)
    table = out["tables"][0]
    assert out["ok"] and table["ok"] and table["error"] == ""
    assert table["arrival_journal"] == {"ok": False, "error": "OSError: disk full"}
    assert "price arrival journal failed" in capsys.readouterr().err


def test_journal_failure_after_rebuild_keeps_rebuild_ok(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    head = {"rev": "rev-a"}

    def loader(r, *, snapshot_date):
        path = _write_prices(r, snapshot_date, [date(2026, 9, 25)], head["rev"])
        return {"path": path, "source_rev": head["rev"], "rows": 1}

    _patch_dolt(monkeypatch, root, head, loader)
    monkeypatch.setattr(
        price_arrivals, "record_price_snapshot", lambda **_k: (_ for _ in ()).throw(ValueError("x"))
    )
    out = _run(root)
    assert out["ok"] and out["tables"][0]["rebuilt"]
    assert out["tables"][0]["arrival_journal"]["ok"] is False


def test_same_snapshot_date_rewritten_is_a_new_observation(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    head = {"rev": "rev-a", "days": [date(2026, 9, 24), date(2026, 9, 25)]}

    def loader(r, *, snapshot_date):
        path = _write_prices(r, snapshot_date, head["days"], head["rev"])
        return {"path": path, "source_rev": head["rev"], "rows": len(head["days"])}

    _patch_dolt(monkeypatch, root, head, loader)
    first = _run(root)
    assert first["ok"]

    # Same snapshot_date, new upstream commit and one more session.
    head["rev"] = "rev-b"
    head["days"] = [date(2026, 9, 24), date(2026, 9, 25), date(2026, 9, 28)]
    second = _run(root)
    assert second["ok"] and second["tables"][0]["arrival_journal"]["ok"] is True

    records = sorted(
        (json.loads(p.read_text()) for p in _journal(root)), key=lambda r: r["captured_at"]
    )
    assert len(records) == 2
    assert {r["snapshot_partition"] for r in records} == {"snapshot_date=2026-09-30"}
    assert records[0]["new_sessions"] == ["2026-09-24", "2026-09-25"]
    assert records[1]["new_sessions"] == ["2026-09-28"]
    assert records[1]["capture_kind"] == "first_local_capture"
    assert price_arrivals.first_local_capture(
        root.output / "us_price_arrivals_v1", "2026-09-28"
    )["source_rev"] == "rev-b"
    # The earlier session keeps its first observation.
    assert price_arrivals.first_local_capture(
        root.output / "us_price_arrivals_v1", "2026-09-25"
    ) is None  # baseline is never qualified evidence

    # A skip after the rewrite records nothing new and still succeeds.
    third = _run(root)
    assert third["ok"] and len(_journal(root)) == 2


def test_dry_run_writes_no_journal(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    _write_prices(root, "2026-09-29", [date(2026, 9, 25)], "rev-a")
    _patch_dolt(monkeypatch, root, {"rev": "rev-b"})
    out = _run(root, dry_run=True)
    assert out["ok"] and "arrival_journal" not in out["tables"][0]
    assert not _journal(root)

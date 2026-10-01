"""A narrow month-boundary rebuild must carry its prior membership and rows."""

from __future__ import annotations

import datetime as dt
import os
import shutil
import stat
import sys
from types import SimpleNamespace

import duckdb
import pytest

from collector.lake import DataRoot
from collector.us.sources import wayback
from collector.us.universe import build


def _write(root, name, partition, sql):
    path = root.derived / "snapshots" / name / f"snapshot_date={partition}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.connect().execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    return path


def test_incremental_uses_previous_month_maintain_threshold(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    def snapshot_path(r, table, day):
        return r.derived / "snapshots" / table / f"snapshot_date={day}" / "part.parquet"

    def latest_snapshot(r, table):
        parts = (r.derived / "snapshots" / table).glob("snapshot_date=*/part.parquet")
        return next(iter(sorted(parts, reverse=True)), None)

    def verify_snapshot(path, _table, unique_on, connection=None):
        count = duckdb.connect().execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
        return {"rows": count, "bytes": path.stat().st_size}

    fake_writer = SimpleNamespace(
        snapshot_path=snapshot_path,
        latest_snapshot=latest_snapshot,
        verify_snapshot=verify_snapshot,
    )
    monkeypatch.setitem(sys.modules, "collector.us.store.writer", fake_writer)
    class FakeCalendar:
        def sessions_in_range(self, start, end):
            first = dt.date.fromisoformat(str(start)) if isinstance(start, str) else start
            last = dt.date.fromisoformat(str(end)) if isinstance(end, str) else end
            return [dt.datetime.combine(first + dt.timedelta(days=offset), dt.time())
                    for offset in range((last - first).days + 1)
                    if (first + dt.timedelta(days=offset)).weekday() < 5]
    monkeypatch.setitem(sys.modules, "exchange_calendars",
                        SimpleNamespace(get_calendar=lambda _name: FakeCalendar()))
    monkeypatch.setattr(wayback, "ticker_cik_map", lambda _root, **_kw: [
        ("AAA", 1, dt.date(2026, 7, 1)), ("AAA", 2, dt.date(2026, 9, 1))])
    ticker = root.raw / "wayback" / "company_tickers" / "company_tickers_20260701.json"
    ticker.parent.mkdir(parents=True)
    ticker.write_text("fixture ticker source")

    _write(root, "prices_daily", "2026-10-01", """
        SELECT CAST(day AS DATE) AS date, 'AAA' AS symbol, 80.0 AS close, 10000 AS volume
        FROM generate_series(DATE '2026-08-01', DATE '2026-10-01', INTERVAL 1 DAY) t(day)
        WHERE dayofweek(day) BETWEEN 1 AND 5
    """)
    _write(root, "listing_snapshots", "2026-09-30", """
        SELECT 'AAA' AS symbol, DATE '2026-07-01' AS as_of, 'nasdaqlisted' AS kind,
               'AAA Common Stock' AS security_name, 'NASDAQ' AS exchange,
               'Q' AS market_category, FALSE AS is_etf, FALSE AS test_issue,
               CAST(NULL AS VARCHAR) AS financial_status
    """)
    _write(root, "filings_sub", "2026-09-30", """
        SELECT 1::BIGINT AS cik, DATE '2026-08-01' AS filed, '1234' AS sic
    """)
    _write(root, "midas_security_daily", "2026-09-30", """
        SELECT DATE '2026-10-01' AS date, 'AAA' AS ticker,
               'Stock' AS security_type, 1 AS mcap_rank
    """)
    prior = _write(root, "universe_daily", "2026-09-30", """
        SELECT d.day::DATE AS date, 'AAA' AS symbol, 1::BIGINT AS cik,
               TRUE AS in_prices, TRUE AS in_listing, 'wayback' AS listing_source,
               FALSE AS is_etf, FALSE AS test_issue, 'NASDAQ' AS exchange,
               '1234' AS sic, 'sec_sub' AS sic_source, 1::INTEGER AS mcap_rank,
               800000.0 AS adv_20d,
               (d.day::DATE <> DATE '2026-09-01') AS in_universe,
               DATE '2018-09-07' AS usable_from,
               TIMESTAMPTZ '2026-09-30 00:00:00+00' AS observed_at
        FROM generate_series(DATE '2026-09-01', DATE '2026-09-30', INTERVAL 1 DAY) d(day)
        WHERE dayofweek(d.day) BETWEEN 1 AND 5
    """)

    backup = prior.with_name("complete.parquet")
    shutil.copy2(prior, backup)
    gap = prior.with_name("gap.parquet")
    duckdb.connect().execute(
        f"COPY (SELECT * FROM read_parquet('{prior}') WHERE date <> DATE '2026-09-15') "
        f"TO '{gap}' (FORMAT PARQUET)"
    )
    os.replace(gap, prior)
    with pytest.raises(ValueError, match="missing XNYS sessions"):
        build.build_universe_incremental(root, snapshot_date="2026-10-02")
    os.replace(backup, prior)

    result = build.build_universe_incremental(
        root, snapshot_date="2026-10-02",
        observed_at=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
    )
    assert result["seed_previous_members"] == 1
    assert result["unjudged_months"] == []
    assert result["rows"] == 23
    output = result["path"]
    rows = duckdb.connect().execute(
        "SELECT date, cik, in_universe FROM read_parquet(?) ORDER BY date", [str(output)]
    ).fetchall()
    assert rows[-1] == (dt.date(2026, 10, 1), 2, True)
    # The published partition must not keep mkdtemp's 0700 (2026-10-01 sj2).
    assert stat.S_IMODE(output.parent.stat().st_mode) == 0o755
    assert result["completion_path"].is_file()
    import json
    marker = json.loads(result["completion_path"].read_text())
    assert str(ticker.relative_to(root.base)) in marker["ticker_source_sha256"]

    # --if-new: nothing newer than the snapshot just written is a skip, not an error.
    with pytest.raises(ValueError, match="new completed price session"):
        build.build_universe_incremental(root, snapshot_date="2026-10-03")
    assert build.build_universe_incremental(root, snapshot_date="2026-10-03", if_new=True) == {
        "skipped": True, "reason": "no new price session",
    }

    # DuckDB date_trunc('month', DATE) returns TIMESTAMP in this runtime.
    # The frozen membership key is a DATE string; a raw str(datetime) lookup
    # silently misses it and recomputes membership from source candidates.
    forced = root.base / "forced-frozen-membership.parquet"
    build.build_universe_daily(
        root, snapshot_date="2026-10-02", start="2026-10-01", end="2026-10-01",
        observed_at=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
        seed_previous_members={"AAA"},
        fixed_month_members={"2026-10-01": {"ZZZ"}},
        prefix_snapshot=prior, destination_path=forced,
    )
    assert duckdb.connect().execute(
        "SELECT in_universe FROM read_parquet(?) WHERE date = DATE '2026-10-01'",
        [str(forced)],
    ).fetchone() == (False,)

    _write(root, "prices_daily", "2026-10-02", """
        SELECT CAST(day AS DATE) AS date, 'AAA' AS symbol, 80.0 AS close, 10000 AS volume
        FROM generate_series(DATE '2026-08-01', DATE '2026-10-02', INTERVAL 1 DAY) t(day)
        WHERE dayofweek(day) BETWEEN 1 AND 5
    """)
    # A new session exists. An existing same-date partition is an error, or a skip with --if-new.
    with pytest.raises(FileExistsError):
        build.build_universe_incremental(root, snapshot_date="2026-10-02")
    same_day = build.build_universe_incremental(root, snapshot_date="2026-10-02", if_new=True)
    assert same_day["skipped"] is True and "already exists" in same_day["reason"]
    # --dry-run checks everything and writes nothing.
    planned = build.build_universe_incremental(root, snapshot_date="2026-10-03", dry_run=True)
    assert planned["would_build"] is True and planned["added_start"] == "2026-10-02"
    assert not (root.derived / "snapshots" / "universe_daily" / "snapshot_date=2026-10-03").exists()
    extended = build.build_universe_incremental(
        root, snapshot_date="2026-10-03",
        observed_at=dt.datetime(2026, 10, 3, tzinfo=dt.UTC),
    )
    assert extended["fixed_months"] == ["2026-10-01"]
    assert duckdb.connect().execute(
        "SELECT in_universe FROM read_parquet(?) WHERE date = DATE '2026-10-02'",
        [str(extended["path"])],
    ).fetchone() == (True,)


def test_duckdb_limits_default_and_env_override(tmp_path, monkeypatch):
    root = DataRoot(tmp_path)
    monkeypatch.delenv("SDC_US_DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("SDC_US_DUCKDB_THREADS", raising=False)

    def settings(con):
        return con.execute(
            "SELECT current_setting('threads'), current_setting('memory_limit'), "
            "current_setting('temp_directory')"
        ).fetchone()

    # Incremental default is bounded; spill files stay outside derived/ (mirrored).
    con = build._bounded_duckdb(root)
    threads, _memory, temporary = settings(con)
    con.close()
    assert int(threads) == 2
    assert str(root.output) in temporary and str(root.derived) not in temporary
    assert not (root.derived / "tmp").exists()

    # Full rebuild default is the historical unlimited connection.
    full = build._bounded_duckdb(root, bounded=False)
    assert settings(full)[2] != temporary
    full.close()

    monkeypatch.setenv("SDC_US_DUCKDB_THREADS", "1")
    monkeypatch.setenv("SDC_US_DUCKDB_MEMORY_LIMIT", "1GB")
    for bounded in (True, False):
        con = build._bounded_duckdb(root, bounded=bounded)
        threads, memory, _temp = settings(con)
        con.close()
        assert int(threads) == 1
        assert memory.startswith("953.6 MiB")

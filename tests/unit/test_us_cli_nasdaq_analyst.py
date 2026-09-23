"""``us-nasdaq-analyst run`` CLI 배선 — argparse 트리와 핸들러가 맞물리는지만 본다."""

from __future__ import annotations

import datetime as dt

import pytest

from collector.cli.app import build_parser
from collector.us.cli import app as us_app


def _parse(*argv: str):
    return build_parser().parse_args(["us-nasdaq-analyst", "run", *argv])


def test_parses_with_defaults():
    args = _parse()
    assert args.handler is us_app._handle_nasdaq_analyst_run
    assert args.symbols is None
    assert args.today is None
    assert args.budget_seconds is None
    assert args.max_retries == 2
    assert args.dry_run is False


def test_parses_overrides():
    args = _parse(
        "--symbols", "AAPL,MSFT",
        "--today", "2026-09-24",
        "--budget-seconds", "120",
        "--max-retries", "0",
        "--interval-seconds", "0.01",
        "--dry-run",
        "--lake-root", "/tmp/whatever",
    )
    assert args.symbols == "AAPL,MSFT"
    assert args.today == "2026-09-24"
    assert args.budget_seconds == 120.0
    assert args.max_retries == 0
    assert args.interval_seconds == 0.01
    assert args.dry_run is True
    assert args.lake_root == "/tmp/whatever"


def test_handler_calls_run_weekly_and_prints_json(tmp_path, monkeypatch, capsys):
    from collector.lake import DataRoot
    from collector.us.ops import nasdaq_analyst

    captured = {}

    def _fake_run_weekly(root, **kwargs):
        captured["root"] = root
        captured["kwargs"] = kwargs
        return {"ok": True, "fetched": 0}

    monkeypatch.setattr(nasdaq_analyst, "run_weekly", _fake_run_weekly)
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()

    args = _parse("--symbols", "AAA", "--today", "2026-09-24", "--lake-root", str(tmp_path))
    args.handler(args)

    assert isinstance(captured["root"], DataRoot)
    assert captured["kwargs"]["symbols"] == ["AAA"]
    assert captured["kwargs"]["today"] == dt.date(2026, 9, 24)
    out = capsys.readouterr().out
    assert '"ok": true' in out


def test_handler_exits_nonzero_when_not_ok(tmp_path, monkeypatch):
    from collector.us.ops import nasdaq_analyst

    monkeypatch.setattr(nasdaq_analyst, "run_weekly", lambda root, **kw: {"ok": False})
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()

    args = _parse("--symbols", "AAA", "--lake-root", str(tmp_path))
    with pytest.raises(SystemExit) as excinfo:
        args.handler(args)
    assert excinfo.value.code == 1


def test_us_load_accepts_the_new_table_choice():
    args = build_parser().parse_args(["us-load", "nasdaq-analyst-estimates"])
    assert args.table == "nasdaq-analyst-estimates"

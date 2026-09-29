"""KRX index daily sync: parsing, empty responses, resumability, max-calls, end default."""

from __future__ import annotations

import ast
import inspect
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from collector.kr.adapters.index_krx_openapi import (
    INDEX_ENDPOINTS,
    KRX_INDEX_HISTORY_START,
    KrxOpenApiIndexProvider,
    parse_index_rows,
)
from collector.kr.domain.models import UpsertResult
from collector.kr.service import sync_krx_index as svc
from collector.kr.util.time import KST

FIXTURE = json.loads("""
{"OutBlock_1": [
 {"BAS_DD": "20260928", "IDX_CLSS": "KOSPI", "IDX_NM": "코스피",
  "CLSPRC_IDX": "3,456.78", "CMPPREVDD_IDX": "-12.34", "FLUC_RT": "-0.36",
  "OPNPRC_IDX": "3,470.00", "HGPRC_IDX": "3,480.10", "LWPRC_IDX": "3,440.00",
  "ACC_TRDVOL": "512,345,678", "ACC_TRDVAL": "12,345,678,901,234",
  "MKTCAP": "2,100,000,000,000,000"},
 {"BAS_DD": "20260928", "IDX_CLSS": "KOSPI", "IDX_NM": "코스피 200 금융",
  "CLSPRC_IDX": "-", "CMPPREVDD_IDX": "", "FLUC_RT": "0.00",
  "OPNPRC_IDX": "-", "HGPRC_IDX": "-", "LWPRC_IDX": "-",
  "ACC_TRDVOL": "-", "ACC_TRDVAL": "", "MKTCAP": "-"},
 {"BAS_DD": "20260928", "IDX_CLSS": "KOSPI", "IDX_NM": "대형주",
  "CLSPRC_IDX": "3300.5", "CMPPREVDD_IDX": "1.5", "FLUC_RT": "0.05",
  "OPNPRC_IDX": "3299", "HGPRC_IDX": "3301", "LWPRC_IDX": "3290",
  "ACC_TRDVOL": "100", "ACC_TRDVAL": "200", "MKTCAP": "300"}
]}
""")
NOW = datetime(2026, 9, 29, 20, 0, tzinfo=KST)


def test_parse_fixture_handles_commas_dash_and_blank() -> None:
    rows = parse_index_rows(FIXTURE["OutBlock_1"], index_group="kospi", fetched_at=NOW)
    assert len(rows) == 3
    first, second, third = rows
    assert first.bas_dd == date(2026, 9, 28)
    assert first.index_group == "kospi"
    assert first.close_idx == Decimal("3456.78")
    assert first.chg_idx == Decimal("-12.34")
    assert first.acc_trdvol == 512_345_678
    assert first.acc_trdval == Decimal("12345678901234")
    assert first.source == "krx_openapi"
    assert second.close_idx is None and second.chg_idx is None
    assert second.acc_trdvol is None and second.acc_trdval is None and second.mktcap is None
    assert second.fluc_rt == Decimal("0.00")  # a real zero stays zero
    assert third.mktcap == Decimal("300")


def test_parse_drops_rows_without_key_fields() -> None:
    rows = parse_index_rows(
        [{"BAS_DD": "bad", "IDX_NM": "x"}, {"BAS_DD": "20260928", "IDX_NM": " "}],
        index_group="krx",
        fetched_at=NOW,
    )
    assert rows == []


class _Client:
    def __init__(self, rows_by_date: dict[str, list[dict]]) -> None:
        self.rows_by_date = rows_by_date
        self.calls: list[tuple[str, str, dict]] = []

    def fetch_rows(self, group, endpoint, params):
        self.calls.append((group, endpoint, params))
        return self.rows_by_date.get(params["basDd"], [])


def test_provider_uses_endpoint_registry_and_empty_is_not_error() -> None:
    assert INDEX_ENDPOINTS == {
        "kospi": ("idx", "kospi_dd_trd"),
        "kosdaq": ("idx", "kosdaq_dd_trd"),
        "krx": ("idx", "krx_dd_trd"),
    }
    client = _Client({"20260928": FIXTURE["OutBlock_1"]})
    provider = KrxOpenApiIndexProvider(client)  # type: ignore[arg-type]
    assert len(provider.fetch_by_date("kospi", date(2026, 9, 28))) == 3
    assert provider.fetch_by_date("kosdaq", date(2026, 9, 26)) == []
    assert client.calls[0][:2] == ("idx", "kospi_dd_trd")
    assert client.calls[1][:2] == ("idx", "kosdaq_dd_trd")
    assert client.calls[1][2] == {"basDd": "20260926"}
    with pytest.raises(ValueError):
        provider.fetch_by_date("nope", date(2026, 9, 28))


class _Provider:
    def __init__(self, empty: set[date] = frozenset(), fail: set[date] = frozenset()) -> None:  # type: ignore[assignment]
        self.calls: list[tuple[str, date]] = []
        self.empty = empty
        self.fail = fail

    def fetch_by_date(self, index_group, day):
        self.calls.append((index_group, day))
        if day in self.fail:
            raise RuntimeError("boom")
        if day in self.empty:
            return []
        return parse_index_rows(
            [{"BAS_DD": day.strftime("%Y%m%d"), "IDX_NM": "코스피", "CLSPRC_IDX": "1"}],
            index_group=index_group,
            fetched_at=NOW,
        )


class _Storage:
    def __init__(self, have: dict[str, set[date]] | None = None) -> None:
        self.have = have or {}
        self.upserts: list[list] = []
        self.runs = []

    def record_run(self, run):
        self.runs.append(run.status)

    def get_krx_index_dates(self, index_group, start, end):
        return {d for d in self.have.get(index_group, set()) if start <= d <= end}

    def upsert_krx_index_daily(self, rows):
        self.upserts.append(rows)
        return UpsertResult(updated=len(rows))


def test_skips_stored_dates_and_counts_empty() -> None:
    # Mon 2026-08-03 .. Fri 2026-08-07: five plain weekdays.
    start, end = date(2026, 8, 3), date(2026, 8, 7)
    provider = _Provider(empty={date(2026, 8, 5)})
    storage = _Storage({"kospi": {date(2026, 8, 3), date(2026, 8, 4)}})
    result = svc.sync_krx_index(
        provider, storage, groups=["kospi"], start=start, end=end  # type: ignore[arg-type]
    )
    assert [d for _, d in provider.calls] == [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)]
    assert result.dates_skipped == 2
    assert result.empty_dates == 1
    assert result.dates_fetched == 2
    assert result.calls == 3
    assert result.rows_upserted == 2
    assert not result.errors


def test_force_refetches_stored_dates() -> None:
    start, end = date(2026, 9, 21), date(2026, 9, 22)
    storage = _Storage({"kospi": {start, end}})
    provider = _Provider()
    svc.sync_krx_index(
        provider, storage, groups=["kospi"], start=start, end=end, force=True  # type: ignore[arg-type]
    )
    assert len(provider.calls) == 2


def test_max_calls_stops_cleanly_and_resumes() -> None:
    start, end = date(2026, 8, 3), date(2026, 8, 14)  # 10 weekdays
    provider = _Provider()
    storage = _Storage()
    first = svc.sync_krx_index(
        provider, storage, groups=["kospi", "kosdaq"], start=start, end=end, max_calls=4  # type: ignore[arg-type]
    )
    assert first.calls == 4 and first.stopped_by_max_calls and not first.errors
    assert storage.runs[-1].value == "success"
    # resume: feed what was stored back in
    stored = {"kospi": {d for g, d in provider.calls if g == "kospi"}}
    provider2 = _Provider()
    second = svc.sync_krx_index(
        provider2,
        _Storage(stored),
        groups=["kospi", "kosdaq"],
        start=start,
        end=end,  # type: ignore[arg-type]
        max_calls=100,
    )
    assert second.dates_skipped == 4
    assert second.calls == 6 + 10
    assert not second.stopped_by_max_calls


def test_consecutive_failures_abort() -> None:
    start, end = date(2026, 8, 3), date(2026, 8, 14)
    days = {start + timedelta(days=i) for i in range(12)}
    provider = _Provider(fail=days)
    result = svc.sync_krx_index(
        provider,
        _Storage(),
        groups=["kospi"],
        start=start,
        end=end,  # type: ignore[arg-type]
        max_consecutive_failures=3,
    )
    assert result.calls == 3
    assert "source_blocked" in result.errors


def test_resolve_range_end_defaults_to_yesterday_and_never_today() -> None:
    today = date(2026, 9, 29)
    start, end = svc.resolve_range(start=None, end=None, incremental=True, today=today)
    assert end == date(2026, 9, 28)
    assert start == end - timedelta(days=svc.DEFAULT_LOOKBACK_DAYS)
    # explicit future / today end is capped
    _, capped = svc.resolve_range(start=date(2026, 9, 1), end=today, incremental=False, today=today)
    assert capped == date(2026, 9, 28)
    # --start is clamped to the history start
    clamped, _ = svc.resolve_range(start=date(2005, 1, 3), end=None, incremental=False, today=today)
    assert clamped == KRX_INDEX_HISTORY_START == date(2010, 1, 4)


def test_no_frozen_date_literals_outside_history_start() -> None:
    from collector.kr.adapters.index_krx_openapi import provider as adapter

    for module in (svc, adapter):
        tree = ast.parse(inspect.getsource(module))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "date"
                and all(isinstance(a, ast.Constant) for a in node.args)
                and node.args
            ):
                assert (module is adapter) and [a.value for a in node.args] == [2010, 1, 4]


def test_cli_registers_index_sync_with_runtime_end_default() -> None:
    from collector.kr.cli import app

    args = app.build_parser().parse_args(["index", "sync", "--incremental"])
    assert args.end is None  # resolved at run time, not a literal
    assert args.groups == "kospi,kosdaq,krx"
    assert args.lookback_days == 7
    assert args.handler is app._handle_index_sync

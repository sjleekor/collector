"""Light receipt-key query for the OpenDART XBRL planner/sync (memory fix)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from collector.kr.cli.app import _dart_xbrl_actual_attempt_estimate
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import DartCorp, DartFinancialStatementLine
from collector.kr.infra.db_postgres import repositories
from collector.kr.infra.db_postgres.repositories import PostgresStorage
from collector.kr.service.sync_dart_xbrl import sync_dart_xbrl
from collector.kr.util.time import now_kst
from tests.helpers.fake_slice_storage import FakeSliceStorageMixin
from tests.unit.test_opendart_xbrl import MockXbrlProvider


class _Cursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, list[Any]]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, params: list[Any]) -> None:
        self.executed.append((statement, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _Conn:
    def __init__(self, cur: _Cursor) -> None:
        self._cur = cur

    def cursor(self, *args: object, **kwargs: object) -> _Cursor:
        return self._cur


def _run_query(
    monkeypatch: pytest.MonkeyPatch, rows: list[tuple[Any, ...]], tickers: list[str] | None
) -> tuple[list[tuple[Any, ...]], _Cursor]:
    cur = _Cursor(rows)

    @contextmanager
    def fake_get_connection(_dsn: str) -> Iterator[_Conn]:
        yield _Conn(cur)

    monkeypatch.setattr(repositories, "get_connection", fake_get_connection)
    out = PostgresStorage("postgresql://unused").get_dart_financial_statement_receipt_keys(
        [2025, 2026], ["11011", "11012"], tickers
    )
    return out, cur


def test_receipt_keys_sql_selects_only_key_columns_without_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out, cur = _run_query(monkeypatch, [("005930", 2025, "11011", "R1")], None)
    sql, params = cur.executed[0]
    flat = " ".join(sql.split())
    assert out == [("005930", 2025, "11011", "R1")]
    assert "raw_payload" not in flat and "SELECT *" not in flat
    assert "DISTINCT ON (ticker, bsns_year, reprt_code, rcept_no)" in flat
    assert "FROM dart_financial_statement_raw" in flat
    assert "bsns_year = ANY(%s) AND reprt_code = ANY(%s)" in flat
    assert "ticker = ANY(%s)" not in flat
    assert flat.endswith("ORDER BY ticker, bsns_year, reprt_code, fs_div, sj_div, ord")
    assert params == [[2025, 2026], ["11011", "11012"]]


def test_receipt_keys_sql_filters_tickers(monkeypatch: pytest.MonkeyPatch) -> None:
    _, cur = _run_query(monkeypatch, [], ["005930"])
    sql, params = cur.executed[0]
    assert "AND ticker = ANY(%s)" in " ".join(sql.split())
    assert params == [[2025, 2026], ["11011", "11012"], ["005930"]]


def _corp(code: str, ticker: str) -> DartCorp:
    return DartCorp(
        corp_code=code,
        corp_name=ticker,
        ticker=ticker,
        market=Market.KOSPI,
        stock_name=ticker,
        modify_date=date(2026, 3, 10),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def _line(ticker: str, year: int, code: str, rcept: str, ord_: int) -> DartFinancialStatementLine:
    return DartFinancialStatementLine(
        corp_code="x",
        ticker=ticker,
        bsns_year=year,
        reprt_code=code,
        fs_div="CFS",
        sj_div="IS",
        sj_nm="",
        account_id="a",
        account_nm="a",
        account_detail="",
        thstrm_nm="",
        thstrm_amount=Decimal("1"),
        thstrm_add_amount=None,
        frmtrm_nm="",
        frmtrm_amount=None,
        frmtrm_q_nm="",
        frmtrm_q_amount=None,
        frmtrm_add_amount=None,
        bfefrmtrm_nm="",
        bfefrmtrm_amount=None,
        ord=ord_,
        currency="KRW",
        rcept_no=rcept,
        source=Source.OPENDART,
        fetched_at=now_kst(),
        raw_payload={},
    )


# Sorted like the SQL (ticker, year, code, fs_div, sj_div, ord); duplicates, empty
# rcept_no/ticker and a disallowed (year, code) pair are included on purpose.
_ROWS = [
    _line("", 2025, "11011", "R0", 1),
    _line("000660", 2025, "11011", "R2", 1),
    _line("000660", 2025, "11011", "R2", 2),
    _line("000660", 2025, "11012", "", 1),
    _line("000660", 2026, "11013", "R9", 1),
    _line("005930", 2025, "11011", "R1B", 1),
    _line("005930", 2025, "11011", "R1A", 2),
    _line("005930", 2025, "11011", "R1B", 3),
    _line("999999", 2025, "11011", "R7", 1),
]


class _Storage(FakeSliceStorageMixin):
    def __init__(self, light: bool) -> None:
        super().__init__()
        self.light = light
        self.corps = [_corp("C1", "000660"), _corp("C2", "005930")]
        self.runs: list[object] = []

    def record_run(self, run: object) -> None:
        self.runs.append(run)

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        return [c for c in self.corps if tickers is None or c.ticker in tickers]

    def get_existing_dart_xbrl_document_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        return {("C1", 2025, "11011", "R2")}

    def get_dart_financial_statement_raw(
        self, bsns_years: list[int], reprt_codes: list[str], tickers: list[str] | None = None
    ) -> list[DartFinancialStatementLine]:
        return [
            r
            for r in _ROWS
            if r.bsns_year in bsns_years
            and r.reprt_code in reprt_codes
            and (not tickers or r.ticker in tickers)
        ]

    def get_dart_financial_statement_receipt_keys(
        self, bsns_years: list[int], reprt_codes: list[str], tickers: list[str] | None = None
    ) -> list[tuple[str | None, int, str, str | None]]:
        assert self.light
        rows = self.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
        return list(dict.fromkeys((r.ticker, r.bsns_year, r.reprt_code, r.rcept_no) for r in rows))

    def upsert_dart_xbrl_documents(self, records: list[object]) -> Any:
        from collector.kr.domain.models import UpsertResult

        return UpsertResult(updated=len(records))

    def upsert_dart_xbrl_fact_raw(self, records: list[object]) -> Any:
        from collector.kr.domain.models import UpsertResult

        return UpsertResult(updated=len(records))


def _old_targets(
    allowed: set[tuple[int, str]],
) -> list[tuple[str, int, str, str]]:
    """The pre-change derivation over full rows (order of first appearance)."""
    out: dict[tuple[str, int, str, str], None] = {}
    for row in _ROWS:
        if row.ticker and row.rcept_no and (row.bsns_year, row.reprt_code) in allowed:
            out.setdefault((row.ticker, row.bsns_year, row.reprt_code, row.rcept_no))
    return list(out)


def test_estimate_matches_old_full_row_derivation() -> None:
    allowed = {(2025, "11011"), (2025, "11012")}
    targets = _old_targets(allowed)
    assert targets == [
        ("000660", 2025, "11011", "R2"),
        ("005930", 2025, "11011", "R1B"),
        ("005930", 2025, "11011", "R1A"),
        ("999999", 2025, "11011", "R7"),
    ]
    storage = _Storage(light=True)
    # 999999 has no corp, C1/R2 already exists -> two attempts; skip key removes one more.
    got = _dart_xbrl_actual_attempt_estimate(
        storage=storage,
        allowed_pairs=allowed,
        tickers=None,
        force=False,
        skip_request_keys=set(),
    )
    assert got == 2
    got_skip = _dart_xbrl_actual_attempt_estimate(
        storage=storage,
        allowed_pairs=allowed,
        tickers=None,
        force=False,
        skip_request_keys={"005930:2025:11011:R1A"},
    )
    assert got_skip == 1
    got_force = _dart_xbrl_actual_attempt_estimate(
        storage=storage,
        allowed_pairs=allowed,
        tickers=None,
        force=True,
        skip_request_keys=set(),
    )
    assert got_force == 3


def test_sync_requests_same_targets_in_same_order_as_old_rows() -> None:
    allowed = {(2025, "11011"), (2025, "11012")}
    expected = [t for t in _old_targets(allowed) if t[0] != "999999"]
    storage = _Storage(light=True)
    provider = MockXbrlProvider()
    seen: list[tuple[str, int, str, str]] = []
    orig = provider.fetch_xbrl

    def spy(corp: DartCorp, bsns_year: int, reprt_code: str, rcept_no: str) -> Any:
        seen.append((corp.ticker or "", bsns_year, reprt_code, rcept_no))
        return orig(corp, bsns_year, reprt_code, rcept_no)

    provider.fetch_xbrl = spy  # type: ignore[method-assign]
    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011", "11012"],
        tickers=None,
        allowed_year_report_pairs=allowed,
        rate_limit_seconds=0.0,
    )
    assert result.errors == {}
    assert result.targets_processed == 4  # 999999 has no corp but is still a target
    # C1/R2 exists already; 999999 has no corp.
    assert seen == [t for t in expected if t != ("000660", 2025, "11011", "R2")]

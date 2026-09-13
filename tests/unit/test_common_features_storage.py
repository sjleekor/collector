from __future__ import annotations

import inspect
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureObservation,
    CommonFeatureSeries,
)
from collector.kr.infra.db_postgres import repositories
from collector.kr.infra.db_postgres.repositories import PostgresStorage


def _normalized_ddl() -> str:
    return " ".join(Path("sql/postgres_ddl.sql").read_text(encoding="utf-8").split())


def test_common_feature_tables_are_declared_in_postgres_ddl() -> None:
    ddl = _normalized_ddl()

    # Decision 7 / refactor §5: only the raw observations + the shared series
    # config remain Postgres tables. The catalog(_input) + daily fact were
    # decommissioned (catalog -> code, daily fact -> DuckDB mart).
    assert "CREATE TABLE IF NOT EXISTS common_feature_series" in ddl
    assert "CREATE TABLE IF NOT EXISTS common_feature_observation_raw" in ddl
    assert "CREATE TABLE IF NOT EXISTS common_feature_catalog" not in ddl
    assert "CREATE TABLE IF NOT EXISTS common_feature_daily_fact" not in ddl
    assert "UNIQUE NULLS NOT DISTINCT" in ddl
    assert "REFERENCES common_feature_series(series_id)" in ddl


def test_common_feature_empty_upserts_do_not_connect() -> None:
    storage = PostgresStorage("postgresql://unused")

    assert storage.upsert_common_feature_series([]).updated == 0
    assert storage.upsert_common_feature_observations([]).updated == 0
    assert storage.upsert_common_feature_catalog([]).updated == 0
    assert storage.upsert_common_feature_daily_facts([]).updated == 0


def test_common_feature_observation_upsert_requires_available_from_date() -> None:
    storage = PostgresStorage("postgresql://unused")
    observation = CommonFeatureObservation(
        source=Source.PYKRX,
        series_id="market_kospi",
        observation_date=date(2026, 6, 8),
        frequency="D",
        fetched_at=datetime(2026, 6, 8, 18, 30, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="available_from_date"):
        storage.upsert_common_feature_observations([observation])


class _FakeCursor:
    def __init__(self) -> None:
        self.rowcount = 0
        self.executed: list[tuple[object, object]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def execute(self, statement: object, params: object = None) -> None:
        self.executed.append((statement, params))


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self, *args: object, **kwargs: object) -> _FakeCursor:
        return self._cursor


def test_upsert_common_feature_series_maps_domain_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_cursor = _FakeCursor()
    execute_values_calls: list[tuple[str, list[tuple[Any, ...]], int]] = []

    @contextmanager
    def fake_get_connection(_dsn: str) -> Iterator[_FakeConnection]:
        yield _FakeConnection(fake_cursor)

    def fake_execute_values(
        cur: _FakeCursor,
        sql: str,
        args: list[tuple[Any, ...]],
        page_size: int,
    ) -> None:
        execute_values_calls.append((sql, args, page_size))
        cur.rowcount = len(args)

    monkeypatch.setattr(repositories, "get_connection", fake_get_connection)
    monkeypatch.setattr(repositories.psycopg2.extras, "execute_values", fake_execute_values)

    storage = PostgresStorage("postgresql://unused")
    result = storage.upsert_common_feature_series(
        [
            CommonFeatureSeries(
                series_id="market_kospi",
                source=Source.PYKRX,
                source_series_key="1001",
                category="market_index",
                frequency="D",
                name_kr="KOSPI",
                endpoint_params={"index_code": "1001"},
                availability_policy="next_krx_session",
            )
        ]
    )

    assert result.updated == 1
    sql, args, page_size = execute_values_calls[0]
    assert "INSERT INTO common_feature_series" in sql
    assert "ON CONFLICT (series_id) DO UPDATE" in sql
    # Batching is owned by _execute_values_counted now: it slices the args and
    # passes the actual page length, so the value seen here is the page size,
    # not the configured maximum. What matters is that every row went through.
    assert page_size == len(args)
    assert args[0][0:6] == (
        "market_kospi",
        "PYKRX",
        "1001",
        "market_index",
        "D",
        "KOSPI",
    )
    assert args[0][10].adapted == {"index_code": "1001"}
    assert args[0][11] == "next_krx_session"


def test_upsert_common_feature_catalog_writes_input_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_cursor = _FakeCursor()
    execute_values_calls: list[tuple[str, list[tuple[Any, ...]], int]] = []

    @contextmanager
    def fake_get_connection(_dsn: str) -> Iterator[_FakeConnection]:
        yield _FakeConnection(fake_cursor)

    def fake_execute_values(
        cur: _FakeCursor,
        sql: str,
        args: list[tuple[Any, ...]],
        page_size: int,
    ) -> None:
        execute_values_calls.append((sql, args, page_size))
        cur.rowcount = len(args)

    monkeypatch.setattr(repositories, "get_connection", fake_get_connection)
    monkeypatch.setattr(repositories.psycopg2.extras, "execute_values", fake_execute_values)

    storage = PostgresStorage("postgresql://unused")
    storage.upsert_common_feature_catalog(
        [
            CommonFeatureCatalogEntry(
                feature_code="market_kospi_close",
                feature_name_kr="KOSPI 종가",
                category="market_index",
                input_series_ids=("market_kospi",),
            ),
            CommonFeatureCatalogEntry(
                feature_code="rate_kr_term_spread_10y_3y",
                feature_name_kr="국고채 10년-3년 스프레드",
                category="rate",
                transform_code="spread",
                input_series_ids=("rate_kr_gov10y", "rate_kr_gov3y"),
                input_roles=("spread_long", "spread_short"),
            ),
        ]
    )

    # Second execute_values call is the link-table insert with role tuples.
    link_sql, link_args, _ = execute_values_calls[1]
    assert "INSERT INTO common_feature_catalog_input" in link_sql
    assert ("market_kospi_close", "market_kospi", "primary") in link_args
    assert ("rate_kr_term_spread_10y_3y", "rate_kr_gov10y", "spread_long") in link_args
    assert ("rate_kr_term_spread_10y_3y", "rate_kr_gov3y", "spread_short") in link_args


# ---------------------------------------------------------------------------
# _execute_values_counted (O-6)
#
# psycopg2 issues one statement per page, so cur.rowcount afterwards reports
# only the FINAL page. Callers compare that number against what they fetched
# to decide whether a write landed, so a 1,704-row batch coming back as 704
# fails every slice larger than a page — which is exactly what the market-cap
# reconciliation reported on 2026-08-15.
# ---------------------------------------------------------------------------


class _CountingCursor:
    """Mimics psycopg2: rowcount reflects only the most recent statement."""

    def __init__(self) -> None:
        self.rowcount = 0
        self.pages: list[int] = []


def _counting_execute_values(cur, sql, args, page_size, **kwargs):  # noqa: ANN001
    cur.pages.append(len(args))
    cur.rowcount = len(args)  # last statement only, as psycopg2 does


def test_execute_values_counted_sums_every_page(monkeypatch) -> None:
    monkeypatch.setattr(repositories.psycopg2.extras, "execute_values", _counting_execute_values)
    cur = _CountingCursor()

    total = repositories._execute_values_counted(
        cur, "INSERT ...", [(i,) for i in range(1704)], page_size=1000
    )

    assert cur.pages == [1000, 704]
    # cur.rowcount alone would say 704 here; that undercount is the defect.
    assert cur.rowcount == 704
    assert total == 1704


def test_execute_values_counted_handles_a_single_page(monkeypatch) -> None:
    monkeypatch.setattr(repositories.psycopg2.extras, "execute_values", _counting_execute_values)
    cur = _CountingCursor()

    total = repositories._execute_values_counted(cur, "INSERT ...", [(1,), (2,)], page_size=1000)

    assert cur.pages == [2]
    assert total == 2


def test_execute_values_counted_on_empty_input_writes_nothing(monkeypatch) -> None:
    monkeypatch.setattr(repositories.psycopg2.extras, "execute_values", _counting_execute_values)
    cur = _CountingCursor()

    assert repositories._execute_values_counted(cur, "INSERT ...", [], page_size=1000) == 0
    assert cur.pages == []


def test_execute_values_counted_omits_template_when_unset(monkeypatch) -> None:
    # Passing template=None explicitly breaks callers that never accepted the
    # keyword; only forward it when the caller actually supplied one.
    seen: list[dict] = []

    def _record(cur, sql, args, page_size, **kwargs):  # noqa: ANN001
        seen.append(kwargs)
        cur.rowcount = len(args)

    monkeypatch.setattr(repositories.psycopg2.extras, "execute_values", _record)
    cur = _CountingCursor()

    repositories._execute_values_counted(cur, "INSERT ...", [(1,)])
    repositories._execute_values_counted(cur, "INSERT ...", [(1,)], template="(%s)")

    assert seen == [{}, {"template": "(%s)"}]


def test_the_helper_is_the_only_execute_values_call_site() -> None:
    """Every batched write goes through the counting helper.

    O-6 replaced 19 call sites and left three behind, and one of them --
    ``upsert_stock_master`` -- read ``cur.rowcount`` straight after a paged
    write. stock_master holds 2,794 rows against a 1,000-row page, so the daily
    universe sync recorded roughly 794 in ``ingestion_runs`` every single day.

    Counting call sites by hand is how the three got missed, so the rule is
    mechanical instead: the module may name ``execute_values`` exactly once, in
    the helper. A new caller that wants no count still routes through it and
    ignores the return value.
    """
    source = inspect.getsource(repositories)
    call_sites = source.count("psycopg2.extras.execute_values(")

    assert call_sites == 1, (
        f"{call_sites} execute_values call sites; batched writes must go through "
        "_execute_values_counted so cur.rowcount cannot report only the last page"
    )

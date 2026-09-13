"""PostgreSQL storage implementation.

Implements the :class:`~collector.kr.ports.storage.Storage` protocol
using ``psycopg2`` against the schema defined in ``sql/postgres_ddl.sql``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2.extras

from collector.kr.domain.enums import (
    ListingStatus,
    Market,
    PeriodicExtraStatement,
    RunStatus,
    RunType,
    SliceStatus,
    Source,
)
from collector.kr.domain.models import (
    CollectionSliceState,
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
    CommonFeatureObservation,
    CommonFeatureSeries,
    CompanyProfile,
    DailyBar,
    DailyMarketCapRow,
    DartCapitalChangeLine,
    DartCorp,
    DartFilingReceiptLine,
    DartFinancialStatementLine,
    DartPeriodicExtraLine,
    DartShareCountLine,
    DartShareholderReturnLine,
    DartXbrlDocument,
    DartXbrlFactLine,
    IngestionRun,
    MetricCatalogEntry,
    MetricMappingRule,
    OperatingMetricFact,
    OperatingSourceDocument,
    SecurityFlowLine,
    Stock,
    StockMetricFact,
    StockUniverseSnapshot,
    UpsertResult,
)
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.infra.db_postgres.connection import get_connection
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

#: Which raw table each DS002 statement type lands in (N6).  Five endpoints,
#: two tables: people and pay in one, control and audit in the other.
PERIODIC_EXTRA_TABLES: dict[PeriodicExtraStatement, str] = {
    PeriodicExtraStatement.EMPLOYEE: "dart_employee_raw",
    PeriodicExtraStatement.EXECUTIVE: "dart_employee_raw",
    PeriodicExtraStatement.MAJOR_SHAREHOLDER: "dart_governance_raw",
    PeriodicExtraStatement.MAJOR_CHANGE: "dart_governance_raw",
    PeriodicExtraStatement.AUDIT_OPINION: "dart_governance_raw",
}


def _execute_values_counted(
    cur: Any,
    statement: str,
    args: list,
    *,
    page_size: int = 1000,
    template: str | None = None,
) -> int:
    """``execute_values`` whose returned count is the total, not the last page.

    psycopg2 issues one statement per page, so ``cur.rowcount`` afterwards
    reports only the FINAL page: a 1,704-row batch at ``page_size=1000`` comes
    back as 704.  Callers compare that number against what they fetched to
    decide whether a write landed, so the undercount is not cosmetic — it fails
    every slice larger than a page.  Found 2026-08-15 when the market-cap
    reconciliation check reported "fetched 1702, stored 702" for every KOSDAQ
    slice.
    """
    total = 0
    for offset in range(0, len(args), page_size):
        page = args[offset : offset + page_size]
        extra = {} if template is None else {"template": template}
        psycopg2.extras.execute_values(cur, statement, page, page_size=len(page), **extra)
        total += cur.rowcount
    return total


class PostgresStorage:
    """PostgreSQL-backed storage conforming to the ``Storage`` protocol.

    Args:
        dsn: PostgreSQL connection string.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    # -- Schema management ----------------------------------------------------

    def init_schema(self) -> None:
        """Execute ``sql/postgres_ddl.sql`` to create / update tables."""
        # Find sql file relative to project root
        # Since this code is in src/collector/kr/infra/db_postgres,
        # we can go up 5 levels and into 'sql'.
        sql_path = Path(__file__).parent.parent.parent.parent.parent.parent / "sql" / "postgres_ddl.sql"
        if not sql_path.exists():
            logger.error("DDL file not found at %s", sql_path)
            raise FileNotFoundError(f"DDL file not found at {sql_path}")

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_path.read_text(encoding="utf-8"))
        logger.info("Schema initialized successfully.")

    # -- Stock master ---------------------------------------------------------

    def upsert_stock_master(
        self,
        stocks: list[Stock],
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Upsert stock_master rows and persist the snapshot."""
        if not stocks:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                # 1. Insert snapshot metadata
                cur.execute(
                    """
                    INSERT INTO stock_master_snapshot (
                        snapshot_id,
                        as_of_date,
                        source,
                        fetched_at,
                        record_count
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.as_of_date,
                        snapshot.source.value,
                        snapshot.fetched_at,
                        snapshot.record_count,
                    ),
                )

                # 2. Insert snapshot items
                snapshot_items_args = [
                    (
                        snapshot.snapshot_id,
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.status.value,
                        s.listing_date,
                    )
                    for s in snapshot.records
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO stock_master_snapshot_items (
                        snapshot_id,
                        ticker,
                        market,
                        name,
                        status,
                        listing_date
                    )
                    VALUES %s
                    ON CONFLICT (snapshot_id, ticker, market) DO NOTHING
                    """,
                    snapshot_items_args,
                    page_size=1000,
                )

                # 3. Upsert stock_master
                master_args = [
                    (
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.status.value,
                        s.last_seen_date,
                        s.source.value,
                        s.listing_date,
                        s.first_seen_date or snapshot.as_of_date,
                    )
                    for s in stocks
                ]

                # The upsert cannot separate inserted from updated, so the whole
                # affected count is reported as `updated`.
                #
                # It goes through the counting helper because `cur.rowcount`
                # after a paged execute_values reports the LAST page only: with
                # 2,794 stocks and a 1,000-row page this silently reported ~794
                # every day (O-6 replaced the other call sites and missed this
                # one).
                #
                # listing_date: a fresh non-NULL source value wins (corrections
                #   propagate); a source NULL never clobbers a stored value.
                # first_seen_date: set once on first insert, never overwritten.
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO stock_master
                        (ticker, market, name, status, last_seen_date, source,
                         listing_date, first_seen_date)
                    VALUES %s
                    ON CONFLICT (ticker, market) DO UPDATE SET
                        name            = EXCLUDED.name,
                        status          = EXCLUDED.status,
                        last_seen_date  = EXCLUDED.last_seen_date,
                        source          = EXCLUDED.source,
                        listing_date    = COALESCE(EXCLUDED.listing_date,
                                                   stock_master.listing_date),
                        first_seen_date = COALESCE(stock_master.first_seen_date,
                                                   EXCLUDED.first_seen_date),
                        updated_at      = now()
                    """,
                    master_args,
                )

        return result

    def get_stocks_seen_only_in_snapshots(
        self,
        sources: list[Source] | None = None,
    ) -> list[Stock]:
        """Return securities present in a snapshot and absent from stock_master.

        The key is ``(ticker, market)`` because that is the master's primary
        key: a security that transferred markets legitimately holds two rows,
        and the older one is exactly the historical record this recovers.
        """
        params: list[object] = []
        source_clause = ""
        if sources:
            source_clause = "WHERE s.source = ANY(%s)"
            params.append([source.value for source in sources])

        query = f"""
            WITH seen AS (
                SELECT
                    i.ticker,
                    i.market,
                    i.name,
                    i.listing_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY i.ticker, i.market ORDER BY s.as_of_date DESC
                    ) AS recency,
                    MIN(s.as_of_date) OVER (PARTITION BY i.ticker, i.market) AS first_seen,
                    MAX(s.as_of_date) OVER (PARTITION BY i.ticker, i.market) AS last_seen
                FROM stock_master_snapshot s
                JOIN stock_master_snapshot_items i ON i.snapshot_id = s.snapshot_id
                {source_clause}
            )
            SELECT ticker, market, name, listing_date, first_seen, last_seen
            FROM seen
            WHERE recency = 1
              AND NOT EXISTS (
                  SELECT 1 FROM stock_master m
                  WHERE m.ticker = seen.ticker AND m.market = seen.market
              )
            ORDER BY ticker, market
        """

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        return [
            Stock(
                ticker=row[0],
                market=Market(row[1]),
                name=row[2] or row[0],
                # The caller decides the status; a row recovered from a
                # snapshot carries no claim about today.
                status=ListingStatus.UNKNOWN,
                last_seen_date=row[5],
                source=Source.PYKRX_BACKFILL,
                listing_date=row[3],
                first_seen_date=row[4],
            )
            for row in rows
        ]

    def upsert_stock_master_rows(self, stocks: list[Stock]) -> int:
        """Upsert stock-master rows on their own, with no snapshot bookkeeping.

        ``upsert_stock_master`` writes a snapshot as well, which is right for a
        universe sync and wrong for recovering historical rows: those rows come
        *from* snapshots that already exist, so writing another would double-count
        the audit trail.
        """
        if not stocks:
            return 0

        args = [
            (
                stock.ticker,
                stock.market.value,
                stock.name,
                stock.status.value,
                stock.last_seen_date,
                stock.source.value,
                stock.listing_date,
                stock.first_seen_date,
            )
            for stock in stocks
        ]

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                return _execute_values_counted(
                    cur,
                    """
                    INSERT INTO stock_master
                        (ticker, market, name, status, last_seen_date, source,
                         listing_date, first_seen_date)
                    VALUES %s
                    ON CONFLICT (ticker, market) DO UPDATE SET
                        name            = EXCLUDED.name,
                        status          = EXCLUDED.status,
                        last_seen_date  = EXCLUDED.last_seen_date,
                        source          = EXCLUDED.source,
                        listing_date    = COALESCE(EXCLUDED.listing_date,
                                                   stock_master.listing_date),
                        first_seen_date = LEAST(
                            COALESCE(stock_master.first_seen_date, EXCLUDED.first_seen_date),
                            COALESCE(EXCLUDED.first_seen_date, stock_master.first_seen_date)
                        ),
                        updated_at      = now()
                    """,
                    args,
                )

    def insert_stock_master_snapshot_only(
        self,
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Persist a snapshot and its items without touching ``stock_master``.

        Deliberately steps 1 and 2 of ``upsert_stock_master`` and NOT step 3 —
        see the port docstring.  A historical snapshot must never rewrite the
        current universe.
        """
        if not snapshot.records:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                # Idempotent on (as_of_date, source): a second run over the same
                # date must not stack duplicate snapshots.
                cur.execute(
                    """
                    SELECT 1 FROM stock_master_snapshot
                    WHERE as_of_date = %s AND source = %s
                    LIMIT 1
                    """,
                    (snapshot.as_of_date, snapshot.source.value),
                )
                if cur.fetchone() is not None:
                    return result

                cur.execute(
                    """
                    INSERT INTO stock_master_snapshot (
                        snapshot_id,
                        as_of_date,
                        source,
                        fetched_at,
                        record_count
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.as_of_date,
                        snapshot.source.value,
                        snapshot.fetched_at,
                        snapshot.record_count,
                    ),
                )

                items = [
                    (
                        snapshot.snapshot_id,
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.status.value,
                        s.listing_date,
                    )
                    for s in snapshot.records
                ]
                result.inserted = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO stock_master_snapshot_items (
                        snapshot_id,
                        ticker,
                        market,
                        name,
                        status,
                        listing_date
                    )
                    VALUES %s
                    ON CONFLICT (snapshot_id, ticker, market) DO NOTHING
                    """,
                    items,
                )

        return result

    def get_universe_as_of(
        self,
        as_of: date,
        market: Market | None = None,
    ) -> tuple[date, set[str]] | None:
        """Return the universe listed on or before ``as_of``, from snapshots."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT snapshot_id, as_of_date
                    FROM stock_master_snapshot
                    WHERE as_of_date <= %s
                    ORDER BY as_of_date DESC, fetched_at DESC
                    LIMIT 1
                    """,
                    (as_of,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                snapshot_id, snapshot_date = row

                query = "SELECT ticker FROM stock_master_snapshot_items WHERE snapshot_id = %s"
                params: list[object] = [snapshot_id]
                if market is not None:
                    query += " AND market = %s"
                    params.append(market.value)

                cur.execute(query, params)
                return snapshot_date, {r[0] for r in cur.fetchall()}

    def get_snapshot_record_counts(
        self,
        limit: int = 24,
    ) -> list[tuple[date, Source, int]]:
        """Return recent snapshot sizes, newest first."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT as_of_date, source, record_count
                    FROM stock_master_snapshot
                    ORDER BY as_of_date DESC, fetched_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return [(r[0], Source(r[1]), r[2]) for r in cur.fetchall()]

    def get_existing_snapshot_dates(self, source: Source) -> set[date]:
        """Return ``as_of_date`` values already captured for *source*."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT as_of_date FROM stock_master_snapshot WHERE source = %s",
                    (source.value,),
                )
                return {row[0] for row in cur.fetchall()}

    def get_stocks(
        self,
        market: Market | None = None,
        statuses: list[ListingStatus] | None = None,
        tickers: list[str] | None = None,
    ) -> list[Stock]:
        """Return stock-master rows without assuming they are still listed."""
        sql = "SELECT * FROM stock_master"
        conditions: list[str] = []
        params: list[object] = []

        if statuses:
            conditions.append("status = ANY(%s)")
            params.append([s.value for s in statuses])
        if market:
            conditions.append("market = %s")
            params.append(market.value)
        if tickers:
            conditions.append("ticker = ANY(%s)")
            params.append(list(tickers))

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY ticker, market"

        stocks: list[Stock] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    stocks.append(
                        Stock(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            name=row["name"],
                            status=ListingStatus(row["status"]),
                            last_seen_date=row["last_seen_date"],
                            source=Source(row["source"]),
                            listing_date=row["listing_date"],
                            first_seen_date=row["first_seen_date"],
                        )
                    )
        return stocks

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        """Return currently active stocks from stock_master."""
        stocks = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if market:
                    cur.execute(
                        "SELECT * FROM stock_master WHERE status = 'ACTIVE' AND market = %s",
                        (market.value,),
                    )
                else:
                    cur.execute("SELECT * FROM stock_master WHERE status = 'ACTIVE'")

                for row in cur.fetchall():
                    stocks.append(
                        Stock(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            name=row["name"],
                            status=ListingStatus(row["status"]),
                            last_seen_date=row["last_seen_date"],
                            source=Source(row["source"]),
                            listing_date=row["listing_date"],
                            first_seen_date=row["first_seen_date"],
                        )
                    )
        return stocks

    def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
        """Upsert OpenDART corp-code master rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.corp_name,
                        record.market.value if record.market else None,
                        record.stock_name,
                        record.modify_date,
                        record.is_active,
                        record.source.value,
                        record.fetched_at,
                    )
                    for record in records
                ]

                _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_corp_master (
                        corp_code,
                        ticker,
                        corp_name,
                        market,
                        stock_name,
                        modify_date,
                        is_active,
                        source,
                        fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (corp_code) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        corp_name = EXCLUDED.corp_name,
                        market = EXCLUDED.market,
                        stock_name = EXCLUDED.stock_name,
                        modify_date = EXCLUDED.modify_date,
                        is_active = EXCLUDED.is_active,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        updated_at = now()
                    """,
                    args,
                )

        return result

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        """Return OpenDART corp master rows mapped to local tickers."""
        records: list[DartCorp] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = "SELECT * FROM dart_corp_master"
                conditions: list[str] = []
                params: list[object] = []

                if include_delisted:
                    # The HISTORICAL listed set: every corp that ever carried a
                    # stock_code, whether or not it is listed today (3,959 vs
                    # 2,657).  Deliberately NOT `active_only=False`, which would
                    # also pull in the ~112k corps that never had a ticker.
                    conditions.append("ticker IS NOT NULL AND ticker <> ''")
                elif active_only:
                    conditions.append("is_active = TRUE")
                if tickers:
                    conditions.append("ticker = ANY(%s)")
                    params.append(tickers)

                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)
                sql += " ORDER BY ticker NULLS LAST, corp_code"

                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartCorp(
                            corp_code=row["corp_code"],
                            corp_name=row["corp_name"],
                            ticker=row["ticker"],
                            market=Market(row["market"]) if row["market"] else None,
                            stock_name=row["stock_name"],
                            modify_date=row["modify_date"],
                            is_active=row["is_active"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                        )
                    )
        return records

    def upsert_company_profiles(self, profiles: list[CompanyProfile]) -> UpsertResult:
        """Write company.json fields onto existing ``dart_corp_master`` rows.

        An UPDATE, not an upsert — a profile whose corp_code is not already in
        the master would be an upstream bug, and inserting it would create a row
        with no corp_name (NOT NULL) anyway.
        """
        import json

        if not profiles:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        p.corp_code,
                        p.induty_code,
                        p.corp_cls,
                        p.est_dt,
                        p.acc_mt,
                        json.dumps(p.raw_payload, ensure_ascii=False),
                        p.fetched_at,
                    )
                    for p in profiles
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    UPDATE dart_corp_master AS m SET
                        induty_code        = v.induty_code,
                        corp_cls           = v.corp_cls,
                        est_dt             = v.est_dt,
                        acc_mt             = v.acc_mt,
                        profile_raw        = v.profile_raw,
                        profile_fetched_at = v.profile_fetched_at,
                        updated_at         = now()
                    FROM (VALUES %s) AS v (
                        corp_code, induty_code, corp_cls, est_dt, acc_mt,
                        profile_raw, profile_fetched_at
                    )
                    WHERE m.corp_code = v.corp_code
                    """,
                    args,
                    template="(%s, %s, %s, %s::date, %s, %s::jsonb, %s::timestamptz)",
                )

        return result

    def get_profiled_corp_codes(self) -> set[str]:
        """Return corp_codes whose profile has already been fetched."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT corp_code FROM dart_corp_master WHERE profile_fetched_at IS NOT NULL"
                )
                return {row[0] for row in cur.fetchall()}

    def append_company_profile_history(
        self,
        profiles: list[CompanyProfile],
        observed_month: date,
        run_id: str | None = None,
    ) -> UpsertResult:
        """Append one monthly observation per profile (F-1).

        ``DO NOTHING`` on ``(corp_code, observed_month)``: the history is
        append-only and a second run in the same month must not rewrite an
        observation that was already made.  ``updated`` therefore counts real
        insertions, and a repeat run reports 0 — the idempotency check.
        """
        import json

        if not profiles:
            return UpsertResult()

        result = UpsertResult()
        month = observed_month.replace(day=1)

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        p.corp_code,
                        month,
                        p.fetched_at,
                        p.ticker,
                        p.corp_cls,
                        p.induty_code,
                        p.est_dt,
                        p.acc_mt,
                        p.corp_name,
                        p.stock_name,
                        json.dumps(p.raw_payload, ensure_ascii=False),
                        run_id,
                        p.fetched_at,
                    )
                    for p in profiles
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_corp_profile_history (
                        corp_code, observed_month, observed_at, ticker, corp_cls,
                        induty_code, est_dt, acc_mt, corp_name, stock_name,
                        is_seed, profile_raw, run_id, source, fetched_at
                    )
                    SELECT
                        v.corp_code, v.observed_month, v.observed_at, v.ticker, v.corp_cls,
                        v.induty_code, v.est_dt, v.acc_mt, v.corp_name, v.stock_name,
                        FALSE, v.profile_raw, v.run_id, 'OPENDART', v.fetched_at
                    FROM (VALUES %s) AS v (
                        corp_code, observed_month, observed_at, ticker, corp_cls,
                        induty_code, est_dt, acc_mt, corp_name, stock_name,
                        profile_raw, run_id, fetched_at
                    )
                    ON CONFLICT (corp_code, observed_month) DO NOTHING
                    """,
                    args,
                    template=(
                        "(%s, %s::date, %s::timestamptz, %s, %s, %s, %s::date, %s, %s, %s, "
                        "%s::jsonb, %s::uuid, %s::timestamptz)"
                    ),
                )

        return result

    def seed_company_profile_history(
        self, observed_month: date, run_id: str | None = None
    ) -> UpsertResult:
        """Copy the corp master's current profiles in as the first month (F-1).

        ``observed_at`` is the master's own ``profile_fetched_at``, not now():
        the value was observed whenever it was fetched, and pretending it was
        observed at seed time would make the first change interval look shorter
        than it is.  ``is_seed`` marks these rows so a consumer that needs a
        real observation window can exclude them.
        """
        result = UpsertResult()
        month = observed_month.replace(day=1)

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dart_corp_profile_history (
                        corp_code, observed_month, observed_at, ticker, corp_cls,
                        induty_code, est_dt, acc_mt, corp_name, stock_name,
                        is_seed, profile_raw, run_id, source, fetched_at
                    )
                    SELECT
                        m.corp_code, %s::date, m.profile_fetched_at, m.ticker, m.corp_cls,
                        m.induty_code, m.est_dt, m.acc_mt, m.corp_name, m.stock_name,
                        TRUE, m.profile_raw, %s::uuid, 'OPENDART', m.profile_fetched_at
                    FROM dart_corp_master m
                    WHERE m.profile_fetched_at IS NOT NULL
                    ON CONFLICT (corp_code, observed_month) DO NOTHING
                    """,
                    (month, run_id),
                )
                result.updated = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

        return result

    def get_existing_dart_financial_statement_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        fs_divs: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, fs_div) tuples already present in raw."""
        if not bsns_years or not reprt_codes or not fs_divs:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code, fs_div
            FROM dart_financial_statement_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
              AND fs_div = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes, fs_divs]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}

    def get_existing_dart_share_count_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        """Return (corp_code, bsns_year, reprt_code) tuples already present."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code
            FROM dart_share_count_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2]) for row in cur.fetchall()}

    def get_existing_dart_shareholder_return_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, statement_type) tuples already present."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code, statement_type
            FROM dart_shareholder_return_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}

    def get_existing_dart_capital_change_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        """Return (corp_code, bsns_year, reprt_code) tuples already present."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT DISTINCT corp_code, bsns_year, reprt_code
            FROM dart_capital_change_raw
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2]) for row in cur.fetchall()}

    def get_existing_dart_filing_receipt_years(
        self,
        years: list[int],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int]]:
        """Return (corp_code, year) pairs with at least one receipt already stored."""
        if not years:
            return set()
        sql = """
            SELECT DISTINCT corp_code, EXTRACT(YEAR FROM rcept_dt)::int AS year
            FROM dart_filing_receipt_raw
            WHERE EXTRACT(YEAR FROM rcept_dt)::int = ANY(%s)
        """
        params: list[object] = [years]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1]) for row in cur.fetchall()}

    def get_existing_dart_xbrl_document_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, rcept_no) tuples already parsed."""
        if not bsns_years or not reprt_codes:
            return set()
        sql = """
            SELECT corp_code, bsns_year, reprt_code, rcept_no
            FROM dart_xbrl_document
            WHERE bsns_year = ANY(%s)
              AND reprt_code = ANY(%s)
        """
        params: list[object] = [bsns_years, reprt_codes]
        if corp_codes:
            sql += " AND corp_code = ANY(%s)"
            params.append(corp_codes)
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}

    def get_last_successful_run(self, run_type: RunType) -> IngestionRun | None:
        """Return the most recent SUCCESS-status run for the given run_type, or None."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        run_type,
                        started_at,
                        ended_at,
                        status,
                        params,
                        counts,
                        error_summary
                    FROM ingestion_runs
                    WHERE run_type = %s
                      AND status = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (run_type.value, RunStatus.SUCCESS.value),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                params = (
                    row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {})
                )
                counts = (
                    row[6] if isinstance(row[6], dict) else (json.loads(row[6]) if row[6] else {})
                )
                return IngestionRun(
                    run_id=str(row[0]),
                    run_type=RunType(row[1]),
                    started_at=row[2],
                    ended_at=row[3],
                    status=RunStatus(row[4]),
                    params=params,
                    counts=counts,
                    error_summary=row[7],
                )

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        """Upsert OpenDART financial-statement raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.fs_div,
                record.sj_div,
                record.account_id,
                record.ord,
                record.rcept_no,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.fs_div,
                        record.sj_div,
                        record.sj_nm,
                        record.account_id,
                        record.account_nm,
                        record.account_detail,
                        record.thstrm_nm,
                        record.thstrm_amount,
                        record.thstrm_add_amount,
                        record.frmtrm_nm,
                        record.frmtrm_amount,
                        record.frmtrm_q_nm,
                        record.frmtrm_q_amount,
                        record.frmtrm_add_amount,
                        record.bfefrmtrm_nm,
                        record.bfefrmtrm_amount,
                        record.ord,
                        record.currency,
                        record.rcept_no,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_financial_statement_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        sj_div,
                        sj_nm,
                        account_id,
                        account_nm,
                        account_detail,
                        thstrm_nm,
                        thstrm_amount,
                        thstrm_add_amount,
                        frmtrm_nm,
                        frmtrm_amount,
                        frmtrm_q_nm,
                        frmtrm_q_amount,
                        frmtrm_add_amount,
                        bfefrmtrm_nm,
                        bfefrmtrm_amount,
                        ord,
                        currency,
                        rcept_no,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (
                        corp_code,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        sj_div,
                        account_id,
                        ord,
                        rcept_no
                    )
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        sj_nm = EXCLUDED.sj_nm,
                        account_nm = EXCLUDED.account_nm,
                        account_detail = EXCLUDED.account_detail,
                        thstrm_nm = EXCLUDED.thstrm_nm,
                        thstrm_amount = EXCLUDED.thstrm_amount,
                        thstrm_add_amount = EXCLUDED.thstrm_add_amount,
                        frmtrm_nm = EXCLUDED.frmtrm_nm,
                        frmtrm_amount = EXCLUDED.frmtrm_amount,
                        frmtrm_q_nm = EXCLUDED.frmtrm_q_nm,
                        frmtrm_q_amount = EXCLUDED.frmtrm_q_amount,
                        frmtrm_add_amount = EXCLUDED.frmtrm_add_amount,
                        bfefrmtrm_nm = EXCLUDED.bfefrmtrm_nm,
                        bfefrmtrm_amount = EXCLUDED.bfefrmtrm_amount,
                        ord = EXCLUDED.ord,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_share_count_raw(
        self,
        records: list[DartShareCountLine],
    ) -> UpsertResult:
        """Upsert OpenDART share-count raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.se,
                record.rcept_no,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.corp_cls,
                        record.se,
                        record.isu_stock_totqy,
                        record.now_to_isu_stock_totqy,
                        record.now_to_dcrs_stock_totqy,
                        record.redc,
                        record.profit_incnr,
                        record.rdmstk_repy,
                        record.etc,
                        record.istc_totqy,
                        record.tesstk_co,
                        record.distb_stock_co,
                        record.stlm_dt,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_share_count_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        corp_cls,
                        se,
                        isu_stock_totqy,
                        now_to_isu_stock_totqy,
                        now_to_dcrs_stock_totqy,
                        redc,
                        profit_incnr,
                        rdmstk_repy,
                        etc,
                        istc_totqy,
                        tesstk_co,
                        distb_stock_co,
                        stlm_dt,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, bsns_year, reprt_code, se, rcept_no)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        corp_cls = EXCLUDED.corp_cls,
                        isu_stock_totqy = EXCLUDED.isu_stock_totqy,
                        now_to_isu_stock_totqy = EXCLUDED.now_to_isu_stock_totqy,
                        now_to_dcrs_stock_totqy = EXCLUDED.now_to_dcrs_stock_totqy,
                        redc = EXCLUDED.redc,
                        profit_incnr = EXCLUDED.profit_incnr,
                        rdmstk_repy = EXCLUDED.rdmstk_repy,
                        etc = EXCLUDED.etc,
                        istc_totqy = EXCLUDED.istc_totqy,
                        tesstk_co = EXCLUDED.tesstk_co,
                        distb_stock_co = EXCLUDED.distb_stock_co,
                        stlm_dt = EXCLUDED.stlm_dt,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_shareholder_return_raw(
        self,
        records: list[DartShareholderReturnLine],
    ) -> UpsertResult:
        """Upsert OpenDART dividend / treasury-stock flattened raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.statement_type,
                record.row_name,
                record.stock_knd,
                record.dim1,
                record.dim2,
                record.dim3,
                record.metric_code,
                record.rcept_no,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.statement_type,
                        record.row_name,
                        record.stock_knd,
                        record.dim1,
                        record.dim2,
                        record.dim3,
                        record.metric_code,
                        record.metric_name,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.rcept_no,
                        record.stlm_dt,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_shareholder_return_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        statement_type,
                        row_name,
                        stock_knd,
                        dim1,
                        dim2,
                        dim3,
                        metric_code,
                        metric_name,
                        value_numeric,
                        value_text,
                        unit,
                        rcept_no,
                        stlm_dt,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (
                        corp_code,
                        bsns_year,
                        reprt_code,
                        statement_type,
                        row_name,
                        stock_knd,
                        dim1,
                        dim2,
                        dim3,
                        metric_code,
                        rcept_no
                    )
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        metric_name = EXCLUDED.metric_name,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        stlm_dt = EXCLUDED.stlm_dt,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_capital_change_raw(
        self,
        records: list[DartCapitalChangeLine],
    ) -> UpsertResult:
        """Upsert OpenDART capital-change (irdsSttus) raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.rcept_no,
                record.isu_dcrs_de,
                record.isu_dcrs_stle,
                record.isu_dcrs_stock_knd,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.corp_cls,
                        record.isu_dcrs_de,
                        record.isu_dcrs_stle,
                        record.isu_dcrs_stock_knd,
                        record.isu_dcrs_qy,
                        record.isu_dcrs_mstvdv_fval_amount,
                        record.isu_dcrs_mstvdv_fval_amount2,
                        record.stlm_dt,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_capital_change_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        corp_cls,
                        isu_dcrs_de,
                        isu_dcrs_stle,
                        isu_dcrs_stock_knd,
                        isu_dcrs_qy,
                        isu_dcrs_mstvdv_fval_amount,
                        isu_dcrs_mstvdv_fval_amount2,
                        stlm_dt,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (
                        corp_code, bsns_year, reprt_code, rcept_no,
                        isu_dcrs_de, isu_dcrs_stle, isu_dcrs_stock_knd
                    )
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        corp_cls = EXCLUDED.corp_cls,
                        isu_dcrs_qy = EXCLUDED.isu_dcrs_qy,
                        isu_dcrs_mstvdv_fval_amount = EXCLUDED.isu_dcrs_mstvdv_fval_amount,
                        isu_dcrs_mstvdv_fval_amount2 = EXCLUDED.isu_dcrs_mstvdv_fval_amount2,
                        stlm_dt = EXCLUDED.stlm_dt,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_periodic_extras_raw(
        self,
        records: list[DartPeriodicExtraLine],
    ) -> UpsertResult:
        """Upsert DS002 periodic-report extras, routing by statement type.

        Two tables, one entry point.  Which table a statement belongs to is a
        schema fact, so it is resolved here rather than at every call site.
        """
        if not records:
            return UpsertResult()

        by_table: dict[str, list[DartPeriodicExtraLine]] = {}
        for record in records:
            table = PERIODIC_EXTRA_TABLES.get(record.statement_type)
            if table is None:
                raise ValueError(
                    f"No raw table is registered for statement type {record.statement_type!r}"
                )
            by_table.setdefault(table, []).append(record)

        result = UpsertResult()
        for table, table_records in by_table.items():
            partial = self._upsert_periodic_extras_table(table, table_records)
            result.updated += partial.updated
            result.errors += partial.errors
        return result

    def _upsert_periodic_extras_table(
        self,
        table: str,
        records: list[DartPeriodicExtraLine],
    ) -> UpsertResult:
        result = UpsertResult()
        # Deduplicate on the unique key so one response carrying a repeated
        # ordinal cannot make execute_values raise mid-batch.
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.statement_type.value,
                record.rcept_no,
                record.row_ordinal,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.statement_type.value,
                        record.row_ordinal,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    f"""
                    INSERT INTO {table} (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        statement_type,
                        row_ordinal,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (
                        corp_code, bsns_year, reprt_code, statement_type, rcept_no, row_ordinal
                    )
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_filing_receipt_raw(
        self,
        records: list[DartFilingReceiptLine],
    ) -> UpsertResult:
        """Upsert OpenDART disclosure-receipt raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {(record.corp_code, record.rcept_no): record for record in records}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.corp_name,
                        record.stock_code,
                        record.corp_cls,
                        record.report_nm,
                        record.rcept_no,
                        record.flr_nm,
                        record.rcept_dt,
                        record.rm,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_filing_receipt_raw (
                        corp_code,
                        ticker,
                        corp_name,
                        stock_code,
                        corp_cls,
                        report_nm,
                        rcept_no,
                        flr_nm,
                        rcept_dt,
                        rm,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, rcept_no)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        corp_name = EXCLUDED.corp_name,
                        stock_code = EXCLUDED.stock_code,
                        corp_cls = EXCLUDED.corp_cls,
                        report_nm = EXCLUDED.report_nm,
                        flr_nm = EXCLUDED.flr_nm,
                        rcept_dt = EXCLUDED.rcept_dt,
                        rm = EXCLUDED.rm,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_xbrl_documents(
        self,
        records: list[DartXbrlDocument],
    ) -> UpsertResult:
        """Upsert parsed OpenDART XBRL document metadata."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (record.corp_code, record.bsns_year, record.reprt_code, record.rcept_no): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.zip_entry_count,
                        record.instance_document_name,
                        record.label_ko_document_name,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_xbrl_document (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        zip_entry_count,
                        instance_document_name,
                        label_ko_document_name,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, bsns_year, reprt_code, rcept_no)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        zip_entry_count = EXCLUDED.zip_entry_count,
                        instance_document_name = EXCLUDED.instance_document_name,
                        label_ko_document_name = EXCLUDED.label_ko_document_name,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_dart_xbrl_fact_raw(
        self,
        records: list[DartXbrlFactLine],
    ) -> UpsertResult:
        """Upsert parsed OpenDART XBRL fact rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.corp_code,
                record.bsns_year,
                record.reprt_code,
                record.rcept_no,
                record.context_id,
                record.concept_id,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.corp_code,
                        record.ticker,
                        record.bsns_year,
                        record.reprt_code,
                        record.rcept_no,
                        record.concept_id,
                        record.concept_name,
                        record.namespace_uri,
                        record.context_id,
                        record.context_type,
                        record.period_start,
                        record.period_end,
                        record.instant_date,
                        psycopg2.extras.Json(record.dimensions),
                        record.unit_id,
                        record.unit_measure,
                        record.decimals,
                        record.value_numeric,
                        record.value_text,
                        record.is_nil,
                        record.label_ko,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO dart_xbrl_fact_raw (
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        concept_id,
                        concept_name,
                        namespace_uri,
                        context_id,
                        context_type,
                        period_start,
                        period_end,
                        instant_date,
                        dimensions,
                        unit_id,
                        unit_measure,
                        decimals,
                        value_numeric,
                        value_text,
                        is_nil,
                        label_ko,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)
                    DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        concept_name = EXCLUDED.concept_name,
                        namespace_uri = EXCLUDED.namespace_uri,
                        context_type = EXCLUDED.context_type,
                        period_start = EXCLUDED.period_start,
                        period_end = EXCLUDED.period_end,
                        instant_date = EXCLUDED.instant_date,
                        dimensions = EXCLUDED.dimensions,
                        unit_id = EXCLUDED.unit_id,
                        unit_measure = EXCLUDED.unit_measure,
                        decimals = EXCLUDED.decimals,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        is_nil = EXCLUDED.is_nil,
                        label_ko = EXCLUDED.label_ko,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_krx_security_flow_raw(
        self,
        records: list[SecurityFlowLine],
    ) -> UpsertResult:
        """Upsert KRX security-flow raw rows."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.trade_date,
                record.ticker,
                record.market.value,
                record.metric_code,
                record.source.value,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.trade_date,
                        record.ticker,
                        record.market.value,
                        record.metric_code,
                        record.metric_name,
                        record.value,
                        record.unit,
                        record.source.value,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO krx_security_flow_raw (
                        trade_date,
                        ticker,
                        market,
                        metric_code,
                        metric_name,
                        value,
                        unit,
                        source,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (trade_date, ticker, market, metric_code, source)
                    DO UPDATE SET
                        metric_name = EXCLUDED.metric_name,
                        value = EXCLUDED.value,
                        unit = EXCLUDED.unit,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def count_krx_security_flow_daily_market_tickers(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_code: str,
        source: Source,
    ) -> dict[tuple[date, str], int]:
        """Count existing tickers by trade_date/market for one flow metric."""
        if not tickers:
            return {}

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date, market, COUNT(DISTINCT ticker)
                    FROM krx_security_flow_raw
                    WHERE trade_date BETWEEN %s AND %s
                      AND ticker = ANY(%s)
                      AND metric_code = %s
                      AND source = %s
                    GROUP BY trade_date, market
                    """,
                    (start, end, tickers, metric_code, source.value),
                )
                return {(row[0], row[1]): int(row[2]) for row in cur.fetchall()}

    def count_krx_security_flow_ticker_metric_dates(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        source: Source,
    ) -> dict[str, int]:
        """Count existing distinct (trade_date, metric_code) pairs by ticker."""
        if not tickers or not metric_codes:
            return {}

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticker, COUNT(DISTINCT (trade_date, metric_code))
                    FROM krx_security_flow_raw
                    WHERE trade_date BETWEEN %s AND %s
                      AND ticker = ANY(%s)
                      AND metric_code = ANY(%s)
                      AND source = %s
                    GROUP BY ticker
                    """,
                    (start, end, tickers, metric_codes, source.value),
                )
                return {row[0]: int(row[1]) for row in cur.fetchall()}

    def get_krx_security_flow_metric_max_dates(
        self,
        metric_codes: list[str],
        sources: Sequence[Source],
    ) -> dict[str, date]:
        """Return latest stored trade_date by security-flow metric code."""
        if not metric_codes or not sources:
            return {}

        source_values = [source.value for source in sources]
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT metric_code, MAX(trade_date)
                    FROM krx_security_flow_raw
                    WHERE metric_code = ANY(%s)
                      AND source = ANY(%s)
                    GROUP BY metric_code
                    """,
                    (metric_codes, source_values),
                )
                return {row[0]: row[1] for row in cur.fetchall() if row[1] is not None}

    def get_krx_security_flow_ticker_metric_coverage(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        sources: Sequence[Source],
    ) -> dict[tuple[str, str], tuple[int, date]]:
        """Return ``(session_count, latest_date)`` per ``(ticker, metric_code)``."""
        if not tickers or not metric_codes or not sources:
            return {}

        source_values = [source.value for source in sources]
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticker, metric_code, COUNT(DISTINCT trade_date), MAX(trade_date)
                    FROM krx_security_flow_raw
                    WHERE trade_date BETWEEN %s AND %s
                      AND ticker = ANY(%s)
                      AND metric_code = ANY(%s)
                      AND source = ANY(%s)
                    GROUP BY ticker, metric_code
                    """,
                    (start, end, tickers, metric_codes, source_values),
                )
                return {
                    (row[0], row[1]): (int(row[2]), row[3])
                    for row in cur.fetchall()
                    if row[3] is not None
                }

    def upsert_operating_source_documents(
        self,
        records: list[OperatingSourceDocument],
    ) -> UpsertResult:
        """Upsert operating KPI source documents."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {record.document_key: record for record in records}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.document_key,
                        record.ticker,
                        record.market.value,
                        record.sector_key,
                        record.document_type,
                        record.title,
                        record.document_date,
                        record.period_end,
                        record.source_system,
                        record.source_url,
                        record.language,
                        record.content_text,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO operating_source_document (
                        document_key,
                        ticker,
                        market,
                        sector_key,
                        document_type,
                        title,
                        document_date,
                        period_end,
                        source_system,
                        source_url,
                        language,
                        content_text,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (document_key) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        market = EXCLUDED.market,
                        sector_key = EXCLUDED.sector_key,
                        document_type = EXCLUDED.document_type,
                        title = EXCLUDED.title,
                        document_date = EXCLUDED.document_date,
                        period_end = EXCLUDED.period_end,
                        source_system = EXCLUDED.source_system,
                        source_url = EXCLUDED.source_url,
                        language = EXCLUDED.language,
                        content_text = EXCLUDED.content_text,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_operating_metric_facts(
        self,
        records: list[OperatingMetricFact],
    ) -> UpsertResult:
        """Upsert extracted operating KPI facts."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (
                record.ticker,
                record.metric_code,
                record.period_end,
                record.document_key,
                record.extractor_code,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.ticker,
                        record.market.value,
                        record.sector_key,
                        record.metric_code,
                        record.metric_name,
                        record.period_end,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.document_key,
                        record.extractor_code,
                        record.raw_snippet,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO operating_metric_fact (
                        ticker,
                        market,
                        sector_key,
                        metric_code,
                        metric_name,
                        period_end,
                        value_numeric,
                        value_text,
                        unit,
                        document_key,
                        extractor_code,
                        raw_snippet,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT (ticker, metric_code, period_end, document_key, extractor_code)
                    DO UPDATE SET
                        market = EXCLUDED.market,
                        sector_key = EXCLUDED.sector_key,
                        metric_name = EXCLUDED.metric_name,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        raw_snippet = EXCLUDED.raw_snippet,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )

        return result

    def upsert_metric_catalog(self, records: list[MetricCatalogEntry]) -> UpsertResult:
        """Upsert canonical metric catalog entries."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.metric_code,
                        record.metric_name,
                        record.category,
                        record.unit,
                        record.description,
                        record.is_active,
                    )
                    for record in records
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO metric_catalog (
                        metric_code, metric_name, category, unit, description, is_active
                    )
                    VALUES %s
                    ON CONFLICT (metric_code) DO UPDATE SET
                        metric_name = EXCLUDED.metric_name,
                        category = EXCLUDED.category,
                        unit = EXCLUDED.unit,
                        description = EXCLUDED.description,
                        is_active = EXCLUDED.is_active,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
        return result

    def replace_metric_mapping_rules(self, records: list[MetricMappingRule]) -> UpsertResult:
        """Replace the active metric mapping rules with the provided set."""
        result = UpsertResult()
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE metric_mapping_rule SET is_active = FALSE, updated_at = now()")
                if not records:
                    return result

                args = [
                    (
                        record.rule_code,
                        record.metric_code,
                        record.source_table,
                        record.value_selector,
                        record.priority,
                        record.statement_type,
                        record.fs_div,
                        record.sj_div,
                        record.account_id,
                        record.account_nm,
                        record.row_name,
                        record.stock_knd,
                        record.dim1,
                        record.dim2,
                        record.dim3,
                        record.metric_code_match,
                        record.is_active,
                    )
                    for record in records
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO metric_mapping_rule (
                        rule_code,
                        metric_code,
                        source_table,
                        value_selector,
                        priority,
                        statement_type,
                        fs_div,
                        sj_div,
                        account_id,
                        account_nm,
                        row_name,
                        stock_knd,
                        dim1,
                        dim2,
                        dim3,
                        metric_code_match,
                        is_active
                    )
                    VALUES %s
                    ON CONFLICT (rule_code) DO UPDATE SET
                        metric_code = EXCLUDED.metric_code,
                        source_table = EXCLUDED.source_table,
                        value_selector = EXCLUDED.value_selector,
                        priority = EXCLUDED.priority,
                        statement_type = EXCLUDED.statement_type,
                        fs_div = EXCLUDED.fs_div,
                        sj_div = EXCLUDED.sj_div,
                        account_id = EXCLUDED.account_id,
                        account_nm = EXCLUDED.account_nm,
                        row_name = EXCLUDED.row_name,
                        stock_knd = EXCLUDED.stock_knd,
                        dim1 = EXCLUDED.dim1,
                        dim2 = EXCLUDED.dim2,
                        dim3 = EXCLUDED.dim3,
                        metric_code_match = EXCLUDED.metric_code_match,
                        is_active = EXCLUDED.is_active,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
        return result

    def get_metric_mapping_rules(self) -> list[MetricMappingRule]:
        """Return active metric mapping rules ordered by priority."""
        records: list[MetricMappingRule] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT *
                    FROM metric_mapping_rule
                    WHERE is_active = TRUE
                    ORDER BY priority ASC, rule_code
                    """)
                for row in cur.fetchall():
                    records.append(
                        MetricMappingRule(
                            rule_code=row["rule_code"],
                            metric_code=row["metric_code"],
                            source_table=row["source_table"],
                            value_selector=row["value_selector"],
                            priority=row["priority"],
                            statement_type=row["statement_type"],
                            fs_div=row["fs_div"],
                            sj_div=row["sj_div"],
                            account_id=row["account_id"],
                            account_nm=row["account_nm"],
                            row_name=row["row_name"],
                            stock_knd=row["stock_knd"],
                            dim1=row["dim1"],
                            dim2=row["dim2"],
                            dim3=row["dim3"],
                            metric_code_match=row["metric_code_match"],
                            is_active=row["is_active"],
                        )
                    )
        return records

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        """Return financial statement raw rows for normalization."""
        records: list[DartFinancialStatementLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_financial_statement_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, fs_div, sj_div, ord"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartFinancialStatementLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            fs_div=row["fs_div"],
                            sj_div=row["sj_div"],
                            sj_nm=row["sj_nm"],
                            account_id=row["account_id"],
                            account_nm=row["account_nm"],
                            account_detail=row["account_detail"],
                            thstrm_nm=row["thstrm_nm"],
                            thstrm_amount=row["thstrm_amount"],
                            thstrm_add_amount=row["thstrm_add_amount"],
                            frmtrm_nm=row["frmtrm_nm"],
                            frmtrm_amount=row["frmtrm_amount"],
                            frmtrm_q_nm=row["frmtrm_q_nm"],
                            frmtrm_q_amount=row["frmtrm_q_amount"],
                            frmtrm_add_amount=row["frmtrm_add_amount"],
                            bfefrmtrm_nm=row["bfefrmtrm_nm"],
                            bfefrmtrm_amount=row["bfefrmtrm_amount"],
                            ord=row["ord"],
                            currency=row["currency"] or "",
                            rcept_no=row["rcept_no"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def get_dart_share_count_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareCountLine]:
        """Return share-count raw rows for normalization."""
        records: list[DartShareCountLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_share_count_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, se"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartShareCountLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            rcept_no=row["rcept_no"],
                            corp_cls=row["corp_cls"],
                            se=row["se"],
                            isu_stock_totqy=row["isu_stock_totqy"],
                            now_to_isu_stock_totqy=row["now_to_isu_stock_totqy"],
                            now_to_dcrs_stock_totqy=row["now_to_dcrs_stock_totqy"],
                            redc=row["redc"],
                            profit_incnr=row["profit_incnr"],
                            rdmstk_repy=row["rdmstk_repy"],
                            etc=row["etc"],
                            istc_totqy=row["istc_totqy"],
                            tesstk_co=row["tesstk_co"],
                            distb_stock_co=row["distb_stock_co"],
                            stlm_dt=row["stlm_dt"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def get_dart_shareholder_return_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareholderReturnLine]:
        """Return dividend / treasury-stock raw rows for normalization."""
        records: list[DartShareholderReturnLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_shareholder_return_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, statement_type, row_name"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartShareholderReturnLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            statement_type=row["statement_type"],
                            row_name=row["row_name"],
                            stock_knd=row["stock_knd"],
                            dim1=row["dim1"],
                            dim2=row["dim2"],
                            dim3=row["dim3"],
                            metric_code=row["metric_code"],
                            metric_name=row["metric_name"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"] or "",
                            rcept_no=row["rcept_no"],
                            stlm_dt=row["stlm_dt"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def get_dart_xbrl_fact_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartXbrlFactLine]:
        """Return parsed OpenDART XBRL fact rows for normalization."""
        records: list[DartXbrlFactLine] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM dart_xbrl_fact_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, concept_id, context_id"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        DartXbrlFactLine(
                            corp_code=row["corp_code"],
                            ticker=row["ticker"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            rcept_no=row["rcept_no"],
                            concept_id=row["concept_id"],
                            concept_name=row["concept_name"],
                            namespace_uri=row["namespace_uri"],
                            context_id=row["context_id"],
                            context_type=row["context_type"],
                            period_start=row["period_start"],
                            period_end=row["period_end"],
                            instant_date=row["instant_date"],
                            dimensions=list(row["dimensions"] or []),
                            unit_id=row["unit_id"],
                            unit_measure=row["unit_measure"],
                            decimals=row["decimals"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            is_nil=row["is_nil"],
                            label_ko=row["label_ko"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def iter_dart_financial_statement_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_account_ids: list[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[DartFinancialStatementLine]:
        """Stream financial rows needed by normalization without raw payloads."""
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if not tickers:
            raise ValueError("tickers must be a non-empty list")
        if rule_account_ids is not None and not rule_account_ids:
            return

        with get_connection(self._dsn) as conn:
            with conn.cursor(
                name="normalize_financial",
                cursor_factory=psycopg2.extras.DictCursor,
            ) as cur:
                cur.itersize = page_size
                query = """
                    SELECT
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        sj_div,
                        sj_nm,
                        account_id,
                        account_nm,
                        account_detail,
                        thstrm_nm,
                        thstrm_amount,
                        thstrm_add_amount,
                        frmtrm_nm,
                        frmtrm_amount,
                        frmtrm_q_nm,
                        frmtrm_q_amount,
                        frmtrm_add_amount,
                        bfefrmtrm_nm,
                        bfefrmtrm_amount,
                        ord,
                        currency,
                        rcept_no,
                        source,
                        fetched_at
                    FROM dart_financial_statement_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                      AND ticker = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes, tickers]
                if rule_account_ids is not None:
                    query += " AND account_id = ANY(%s)"
                    params.append(rule_account_ids)
                query += (
                    " ORDER BY ticker, bsns_year, reprt_code, fs_div, sj_div, "
                    "account_id, rcept_no, ord NULLS LAST"
                )
                cur.execute(query, params)
                for row in cur:
                    yield DartFinancialStatementLine(
                        corp_code=row["corp_code"],
                        ticker=row["ticker"],
                        bsns_year=row["bsns_year"],
                        reprt_code=row["reprt_code"],
                        fs_div=row["fs_div"],
                        sj_div=row["sj_div"],
                        sj_nm=row["sj_nm"],
                        account_id=row["account_id"],
                        account_nm=row["account_nm"],
                        account_detail=row["account_detail"],
                        thstrm_nm=row["thstrm_nm"],
                        thstrm_amount=row["thstrm_amount"],
                        thstrm_add_amount=row["thstrm_add_amount"],
                        frmtrm_nm=row["frmtrm_nm"],
                        frmtrm_amount=row["frmtrm_amount"],
                        frmtrm_q_nm=row["frmtrm_q_nm"],
                        frmtrm_q_amount=row["frmtrm_q_amount"],
                        frmtrm_add_amount=row["frmtrm_add_amount"],
                        bfefrmtrm_nm=row["bfefrmtrm_nm"],
                        bfefrmtrm_amount=row["bfefrmtrm_amount"],
                        ord=row["ord"],
                        currency=row["currency"] or "",
                        rcept_no=row["rcept_no"],
                        source=Source(row["source"]),
                        fetched_at=row["fetched_at"],
                        raw_payload={},
                    )

    def iter_dart_share_count_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_se_values: list[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[DartShareCountLine]:
        """Stream share-count rows needed by normalization without raw payloads."""
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if not tickers:
            raise ValueError("tickers must be a non-empty list")
        if rule_se_values is not None and not rule_se_values:
            return

        with get_connection(self._dsn) as conn:
            with conn.cursor(
                name="normalize_share_count",
                cursor_factory=psycopg2.extras.DictCursor,
            ) as cur:
                cur.itersize = page_size
                query = """
                    SELECT
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        corp_cls,
                        se,
                        isu_stock_totqy,
                        now_to_isu_stock_totqy,
                        now_to_dcrs_stock_totqy,
                        redc,
                        profit_incnr,
                        rdmstk_repy,
                        etc,
                        istc_totqy,
                        tesstk_co,
                        distb_stock_co,
                        stlm_dt,
                        source,
                        fetched_at
                    FROM dart_share_count_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                      AND ticker = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes, tickers]
                if rule_se_values is not None:
                    query += " AND se = ANY(%s)"
                    params.append(rule_se_values)
                query += " ORDER BY ticker, bsns_year, reprt_code, se, rcept_no"
                cur.execute(query, params)
                for row in cur:
                    yield DartShareCountLine(
                        corp_code=row["corp_code"],
                        ticker=row["ticker"],
                        bsns_year=row["bsns_year"],
                        reprt_code=row["reprt_code"],
                        rcept_no=row["rcept_no"],
                        corp_cls=row["corp_cls"],
                        se=row["se"],
                        isu_stock_totqy=row["isu_stock_totqy"],
                        now_to_isu_stock_totqy=row["now_to_isu_stock_totqy"],
                        now_to_dcrs_stock_totqy=row["now_to_dcrs_stock_totqy"],
                        redc=row["redc"],
                        profit_incnr=row["profit_incnr"],
                        rdmstk_repy=row["rdmstk_repy"],
                        etc=row["etc"],
                        istc_totqy=row["istc_totqy"],
                        tesstk_co=row["tesstk_co"],
                        distb_stock_co=row["distb_stock_co"],
                        stlm_dt=row["stlm_dt"],
                        source=Source(row["source"]),
                        fetched_at=row["fetched_at"],
                        raw_payload={},
                    )

    def iter_dart_shareholder_return_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        page_size: int = 5000,
    ) -> Iterator[DartShareholderReturnLine]:
        """Stream shareholder-return rows needed by normalization without raw payloads."""
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if not tickers:
            raise ValueError("tickers must be a non-empty list")

        with get_connection(self._dsn) as conn:
            with conn.cursor(
                name="normalize_shareholder_return",
                cursor_factory=psycopg2.extras.DictCursor,
            ) as cur:
                cur.itersize = page_size
                cur.execute(
                    """
                    SELECT
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        statement_type,
                        row_name,
                        stock_knd,
                        dim1,
                        dim2,
                        dim3,
                        metric_code,
                        metric_name,
                        value_numeric,
                        value_text,
                        unit,
                        rcept_no,
                        stlm_dt,
                        source,
                        fetched_at
                    FROM dart_shareholder_return_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                      AND ticker = ANY(%s)
                    ORDER BY ticker, bsns_year, reprt_code, statement_type, row_name,
                             stock_knd, dim1, dim2, dim3, metric_code, rcept_no
                    """,
                    (bsns_years, reprt_codes, tickers),
                )
                for row in cur:
                    yield DartShareholderReturnLine(
                        corp_code=row["corp_code"],
                        ticker=row["ticker"],
                        bsns_year=row["bsns_year"],
                        reprt_code=row["reprt_code"],
                        statement_type=row["statement_type"],
                        row_name=row["row_name"],
                        stock_knd=row["stock_knd"],
                        dim1=row["dim1"],
                        dim2=row["dim2"],
                        dim3=row["dim3"],
                        metric_code=row["metric_code"],
                        metric_name=row["metric_name"],
                        value_numeric=row["value_numeric"],
                        value_text=row["value_text"],
                        unit=row["unit"] or "",
                        rcept_no=row["rcept_no"],
                        stlm_dt=row["stlm_dt"],
                        source=Source(row["source"]),
                        fetched_at=row["fetched_at"],
                        raw_payload={},
                    )

    def iter_dart_xbrl_fact_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_concept_ids: list[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[DartXbrlFactLine]:
        """Stream XBRL fact rows needed by normalization without raw payloads."""
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if not tickers:
            raise ValueError("tickers must be a non-empty list")
        if rule_concept_ids is not None and not rule_concept_ids:
            return

        with get_connection(self._dsn) as conn:
            with conn.cursor(
                name="normalize_xbrl_fact",
                cursor_factory=psycopg2.extras.DictCursor,
            ) as cur:
                cur.itersize = page_size
                query = """
                    SELECT
                        corp_code,
                        ticker,
                        bsns_year,
                        reprt_code,
                        rcept_no,
                        concept_id,
                        concept_name,
                        namespace_uri,
                        context_id,
                        context_type,
                        period_start,
                        period_end,
                        instant_date,
                        dimensions,
                        unit_id,
                        unit_measure,
                        decimals,
                        value_numeric,
                        value_text,
                        is_nil,
                        label_ko,
                        source,
                        fetched_at
                    FROM dart_xbrl_fact_raw
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                      AND ticker = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes, tickers]
                if rule_concept_ids is not None:
                    query += " AND concept_id = ANY(%s)"
                    params.append(rule_concept_ids)
                query += (
                    " ORDER BY ticker, bsns_year, reprt_code, concept_id, " "context_id, rcept_no"
                )
                cur.execute(query, params)
                for row in cur:
                    yield DartXbrlFactLine(
                        corp_code=row["corp_code"],
                        ticker=row["ticker"],
                        bsns_year=row["bsns_year"],
                        reprt_code=row["reprt_code"],
                        rcept_no=row["rcept_no"],
                        concept_id=row["concept_id"],
                        concept_name=row["concept_name"],
                        namespace_uri=row["namespace_uri"],
                        context_id=row["context_id"],
                        context_type=row["context_type"],
                        period_start=row["period_start"],
                        period_end=row["period_end"],
                        instant_date=row["instant_date"],
                        dimensions=list(row["dimensions"] or []),
                        unit_id=row["unit_id"],
                        unit_measure=row["unit_measure"],
                        decimals=row["decimals"],
                        value_numeric=row["value_numeric"],
                        value_text=row["value_text"],
                        is_nil=row["is_nil"],
                        label_ko=row["label_ko"],
                        source=Source(row["source"]),
                        fetched_at=row["fetched_at"],
                        raw_payload={},
                    )

    def upsert_stock_metric_facts(self, records: list[StockMetricFact]) -> UpsertResult:
        """Upsert normalized canonical metric facts."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {
            (record.ticker, record.metric_code, record.bsns_year, record.reprt_code): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.ticker,
                        record.market.value,
                        record.corp_code,
                        record.metric_code,
                        record.period_type,
                        record.period_end,
                        record.bsns_year,
                        record.reprt_code,
                        record.fs_div,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.source_table,
                        record.source_key,
                        record.mapping_rule_code,
                        record.fetched_at,
                    )
                    for record in deduped_records.values()
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO stock_metric_fact (
                        ticker,
                        market,
                        corp_code,
                        metric_code,
                        period_type,
                        period_end,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        value_numeric,
                        value_text,
                        unit,
                        source_table,
                        source_key,
                        mapping_rule_code,
                        fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (ticker, metric_code, bsns_year, reprt_code)
                    DO UPDATE SET
                        market = EXCLUDED.market,
                        corp_code = EXCLUDED.corp_code,
                        period_type = EXCLUDED.period_type,
                        period_end = EXCLUDED.period_end,
                        fs_div = EXCLUDED.fs_div,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        source_table = EXCLUDED.source_table,
                        source_key = EXCLUDED.source_key,
                        mapping_rule_code = EXCLUDED.mapping_rule_code,
                        fetched_at = EXCLUDED.fetched_at,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
        return result

    def delete_stock_metric_facts_for_inactive_rules(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
    ) -> int:
        """Delete normalized facts in scope whose mapping rule is no longer active."""
        if not bsns_years or not reprt_codes or not tickers:
            return 0

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM stock_metric_fact f
                    USING metric_mapping_rule r
                    WHERE f.mapping_rule_code = r.rule_code
                      AND r.is_active = FALSE
                      AND f.bsns_year = ANY(%s)
                      AND f.reprt_code = ANY(%s)
                      AND f.ticker = ANY(%s)
                    """,
                    (bsns_years, reprt_codes, tickers),
                )
                return cur.rowcount

    def get_metric_catalog_entries(self) -> list[MetricCatalogEntry]:
        """Return active canonical metric catalog entries."""
        records: list[MetricCatalogEntry] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT *
                    FROM metric_catalog
                    WHERE is_active = TRUE
                    ORDER BY metric_code
                    """)
                for row in cur.fetchall():
                    records.append(
                        MetricCatalogEntry(
                            metric_code=row["metric_code"],
                            metric_name=row["metric_name"],
                            category=row["category"],
                            unit=row["unit"],
                            description=row["description"],
                            is_active=row["is_active"],
                        )
                    )
        return records

    def get_stock_metric_facts(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[StockMetricFact]:
        """Return normalized canonical metric facts."""
        records: list[StockMetricFact] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM stock_metric_fact
                    WHERE bsns_year = ANY(%s)
                      AND reprt_code = ANY(%s)
                """
                params: list[object] = [bsns_years, reprt_codes]
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                sql += " ORDER BY ticker, bsns_year, reprt_code, metric_code"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        StockMetricFact(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            corp_code=row["corp_code"],
                            metric_code=row["metric_code"],
                            period_type=row["period_type"],
                            period_end=row["period_end"],
                            bsns_year=row["bsns_year"],
                            reprt_code=row["reprt_code"],
                            fs_div=row["fs_div"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"],
                            source_table=row["source_table"],
                            source_key=row["source_key"],
                            mapping_rule_code=row["mapping_rule_code"],
                            fetched_at=row["fetched_at"],
                        )
                    )
        return records

    def get_operating_metric_facts(
        self,
        tickers: list[str] | None = None,
        sector_keys: list[str] | None = None,
    ) -> list[OperatingMetricFact]:
        """Return extracted operating KPI facts."""
        records: list[OperatingMetricFact] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = "SELECT * FROM operating_metric_fact WHERE 1=1"
                params: list[object] = []
                if tickers:
                    sql += " AND ticker = ANY(%s)"
                    params.append(tickers)
                if sector_keys:
                    sql += " AND sector_key = ANY(%s)"
                    params.append(sector_keys)
                sql += " ORDER BY ticker, sector_key, metric_code, period_end DESC NULLS LAST"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        OperatingMetricFact(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            sector_key=row["sector_key"],
                            metric_code=row["metric_code"],
                            metric_name=row["metric_name"],
                            period_end=row["period_end"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"],
                            document_key=row["document_key"],
                            extractor_code=row["extractor_code"],
                            raw_snippet=row["raw_snippet"],
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"],
                        )
                    )
        return records

    def upsert_common_feature_series(
        self,
        records: list[CommonFeatureSeries],
    ) -> UpsertResult:
        """Upsert source-series catalog rows for common features."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {record.series_id: record for record in records}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.series_id,
                        record.source.value,
                        record.source_series_key,
                        record.category,
                        record.frequency,
                        record.name_kr,
                        record.name_en,
                        record.unit,
                        record.country,
                        record.market,
                        psycopg2.extras.Json(record.endpoint_params),
                        record.availability_policy,
                        record.manual_lag_days,
                        record.source_timezone,
                        record.history_start_date,
                        record.max_stale_business_days,
                        record.default_transform,
                        record.active,
                        record.notes,
                    )
                    for record in deduped_records.values()
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO common_feature_series (
                        series_id,
                        source,
                        source_series_key,
                        category,
                        frequency,
                        name_kr,
                        name_en,
                        unit,
                        country,
                        market,
                        endpoint_params,
                        availability_policy,
                        manual_lag_days,
                        source_timezone,
                        history_start_date,
                        max_stale_business_days,
                        default_transform,
                        active,
                        notes
                    )
                    VALUES %s
                    ON CONFLICT (series_id) DO UPDATE SET
                        source = EXCLUDED.source,
                        source_series_key = EXCLUDED.source_series_key,
                        category = EXCLUDED.category,
                        frequency = EXCLUDED.frequency,
                        name_kr = EXCLUDED.name_kr,
                        name_en = EXCLUDED.name_en,
                        unit = EXCLUDED.unit,
                        country = EXCLUDED.country,
                        market = EXCLUDED.market,
                        endpoint_params = EXCLUDED.endpoint_params,
                        availability_policy = EXCLUDED.availability_policy,
                        manual_lag_days = EXCLUDED.manual_lag_days,
                        source_timezone = EXCLUDED.source_timezone,
                        history_start_date = EXCLUDED.history_start_date,
                        max_stale_business_days = EXCLUDED.max_stale_business_days,
                        default_transform = EXCLUDED.default_transform,
                        active = EXCLUDED.active,
                        notes = EXCLUDED.notes,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )
        return result

    def get_common_feature_series(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureSeries]:
        """Return common feature source-series catalog rows."""
        records: list[CommonFeatureSeries] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = "SELECT * FROM common_feature_series WHERE 1=1"
                params: list[object] = []
                if sources:
                    sql += " AND source = ANY(%s)"
                    params.append([source.value for source in sources])
                if series_ids:
                    sql += " AND series_id = ANY(%s)"
                    params.append(series_ids)
                if active_only:
                    sql += " AND active = TRUE"
                sql += " ORDER BY series_id"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        CommonFeatureSeries(
                            series_id=row["series_id"],
                            source=Source(row["source"]),
                            source_series_key=row["source_series_key"],
                            category=row["category"],
                            frequency=row["frequency"],
                            name_kr=row["name_kr"],
                            name_en=row["name_en"],
                            unit=row["unit"],
                            country=row["country"],
                            market=row["market"],
                            endpoint_params=row["endpoint_params"] or {},
                            availability_policy=row["availability_policy"],
                            manual_lag_days=row["manual_lag_days"],
                            source_timezone=row["source_timezone"],
                            history_start_date=row["history_start_date"],
                            max_stale_business_days=row["max_stale_business_days"],
                            default_transform=row["default_transform"],
                            active=row["active"],
                            notes=row["notes"],
                        )
                    )
        return records

    def upsert_common_feature_observations(
        self,
        records: list[CommonFeatureObservation],
    ) -> UpsertResult:
        """Upsert raw common feature observations."""
        if not records:
            return UpsertResult()
        missing_availability = [
            record.series_id for record in records if record.available_from_date is None
        ]
        if missing_availability:
            raise ValueError(
                "common feature observations require available_from_date before upsert"
            )

        result = UpsertResult()
        deduped_records = {
            (
                record.source,
                record.series_id,
                record.observation_date,
                record.period_end_date,
                record.release_date,
                record.vintage,
            ): record
            for record in records
        }
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.source.value,
                        record.series_id,
                        record.observation_date,
                        record.period_end_date,
                        record.release_date,
                        record.available_from_date,
                        record.vintage,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        record.frequency,
                        record.source_updated_at,
                        record.fetched_at,
                        psycopg2.extras.Json(record.raw_payload),
                    )
                    for record in deduped_records.values()
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO common_feature_observation_raw (
                        source,
                        series_id,
                        observation_date,
                        period_end_date,
                        release_date,
                        available_from_date,
                        vintage,
                        value_numeric,
                        value_text,
                        unit,
                        frequency,
                        source_updated_at,
                        fetched_at,
                        raw_payload
                    )
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT uq_common_feature_observation_raw
                    DO UPDATE SET
                        available_from_date = EXCLUDED.available_from_date,
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        frequency = EXCLUDED.frequency,
                        source_updated_at = EXCLUDED.source_updated_at,
                        fetched_at = EXCLUDED.fetched_at,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    args,
                    page_size=1000,
                )
        return result

    def count_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
    ) -> dict[str, int]:
        """Return raw observation counts grouped by series_id."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT series_id, COUNT(*)::int
                    FROM common_feature_observation_raw
                    WHERE 1=1
                """
                params: list[object] = []
                if series_ids:
                    sql += " AND series_id = ANY(%s)"
                    params.append(series_ids)
                if start:
                    sql += " AND observation_date >= %s"
                    params.append(start)
                if end:
                    sql += " AND observation_date <= %s"
                    params.append(end)
                if source:
                    sql += " AND source = %s"
                    params.append(source.value)
                sql += " GROUP BY series_id"
                cur.execute(sql, params)
                return {row[0]: row[1] for row in cur.fetchall()}

    def get_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
        available_from_end: date | None = None,
    ) -> list[CommonFeatureObservation]:
        """Return raw common feature observations for sync/build services."""
        records: list[CommonFeatureObservation] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = "SELECT * FROM common_feature_observation_raw WHERE 1=1"
                params: list[object] = []
                if series_ids:
                    sql += " AND series_id = ANY(%s)"
                    params.append(series_ids)
                if start:
                    sql += " AND observation_date >= %s"
                    params.append(start)
                if end:
                    sql += " AND observation_date <= %s"
                    params.append(end)
                if source:
                    sql += " AND source = %s"
                    params.append(source.value)
                if available_from_end:
                    sql += " AND available_from_date <= %s"
                    params.append(available_from_end)
                sql += """
                    ORDER BY
                        series_id,
                        observation_date,
                        release_date NULLS LAST,
                        vintage,
                        raw_id
                """
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        CommonFeatureObservation(
                            raw_id=row["raw_id"],
                            source=Source(row["source"]),
                            series_id=row["series_id"],
                            observation_date=row["observation_date"],
                            period_end_date=row["period_end_date"],
                            release_date=row["release_date"],
                            available_from_date=row["available_from_date"],
                            vintage=row["vintage"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"],
                            frequency=row["frequency"],
                            source_updated_at=row["source_updated_at"],
                            fetched_at=row["fetched_at"],
                            raw_payload=row["raw_payload"] or {},
                        )
                    )
        return records

    def get_common_feature_observation_max_dates(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
    ) -> dict[str, date]:
        """Return latest raw observation date grouped by series_id."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT series_id, MAX(observation_date)
                    FROM common_feature_observation_raw
                    WHERE 1=1
                """
                params: list[object] = []
                if sources:
                    sql += " AND source = ANY(%s)"
                    params.append([source.value for source in sources])
                if series_ids:
                    sql += " AND series_id = ANY(%s)"
                    params.append(series_ids)
                sql += " GROUP BY series_id"
                cur.execute(sql, params)
                return {row[0]: row[1] for row in cur.fetchall() if row[1] is not None}

    def get_common_feature_observation_dates(
        self,
        *,
        source: Source,
        series_id: str,
        start: date,
        end: date,
    ) -> set[date]:
        """Return stored raw observation dates for one source series and range."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT observation_date
                    FROM common_feature_observation_raw
                    WHERE source = %s
                      AND series_id = %s
                      AND observation_date >= %s
                      AND observation_date <= %s
                    """,
                    (source.value, series_id, start, end),
                )
                return {row[0] for row in cur.fetchall()}

    def upsert_common_feature_catalog(
        self,
        records: list[CommonFeatureCatalogEntry],
    ) -> UpsertResult:
        """Upsert model-facing common feature catalog rows and input links."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {record.feature_code: record for record in records}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.feature_code,
                        record.feature_name_kr,
                        record.category,
                        record.frequency,
                        record.unit,
                        record.transform_code,
                        record.description,
                        record.active,
                    )
                    for record in deduped_records.values()
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO common_feature_catalog (
                        feature_code,
                        feature_name_kr,
                        category,
                        frequency,
                        unit,
                        transform_code,
                        description,
                        active
                    )
                    VALUES %s
                    ON CONFLICT (feature_code) DO UPDATE SET
                        feature_name_kr = EXCLUDED.feature_name_kr,
                        category = EXCLUDED.category,
                        frequency = EXCLUDED.frequency,
                        unit = EXCLUDED.unit,
                        transform_code = EXCLUDED.transform_code,
                        description = EXCLUDED.description,
                        active = EXCLUDED.active,
                        updated_at = now()
                    """,
                    args,
                    page_size=1000,
                )

                feature_codes = list(deduped_records)
                cur.execute(
                    "DELETE FROM common_feature_catalog_input WHERE feature_code = ANY(%s)",
                    (feature_codes,),
                )
                input_args = [
                    (record.feature_code, series_id, role)
                    for record in deduped_records.values()
                    for series_id, role in zip(
                        record.input_series_ids, record.roles(), strict=False
                    )
                ]
                if input_args:
                    _execute_values_counted(
                        cur,
                        """
                        INSERT INTO common_feature_catalog_input (
                            feature_code,
                            series_id,
                            role
                        )
                        VALUES %s
                        ON CONFLICT (feature_code, series_id, role) DO NOTHING
                        """,
                        input_args,
                    )
        return result

    def get_common_feature_catalog(
        self,
        feature_codes: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureCatalogEntry]:
        """Return model-facing common feature catalog rows."""
        records: list[CommonFeatureCatalogEntry] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT
                        c.*,
                        COALESCE(
                            array_agg(i.series_id ORDER BY i.role, i.series_id)
                                FILTER (WHERE i.series_id IS NOT NULL),
                            ARRAY[]::text[]
                        ) AS input_series_ids,
                        COALESCE(
                            array_agg(i.role ORDER BY i.role, i.series_id)
                                FILTER (WHERE i.series_id IS NOT NULL),
                            ARRAY[]::text[]
                        ) AS input_roles
                    FROM common_feature_catalog c
                    LEFT JOIN common_feature_catalog_input i
                        ON i.feature_code = c.feature_code
                    WHERE 1=1
                """
                params: list[object] = []
                if feature_codes:
                    sql += " AND c.feature_code = ANY(%s)"
                    params.append(feature_codes)
                if active_only:
                    sql += " AND c.active = TRUE"
                sql += " GROUP BY c.feature_code ORDER BY c.feature_code"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        CommonFeatureCatalogEntry(
                            feature_code=row["feature_code"],
                            feature_name_kr=row["feature_name_kr"],
                            category=row["category"],
                            frequency=row["frequency"],
                            unit=row["unit"],
                            transform_code=row["transform_code"],
                            description=row["description"],
                            input_series_ids=tuple(row["input_series_ids"]),
                            input_roles=tuple(row["input_roles"]),
                            active=row["active"],
                        )
                    )
        return records

    def upsert_common_feature_daily_facts(
        self,
        records: list[CommonFeatureDailyFact],
    ) -> UpsertResult:
        """Upsert KRX-date-aligned common feature facts."""
        if not records:
            return UpsertResult()

        result = UpsertResult()
        deduped_records = {(record.feature_date, record.feature_code): record for record in records}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        record.feature_date,
                        record.feature_code,
                        record.value_numeric,
                        record.value_text,
                        record.unit,
                        psycopg2.extras.Json(record.source_series_ids),
                        psycopg2.extras.Json(record.source_observation_ids),
                        record.asof_available_date,
                        record.selected_vintage,
                        record.generated_at,
                        record.generation_run_id,
                    )
                    for record in deduped_records.values()
                ]
                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO common_feature_daily_fact (
                        feature_date,
                        feature_code,
                        value_numeric,
                        value_text,
                        unit,
                        source_series_ids,
                        source_observation_ids,
                        asof_available_date,
                        selected_vintage,
                        generated_at,
                        generation_run_id
                    )
                    VALUES %s
                    ON CONFLICT (feature_date, feature_code) DO UPDATE SET
                        value_numeric = EXCLUDED.value_numeric,
                        value_text = EXCLUDED.value_text,
                        unit = EXCLUDED.unit,
                        source_series_ids = EXCLUDED.source_series_ids,
                        source_observation_ids = EXCLUDED.source_observation_ids,
                        asof_available_date = EXCLUDED.asof_available_date,
                        selected_vintage = EXCLUDED.selected_vintage,
                        generated_at = EXCLUDED.generated_at,
                        generation_run_id = EXCLUDED.generation_run_id
                    """,
                    args,
                    page_size=1000,
                )
        return result

    def get_common_feature_daily_facts(
        self,
        start: date,
        end: date,
        feature_codes: list[str] | None = None,
    ) -> list[CommonFeatureDailyFact]:
        """Return KRX-date-aligned common feature facts."""
        records: list[CommonFeatureDailyFact] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                sql = """
                    SELECT *
                    FROM common_feature_daily_fact
                    WHERE feature_date BETWEEN %s AND %s
                """
                params: list[object] = [start, end]
                if feature_codes:
                    sql += " AND feature_code = ANY(%s)"
                    params.append(feature_codes)
                sql += " ORDER BY feature_date, feature_code"
                cur.execute(sql, params)
                for row in cur.fetchall():
                    records.append(
                        CommonFeatureDailyFact(
                            feature_date=row["feature_date"],
                            feature_code=row["feature_code"],
                            value_numeric=row["value_numeric"],
                            value_text=row["value_text"],
                            unit=row["unit"],
                            source_series_ids=list(row["source_series_ids"] or []),
                            source_observation_ids=list(row["source_observation_ids"] or []),
                            asof_available_date=row["asof_available_date"],
                            selected_vintage=row["selected_vintage"],
                            generated_at=row["generated_at"],
                            generation_run_id=(
                                str(row["generation_run_id"])
                                if row["generation_run_id"] is not None
                                else None
                            ),
                        )
                    )
        return records

    def count_common_feature_daily_facts(
        self,
        start: date,
        end: date,
        feature_codes: list[str] | None = None,
    ) -> dict[str, int]:
        """Return daily fact counts grouped by feature_code."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT feature_code, COUNT(*)::int
                    FROM common_feature_daily_fact
                    WHERE feature_date BETWEEN %s AND %s
                """
                params: list[object] = [start, end]
                if feature_codes:
                    sql += " AND feature_code = ANY(%s)"
                    params.append(feature_codes)
                sql += " GROUP BY feature_code"
                cur.execute(sql, params)
                return {row[0]: row[1] for row in cur.fetchall()}

    def get_common_feature_daily_fact_max_dates(
        self,
        feature_codes: list[str] | None = None,
    ) -> dict[str, date]:
        """Return latest daily fact date grouped by feature_code."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT feature_code, MAX(feature_date)
                    FROM common_feature_daily_fact
                    WHERE 1=1
                """
                params: list[object] = []
                if feature_codes:
                    sql += " AND feature_code = ANY(%s)"
                    params.append(feature_codes)
                sql += " GROUP BY feature_code"
                cur.execute(sql, params)
                return {row[0]: row[1] for row in cur.fetchall() if row[1] is not None}

    # -- Daily OHLCV ----------------------------------------------------------

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        """Upsert daily OHLCV bars."""
        if not bars:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        b.trade_date,
                        b.ticker,
                        b.market.value,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume,
                        b.source.value,
                        b.fetched_at,
                    )
                    for b in bars
                ]

                result.updated = _execute_values_counted(
                    cur,
                    """
                    INSERT INTO daily_ohlcv (
                        trade_date,
                        ticker,
                        market,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        source,
                        fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at
                    """,
                    args,
                    page_size=1000,
                )

        return result

    # -- Daily market cap -----------------------------------------------------

    def upsert_daily_market_cap(self, rows: list[DailyMarketCapRow]) -> UpsertResult:
        """Upsert one ``(trade_date, market)`` slice of market-cap rows.

        The whole slice goes in one ``execute_values`` inside one connection
        context, so it commits or it does not.  A slice split across
        transactions could be left permanently half-written — see the port
        docstring.
        """
        if not rows:
            return UpsertResult()

        result = UpsertResult()
        page_size = 1000

        statement = """
            INSERT INTO daily_market_cap (
                trade_date,
                ticker,
                market,
                source_close,
                market_cap,
                trading_value,
                listed_shares,
                volume,
                source,
                fetched_at,
                source_open,
                source_high,
                source_low
            )
            VALUES %s
            ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
                source_close = EXCLUDED.source_close,
                market_cap = EXCLUDED.market_cap,
                trading_value = EXCLUDED.trading_value,
                listed_shares = EXCLUDED.listed_shares,
                volume = EXCLUDED.volume,
                source = EXCLUDED.source,
                fetched_at = EXCLUDED.fetched_at,
                source_open = EXCLUDED.source_open,
                source_high = EXCLUDED.source_high,
                source_low = EXCLUDED.source_low
        """

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        r.trade_date,
                        r.ticker,
                        r.market.value,
                        r.source_close,
                        r.market_cap,
                        r.trading_value,
                        r.listed_shares,
                        r.volume,
                        r.source.value,
                        r.fetched_at,
                        r.source_open,
                        r.source_high,
                        r.source_low,
                    )
                    for r in rows
                ]

                # Paged explicitly so the counts add up.  ``execute_values``
                # with ``page_size`` issues one statement per page, and
                # ``cur.rowcount`` then reports only the LAST page — a 1,704-row
                # slice comes back as 704.  The caller reconciles this count
                # against the response to decide whether the slice is complete,
                # so an under-count is not cosmetic here: it would fail every
                # slice over 1,000 rows.  (``upsert_daily_bars`` has the same
                # pattern; there the count is only reported, not acted on.)
                result.updated = _execute_values_counted(cur, statement, args, page_size=page_size)

        return result

    def get_market_cap_slice_row_counts(
        self,
        start: date,
        end: date,
        market: Market | None = None,
    ) -> dict[tuple[date, Market], int]:
        """Return stored row counts per ``(trade_date, market)`` slice."""
        query = """
            SELECT trade_date, market, COUNT(*)
            FROM daily_market_cap
            WHERE trade_date BETWEEN %s AND %s
        """
        params: list[object] = [start, end]
        if market is not None:
            query += " AND market = %s"
            params.append(market.value)
        query += " GROUP BY trade_date, market"

        counts: dict[tuple[date, Market], int] = {}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                for trade_date, market_value, row_count in cur.fetchall():
                    counts[(trade_date, Market(market_value))] = row_count

        return counts

    # -- Ingestion runs -------------------------------------------------------

    def record_run(self, run: IngestionRun) -> None:
        """Insert or update an ingestion-run audit record."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_runs (
                        run_id,
                        run_type,
                        started_at,
                        ended_at,
                        status,
                        params,
                        counts,
                        error_summary
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        ended_at = EXCLUDED.ended_at,
                        status = EXCLUDED.status,
                        params = EXCLUDED.params,
                        counts = EXCLUDED.counts,
                        error_summary = EXCLUDED.error_summary
                    """,
                    (
                        run.run_id,
                        run.run_type.value,
                        run.started_at,
                        run.ended_at,
                        run.status.value,
                        json.dumps(run.params) if run.params else None,
                        json.dumps(run.counts) if run.counts else None,
                        run.error_summary,
                    ),
                )

    def get_table_bsns_year_range(self, table_name: str) -> tuple[int, int, int] | None:
        """Return min/max business-year coverage and row count for a known table."""
        allowed_tables = {
            "dart_financial_statement_raw",
            "dart_share_count_raw",
            "dart_shareholder_return_raw",
            "dart_xbrl_document",
            "dart_xbrl_fact_raw",
            "stock_metric_fact",
        }
        if table_name not in allowed_tables:
            raise ValueError(f"Unsupported business-year table: {table_name}")

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MIN(bsns_year), MAX(bsns_year), COUNT(*)::int FROM {table_name}"
                )
                row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            return None
        return int(row[0]), int(row[1]), int(row[2])

    def get_running_ingestion_runs(self, limit: int = 20) -> list[IngestionRun]:
        """Return currently running ingestion runs ordered by start time."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        run_type,
                        started_at,
                        ended_at,
                        status,
                        params,
                        counts,
                        error_summary
                    FROM ingestion_runs
                    WHERE status = %s
                    ORDER BY started_at
                    LIMIT %s
                    """,
                    (RunStatus.RUNNING.value, max(1, limit)),
                )
                rows = cur.fetchall()

        runs: list[IngestionRun] = []
        for row in rows:
            params = row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {})
            counts = row[6] if isinstance(row[6], dict) else (json.loads(row[6]) if row[6] else {})
            runs.append(
                IngestionRun(
                    run_id=str(row[0]),
                    run_type=RunType(row[1]),
                    started_at=row[2],
                    ended_at=row[3],
                    status=RunStatus(row[4]),
                    params=params,
                    counts=counts,
                    error_summary=row[7],
                )
            )
        return runs

    def get_recent_ingestion_runs(self, run_type: RunType, limit: int = 20) -> list[IngestionRun]:
        """Return recent ingestion runs for one run type, newest first."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        run_type,
                        started_at,
                        ended_at,
                        status,
                        params,
                        counts,
                        error_summary
                    FROM ingestion_runs
                    WHERE run_type = %s
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (run_type.value, max(1, limit)),
                )
                rows = cur.fetchall()

        runs: list[IngestionRun] = []
        for row in rows:
            params = row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {})
            counts = row[6] if isinstance(row[6], dict) else (json.loads(row[6]) if row[6] else {})
            runs.append(
                IngestionRun(
                    run_id=str(row[0]),
                    run_type=RunType(row[1]),
                    started_at=row[2],
                    ended_at=row[3],
                    status=RunStatus(row[4]),
                    params=params,
                    counts=counts,
                    error_summary=row[7],
                )
            )
        return runs

    # -- Collection slice ledger (L-1) ----------------------------------------

    def get_collection_slice_states(
        self,
        source: Source,
        endpoint: str,
        slice_keys: list[str] | None = None,
    ) -> dict[str, CollectionSliceState]:
        """Return the per-slice completion ledger for one ``(source, endpoint)``."""
        sql = """
            SELECT slice_key, status, expected_rows, actual_rows,
                   attempt_count, last_error, updated_at
            FROM collection_slice_state
            WHERE source = %s AND endpoint = %s
        """
        params: list[object] = [source.value, endpoint]
        if slice_keys is not None:
            if not slice_keys:
                return {}
            sql += " AND slice_key = ANY(%s)"
            params.append(list(slice_keys))

        states: dict[str, CollectionSliceState] = {}
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    states[row[0]] = CollectionSliceState(
                        source=source,
                        endpoint=endpoint,
                        slice_key=row[0],
                        status=SliceStatus(row[1]),
                        expected_rows=row[2],
                        actual_rows=row[3],
                        attempt_count=row[4],
                        last_error=row[5],
                        updated_at=row[6],
                    )
        return states

    def upsert_collection_slice_states(self, states: list[CollectionSliceState]) -> UpsertResult:
        """Record slice outcomes, incrementing ``attempt_count`` in SQL."""
        if not states:
            return UpsertResult()

        statement = """
            INSERT INTO collection_slice_state (
                source, endpoint, slice_key, status,
                expected_rows, actual_rows, attempt_count, last_error, updated_at
            )
            VALUES %s
            ON CONFLICT (source, endpoint, slice_key) DO UPDATE SET
                status = EXCLUDED.status,
                expected_rows = EXCLUDED.expected_rows,
                actual_rows = EXCLUDED.actual_rows,
                -- Incremented here rather than read-modify-written by the
                -- caller: two runs working the same endpoint would otherwise
                -- overwrite each other's attempt counts, and the count exists
                -- precisely to show which slices keep failing.
                attempt_count = collection_slice_state.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
        """

        now = now_kst()
        args = [
            (
                state.source.value,
                state.endpoint,
                state.slice_key,
                state.status.value,
                state.expected_rows,
                state.actual_rows,
                1,
                (state.last_error or None) and state.last_error[:2000],
                state.updated_at or now,
            )
            for state in states
        ]

        result = UpsertResult()
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                result.updated = _execute_values_counted(cur, statement, args, page_size=1000)
        return result

    # -- Query helpers --------------------------------------------------------

    def get_daily_bars(self, target_date: date, market: Market | None = None) -> list[DailyBar]:
        """Return all daily bars for a given date."""
        bars = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if market:
                    cur.execute(
                        "SELECT * FROM daily_ohlcv WHERE trade_date = %s AND market = %s",
                        (target_date, market.value),
                    )
                else:
                    cur.execute("SELECT * FROM daily_ohlcv WHERE trade_date = %s", (target_date,))

                for row in cur.fetchall():
                    bars.append(
                        DailyBar(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            trade_date=row["trade_date"],
                            open=row["open"],
                            high=row["high"],
                            low=row["low"],
                            close=row["close"],
                            volume=row["volume"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                        )
                    )
        return bars

    def query_missing_days(
        self,
        ticker: str,
        start: date,
        end: date,
    ) -> list[date]:
        """Return KRX trading days in [start, end] without stored bars.

        Uses the trading-day calendar to enumerate expected sessions
        (excluding weekends and known holidays), then subtracts the
        ``trade_date`` values already present in ``daily_ohlcv`` for the
        given ticker. This avoids ever flagging weekends/holidays as
        "missing", which would otherwise trigger pointless re-fetches on
        every backfill run.
        """
        if start > end:
            return []

        expected = get_trading_days(start, end)
        if not expected:
            return []

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date
                    FROM daily_ohlcv
                    WHERE ticker = %s
                      AND trade_date BETWEEN %s AND %s
                    """,
                    (ticker, start, end),
                )
                stored = {row[0] for row in cur.fetchall()}

        return [d for d in expected if d not in stored]

    def get_min_trade_date(self, ticker: str) -> date | None:
        """Return the earliest stored ``trade_date`` for *ticker*, if any.

        Used by the backfill service as a lower-bound clamp so that
        date ranges before the ticker's known data start are not
        re-requested on every run.
        """
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MIN(trade_date) FROM daily_ohlcv WHERE ticker = %s",
                    (ticker,),
                )
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]

    def get_max_trade_date(self, ticker: str) -> date | None:
        """Return the latest stored ``trade_date`` for *ticker*, if any.

        Used by the backfill service in incremental mode to fetch only
        days strictly after this date.
        """
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(trade_date) FROM daily_ohlcv WHERE ticker = %s",
                    (ticker,),
                )
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]

    def get_daily_price_date_range(
        self,
        tickers: list[str] | None = None,
    ) -> tuple[date, date] | None:
        """Return the min/max stored daily OHLCV trade dates for selected tickers."""
        params: list[object] = []
        where_clause = ""
        if tickers:
            where_clause = "WHERE ticker = ANY(%s)"
            params.append(tickers)

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MIN(trade_date), MAX(trade_date) FROM daily_ohlcv {where_clause}",
                    params,
                )
                row = cur.fetchone()

        if not row or row[0] is None or row[1] is None:
            return None
        return row[0], row[1]

    def get_latest_daily_price_date(
        self,
        tickers: list[str] | None = None,
    ) -> date | None:
        """Return the latest stored daily OHLCV trade date for selected tickers."""
        params: list[object] = []
        where_clause = ""
        if tickers:
            where_clause = "WHERE ticker = ANY(%s)"
            params.append(tickers)

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(trade_date) FROM daily_ohlcv {where_clause}", params)
                row = cur.fetchone()

        if not row or row[0] is None:
            return None
        return row[0]

    def get_latest_market_cap_date(self) -> date | None:
        """Return the latest stored ``daily_market_cap`` trade_date."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(trade_date) FROM daily_market_cap")
                row = cur.fetchone()

        if not row or row[0] is None:
            return None
        return row[0]

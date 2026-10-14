"""PostgreSQL store for 거래소공시 rows and their request-window ledger."""

from __future__ import annotations

from datetime import date

import psycopg2.extras

from collector.kr.domain.exchange_notice import (
    PBLNTF_TY_EXCHANGE,
    ExchangeNoticeRecord,
    ExchangeNoticeWindow,
)
from collector.kr.infra.db_postgres.connection import get_connection

_WINDOW_COLUMNS = (
    "bgn_de, end_de, pblntf_ty, status, total_count, total_page, pages_done, "
    "rows_fetched, requests, last_error, started_at, updated_at, completed_at"
)


def _row_to_window(row: tuple) -> ExchangeNoticeWindow:
    return ExchangeNoticeWindow(
        bgn_de=row[0],
        end_de=row[1],
        pblntf_ty=row[2],
        status=row[3],
        total_count=row[4],
        total_page=row[5],
        pages_done=row[6],
        rows_fetched=row[7],
        requests=row[8],
        last_error=row[9],
        started_at=row[10],
        updated_at=row[11],
        completed_at=row[12],
    )


class PostgresExchangeNoticeStore:
    """``dart_exchange_notice_raw`` + ``dart_exchange_notice_window``."""

    def __init__(self, dsn: str, pblntf_ty: str = PBLNTF_TY_EXCHANGE) -> None:
        self._dsn = dsn
        self._pblntf_ty = pblntf_ty

    def get_window(self, bgn_de: date, end_de: date) -> ExchangeNoticeWindow | None:
        with get_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {_WINDOW_COLUMNS} FROM dart_exchange_notice_window "
                "WHERE pblntf_ty = %s AND bgn_de = %s AND end_de = %s",
                (self._pblntf_ty, bgn_de, end_de),
            )
            row = cur.fetchone()
        return _row_to_window(row) if row else None

    def save_window(self, window: ExchangeNoticeWindow) -> None:
        with get_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dart_exchange_notice_window (
                    pblntf_ty, bgn_de, end_de, status, total_count, total_page,
                    pages_done, rows_fetched, requests, last_error,
                    started_at, updated_at, completed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pblntf_ty, bgn_de, end_de)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    total_count = EXCLUDED.total_count,
                    total_page = EXCLUDED.total_page,
                    pages_done = EXCLUDED.pages_done,
                    rows_fetched = EXCLUDED.rows_fetched,
                    requests = EXCLUDED.requests,
                    last_error = EXCLUDED.last_error,
                    started_at = EXCLUDED.started_at,
                    updated_at = EXCLUDED.updated_at,
                    completed_at = EXCLUDED.completed_at
                """,
                (
                    self._pblntf_ty,
                    window.bgn_de,
                    window.end_de,
                    window.status,
                    window.total_count,
                    window.total_page,
                    window.pages_done,
                    window.rows_fetched,
                    window.requests,
                    window.last_error,
                    window.started_at,
                    window.updated_at,
                    window.completed_at,
                ),
            )

    def list_windows(self, start: date, end: date) -> list[ExchangeNoticeWindow]:
        with get_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {_WINDOW_COLUMNS} FROM dart_exchange_notice_window "
                "WHERE pblntf_ty = %s AND end_de >= %s AND bgn_de <= %s "
                "ORDER BY bgn_de, end_de",
                (self._pblntf_ty, start, end),
            )
            rows = cur.fetchall()
        return [_row_to_window(row) for row in rows]

    def upsert_notices(self, records: list[ExchangeNoticeRecord]) -> int:
        if not records:
            return 0
        deduped = {record.rcept_no: record for record in records}
        args = [
            (
                r.rcept_no,
                r.pblntf_ty,
                r.corp_code,
                r.corp_name,
                r.stock_code,
                r.corp_cls,
                r.report_nm,
                r.flr_nm,
                r.rcept_dt,
                r.rm,
                r.source.value,
                r.fetched_at,
                r.fetched_at,
                psycopg2.extras.Json(r.raw_payload),
            )
            for r in deduped.values()
        ]
        with get_connection(self._dsn) as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO dart_exchange_notice_raw (
                    rcept_no, pblntf_ty, corp_code, corp_name, stock_code, corp_cls,
                    report_nm, flr_nm, rcept_dt, rm, source,
                    first_fetched_at, fetched_at, raw_payload
                )
                VALUES %s
                ON CONFLICT (rcept_no)
                DO UPDATE SET
                    pblntf_ty = EXCLUDED.pblntf_ty,
                    corp_code = EXCLUDED.corp_code,
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
                page_size=len(args),
            )
            return len(args)

    def count_notices(self, bgn_de: date, end_de: date) -> int:
        with get_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM dart_exchange_notice_raw "
                "WHERE pblntf_ty = %s AND rcept_dt BETWEEN %s AND %s",
                (self._pblntf_ty, bgn_de, end_de),
            )
            return int(cur.fetchone()[0])

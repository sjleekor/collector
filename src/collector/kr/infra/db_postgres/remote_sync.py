"""Remote-to-local PostgreSQL sync helpers.

This module copies the pipeline tables from a remote PostgreSQL instance
into the local PostgreSQL database in batches. Incremental sync uses a
stable composite cursor of ``(watermark_timestamp, primary_key...)`` so
that rows sharing the same timestamp are not skipped across batch
boundaries.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import psycopg2
import psycopg2.extras
from psycopg2 import sql

logger = logging.getLogger(__name__)

DAILY_OHLCV_SYNC_NAME = "remote_db_sync.daily_ohlcv"
PUBLIC_SCHEMA = "public"


@dataclass(frozen=True, slots=True)
class RemoteDbInfo:
    """Connection details for the remote PostgreSQL instance."""

    host: str
    port: int
    db_name: str
    user: str
    password: str
    container: str | None = None

    def to_dsn(self, host_override: str | None = None, port_override: int | None = None) -> str:
        """Build a PostgreSQL DSN string."""
        host = host_override or self.host
        port = port_override or self.port
        return (
            f"postgresql://{quote(self.user, safe='')}:{quote(self.password, safe='')}"
            f"@{host}:{port}/{quote(self.db_name, safe='')}"
        )


@dataclass(frozen=True, slots=True)
class TableSyncSpec:
    """Metadata describing how to copy a single table."""

    name: str
    select_list: str
    from_clause: str
    order_columns: tuple[str, ...]
    insert_columns: tuple[str, ...]
    conflict_columns: tuple[str, ...]
    update_columns: tuple[str, ...]
    local_cursor_sql: str
    cursor_indexes: tuple[int, ...]
    json_columns: tuple[str, ...] = ()
    conflict_constraint: str | None = None
    do_nothing_when_no_update_columns: bool = False
    always_full_scan: bool = False
    prune_missing_after_full_scan: bool = False
    preserve_remote_surrogate_columns: tuple[str, ...] = ()
    copy_merge_enabled: bool = False
    conflict_update_where_sql: str | None = None


@dataclass(frozen=True, slots=True)
class DatabaseTable:
    """A physical database table to copy during full-refresh sync."""

    schema: str
    name: str

    @property
    def display_name(self) -> str:
        """Return a compact table name for sync result output."""
        if self.schema == PUBLIC_SCHEMA:
            return self.name
        return f"{self.schema}.{self.name}"


PIPELINE_FULL_REFRESH_TABLE_NAMES: tuple[str, ...] = (
    # universe sync
    "stock_master",
    "stock_master_snapshot",
    "stock_master_snapshot_items",
    # prices backfill
    "daily_ohlcv",
    "daily_market_cap",
    # KRX security-level flow metrics
    "krx_security_flow_raw",
    # account / financial / XBRL pipeline
    "dart_corp_master",
    "dart_corp_profile_history",
    "dart_financial_statement_raw",
    "dart_share_count_raw",
    "dart_shareholder_return_raw",
    "dart_capital_change_raw",
    "dart_filing_receipt_raw",
    "dart_employee_raw",
    "dart_governance_raw",
    "dart_xbrl_document",
    "dart_xbrl_fact_raw",
    # model-facing common feature layer. Only the raw observations + the shared
    # series config are mirrored; the derived facts (stock_metric_fact,
    # common_feature_daily_fact) and the compute-only catalog/rule tables are no
    # longer mirrored — the DuckDB marts recompute them from raw (refactor §5.2,
    # decision 7). common_feature_series is kept (collector + compute share it).
    "common_feature_series",
    "common_feature_observation_raw",
)
PIPELINE_FULL_REFRESH_TABLES: tuple[DatabaseTable, ...] = tuple(
    DatabaseTable(schema=PUBLIC_SCHEMA, name=name) for name in PIPELINE_FULL_REFRESH_TABLE_NAMES
)

SYNC_TABLE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "stock_master_snapshot_items": ("stock_master_snapshot",),
    "common_feature_observation_raw": ("common_feature_series",),
}


SYNC_TABLE_SPECS: tuple[TableSyncSpec, ...] = (
    TableSyncSpec(
        name="stock_master",
        # listing_date/first_seen_date appended after updated_at so insert_columns
        # stays a positional prefix of select_list and the updated_at cursor at
        # index 6 is unchanged.
        select_list="ticker, market, name, status, last_seen_date, source, updated_at, "
        "listing_date, first_seen_date",
        from_clause="stock_master",
        order_columns=("updated_at", "ticker", "market"),
        insert_columns=(
            "ticker",
            "market",
            "name",
            "status",
            "last_seen_date",
            "source",
            "updated_at",
            "listing_date",
            "first_seen_date",
        ),
        conflict_columns=("ticker", "market"),
        update_columns=(
            "name",
            "status",
            "last_seen_date",
            "source",
            "updated_at",
            "listing_date",
            "first_seen_date",
        ),
        local_cursor_sql=(
            "SELECT updated_at, ticker, market "
            "FROM stock_master "
            "ORDER BY updated_at DESC, ticker DESC, market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(6, 0, 1),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="stock_master_snapshot",
        select_list="snapshot_id, as_of_date, source, fetched_at, record_count",
        from_clause="stock_master_snapshot",
        order_columns=("fetched_at", "snapshot_id"),
        insert_columns=("snapshot_id", "as_of_date", "source", "fetched_at", "record_count"),
        conflict_columns=("snapshot_id",),
        update_columns=("as_of_date", "source", "fetched_at", "record_count"),
        local_cursor_sql=(
            "SELECT fetched_at, snapshot_id "
            "FROM stock_master_snapshot "
            "ORDER BY fetched_at DESC, snapshot_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(3, 0),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="stock_master_snapshot_items",
        # s.fetched_at is the joined parent watermark and is NOT inserted, so
        # i.listing_date must precede it to keep insert_columns a positional
        # prefix of select_list; the fetched_at cursor moves 5 -> 6.
        select_list="i.snapshot_id, i.ticker, i.market, i.name, i.status, "
        "i.listing_date, s.fetched_at",
        from_clause=(
            "stock_master_snapshot_items i "
            "JOIN stock_master_snapshot s ON s.snapshot_id = i.snapshot_id"
        ),
        order_columns=("s.fetched_at", "i.snapshot_id", "i.ticker", "i.market"),
        insert_columns=("snapshot_id", "ticker", "market", "name", "status", "listing_date"),
        conflict_columns=("snapshot_id", "ticker", "market"),
        update_columns=("name", "status", "listing_date"),
        local_cursor_sql=(
            "SELECT s.fetched_at, i.snapshot_id, i.ticker, i.market "
            "FROM stock_master_snapshot_items i "
            "JOIN stock_master_snapshot s ON s.snapshot_id = i.snapshot_id "
            "ORDER BY s.fetched_at DESC, i.snapshot_id DESC, i.ticker DESC, i.market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(6, 0, 1, 2),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="daily_ohlcv",
        select_list=(
            "trade_date, ticker, market, open, high, low, close, volume, source, fetched_at"
        ),
        from_clause="daily_ohlcv",
        order_columns=("fetched_at", "trade_date", "ticker", "market"),
        insert_columns=(
            "trade_date",
            "ticker",
            "market",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
            "fetched_at",
        ),
        conflict_columns=("trade_date", "ticker", "market"),
        update_columns=("open", "high", "low", "close", "volume", "source", "fetched_at"),
        local_cursor_sql=(
            "SELECT fetched_at, trade_date, ticker, market "
            "FROM daily_ohlcv "
            "ORDER BY fetched_at DESC, trade_date DESC, ticker DESC, market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0, 1, 2),
        copy_merge_enabled=True,
        conflict_update_where_sql="daily_ohlcv.fetched_at <= EXCLUDED.fetched_at",
    ),
    TableSyncSpec(
        name="daily_market_cap",
        # The unadjusted OHLC trio is appended AFTER fetched_at so that
        # cursor_indexes below keeps pointing at the same columns.  Inserting
        # them mid-list would silently repoint the resume cursor.
        select_list=(
            "trade_date, ticker, market, source_close, market_cap, trading_value, "
            "listed_shares, volume, source, fetched_at, "
            "source_open, source_high, source_low"
        ),
        from_clause="daily_market_cap",
        order_columns=("fetched_at", "trade_date", "ticker", "market"),
        insert_columns=(
            "trade_date",
            "ticker",
            "market",
            "source_close",
            "market_cap",
            "trading_value",
            "listed_shares",
            "volume",
            "source",
            "fetched_at",
            "source_open",
            "source_high",
            "source_low",
        ),
        conflict_columns=("trade_date", "ticker", "market"),
        update_columns=(
            "source_close",
            "market_cap",
            "trading_value",
            "listed_shares",
            "volume",
            "source",
            "fetched_at",
            "source_open",
            "source_high",
            "source_low",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, trade_date, ticker, market "
            "FROM daily_market_cap "
            "ORDER BY fetched_at DESC, trade_date DESC, ticker DESC, market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0, 1, 2),
        copy_merge_enabled=True,
        conflict_update_where_sql="daily_market_cap.fetched_at <= EXCLUDED.fetched_at",
    ),
    TableSyncSpec(
        name="krx_security_flow_raw",
        select_list=(
            "raw_id, trade_date, ticker, market, metric_code, metric_name, value, unit, "
            "source, fetched_at, raw_payload"
        ),
        from_clause="krx_security_flow_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "trade_date",
            "ticker",
            "market",
            "metric_code",
            "metric_name",
            "value",
            "unit",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=("trade_date", "ticker", "market", "metric_code", "source"),
        update_columns=("metric_name", "value", "unit", "fetched_at", "raw_payload"),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM krx_security_flow_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_corp_master",
        # N2 profile columns are appended AFTER updated_at so the cursor stays
        # at index 9.  They must be listed here explicitly: this select/insert
        # list is not derived from the DDL, so a column missing here is dropped
        # from the mirror silently — and unlike the profile catalog, no test
        # catches it for free.
        select_list=(
            "corp_code, ticker, corp_name, market, stock_name, modify_date, is_active, "
            "source, fetched_at, updated_at, "
            "induty_code, corp_cls, est_dt, acc_mt, profile_raw, profile_fetched_at"
        ),
        from_clause="dart_corp_master",
        order_columns=("updated_at", "corp_code"),
        insert_columns=(
            "corp_code",
            "ticker",
            "corp_name",
            "market",
            "stock_name",
            "modify_date",
            "is_active",
            "source",
            "fetched_at",
            "updated_at",
            "induty_code",
            "corp_cls",
            "est_dt",
            "acc_mt",
            "profile_raw",
            "profile_fetched_at",
        ),
        conflict_columns=("corp_code",),
        update_columns=(
            "ticker",
            "corp_name",
            "market",
            "stock_name",
            "modify_date",
            "is_active",
            "source",
            "fetched_at",
            "updated_at",
            "induty_code",
            "corp_cls",
            "est_dt",
            "acc_mt",
            "profile_raw",
            "profile_fetched_at",
        ),
        local_cursor_sql=(
            "SELECT updated_at, corp_code "
            "FROM dart_corp_master "
            "ORDER BY updated_at DESC, corp_code DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0),
    ),
    TableSyncSpec(
        # F-1. Append-only, so the mirror never has to update a row: the
        # conflict target is the natural key and update_columns is empty except
        # for the audit fields, which a re-seed can legitimately move.
        name="dart_corp_profile_history",
        select_list=(
            "corp_code, observed_month, observed_at, ticker, corp_cls, induty_code, "
            "est_dt, acc_mt, corp_name, stock_name, is_seed, profile_raw, run_id, "
            "source, fetched_at"
        ),
        from_clause="dart_corp_profile_history",
        order_columns=("fetched_at", "corp_code", "observed_month"),
        insert_columns=(
            "corp_code",
            "observed_month",
            "observed_at",
            "ticker",
            "corp_cls",
            "induty_code",
            "est_dt",
            "acc_mt",
            "corp_name",
            "stock_name",
            "is_seed",
            "profile_raw",
            "run_id",
            "source",
            "fetched_at",
        ),
        conflict_columns=("corp_code", "observed_month"),
        update_columns=(
            "observed_at",
            "ticker",
            "corp_cls",
            "induty_code",
            "est_dt",
            "acc_mt",
            "corp_name",
            "stock_name",
            "is_seed",
            "profile_raw",
            "run_id",
            "source",
            "fetched_at",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, corp_code, observed_month "
            "FROM dart_corp_profile_history "
            "ORDER BY fetched_at DESC, corp_code DESC, observed_month DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(14, 0, 1),
        json_columns=("profile_raw",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_financial_statement_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, sj_nm, "
            "account_id, account_nm, account_detail, thstrm_nm, thstrm_add_amount, "
            "frmtrm_nm, frmtrm_q_nm, frmtrm_q_amount, frmtrm_add_amount, "
            "bfefrmtrm_nm, ord, thstrm_amount, frmtrm_amount, bfefrmtrm_amount, "
            "currency, rcept_no, source, fetched_at, raw_payload"
        ),
        from_clause="dart_financial_statement_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "fs_div",
            "sj_div",
            "sj_nm",
            "account_id",
            "account_nm",
            "account_detail",
            "thstrm_nm",
            "thstrm_add_amount",
            "frmtrm_nm",
            "frmtrm_q_nm",
            "frmtrm_q_amount",
            "frmtrm_add_amount",
            "bfefrmtrm_nm",
            "ord",
            "thstrm_amount",
            "frmtrm_amount",
            "bfefrmtrm_amount",
            "currency",
            "rcept_no",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "corp_code",
            "bsns_year",
            "reprt_code",
            "fs_div",
            "sj_div",
            "account_id",
            "ord",
            "rcept_no",
        ),
        update_columns=(
            "ticker",
            "sj_nm",
            "account_nm",
            "account_detail",
            "thstrm_nm",
            "thstrm_add_amount",
            "frmtrm_nm",
            "frmtrm_q_nm",
            "frmtrm_q_amount",
            "frmtrm_add_amount",
            "bfefrmtrm_nm",
            "thstrm_amount",
            "frmtrm_amount",
            "bfefrmtrm_amount",
            "currency",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_financial_statement_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(25, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_share_count_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, rcept_no, corp_cls, se, "
            "isu_stock_totqy, now_to_isu_stock_totqy, now_to_dcrs_stock_totqy, redc, "
            "profit_incnr, rdmstk_repy, etc, istc_totqy, tesstk_co, distb_stock_co, "
            "stlm_dt, source, fetched_at, raw_payload"
        ),
        from_clause="dart_share_count_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "corp_cls",
            "se",
            "isu_stock_totqy",
            "now_to_isu_stock_totqy",
            "now_to_dcrs_stock_totqy",
            "redc",
            "profit_incnr",
            "rdmstk_repy",
            "etc",
            "istc_totqy",
            "tesstk_co",
            "distb_stock_co",
            "stlm_dt",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=("corp_code", "bsns_year", "reprt_code", "se", "rcept_no"),
        update_columns=(
            "ticker",
            "corp_cls",
            "isu_stock_totqy",
            "now_to_isu_stock_totqy",
            "now_to_dcrs_stock_totqy",
            "redc",
            "profit_incnr",
            "rdmstk_repy",
            "etc",
            "istc_totqy",
            "tesstk_co",
            "distb_stock_co",
            "stlm_dt",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_share_count_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(20, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_shareholder_return_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, statement_type, row_name, "
            "stock_knd, dim1, dim2, dim3, metric_code, metric_name, value_numeric, "
            "value_text, unit, rcept_no, stlm_dt, source, fetched_at, raw_payload"
        ),
        from_clause="dart_shareholder_return_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "statement_type",
            "row_name",
            "stock_knd",
            "dim1",
            "dim2",
            "dim3",
            "metric_code",
            "metric_name",
            "value_numeric",
            "value_text",
            "unit",
            "rcept_no",
            "stlm_dt",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "corp_code",
            "bsns_year",
            "reprt_code",
            "statement_type",
            "row_name",
            "stock_knd",
            "dim1",
            "dim2",
            "dim3",
            "metric_code",
            "rcept_no",
        ),
        update_columns=(
            "ticker",
            "metric_name",
            "value_numeric",
            "value_text",
            "unit",
            "stlm_dt",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_shareholder_return_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(19, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_capital_change_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, rcept_no, corp_cls, "
            "isu_dcrs_de, isu_dcrs_stle, isu_dcrs_stock_knd, isu_dcrs_qy, "
            "isu_dcrs_mstvdv_fval_amount, isu_dcrs_mstvdv_fval_amount2, stlm_dt, "
            "source, fetched_at, raw_payload"
        ),
        from_clause="dart_capital_change_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "corp_cls",
            "isu_dcrs_de",
            "isu_dcrs_stle",
            "isu_dcrs_stock_knd",
            "isu_dcrs_qy",
            "isu_dcrs_mstvdv_fval_amount",
            "isu_dcrs_mstvdv_fval_amount2",
            "stlm_dt",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "corp_code",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "isu_dcrs_de",
            "isu_dcrs_stle",
            "isu_dcrs_stock_knd",
        ),
        update_columns=(
            "ticker",
            "corp_cls",
            "isu_dcrs_qy",
            "isu_dcrs_mstvdv_fval_amount",
            "isu_dcrs_mstvdv_fval_amount2",
            "stlm_dt",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_capital_change_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(15, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_employee_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, rcept_no, "
            "statement_type, row_ordinal, source, fetched_at, raw_payload"
        ),
        from_clause="dart_employee_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "statement_type",
            "row_ordinal",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "corp_code",
            "bsns_year",
            "reprt_code",
            "statement_type",
            "rcept_no",
            "row_ordinal",
        ),
        update_columns=(
            "ticker",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_employee_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_governance_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, rcept_no, "
            "statement_type, row_ordinal, source, fetched_at, raw_payload"
        ),
        from_clause="dart_governance_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "statement_type",
            "row_ordinal",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "corp_code",
            "bsns_year",
            "reprt_code",
            "statement_type",
            "rcept_no",
            "row_ordinal",
        ),
        update_columns=(
            "ticker",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_governance_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_filing_receipt_raw",
        select_list=(
            "raw_id, corp_code, ticker, corp_name, stock_code, corp_cls, report_nm, "
            "rcept_no, flr_nm, rcept_dt, rm, source, fetched_at, raw_payload"
        ),
        from_clause="dart_filing_receipt_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "corp_name",
            "stock_code",
            "corp_cls",
            "report_nm",
            "rcept_no",
            "flr_nm",
            "rcept_dt",
            "rm",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=("corp_code", "rcept_no"),
        update_columns=(
            "ticker",
            "corp_name",
            "stock_code",
            "corp_cls",
            "report_nm",
            "flr_nm",
            "rcept_dt",
            "rm",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_filing_receipt_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(12, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_xbrl_document",
        select_list=(
            "document_id, corp_code, ticker, bsns_year, reprt_code, rcept_no, "
            "zip_entry_count, instance_document_name, label_ko_document_name, source, "
            "fetched_at, raw_payload"
        ),
        from_clause="dart_xbrl_document",
        order_columns=("fetched_at", "document_id"),
        insert_columns=(
            "document_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "zip_entry_count",
            "instance_document_name",
            "label_ko_document_name",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=("corp_code", "bsns_year", "reprt_code", "rcept_no"),
        update_columns=(
            "ticker",
            "zip_entry_count",
            "instance_document_name",
            "label_ko_document_name",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, document_id "
            "FROM dart_xbrl_document "
            "ORDER BY fetched_at DESC, document_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(10, 0),
        json_columns=("raw_payload",),
        preserve_remote_surrogate_columns=("document_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="dart_xbrl_fact_raw",
        select_list=(
            "raw_id, corp_code, ticker, bsns_year, reprt_code, rcept_no, concept_id, "
            "concept_name, namespace_uri, context_id, context_type, period_start, "
            "period_end, instant_date, dimensions, unit_id, unit_measure, decimals, "
            "value_numeric, value_text, is_nil, label_ko, source, fetched_at, raw_payload"
        ),
        from_clause="dart_xbrl_fact_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "corp_code",
            "ticker",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "concept_id",
            "concept_name",
            "namespace_uri",
            "context_id",
            "context_type",
            "period_start",
            "period_end",
            "instant_date",
            "dimensions",
            "unit_id",
            "unit_measure",
            "decimals",
            "value_numeric",
            "value_text",
            "is_nil",
            "label_ko",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "corp_code",
            "bsns_year",
            "reprt_code",
            "rcept_no",
            "context_id",
            "concept_id",
        ),
        update_columns=(
            "ticker",
            "concept_name",
            "namespace_uri",
            "context_type",
            "period_start",
            "period_end",
            "instant_date",
            "dimensions",
            "unit_id",
            "unit_measure",
            "decimals",
            "value_numeric",
            "value_text",
            "is_nil",
            "label_ko",
            "source",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM dart_xbrl_fact_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(23, 0),
        json_columns=("dimensions", "raw_payload"),
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="metric_catalog",
        select_list=(
            "metric_code, metric_name, category, unit, description, is_active, updated_at"
        ),
        from_clause="metric_catalog",
        order_columns=("updated_at", "metric_code"),
        insert_columns=(
            "metric_code",
            "metric_name",
            "category",
            "unit",
            "description",
            "is_active",
            "updated_at",
        ),
        conflict_columns=("metric_code",),
        update_columns=(
            "metric_name",
            "category",
            "unit",
            "description",
            "is_active",
            "updated_at",
        ),
        local_cursor_sql=(
            "SELECT updated_at, metric_code "
            "FROM metric_catalog "
            "ORDER BY updated_at DESC, metric_code DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(6, 0),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="metric_mapping_rule",
        select_list=(
            "rule_code, metric_code, source_table, value_selector, priority, "
            "statement_type, fs_div, sj_div, account_id, account_nm, row_name, "
            "stock_knd, dim1, dim2, dim3, metric_code_match, is_active, updated_at"
        ),
        from_clause="metric_mapping_rule",
        order_columns=("updated_at", "rule_code"),
        insert_columns=(
            "rule_code",
            "metric_code",
            "source_table",
            "value_selector",
            "priority",
            "statement_type",
            "fs_div",
            "sj_div",
            "account_id",
            "account_nm",
            "row_name",
            "stock_knd",
            "dim1",
            "dim2",
            "dim3",
            "metric_code_match",
            "is_active",
            "updated_at",
        ),
        conflict_columns=("rule_code",),
        update_columns=(
            "metric_code",
            "source_table",
            "value_selector",
            "priority",
            "statement_type",
            "fs_div",
            "sj_div",
            "account_id",
            "account_nm",
            "row_name",
            "stock_knd",
            "dim1",
            "dim2",
            "dim3",
            "metric_code_match",
            "is_active",
            "updated_at",
        ),
        local_cursor_sql=(
            "SELECT updated_at, rule_code "
            "FROM metric_mapping_rule "
            "ORDER BY updated_at DESC, rule_code DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(17, 0),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="stock_metric_fact",
        select_list=(
            "fact_id, ticker, market, corp_code, metric_code, period_type, period_end, "
            "bsns_year, reprt_code, fs_div, value_numeric, value_text, unit, "
            "source_table, source_key, mapping_rule_code, fetched_at, updated_at"
        ),
        from_clause="stock_metric_fact",
        order_columns=("updated_at", "fact_id"),
        insert_columns=(
            "fact_id",
            "ticker",
            "market",
            "corp_code",
            "metric_code",
            "period_type",
            "period_end",
            "bsns_year",
            "reprt_code",
            "fs_div",
            "value_numeric",
            "value_text",
            "unit",
            "source_table",
            "source_key",
            "mapping_rule_code",
            "fetched_at",
            "updated_at",
        ),
        conflict_columns=("ticker", "metric_code", "bsns_year", "reprt_code"),
        update_columns=(
            "market",
            "corp_code",
            "period_type",
            "period_end",
            "fs_div",
            "value_numeric",
            "value_text",
            "unit",
            "source_table",
            "source_key",
            "mapping_rule_code",
            "fetched_at",
            "updated_at",
        ),
        local_cursor_sql=(
            "SELECT updated_at, fact_id "
            "FROM stock_metric_fact "
            "ORDER BY updated_at DESC, fact_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(17, 0),
        preserve_remote_surrogate_columns=("fact_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="common_feature_series",
        select_list=(
            "series_id, source, source_series_key, category, frequency, name_kr, name_en, "
            "unit, country, market, endpoint_params, availability_policy, "
            "manual_lag_days, source_timezone, history_start_date, "
            "max_stale_business_days, default_transform, active, notes, updated_at"
        ),
        from_clause="common_feature_series",
        order_columns=("updated_at", "series_id"),
        insert_columns=(
            "series_id",
            "source",
            "source_series_key",
            "category",
            "frequency",
            "name_kr",
            "name_en",
            "unit",
            "country",
            "market",
            "endpoint_params",
            "availability_policy",
            "manual_lag_days",
            "source_timezone",
            "history_start_date",
            "max_stale_business_days",
            "default_transform",
            "active",
            "notes",
            "updated_at",
        ),
        conflict_columns=("series_id",),
        update_columns=(
            "source",
            "source_series_key",
            "category",
            "frequency",
            "name_kr",
            "name_en",
            "unit",
            "country",
            "market",
            "endpoint_params",
            "availability_policy",
            "manual_lag_days",
            "source_timezone",
            "history_start_date",
            "max_stale_business_days",
            "default_transform",
            "active",
            "notes",
            "updated_at",
        ),
        local_cursor_sql=(
            "SELECT updated_at, series_id "
            "FROM common_feature_series "
            "ORDER BY updated_at DESC, series_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(19, 0),
        json_columns=("endpoint_params",),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="common_feature_observation_raw",
        select_list=(
            "raw_id, source, series_id, observation_date, period_end_date, release_date, "
            "available_from_date, vintage, value_numeric, value_text, unit, frequency, "
            "source_updated_at, fetched_at, raw_payload"
        ),
        from_clause="common_feature_observation_raw",
        order_columns=("fetched_at", "raw_id"),
        insert_columns=(
            "raw_id",
            "source",
            "series_id",
            "observation_date",
            "period_end_date",
            "release_date",
            "available_from_date",
            "vintage",
            "value_numeric",
            "value_text",
            "unit",
            "frequency",
            "source_updated_at",
            "fetched_at",
            "raw_payload",
        ),
        conflict_columns=(
            "source",
            "series_id",
            "observation_date",
            "period_end_date",
            "release_date",
            "vintage",
        ),
        update_columns=(
            "available_from_date",
            "value_numeric",
            "value_text",
            "unit",
            "frequency",
            "source_updated_at",
            "fetched_at",
            "raw_payload",
        ),
        local_cursor_sql=(
            "SELECT fetched_at, raw_id "
            "FROM common_feature_observation_raw "
            "ORDER BY fetched_at DESC, raw_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(13, 0),
        json_columns=("raw_payload",),
        conflict_constraint="uq_common_feature_observation_raw",
        preserve_remote_surrogate_columns=("raw_id",),
        copy_merge_enabled=True,
    ),
    TableSyncSpec(
        name="common_feature_catalog",
        select_list=(
            "feature_code, feature_name_kr, category, frequency, unit, transform_code, "
            "description, active, updated_at"
        ),
        from_clause="common_feature_catalog",
        order_columns=("updated_at", "feature_code"),
        insert_columns=(
            "feature_code",
            "feature_name_kr",
            "category",
            "frequency",
            "unit",
            "transform_code",
            "description",
            "active",
            "updated_at",
        ),
        conflict_columns=("feature_code",),
        update_columns=(
            "feature_name_kr",
            "category",
            "frequency",
            "unit",
            "transform_code",
            "description",
            "active",
            "updated_at",
        ),
        local_cursor_sql=(
            "SELECT updated_at, feature_code "
            "FROM common_feature_catalog "
            "ORDER BY updated_at DESC, feature_code DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(8, 0),
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="common_feature_catalog_input",
        select_list="feature_code, series_id, role",
        from_clause="common_feature_catalog_input",
        order_columns=("feature_code", "series_id", "role"),
        insert_columns=("feature_code", "series_id", "role"),
        conflict_columns=("feature_code", "series_id", "role"),
        update_columns=(),
        local_cursor_sql=(
            "SELECT feature_code, series_id, role "
            "FROM common_feature_catalog_input "
            "ORDER BY feature_code DESC, series_id DESC, role DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(0, 1, 2),
        do_nothing_when_no_update_columns=True,
        always_full_scan=True,
        prune_missing_after_full_scan=True,
    ),
    TableSyncSpec(
        name="common_feature_daily_fact",
        select_list=(
            "feature_date, feature_code, value_numeric, value_text, unit, "
            "source_series_ids, source_observation_ids, asof_available_date, "
            "selected_vintage, generated_at, generation_run_id"
        ),
        from_clause="common_feature_daily_fact",
        order_columns=("generated_at", "feature_date", "feature_code"),
        insert_columns=(
            "feature_date",
            "feature_code",
            "value_numeric",
            "value_text",
            "unit",
            "source_series_ids",
            "source_observation_ids",
            "asof_available_date",
            "selected_vintage",
            "generated_at",
            "generation_run_id",
        ),
        conflict_columns=("feature_date", "feature_code"),
        update_columns=(
            "value_numeric",
            "value_text",
            "unit",
            "source_series_ids",
            "source_observation_ids",
            "asof_available_date",
            "selected_vintage",
            "generated_at",
            "generation_run_id",
        ),
        local_cursor_sql=(
            "SELECT generated_at, feature_date, feature_code "
            "FROM common_feature_daily_fact "
            "ORDER BY generated_at DESC, feature_date DESC, feature_code DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0, 1),
        json_columns=("source_series_ids", "source_observation_ids"),
        copy_merge_enabled=True,
    ),
)


def reset_local_public_tables(
    local_dsn: str,
    tables: tuple[DatabaseTable, ...] = PIPELINE_FULL_REFRESH_TABLES,
) -> int:
    """Drop selected local public-schema tables before schema reinitialization.

    Called before ``init_schema()`` during ``--full-refresh --all-tables`` so the
    rerun of ``sql/postgres_ddl.sql`` rebuilds pipeline tables that have drifted
    from the canonical schema (renamed/removed columns, changed constraints)
    without deleting unrelated local public tables.
    """
    target_tables = tuple(dict.fromkeys(tables))
    if not target_tables:
        return 0
    non_public_tables = [
        table.display_name for table in target_tables if table.schema != PUBLIC_SCHEMA
    ]
    if non_public_tables:
        raise ValueError(
            "Only public-schema tables can be reset; got: " + ", ".join(non_public_tables)
        )

    target_names = [table.name for table in target_tables]
    with contextlib.closing(psycopg2.connect(local_dsn)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s "
                "AND tablename = ANY(%s) ORDER BY tablename",
                (PUBLIC_SCHEMA, target_names),
            )
            existing_names = {row[0] for row in cur.fetchall()}

            tables_to_drop = tuple(table for table in target_tables if table.name in existing_names)
            if tables_to_drop:
                table_list = sql.SQL(", ").join(
                    _table_identifier(table) for table in tables_to_drop
                )
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_list))

    if tables_to_drop:
        logger.info(
            "Dropped %s local public-schema pipeline tables before schema reinit: %s",
            len(tables_to_drop),
            ", ".join(table.display_name for table in tables_to_drop),
        )
    return len(tables_to_drop)


def reset_local_public_schema(local_dsn: str) -> int:
    """Drop local pipeline sync tables before schema reinitialization.

    Kept as a compatibility wrapper for older callers; it no longer drops every
    public-schema table.
    """
    return reset_local_public_tables(local_dsn)


def load_remote_db_info(path: str | Path) -> RemoteDbInfo:
    """Parse the secret metadata file for the remote PostgreSQL instance."""
    info_path = Path(path)
    values: dict[str, str] = {}

    for raw_line in info_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip().lower()] = value.strip()

    missing = [
        key
        for key in (
            "server host",
            "host port",
            "postgres_user",
            "postgres_password",
            "postgres_db",
        )
        if key not in values
    ]
    if missing:
        missing_fields = ", ".join(missing)
        raise ValueError(f"Missing required remote DB fields in {info_path}: {missing_fields}")

    return RemoteDbInfo(
        host=values["server host"],
        port=int(values["host port"]),
        db_name=values["postgres_db"],
        user=values["postgres_user"],
        password=values["postgres_password"],
        container=values.get("container"),
    )


@contextlib.contextmanager
def resolve_remote_dsn(
    *,
    db_info_path: str | Path,
    host_override: str | None = None,
    ssh_host: str | None = None,
    ssh_local_port: int | None = None,
    ssh_compression: bool = False,
) -> tuple[RemoteDbInfo, str]:
    """Yield the remote DB metadata and a connectable DSN.

    When ``ssh_host`` is provided, an SSH local-port forward is opened and
    the returned DSN points to ``127.0.0.1:<forwarded-port>``.
    """
    info = load_remote_db_info(db_info_path)

    if ssh_host:
        with _open_ssh_tunnel(
            ssh_host=ssh_host,
            remote_port=info.port,
            local_port=ssh_local_port,
            compression=ssh_compression,
        ) as forwarded_port:
            yield info, info.to_dsn(host_override="127.0.0.1", port_override=forwarded_port)
        return

    yield info, info.to_dsn(host_override=host_override)


def sync_remote_tables_to_local(
    *,
    remote_dsn: str,
    local_dsn: str,
    batch_size: int,
    full_refresh: bool,
    all_tables: bool = False,
    tables: tuple[str, ...] | None = None,
) -> dict[str, int]:
    """Copy the supported remote tables into the local PostgreSQL database."""
    validate_remote_sync_options(
        batch_size=batch_size,
        full_refresh=full_refresh,
        all_tables=all_tables,
        tables=tables,
    )

    results: dict[str, int] = {}
    with contextlib.closing(psycopg2.connect(remote_dsn)) as remote_conn:
        remote_conn.set_session(readonly=True, autocommit=False)
        with contextlib.closing(psycopg2.connect(local_dsn)) as local_conn:
            local_conn.autocommit = False
            if all_tables:
                return _sync_pipeline_public_tables_to_local(
                    remote_conn=remote_conn,
                    local_conn=local_conn,
                )

            target_specs = _select_sync_specs(tables)
            if full_refresh:
                return _sync_selected_public_tables_to_local(
                    remote_conn=remote_conn,
                    local_conn=local_conn,
                    specs=target_specs,
                )

            dependencies = _list_foreign_key_dependencies(local_conn)
            _validate_prune_external_fk_children(
                specs=target_specs,
                dependencies=dependencies,
            )
            prune_keys_by_table: dict[str, set[tuple[Any, ...]]] = {}
            for spec in target_specs:
                if spec.copy_merge_enabled:
                    copied = _sync_table_via_copy_merge(
                        remote_conn=remote_conn,
                        local_conn=local_conn,
                        spec=spec,
                        batch_size=batch_size,
                    )
                else:
                    copied, remote_keys = _sync_table(
                        remote_conn=remote_conn,
                        local_conn=local_conn,
                        spec=spec,
                        batch_size=batch_size,
                        full_refresh=full_refresh,
                    )
                    if remote_keys is not None:
                        prune_keys_by_table[spec.name] = remote_keys
                results[spec.name] = copied

            _prune_missing_rows_for_specs(
                local_conn=local_conn,
                specs=target_specs,
                keys_by_table=prune_keys_by_table,
                dependencies=dependencies,
            )
            _sync_owned_sequences(
                remote_conn=remote_conn,
                local_conn=local_conn,
                tables=_database_tables_for_specs(target_specs),
            )
            local_conn.commit()

    return results


def validate_remote_sync_options(
    *,
    batch_size: int,
    full_refresh: bool,
    all_tables: bool = False,
    tables: tuple[str, ...] | None = None,
) -> None:
    """Validate remote sync options before any local destructive operation."""
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if all_tables and not full_refresh:
        raise ValueError("all_tables sync requires full_refresh=True")
    if all_tables and tables is not None:
        raise ValueError("all_tables sync cannot be combined with explicit tables")
    if tables is not None:
        _select_sync_specs(tables)


def _select_sync_specs(table_names: tuple[str, ...] | None) -> tuple[TableSyncSpec, ...]:
    """Resolve requested table names to ordered sync specs with FK parents included.

    The default ("all") set is the mirror list ``PIPELINE_FULL_REFRESH_TABLE_NAMES``,
    not every spec: the specs for the decommissioned derived/catalog tables remain
    defined (harmless) but are no longer mirrored (refactor §5.2, decision 7).
    """
    specs_by_name = {spec.name: spec for spec in SYNC_TABLE_SPECS}
    if table_names is None:
        selected_names = set(PIPELINE_FULL_REFRESH_TABLE_NAMES)
    else:
        requested = tuple(dict.fromkeys(name.strip() for name in table_names if name.strip()))
        unknown = sorted(name for name in requested if name not in specs_by_name)
        if unknown:
            raise ValueError(
                "Unsupported sync table(s): "
                + ", ".join(unknown)
                + ". Supported tables: "
                + ", ".join(specs_by_name)
            )
        selected_names = set(_expand_sync_table_dependencies(requested))

    return tuple(spec for spec in SYNC_TABLE_SPECS if spec.name in selected_names)


def _expand_sync_table_dependencies(table_names: tuple[str, ...]) -> tuple[str, ...]:
    """Return requested sync table names plus all required FK parent tables."""
    selected: set[str] = set()
    visiting: set[str] = set()

    def visit(table_name: str) -> None:
        if table_name in selected:
            return
        if table_name in visiting:
            raise ValueError(f"Cyclic sync table dependency detected at {table_name}")
        visiting.add(table_name)
        for parent_name in SYNC_TABLE_DEPENDENCIES.get(table_name, ()):
            visit(parent_name)
        visiting.remove(table_name)
        selected.add(table_name)

    for table_name in table_names:
        visit(table_name)

    return tuple(spec.name for spec in SYNC_TABLE_SPECS if spec.name in selected)


def _database_tables_for_specs(specs: tuple[TableSyncSpec, ...]) -> tuple[DatabaseTable, ...]:
    """Return physical public tables for sync specs."""
    return tuple(DatabaseTable(schema=PUBLIC_SCHEMA, name=spec.name) for spec in specs)


def _sync_pipeline_public_tables_to_local(*, remote_conn: Any, local_conn: Any) -> dict[str, int]:
    """Replace selected local pipeline tables with the matching remote table data."""
    _prepare_local_full_refresh_session(local_conn)

    remote_tables = _list_public_tables(remote_conn)
    local_tables = _list_public_tables(local_conn)
    target_tables = _select_required_public_tables(
        remote_tables=remote_tables,
        local_tables=local_tables,
        required_tables=PIPELINE_FULL_REFRESH_TABLES,
    )
    _validate_full_database_columns(
        remote_conn=remote_conn,
        local_conn=local_conn,
        tables=target_tables,
    )

    table_order = _sort_tables_by_fk_dependencies(
        tables=target_tables,
        dependencies=_list_foreign_key_dependencies(remote_conn),
    )
    _truncate_database_tables(local_conn=local_conn, tables=table_order)

    results: dict[str, int] = {}
    for table in table_order:
        columns = _list_table_columns(remote_conn, table)
        copied = _copy_database_table(
            remote_conn=remote_conn,
            local_conn=local_conn,
            table=table,
            columns=columns,
        )
        local_conn.commit()
        results[table.display_name] = copied
        logger.info("pipeline table sync copied table=%s rows=%s", table.display_name, copied)

    _sync_owned_sequences(remote_conn=remote_conn, local_conn=local_conn, tables=table_order)
    _reset_daily_ohlcv_checkpoint_from_local(local_conn)
    local_conn.commit()
    return results


def _sync_selected_public_tables_to_local(
    *,
    remote_conn: Any,
    local_conn: Any,
    specs: tuple[TableSyncSpec, ...],
) -> dict[str, int]:
    """Full-refresh selected managed mirror tables through direct binary COPY."""
    _prepare_local_full_refresh_session(local_conn)
    tables = _database_tables_for_specs(specs)
    dependencies = _list_foreign_key_dependencies(local_conn)
    _validate_no_external_fk_children(tables=tables, dependencies=dependencies)
    _validate_full_database_columns(
        remote_conn=remote_conn,
        local_conn=local_conn,
        tables=tables,
    )

    table_order = _sort_tables_by_fk_dependencies(
        tables=tables,
        dependencies=dependencies,
    )

    try:
        _truncate_database_tables(local_conn=local_conn, tables=table_order, commit=False)

        results: dict[str, int] = {}
        for table in table_order:
            columns = _list_table_columns(remote_conn, table)
            copied = _copy_database_table(
                remote_conn=remote_conn,
                local_conn=local_conn,
                table=table,
                columns=columns,
            )
            results[table.display_name] = copied
            logger.info(
                "selected table sync copied table=%s rows=%s",
                table.display_name,
                copied,
            )

        if any(spec.name == "daily_ohlcv" for spec in specs):
            _reset_daily_ohlcv_checkpoint_from_local(local_conn)
        local_conn.commit()
    except Exception:
        local_conn.rollback()
        raise

    try:
        _sync_owned_sequences(remote_conn=remote_conn, local_conn=local_conn, tables=table_order)
        local_conn.commit()
    except Exception:
        local_conn.rollback()
        raise

    return results


def _list_public_tables(conn: Any) -> tuple[DatabaseTable, ...]:
    """Return non-partition public tables in deterministic order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relkind IN ('r', 'p')
              AND NOT c.relispartition
            ORDER BY n.nspname, c.relname
            """,
            (PUBLIC_SCHEMA,),
        )
        return tuple(DatabaseTable(schema=row[0], name=row[1]) for row in cur.fetchall())


def _select_required_public_tables(
    *,
    remote_tables: tuple[DatabaseTable, ...],
    local_tables: tuple[DatabaseTable, ...],
    required_tables: tuple[DatabaseTable, ...],
) -> tuple[DatabaseTable, ...]:
    """Return required tables when they exist on both sides, allowing extras."""
    remote_set = set(remote_tables)
    local_set = set(local_tables)
    missing_remote = sorted(
        table.display_name for table in required_tables if table not in remote_set
    )
    missing_local = sorted(
        table.display_name for table in required_tables if table not in local_set
    )

    messages = []
    if missing_remote:
        messages.append(f"missing remotely: {', '.join(missing_remote)}")
    if missing_local:
        messages.append(f"missing locally: {', '.join(missing_local)}")
    if messages:
        raise ValueError("Required pipeline sync tables are unavailable; " + "; ".join(messages))

    return tuple(table for table in required_tables if table in remote_set)


def _validate_full_database_table_sets(
    *,
    remote_tables: tuple[DatabaseTable, ...],
    local_tables: tuple[DatabaseTable, ...],
) -> None:
    """Ensure destructive full-database sync only runs against matching table sets."""
    remote_set = set(remote_tables)
    local_set = set(local_tables)
    missing_local = sorted(table.display_name for table in remote_set - local_set)
    missing_remote = sorted(table.display_name for table in local_set - remote_set)

    messages = []
    if missing_local:
        messages.append(f"missing locally: {', '.join(missing_local)}")
    if missing_remote:
        messages.append(f"missing remotely: {', '.join(missing_remote)}")
    if messages:
        raise ValueError("Remote/local public table sets differ; " + "; ".join(messages))


def _validate_full_database_columns(
    *,
    remote_conn: Any,
    local_conn: Any,
    tables: tuple[DatabaseTable, ...],
) -> None:
    """Ensure all copied tables expose the same writable columns on both sides."""
    mismatches: list[str] = []
    for table in tables:
        remote_columns = _list_table_columns(remote_conn, table)
        local_columns = _list_table_columns(local_conn, table)
        if remote_columns != local_columns:
            mismatches.append(
                f"{table.display_name}: remote=({', '.join(remote_columns)}) "
                f"local=({', '.join(local_columns)})"
            )

    if mismatches:
        raise ValueError("Remote/local table columns differ; " + "; ".join(mismatches))


def _list_table_columns(conn: Any, table: DatabaseTable) -> tuple[str, ...]:
    """Return insertable columns in physical order for a table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_attribute a
            WHERE a.attrelid = %s::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attgenerated = ''
            ORDER BY a.attnum
            """,
            (_regclass_text(table),),
        )
        return tuple(row[0] for row in cur.fetchall())


def _list_foreign_key_dependencies(conn: Any) -> tuple[tuple[DatabaseTable, DatabaseTable], ...]:
    """Return child-to-parent FK dependencies between public tables."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                child_ns.nspname AS child_schema,
                child.relname AS child_table,
                parent_ns.nspname AS parent_schema,
                parent.relname AS parent_table
            FROM pg_constraint con
            JOIN pg_class child ON child.oid = con.conrelid
            JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            JOIN pg_class parent ON parent.oid = con.confrelid
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            WHERE con.contype = 'f'
              AND child_ns.nspname = %s
              AND parent_ns.nspname = %s
            ORDER BY child_ns.nspname, child.relname, parent_ns.nspname, parent.relname
            """,
            (PUBLIC_SCHEMA, PUBLIC_SCHEMA),
        )
        return tuple(
            (
                DatabaseTable(schema=row[0], name=row[1]),
                DatabaseTable(schema=row[2], name=row[3]),
            )
            for row in cur.fetchall()
        )


def _sort_tables_by_fk_dependencies(
    *,
    tables: tuple[DatabaseTable, ...],
    dependencies: tuple[tuple[DatabaseTable, DatabaseTable], ...],
) -> tuple[DatabaseTable, ...]:
    """Order tables so parents are copied before FK children."""
    table_set = set(tables)
    remaining_parents = {table: set[DatabaseTable]() for table in tables}
    children_by_parent = {table: set[DatabaseTable]() for table in tables}

    for child, parent in dependencies:
        if child not in table_set or parent not in table_set or child == parent:
            continue
        remaining_parents[child].add(parent)
        children_by_parent[parent].add(child)

    ready = sorted(
        (table for table in tables if not remaining_parents[table]),
        key=_table_sort_key,
    )
    ordered: list[DatabaseTable] = []

    while ready:
        table = ready.pop(0)
        ordered.append(table)
        for child in sorted(children_by_parent[table], key=_table_sort_key):
            remaining_parents[child].discard(table)
            if not remaining_parents[child] and child not in ordered and child not in ready:
                ready.append(child)
        ready.sort(key=_table_sort_key)

    if len(ordered) != len(tables):
        cyclic_tables = sorted(table.display_name for table in tables if table not in set(ordered))
        raise ValueError(
            "Cannot determine full database copy order due to cyclic foreign keys: "
            + ", ".join(cyclic_tables)
        )

    return tuple(ordered)


def _truncate_database_tables(
    *,
    local_conn: Any,
    tables: tuple[DatabaseTable, ...],
    commit: bool = True,
) -> None:
    """Truncate target tables before a full refresh."""
    if not tables:
        return

    table_list = sql.SQL(", ").join(_table_identifier(table) for table in tables)
    statement = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(table_list)
    with local_conn.cursor() as cur:
        cur.execute(statement)
    if commit:
        local_conn.commit()


def _copy_database_table(
    *,
    remote_conn: Any,
    local_conn: Any,
    table: DatabaseTable,
    columns: tuple[str, ...],
) -> int:
    """Stream one table from remote to local with PostgreSQL binary COPY."""
    if not columns:
        return 0

    column_list = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    copy_to = sql.SQL("COPY {} ({}) TO STDOUT WITH (FORMAT BINARY)").format(
        _table_identifier(table),
        column_list,
    )
    copy_from = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)").format(
        _table_identifier(table),
        column_list,
    )

    read_fd, write_fd = os.pipe()
    producer_errors: list[BaseException] = []

    def produce_copy_stream() -> None:
        try:
            with os.fdopen(write_fd, "wb", closefd=True) as write_file:
                with remote_conn.cursor() as remote_cur:
                    remote_cur.copy_expert(copy_to.as_string(remote_conn), write_file)
        except BaseException as exc:  # pragma: no cover - surfaced through main thread
            producer_errors.append(exc)

    producer = threading.Thread(target=produce_copy_stream, daemon=True)
    producer.start()

    status_message = ""
    try:
        with os.fdopen(read_fd, "rb", closefd=True) as read_file:
            with local_conn.cursor() as local_cur:
                local_cur.copy_expert(copy_from.as_string(local_conn), read_file)
                status_message = local_cur.statusmessage
    finally:
        producer.join()

    if producer_errors:
        raise RuntimeError(
            f"Remote COPY failed for {table.display_name}: {producer_errors[0]}"
        ) from producer_errors[0]

    copied_rows = _copy_status_row_count(status_message)
    if copied_rows is not None:
        return copied_rows
    return _count_table_rows(local_conn=local_conn, table=table)


def _copy_status_row_count(status_message: str) -> int | None:
    """Extract row count from a PostgreSQL COPY status message."""
    parts = status_message.split()
    if len(parts) == 2 and parts[0] == "COPY" and parts[1].isdigit():
        return int(parts[1])
    return None


def _count_table_rows(*, local_conn: Any, table: DatabaseTable) -> int:
    """Count rows in a copied table when COPY status does not expose a count."""
    with local_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(_table_identifier(table)))
        row = cur.fetchone()
    return int(row[0])


def _sync_owned_sequences(
    *,
    remote_conn: Any,
    local_conn: Any,
    tables: tuple[DatabaseTable, ...],
) -> int:
    """Copy owned sequence states after explicit table data loads."""
    table_set = set(tables)
    sequences = [
        sequence
        for sequence, owner_table in _list_owned_sequences(remote_conn)
        if owner_table in table_set
    ]

    for sequence in sequences:
        last_value, is_called = _read_sequence_state(remote_conn, sequence)
        with local_conn.cursor() as cur:
            cur.execute(
                "SELECT setval(%s::regclass, %s, %s)",
                (_regclass_text(sequence), last_value, is_called),
            )

    if sequences:
        logger.info("pipeline table sync copied sequence states count=%s", len(sequences))
    return len(sequences)


def _reset_daily_ohlcv_checkpoint_from_local(local_conn: Any) -> None:
    """Align the local remote-sync checkpoint with copied daily OHLCV rows."""
    daily_spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "daily_ohlcv")
    cursor_values = _get_local_cursor(local_conn=local_conn, spec=daily_spec)
    if cursor_values is None:
        with local_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sync_checkpoints WHERE sync_name = %s",
                (DAILY_OHLCV_SYNC_NAME,),
            )
        return

    _save_daily_ohlcv_checkpoint(local_conn, cursor_values)


def _list_owned_sequences(conn: Any) -> tuple[tuple[DatabaseTable, DatabaseTable], ...]:
    """Return public sequences and their owning public tables."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                sequence_ns.nspname AS sequence_schema,
                sequence.relname AS sequence_name,
                table_ns.nspname AS table_schema,
                table_class.relname AS table_name
            FROM pg_class sequence
            JOIN pg_namespace sequence_ns ON sequence_ns.oid = sequence.relnamespace
            JOIN pg_depend dep ON dep.objid = sequence.oid AND dep.deptype = 'a'
            JOIN pg_class table_class ON table_class.oid = dep.refobjid
            JOIN pg_namespace table_ns ON table_ns.oid = table_class.relnamespace
            WHERE sequence.relkind = 'S'
              AND sequence_ns.nspname = %s
              AND table_ns.nspname = %s
            ORDER BY sequence_ns.nspname, sequence.relname
            """,
            (PUBLIC_SCHEMA, PUBLIC_SCHEMA),
        )
        return tuple(
            (
                DatabaseTable(schema=row[0], name=row[1]),
                DatabaseTable(schema=row[2], name=row[3]),
            )
            for row in cur.fetchall()
        )


def _read_sequence_state(conn: Any, sequence: DatabaseTable) -> tuple[int, bool]:
    """Read last_value/is_called from a sequence."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT last_value, is_called FROM {}").format(_table_identifier(sequence))
        )
        row = cur.fetchone()
    return int(row[0]), bool(row[1])


def _table_identifier(table: DatabaseTable) -> sql.Identifier:
    """Build a safely quoted SQL identifier for a table or sequence."""
    return sql.Identifier(table.schema, table.name)


def _regclass_text(table: DatabaseTable) -> str:
    """Build a regclass input string for parameterized catalog queries."""
    return f"{_quote_identifier_text(table.schema)}.{_quote_identifier_text(table.name)}"


def _quote_identifier_text(value: str) -> str:
    """Quote one SQL identifier for use inside a regclass text value."""
    return '"' + value.replace('"', '""') + '"'


def _table_sort_key(table: DatabaseTable) -> tuple[str, str]:
    """Return stable sort key for database table metadata."""
    return table.schema, table.name


def _truncate_sync_tables(
    *,
    local_conn: Any,
    specs: tuple[TableSyncSpec, ...],
) -> None:
    """Remove previously synced rows before a full refresh."""
    tables = _database_tables_for_specs(specs)
    dependencies = _list_foreign_key_dependencies(local_conn)
    _validate_no_external_fk_children(tables=tables, dependencies=dependencies)
    table_order = _sort_tables_by_fk_dependencies(
        tables=tables,
        dependencies=dependencies,
    )
    _truncate_database_tables(local_conn=local_conn, tables=table_order)

    if any(spec.name == "daily_ohlcv" for spec in specs):
        with local_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sync_checkpoints WHERE sync_name = %s",
                (DAILY_OHLCV_SYNC_NAME,),
            )
        local_conn.commit()


def _truncate_target_tables(local_conn: Any) -> None:
    """Compatibility wrapper for the legacy five-table full refresh path."""
    legacy_specs = tuple(
        spec
        for spec in SYNC_TABLE_SPECS
        if spec.name
        in {
            "stock_master",
            "stock_master_snapshot",
            "stock_master_snapshot_items",
            "daily_ohlcv",
            "krx_security_flow_raw",
        }
    )
    _truncate_sync_tables(local_conn=local_conn, specs=legacy_specs)


def _validate_no_external_fk_children(
    *,
    tables: tuple[DatabaseTable, ...],
    dependencies: tuple[tuple[DatabaseTable, DatabaseTable], ...],
) -> None:
    """Reject partial full-refresh subsets that omit FK child tables."""
    table_set = set(tables)
    external_children = sorted(
        child.display_name
        for child, parent in dependencies
        if parent in table_set and child not in table_set
    )
    if external_children:
        raise ValueError(
            "Unsafe full-refresh table subset; include FK child tables or choose "
            "incremental sync: " + ", ".join(external_children)
        )


def _validate_prune_external_fk_children(
    *,
    specs: tuple[TableSyncSpec, ...],
    dependencies: tuple[tuple[DatabaseTable, DatabaseTable], ...],
) -> None:
    """Reject pruning a parent table while an omitted child can still reference it."""
    selected_tables = set(_database_tables_for_specs(specs))
    prune_tables = {
        DatabaseTable(schema=PUBLIC_SCHEMA, name=spec.name)
        for spec in specs
        if spec.prune_missing_after_full_scan
    }
    external_children = sorted(
        child.display_name
        for child, parent in dependencies
        if parent in prune_tables and child not in selected_tables
    )
    if external_children:
        raise ValueError(
            "Unsafe pruning table subset; include FK child tables or choose a "
            "non-pruning table set: " + ", ".join(external_children)
        )


def _sync_table(
    *,
    remote_conn: Any,
    local_conn: Any,
    spec: TableSyncSpec,
    batch_size: int,
    full_refresh: bool,
) -> tuple[int, set[tuple[Any, ...]] | None]:
    """Copy one table in batches using a stable incremental cursor."""
    copied_rows = 0
    cursor_values = (
        None
        if full_refresh or spec.always_full_scan
        else _get_local_cursor(local_conn=local_conn, spec=spec)
    )
    remote_keys: set[tuple[Any, ...]] | None = set() if spec.prune_missing_after_full_scan else None

    while True:
        rows = _fetch_remote_rows(
            remote_conn=remote_conn,
            spec=spec,
            cursor_values=cursor_values,
            batch_size=batch_size,
        )
        if not rows:
            return copied_rows, remote_keys

        _upsert_rows(local_conn=local_conn, spec=spec, rows=rows)
        if remote_keys is not None:
            remote_keys.update(_row_conflict_key(spec=spec, row=row) for row in rows)
        copied_rows += len(rows)
        cursor_values = tuple(rows[-1][index] for index in spec.cursor_indexes)


def _sync_table_via_copy_merge(
    *,
    remote_conn: Any,
    local_conn: Any,
    spec: TableSyncSpec,
    batch_size: int,
) -> int:
    """Copy one incremental table through remote COPY into local staging."""
    _ensure_copy_merge_staging_table(local_conn=local_conn, spec=spec)
    copied_rows = 0
    batch_number = 0

    if spec.name == "daily_ohlcv":
        checkpoint_cursor = _load_daily_ohlcv_checkpoint(local_conn)
        local_cursor = _get_local_cursor(local_conn=local_conn, spec=spec)
        cursor_values = _select_resume_cursor(checkpoint_cursor, local_cursor)
    else:
        cursor_values = _get_local_cursor(local_conn=local_conn, spec=spec)

    stage_table = _copy_merge_stage_table_name(spec)

    try:
        while True:
            started_at = time.monotonic()
            batch_rows = _copy_remote_select_to_staging(
                remote_conn=remote_conn,
                local_conn=local_conn,
                spec=spec,
                stage_table=stage_table,
                cursor_values=cursor_values,
                batch_size=batch_size,
            )
            if batch_rows == 0:
                break

            try:
                _merge_staging_rows(local_conn=local_conn, spec=spec, stage_table=stage_table)
                next_cursor = _get_staging_cursor(
                    local_conn=local_conn,
                    spec=spec,
                    stage_table=stage_table,
                )
                if next_cursor is None:
                    raise RuntimeError(f"Staging table {stage_table} has no cursor row")
                cursor_values = next_cursor
                if spec.name == "daily_ohlcv":
                    _save_daily_ohlcv_checkpoint(local_conn, cursor_values)
                local_conn.commit()
            except Exception:
                local_conn.rollback()
                raise

            copied_rows += batch_rows
            batch_number += 1
            elapsed = max(time.monotonic() - started_at, 0.001)
            logger.info(
                "%s copy-merge batch=%s rows=%s total=%s rate=%.0f rows/s cursor=%s",
                spec.name,
                batch_number,
                batch_rows,
                copied_rows,
                batch_rows / elapsed,
                _format_cursor_for_log(cursor_values),
            )
    finally:
        remote_conn.rollback()

    return copied_rows


def _get_local_cursor(*, local_conn: Any, spec: TableSyncSpec) -> tuple[Any, ...] | None:
    """Return the most recent local cursor state for a table."""
    with local_conn.cursor() as cur:
        cur.execute(spec.local_cursor_sql)
        row = cur.fetchone()

    if row is None:
        return None
    return tuple(row)


def _load_daily_ohlcv_checkpoint(local_conn: Any) -> tuple[Any, ...] | None:
    """Load the saved resume cursor for ``daily_ohlcv``."""
    with local_conn.cursor() as cur:
        cur.execute(
            "SELECT cursor_payload FROM sync_checkpoints WHERE sync_name = %s",
            (DAILY_OHLCV_SYNC_NAME,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return (
        datetime.fromisoformat(payload["fetched_at"]),
        date.fromisoformat(payload["trade_date"]),
        payload["ticker"],
        payload["market"],
    )


def _save_daily_ohlcv_checkpoint(local_conn: Any, cursor_values: tuple[Any, ...]) -> None:
    """Persist the latest successfully merged ``daily_ohlcv`` cursor."""
    payload = _daily_ohlcv_checkpoint_payload(cursor_values)
    with local_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_checkpoints (sync_name, cursor_payload, updated_at)
            VALUES (%s, %s::jsonb, now())
            ON CONFLICT (sync_name) DO UPDATE SET
                cursor_payload = EXCLUDED.cursor_payload,
                updated_at = EXCLUDED.updated_at
            """,
            (DAILY_OHLCV_SYNC_NAME, json.dumps(payload)),
        )


def _daily_ohlcv_checkpoint_payload(cursor_values: tuple[Any, ...]) -> dict[str, str]:
    """Serialize a ``daily_ohlcv`` cursor tuple into JSON-friendly form."""
    fetched_at, trade_date, ticker, market = cursor_values
    return {
        "fetched_at": fetched_at.isoformat(),
        "trade_date": trade_date.isoformat(),
        "ticker": ticker,
        "market": market,
    }


def _select_resume_cursor(
    checkpoint_cursor: tuple[Any, ...] | None,
    local_cursor: tuple[Any, ...] | None,
) -> tuple[Any, ...] | None:
    """Choose the furthest-known resume cursor."""
    if checkpoint_cursor is None:
        return local_cursor
    if local_cursor is None:
        return checkpoint_cursor
    return max(checkpoint_cursor, local_cursor)


def _fetch_remote_rows(
    *,
    remote_conn: Any,
    spec: TableSyncSpec,
    cursor_values: tuple[Any, ...] | None,
    batch_size: int,
) -> list[tuple[Any, ...]]:
    """Fetch the next batch from the remote table."""
    predicate = ""
    params: list[Any] = []

    if cursor_values is not None:
        tuple_expr = ", ".join(spec.order_columns)
        placeholders = ", ".join(["%s"] * len(cursor_values))
        predicate = f"WHERE ({tuple_expr}) > ({placeholders})"
        params.extend(cursor_values)

    query = (
        f"SELECT {spec.select_list} "
        f"FROM {spec.from_clause} "
        f"{predicate} "
        f"ORDER BY {', '.join(spec.order_columns)} "
        f"LIMIT %s"
    )
    params.append(batch_size)

    with remote_conn.cursor() as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def _copy_merge_stage_table_name(spec: TableSyncSpec) -> str:
    """Return the per-table temporary staging table name for COPY merge."""
    return f"remote_sync_stage_{spec.name}"


def _ensure_copy_merge_staging_table(*, local_conn: Any, spec: TableSyncSpec) -> None:
    """Create the temp staging table used by generic COPY merge."""
    stage_table = _copy_merge_stage_table_name(spec)
    column_list = sql.SQL(", ").join(sql.Identifier(column) for column in spec.insert_columns)
    statement = sql.SQL(
        "CREATE TEMP TABLE IF NOT EXISTS {} "
        "ON COMMIT DELETE ROWS AS "
        "SELECT {} FROM {} WHERE FALSE"
    ).format(
        sql.Identifier(stage_table),
        column_list,
        _table_identifier(DatabaseTable(schema=PUBLIC_SCHEMA, name=spec.name)),
    )
    with local_conn.cursor() as cur:
        cur.execute(statement)


def _copy_remote_select_to_staging(
    *,
    remote_conn: Any,
    local_conn: Any,
    spec: TableSyncSpec,
    stage_table: str,
    cursor_values: tuple[Any, ...] | None,
    batch_size: int,
) -> int:
    """Stream one remote SELECT batch into a local temp staging table via COPY."""
    with local_conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(stage_table)))

    read_fd, write_fd = os.pipe()
    producer_errors: list[BaseException] = []

    def produce_copy_stream() -> None:
        try:
            with os.fdopen(write_fd, "wb", closefd=True) as write_file:
                with remote_conn.cursor() as remote_cur:
                    copy_to = _build_copy_select_sql(
                        remote_cur=remote_cur,
                        spec=spec,
                        cursor_values=cursor_values,
                        batch_size=batch_size,
                    )
                    remote_cur.copy_expert(copy_to, write_file)
        except BaseException as exc:  # pragma: no cover - surfaced through main thread
            producer_errors.append(exc)

    producer = threading.Thread(target=produce_copy_stream, daemon=True)
    producer.start()

    status_message = ""
    try:
        with os.fdopen(read_fd, "rb", closefd=True) as read_file:
            with local_conn.cursor() as local_cur:
                local_cur.copy_expert(
                    _build_copy_stage_from_stdin_sql(
                        local_conn=local_conn,
                        spec=spec,
                        stage_table=stage_table,
                    ),
                    read_file,
                )
                status_message = local_cur.statusmessage
    finally:
        producer.join()

    if producer_errors:
        raise RuntimeError(
            f"Remote COPY merge failed for {spec.name}: {producer_errors[0]}"
        ) from producer_errors[0]

    copied_rows = _copy_status_row_count(status_message)
    if copied_rows is not None:
        return copied_rows
    return _count_stage_rows(local_conn=local_conn, stage_table=stage_table)


def _build_copy_select_sql(
    *,
    remote_cur: Any,
    spec: TableSyncSpec,
    cursor_values: tuple[Any, ...] | None,
    batch_size: int,
) -> str:
    """Build a COPY SELECT statement with safely quoted cursor literals."""
    predicate = ""
    if cursor_values is not None:
        tuple_expr = ", ".join(spec.order_columns)
        placeholders = ", ".join(["%s"] * len(cursor_values))
        predicate = remote_cur.mogrify(
            f"WHERE ({tuple_expr}) > ({placeholders})",
            cursor_values,
        ).decode()

    limit_literal = remote_cur.mogrify("%s", (batch_size,)).decode()
    return (
        "COPY ("
        f"SELECT {spec.select_list} "
        f"FROM {spec.from_clause} "
        f"{predicate} "
        f"ORDER BY {', '.join(spec.order_columns)} "
        f"LIMIT {limit_literal}"
        ") TO STDOUT WITH (FORMAT CSV, NULL '\\N')"
    )


def _build_copy_stage_from_stdin_sql(
    *,
    local_conn: Any,
    spec: TableSyncSpec,
    stage_table: str,
) -> str:
    """Build the local COPY FROM STDIN statement for a staging table."""
    column_list = sql.SQL(", ").join(sql.Identifier(column) for column in spec.insert_columns)
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')").format(
        sql.Identifier(stage_table),
        column_list,
    )
    return statement.as_string(local_conn)


def _count_stage_rows(*, local_conn: Any, stage_table: str) -> int:
    """Count rows in a staging table when COPY status does not expose a count."""
    with local_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(stage_table)))
        row = cur.fetchone()
    return int(row[0])


def _merge_staging_rows(
    *,
    local_conn: Any,
    spec: TableSyncSpec,
    stage_table: str,
) -> None:
    """Merge staged rows into the target table using the spec's conflict action."""
    statement = _build_insert_select_from_stage_statement(
        spec=spec,
        stage_table=stage_table,
    )
    with local_conn.cursor() as cur:
        cur.execute(statement)


def _build_insert_select_from_stage_statement(
    *,
    spec: TableSyncSpec,
    stage_table: str,
) -> str:
    """Build an INSERT ... SELECT FROM stage merge statement."""
    insert_columns = ", ".join(spec.insert_columns)
    return (
        f"INSERT INTO {spec.name} ({insert_columns}) "
        f"SELECT {insert_columns} FROM {stage_table} "
        f"{_build_conflict_action(spec)}"
    )


def _get_staging_cursor(
    *,
    local_conn: Any,
    spec: TableSyncSpec,
    stage_table: str,
) -> tuple[Any, ...] | None:
    """Return the furthest cursor represented by the current staging batch."""
    order_columns = ", ".join(spec.order_columns)
    with local_conn.cursor() as cur:
        cur.execute(
            f"SELECT {order_columns} FROM {stage_table} " f"ORDER BY {order_columns} DESC LIMIT 1"
        )
        row = cur.fetchone()

    if row is None:
        return None
    return tuple(row)


def _format_cursor_for_log(cursor_values: tuple[Any, ...] | None) -> str:
    """Format cursor values for compact progress logging."""
    if cursor_values is None:
        return "None"
    return ", ".join(str(value) for value in cursor_values)


def _prepare_local_full_refresh_session(local_conn: Any) -> None:
    """Relax durability for this dedicated full-refresh session."""
    with local_conn.cursor() as cur:
        cur.execute("SET synchronous_commit = OFF")


def _build_conflict_action(spec: TableSyncSpec) -> str:
    """Build the ON CONFLICT action for a table sync spec."""
    if spec.conflict_constraint is not None:
        conflict_target = f"ON CONFLICT ON CONSTRAINT {spec.conflict_constraint}"
    else:
        conflict_columns = ", ".join(spec.conflict_columns)
        conflict_target = f"ON CONFLICT ({conflict_columns})"

    assignment_columns = tuple(
        dict.fromkeys((*spec.preserve_remote_surrogate_columns, *spec.update_columns))
    )
    if assignment_columns:
        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in assignment_columns)
        conflict_action = f"{conflict_target} DO UPDATE SET {assignments}"
        if spec.conflict_update_where_sql:
            conflict_action = f"{conflict_action} WHERE {spec.conflict_update_where_sql}"
    elif spec.do_nothing_when_no_update_columns:
        conflict_action = f"{conflict_target} DO NOTHING"
    else:
        raise ValueError(f"Sync spec {spec.name} has no update columns")
    return conflict_action


def _upsert_rows(*, local_conn: Any, spec: TableSyncSpec, rows: list[tuple[Any, ...]]) -> None:
    """Upsert a batch into the local table."""
    insert_columns = ", ".join(spec.insert_columns)
    values = [_adapt_insert_row(spec=spec, row=row) for row in rows]
    statement = (
        f"INSERT INTO {spec.name} ({insert_columns}) "
        f"VALUES %s "
        f"{_build_conflict_action(spec)}"
    )

    try:
        with local_conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                statement,
                values,
                page_size=min(len(values), 1000),
            )
        local_conn.commit()
    except Exception:
        local_conn.rollback()
        raise


def _row_conflict_key(*, spec: TableSyncSpec, row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Extract a row's conflict-key values from the selected column order."""
    indexes = tuple(spec.insert_columns.index(column) for column in spec.conflict_columns)
    return tuple(row[index] for index in indexes)


def _prune_missing_rows(
    *,
    local_conn: Any,
    spec: TableSyncSpec,
    keys: set[tuple[Any, ...]],
) -> None:
    """Delete local rows that are absent from a completed remote full scan."""
    if not spec.conflict_columns:
        raise ValueError(f"Sync spec {spec.name} has no conflict columns for pruning")

    try:
        with local_conn.cursor() as cur:
            if not keys:
                cur.execute(f"DELETE FROM {spec.name}")
            else:
                temp_table = "remote_sync_prune_keys"
                key_columns = ", ".join(spec.conflict_columns)
                join_predicate = " AND ".join(
                    f"target.{column} IS NOT DISTINCT FROM remote_keys.{column}"
                    for column in spec.conflict_columns
                )
                cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
                cur.execute(
                    f"CREATE TEMP TABLE {temp_table} ON COMMIT DROP AS "
                    f"SELECT {key_columns} FROM {spec.name} WHERE FALSE"
                )
                insert_statement = f"INSERT INTO {temp_table} ({key_columns}) VALUES %s"
                psycopg2.extras.execute_values(
                    cur,
                    insert_statement,
                    list(keys),
                    page_size=1000,
                )
                delete_statement = (
                    f"DELETE FROM {spec.name} AS target "
                    f"WHERE NOT EXISTS ("
                    f"SELECT 1 FROM {temp_table} AS remote_keys "
                    f"WHERE {join_predicate}"
                    f")"
                )
                cur.execute(delete_statement)
        local_conn.commit()
    except Exception:
        local_conn.rollback()
        raise


def _prune_missing_rows_for_specs(
    *,
    local_conn: Any,
    specs: tuple[TableSyncSpec, ...],
    keys_by_table: dict[str, set[tuple[Any, ...]]],
    dependencies: tuple[tuple[DatabaseTable, DatabaseTable], ...],
) -> None:
    """Prune full-scan tables after all upserts, deleting FK children first."""
    if not keys_by_table:
        return

    specs_by_name = {spec.name: spec for spec in specs}
    prune_tables = tuple(
        DatabaseTable(schema=PUBLIC_SCHEMA, name=table_name) for table_name in keys_by_table
    )
    prune_order = reversed(
        _sort_tables_by_fk_dependencies(tables=prune_tables, dependencies=dependencies)
    )
    for table in prune_order:
        spec = specs_by_name[table.name]
        _prune_missing_rows(
            local_conn=local_conn,
            spec=spec,
            keys=keys_by_table[spec.name],
        )


def _adapt_insert_row(*, spec: TableSyncSpec, row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Adapt row values that need psycopg2 wrappers before ``execute_values``."""
    values = row[: len(spec.insert_columns)]
    if not spec.json_columns:
        return values

    json_columns = set(spec.json_columns)
    adapted: list[Any] = []
    for column, value in zip(spec.insert_columns, values, strict=True):
        if column in json_columns and isinstance(value, (dict, list)):
            adapted.append(psycopg2.extras.Json(value))
        else:
            adapted.append(value)
    return tuple(adapted)


@contextlib.contextmanager
def _open_ssh_tunnel(
    *,
    ssh_host: str,
    remote_port: int,
    local_port: int | None,
    compression: bool = False,
) -> int:
    """Open an SSH tunnel to the remote PostgreSQL host."""
    forwarded_port = local_port or _find_free_port()
    cmd = [
        "ssh",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-N",
        "-L",
        f"{forwarded_port}:127.0.0.1:{remote_port}",
        ssh_host,
    ]
    if compression:
        cmd.insert(1, "-C")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_local_port(proc=proc, local_port=forwarded_port)
        yield forwarded_port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _wait_for_local_port(
    *,
    proc: subprocess.Popen[str],
    local_port: int,
    timeout_seconds: float = 5.0,
) -> None:
    """Wait until the forwarded local port is accepting connections."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stderr = proc.stderr.read().strip() if proc.stderr else ""
            raise RuntimeError(f"SSH tunnel process exited early: {stderr or 'no stderr output'}")

        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)

    raise TimeoutError(f"Timed out waiting for SSH tunnel on local port {local_port}")


def _find_free_port() -> int:
    """Return an available local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])

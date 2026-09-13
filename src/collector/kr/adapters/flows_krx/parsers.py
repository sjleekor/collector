"""Parsers for raw KRX MDC security-flow JSON rows."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from collector.kr.adapters.flows_common import (
    FOREIGN_HOLDING_SHARES,
    FOREIGN_NET_BUY_VOLUME,
    INDIVIDUAL_NET_BUY_VOLUME,
    INSTITUTION_NET_BUY_VOLUME,
    KRX_INVESTOR_COLUMNS,
    SHORT_SELLING_BALANCE_QUANTITY,
    SHORT_SELLING_VALUE,
    SHORT_SELLING_VOLUME,
    FlowMetricSpec,
)
from collector.kr.adapters.krx_common.client import KrxMdcRow
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import SecurityFlowLine
from collector.kr.util.time import now_kst

INVESTOR_BLD = "dbms/MDC/STAT/standard/MDCSTAT02302"
INVESTOR_BULK_BLD = "dbms/MDC/STAT/standard/MDCSTAT02401"
FOREIGN_HOLDING_BLD = "dbms/MDC/STAT/standard/MDCSTAT03701"
SHORTING_STATUS_BLD = "dbms/MDC/STAT/srt/MDCSTAT30001"
SHORTING_TRADING_BULK_BLD = "dbms/MDC/STAT/srt/MDCSTAT30101"
SHORTING_BALANCE_BULK_BLD = "dbms/MDC/STAT/srt/MDCSTAT30501"
SHORTING_BALANCE_BLD = "dbms/MDC/STAT/srt/MDCSTAT30502"


def parse_investor_net_volume_rows(
    rows: Iterable[KrxMdcRow],
    ticker: str,
    market: Market,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT02302 rows into investor net-buy volume metrics."""
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []
    normalized_ticker = str(ticker).zfill(6)

    for item in rows:
        row = item.row
        trade_date = parse_trade_date(row.get("TRD_DD"))
        for column_name, spec in KRX_INVESTOR_COLUMNS.items():
            value = parse_decimal(row.get(column_name))
            if value is None:
                continue
            records.append(
                _build_line(
                    trade_date=trade_date,
                    ticker=normalized_ticker,
                    market=market,
                    spec=spec,
                    value=value,
                    fetched_at=fetched_at,
                    source_bld=INVESTOR_BLD,
                    request=item.request,
                    row=row,
                )
            )
    return records


def parse_foreign_holding_rows(
    rows: Iterable[KrxMdcRow],
    market: Market,
    trade_date: date,
    tickers: list[str] | None = None,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT03701 rows into foreign holding share metrics."""
    ticker_filter = {str(ticker).zfill(6) for ticker in tickers or []}
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []

    for item in rows:
        row = item.row
        ticker = str(row.get("ISU_SRT_CD", "")).strip().zfill(6)
        if not ticker or (ticker_filter and ticker not in ticker_filter):
            continue
        value = parse_decimal(row.get("FORN_HD_QTY"))
        if value is None:
            continue
        records.append(
            _build_line(
                trade_date=trade_date,
                ticker=ticker,
                market=market,
                spec=FOREIGN_HOLDING_SHARES,
                value=value,
                fetched_at=fetched_at,
                source_bld=FOREIGN_HOLDING_BLD,
                request=item.request,
                row=row,
            )
        )
    return records


def parse_investor_net_volume_bulk_rows(
    *,
    individual_rows: Iterable[KrxMdcRow],
    institution_rows: Iterable[KrxMdcRow],
    foreign_rows: Iterable[KrxMdcRow],
    other_foreign_rows: Iterable[KrxMdcRow],
    market: Market,
    trade_date: date,
    tickers: list[str] | None = None,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT02401 rows into investor net-buy volume metrics.

    KRX omits some zero-valued institution/foreign rows from MDCSTAT02401.
    The individual investor response has matched the legacy per-ticker endpoint
    coverage in live checks, so it is the base row set. Missing institution,
    foreign, or other-foreign rows are interpreted as zero within that base set.
    """
    ticker_filter = {str(ticker).zfill(6) for ticker in tickers or []}
    fetched_at = now_kst()
    individual_by_ticker = _rows_by_ticker(individual_rows)
    institution_by_ticker = _rows_by_ticker(institution_rows)
    foreign_by_ticker = _rows_by_ticker(foreign_rows)
    other_foreign_by_ticker = _rows_by_ticker(other_foreign_rows)
    records: list[SecurityFlowLine] = []

    for ticker, individual_item in sorted(individual_by_ticker.items()):
        if ticker_filter and ticker not in ticker_filter:
            continue

        records.append(
            _build_line(
                trade_date=trade_date,
                ticker=ticker,
                market=market,
                spec=INDIVIDUAL_NET_BUY_VOLUME,
                value=_net_bid_volume(individual_item),
                fetched_at=fetched_at,
                source_bld=INVESTOR_BULK_BLD,
                request=individual_item.request,
                row=_investor_bulk_payload(
                    base_item=individual_item,
                    component_items={"individual": individual_item},
                ),
            )
        )

        institution_item = institution_by_ticker.get(ticker)
        records.append(
            _build_line(
                trade_date=trade_date,
                ticker=ticker,
                market=market,
                spec=INSTITUTION_NET_BUY_VOLUME,
                value=_net_bid_volume(institution_item),
                fetched_at=fetched_at,
                source_bld=INVESTOR_BULK_BLD,
                request=(institution_item or individual_item).request,
                row=_investor_bulk_payload(
                    base_item=individual_item,
                    component_items={"institution": institution_item},
                ),
            )
        )

        foreign_item = foreign_by_ticker.get(ticker)
        other_foreign_item = other_foreign_by_ticker.get(ticker)
        records.append(
            _build_line(
                trade_date=trade_date,
                ticker=ticker,
                market=market,
                spec=FOREIGN_NET_BUY_VOLUME,
                value=_net_bid_volume(foreign_item) + _net_bid_volume(other_foreign_item),
                fetched_at=fetched_at,
                source_bld=INVESTOR_BULK_BLD,
                request=(foreign_item or other_foreign_item or individual_item).request,
                row=_investor_bulk_payload(
                    base_item=individual_item,
                    component_items={
                        "foreign": foreign_item,
                        "other_foreign": other_foreign_item,
                    },
                ),
            )
        )

    return records


def parse_shorting_rows(
    status_rows: Iterable[KrxMdcRow],
    balance_rows: Iterable[KrxMdcRow],
    ticker: str,
    market: Market,
) -> list[SecurityFlowLine]:
    """Parse short-selling status and balance rows with balance fallback."""
    fetched_at = now_kst()
    normalized_ticker = str(ticker).zfill(6)
    status_by_date: dict[date, KrxMdcRow] = {}
    balance_by_date: dict[date, KrxMdcRow] = {}

    for item in status_rows:
        status_by_date[parse_trade_date(item.row.get("TRD_DD"))] = item
    for item in balance_rows:
        balance_by_date[parse_trade_date(item.row.get("RPT_DUTY_OCCR_DD"))] = item

    records: list[SecurityFlowLine] = []
    for trade_date in sorted(status_by_date.keys() | balance_by_date.keys()):
        status_item = status_by_date.get(trade_date)
        balance_item = balance_by_date.get(trade_date)

        if status_item is not None:
            status_row = status_item.row
            for column_name, spec in [
                ("CVSRTSELL_TRDVOL", SHORT_SELLING_VOLUME),
                ("CVSRTSELL_TRDVAL", SHORT_SELLING_VALUE),
            ]:
                value = parse_decimal(status_row.get(column_name))
                if value is None:
                    continue
                records.append(
                    _build_line(
                        trade_date=trade_date,
                        ticker=normalized_ticker,
                        market=market,
                        spec=spec,
                        value=value,
                        fetched_at=fetched_at,
                        source_bld=SHORTING_STATUS_BLD,
                        request=status_item.request,
                        row=status_row,
                    )
                )

        balance_source_item = balance_item
        balance_source_bld = SHORTING_BALANCE_BLD
        balance_value = (
            parse_decimal(balance_item.row.get("BAL_QTY")) if balance_item is not None else None
        )
        if balance_value is None and status_item is not None:
            balance_value = parse_decimal(status_item.row.get("STR_CONST_VAL1"))
            balance_source_item = status_item
            balance_source_bld = SHORTING_STATUS_BLD

        if balance_value is not None and balance_source_item is not None:
            records.append(
                _build_line(
                    trade_date=trade_date,
                    ticker=normalized_ticker,
                    market=market,
                    spec=SHORT_SELLING_BALANCE_QUANTITY,
                    value=balance_value,
                    fetched_at=fetched_at,
                    source_bld=balance_source_bld,
                    request=balance_source_item.request,
                    row=balance_source_item.row,
                )
            )

    return records


def parse_shorting_trading_bulk_rows(
    rows: Iterable[KrxMdcRow],
    market: Market,
    trade_date: date,
    tickers: list[str] | None = None,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT30101 all-ticker short-selling trading rows."""
    ticker_filter = {str(ticker).zfill(6) for ticker in tickers or []}
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []

    for item in rows:
        row = item.row
        ticker = _parse_ticker(row)
        if ticker is None or (ticker_filter and ticker not in ticker_filter):
            continue
        for column_name, spec in [
            ("CVSRTSELL_TRDVOL", SHORT_SELLING_VOLUME),
            ("CVSRTSELL_TRDVAL", SHORT_SELLING_VALUE),
        ]:
            value = parse_decimal(row.get(column_name))
            if value is None:
                continue
            records.append(
                _build_line(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    spec=spec,
                    value=value,
                    fetched_at=fetched_at,
                    source_bld=SHORTING_TRADING_BULK_BLD,
                    request=item.request,
                    row=row,
                )
            )
    return records


def parse_shorting_balance_bulk_rows(
    rows: Iterable[KrxMdcRow],
    market: Market,
    trade_date: date,
    tickers: list[str] | None = None,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT30501 all-ticker short-selling balance rows."""
    ticker_filter = {str(ticker).zfill(6) for ticker in tickers or []}
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []

    for item in rows:
        row = item.row
        ticker = _parse_ticker(row)
        if ticker is None or (ticker_filter and ticker not in ticker_filter):
            continue
        value = parse_decimal(row.get("BAL_QTY"))
        if value is None:
            continue
        row_trade_date = parse_trade_date(row.get("RPT_DUTY_OCCR_DD") or trade_date)
        records.append(
            _build_line(
                trade_date=row_trade_date,
                ticker=ticker,
                market=market,
                spec=SHORT_SELLING_BALANCE_QUANTITY,
                value=value,
                fetched_at=fetched_at,
                source_bld=SHORTING_BALANCE_BULK_BLD,
                request=item.request,
                row=row,
            )
        )
    return records


def parse_decimal(value: object) -> Decimal | None:
    """Parse a KRX numeric string into Decimal."""
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "-", "--", "N/A", "nan", "None"}:
        return None
    text = text.replace(",", "")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid KRX numeric value: {value!r}") from exc


def parse_trade_date(value: object) -> date:
    """Parse KRX date values in YYYY/MM/DD, YYYY-MM-DD, or YYYYMMDD format."""
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("Missing KRX trade date")
    if "/" in text:
        text = text.replace("/", "-")
    if "-" in text:
        return date.fromisoformat(text)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    raise ValueError(f"Invalid KRX trade date: {value!r}")


def _parse_ticker(row: dict[str, Any]) -> str | None:
    for key in ("ISU_CD", "ISU_SRT_CD", "isuCd", "isuSrtCd"):
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            if text.upper().startswith("KR") and len(text) >= 9 and text[3:9].isdigit():
                return text[3:9]
            return text.zfill(6)
    return None


def _rows_by_ticker(rows: Iterable[KrxMdcRow]) -> dict[str, KrxMdcRow]:
    rows_by_ticker: dict[str, KrxMdcRow] = {}
    for item in rows:
        ticker = _parse_ticker(item.row)
        if ticker is not None:
            rows_by_ticker[ticker] = item
    return rows_by_ticker


def _net_bid_volume(item: KrxMdcRow | None) -> Decimal:
    if item is None:
        return Decimal("0")
    return parse_decimal(item.row.get("NETBID_TRDVOL")) or Decimal("0")


def _investor_bulk_payload(
    *,
    base_item: KrxMdcRow,
    component_items: dict[str, KrxMdcRow | None],
) -> dict[str, Any]:
    return {
        "base_row": dict(base_item.row),
        "components": {
            name: dict(item.row) if item is not None else None
            for name, item in component_items.items()
        },
        "filled_missing_components_as_zero": [
            name for name, item in component_items.items() if item is None
        ],
    }


def _build_line(
    *,
    trade_date: date,
    ticker: str,
    market: Market,
    spec: FlowMetricSpec,
    value: Decimal,
    fetched_at,
    source_bld: str,
    request: dict[str, Any],
    row: dict[str, Any],
) -> SecurityFlowLine:
    return SecurityFlowLine(
        trade_date=trade_date,
        ticker=ticker,
        market=market,
        metric_code=spec.metric_code,
        metric_name=spec.metric_name,
        value=value,
        unit=spec.unit,
        source=Source.KRX,
        fetched_at=fetched_at,
        raw_payload={
            "source_bld": source_bld,
            "request": dict(request),
            "row": dict(row),
        },
    )

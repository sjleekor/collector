"""Parsers for KIS domestic-stock flow responses.

Field names are from live responses on 2026-08-16, not from documentation —
the KIS reference renders its field tables client-side.  The fixtures in
``tests/unit/test_flows_kis.py`` are trimmed copies of those same responses, so
a silent upstream rename fails a test instead of quietly writing NULLs.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from collector.kr.adapters.flows_common import (
    FOREIGN_HOLDING_SHARES,
    FOREIGN_NET_BUY_VOLUME,
    INDIVIDUAL_NET_BUY_VOLUME,
    INSTITUTION_NET_BUY_VOLUME,
    SHORT_SELLING_VALUE,
    SHORT_SELLING_VOLUME,
    FlowMetricSpec,
)
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import SecurityFlowLine
from collector.kr.util.time import now_kst

INQUIRE_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
INQUIRE_PRICE_TR_ID = "FHKST01010100"

INVESTOR_DAILY_PATH = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
INVESTOR_DAILY_TR_ID = "FHPTJ04160001"
# One call returns at most 30 sessions, ending at FID_INPUT_DATE_1.
INVESTOR_DAILY_PAGE_ROWS = 30

DAILY_SHORT_SALE_PATH = "/uapi/domestic-stock/v1/quotations/daily-short-sale"
DAILY_SHORT_SALE_TR_ID = "FHPST04830000"
# One call returns at most 100 sessions inside [FID_INPUT_DATE_1, _DATE_2].
DAILY_SHORT_SALE_PAGE_ROWS = 100

TRADE_DATE_FIELD = "stck_bsop_date"

# ``frgn_ntby_qty`` is registered + unregistered foreign combined, which is the
# same definition as the KRX path's 외국인(9000) + 기타외국인(9001) sum.  The
# metric keeps its meaning across the source change; only provenance differs.
INVESTOR_NET_VOLUME_FIELDS: dict[str, FlowMetricSpec] = {
    "prsn_ntby_qty": INDIVIDUAL_NET_BUY_VOLUME,
    "frgn_ntby_qty": FOREIGN_NET_BUY_VOLUME,
    "orgn_ntby_qty": INSTITUTION_NET_BUY_VOLUME,
}

SHORT_SELLING_FIELDS: dict[str, FlowMetricSpec] = {
    "ssts_cntg_qty": SHORT_SELLING_VOLUME,
    "ssts_tr_pbmn": SHORT_SELLING_VALUE,
}

FOREIGN_HOLDING_FIELD = "frgn_hldn_qty"


class KisFieldError(ValueError):
    """Raised when a response is missing every field we came for.

    A rename upstream would otherwise look like a run that legitimately found
    nothing, and forward-fill collectors are exactly where that goes unnoticed.
    """


def parse_investor_net_volume_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    ticker: str,
    market: Market,
    start: date,
    end: date,
    request: Mapping[str, Any],
) -> list[SecurityFlowLine]:
    """Parse ``investor-trade-by-stock-daily`` output2 rows."""
    return _parse_dated_rows(
        rows,
        ticker=ticker,
        market=market,
        start=start,
        end=end,
        request=request,
        field_specs=INVESTOR_NET_VOLUME_FIELDS,
        tr_id=INVESTOR_DAILY_TR_ID,
    )


def parse_short_selling_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    ticker: str,
    market: Market,
    start: date,
    end: date,
    request: Mapping[str, Any],
) -> list[SecurityFlowLine]:
    """Parse ``daily-short-sale`` output2 rows."""
    return _parse_dated_rows(
        rows,
        ticker=ticker,
        market=market,
        start=start,
        end=end,
        request=request,
        field_specs=SHORT_SELLING_FIELDS,
        tr_id=DAILY_SHORT_SALE_TR_ID,
    )


def parse_foreign_holding_row(
    row: Mapping[str, Any],
    *,
    ticker: str,
    market: Market,
    trade_date: date,
    request: Mapping[str, Any],
) -> list[SecurityFlowLine]:
    """Parse ``inquire-price`` output into a foreign holding row.

    The response carries no business date — it is the live figure — so
    ``trade_date`` comes from the caller, which is why the service refuses to
    run this group for anything but the newest session.
    """
    if FOREIGN_HOLDING_FIELD not in row:
        raise KisFieldError(
            f"KIS {INQUIRE_PRICE_TR_ID} response has no {FOREIGN_HOLDING_FIELD} field "
            f"(fields: {sorted(row)[:12]}...)"
        )
    value = parse_decimal(row.get(FOREIGN_HOLDING_FIELD))
    if value is None:
        return []
    return [
        _build_line(
            trade_date=trade_date,
            ticker=str(ticker).zfill(6),
            market=market,
            spec=FOREIGN_HOLDING_SHARES,
            value=value,
            fetched_at=now_kst(),
            tr_id=INQUIRE_PRICE_TR_ID,
            request=request,
            row=row,
        )
    ]


def _parse_dated_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    ticker: str,
    market: Market,
    start: date,
    end: date,
    request: Mapping[str, Any],
    field_specs: Mapping[str, FlowMetricSpec],
    tr_id: str,
) -> list[SecurityFlowLine]:
    normalized_ticker = str(ticker).zfill(6)
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []
    row_count = 0
    matched_any_field = False

    for row in rows:
        row_count += 1
        # Field presence is checked on EVERY row, before the window filter.
        #
        # This endpoint answers with the ticker's most recent TRADED sessions,
        # so a ticker that did not trade during the requested window returns
        # thirty perfectly good rows, all older than it. Measured 2026-08-18:
        # 000300, 000880, 00088K, 001470 and 001570 all ended at 08-14, and
        # daily_ohlcv shows volume zero with a frozen price since 08-10 —
        # trading halts.
        #
        # Counting matches only inside the window turned an ordinary halt into
        # "the response shape changed", which then tripped the
        # consecutive-failure guard and stopped the whole run on its first five
        # tickers. The check exists to catch a rename, and a rename is visible
        # in any row.
        if any(field_name in row for field_name in field_specs):
            matched_any_field = True

        trade_date = parse_trade_date(row.get(TRADE_DATE_FIELD))
        if trade_date is None:
            continue
        # KIS pages backwards from an end date and ignores our lower bound, so
        # the caller's window is enforced here rather than trusted upstream.
        if trade_date < start or trade_date > end:
            continue
        for field_name, spec in field_specs.items():
            value = parse_decimal(row.get(field_name))
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
                    tr_id=tr_id,
                    request=request,
                    row=row,
                )
            )

    if row_count and not matched_any_field:
        raise KisFieldError(
            f"KIS {tr_id} returned {row_count} rows but none carried any of "
            f"{sorted(field_specs)} — the response shape changed."
        )
    return records


def parse_trade_date(value: object) -> date | None:
    """Parse a ``YYYYMMDD`` business date, tolerating blanks."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def parse_decimal(value: object) -> Decimal | None:
    """Parse a KIS numeric string, treating blanks and placeholders as absent."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float):
        return Decimal(str(value))
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "N/A", "nan", "None"}:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _build_line(
    *,
    trade_date: date,
    ticker: str,
    market: Market,
    spec: FlowMetricSpec,
    value: Decimal,
    fetched_at: datetime,
    tr_id: str,
    request: Mapping[str, Any],
    row: Mapping[str, Any],
) -> SecurityFlowLine:
    return SecurityFlowLine(
        trade_date=trade_date,
        ticker=ticker,
        market=market,
        metric_code=spec.metric_code,
        metric_name=spec.metric_name,
        value=value,
        unit=spec.unit,
        source=Source.KIS,
        fetched_at=fetched_at,
        raw_payload={
            "source_tr_id": tr_id,
            "request": dict(request),
            "row": dict(row),
        },
    )

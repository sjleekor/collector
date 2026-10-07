"""KIS ``inquire-price`` status flags (관리종목·거래정지·시장경보).

The endpoint is the same one ``flows_kis`` reads foreign holding from, but this
adapter keeps the status fields.  Field names come from the 2026-09-27 probe on
sj2-server (``my/milestones/kr/modeling/plan/20260927_october_plan/03_probe_20260927.md``
§6): ``mang_issu_cls_code`` Y/N was confirmed on a positive case (KR모터스) and a
negative one (삼성전자); ``temp_stop_yn``·``invt_caful_yn``·``short_over_yn``·
``sltr_yn`` exist but were never seen positive; ``mrkt_warn_cls_code`` was ``00``
in both.  ``iscd_stat_cls_code`` is optional and stored when present.

The three flags the plan names are *required*: a response without one of them is
a field rename or a truncated payload, and it must fail that ticker loudly
instead of filing a NULL that reads as "not flagged".
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date
from typing import Any

from collector.kr.adapters.flows_kis.parsers import (
    INQUIRE_PRICE_PATH,
    INQUIRE_PRICE_TR_ID,
    KisFieldError,
)
from collector.kr.adapters.kis_common.client import KisClient
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import (
    FlowRequestStats,
    StockStatusFetchResult,
    StockStatusRecord,
)
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

REQUIRED_STATUS_FIELDS: tuple[str, ...] = (
    "mang_issu_cls_code",
    "temp_stop_yn",
    "mrkt_warn_cls_code",
)
OPTIONAL_STATUS_FIELDS: tuple[str, ...] = (
    "invt_caful_yn",
    "short_over_yn",
    "sltr_yn",
    "iscd_stat_cls_code",
)
# Kept in ``raw_payload`` next to the flags. The full ``output`` is ~70 fields
# per row per day; the status flags and the two margin fields the probe saw are
# what this table exists for.
RAW_PAYLOAD_FIELDS: tuple[str, ...] = (
    *REQUIRED_STATUS_FIELDS,
    *OPTIONAL_STATUS_FIELDS,
    "marg_rate",
    "crdt_able_yn",
)


def parse_stock_status_row(
    row: Mapping[str, Any],
    *,
    ticker: str,
    market: Market,
    snapshot_date: date,
) -> StockStatusRecord:
    """Turn one ``inquire-price`` ``output`` object into a status record."""
    missing = [name for name in REQUIRED_STATUS_FIELDS if name not in row]
    if missing:
        raise KisFieldError(
            f"KIS {INQUIRE_PRICE_TR_ID} response has no {', '.join(missing)} "
            f"(fields: {sorted(row)[:12]}...)"
        )

    def text(name: str) -> str | None:
        value = row.get(name)
        if value is None:
            return None
        return str(value).strip()

    flags = {name: text(name) for name in REQUIRED_STATUS_FIELDS}
    empty = [name for name, value in flags.items() if not value]
    if empty:
        # A blank flag is not "N". Filing it would make an unknown status read
        # as clean, so this ticker fails and is retried.
        raise KisFieldError(f"KIS {INQUIRE_PRICE_TR_ID} returned empty {', '.join(empty)}")

    return StockStatusRecord(
        snapshot_date=snapshot_date,
        ticker=str(ticker).zfill(6),
        market=market,
        mang_issu_cls_code=flags["mang_issu_cls_code"] or "",
        temp_stop_yn=flags["temp_stop_yn"] or "",
        mrkt_warn_cls_code=flags["mrkt_warn_cls_code"] or "",
        observed_at=now_kst(),
        invt_caful_yn=text("invt_caful_yn"),
        short_over_yn=text("short_over_yn"),
        sltr_yn=text("sltr_yn"),
        iscd_stat_cls_code=text("iscd_stat_cls_code"),
        source=Source.KIS,
        raw_payload={name: row[name] for name in RAW_PAYLOAD_FIELDS if name in row},
    )


class KisStockStatusProvider:
    """Fetch the status flags of one ticker per call (one HTTP request)."""

    def __init__(self, *, client: KisClient, snapshot_date: date) -> None:
        self._client = client
        self._snapshot_date = snapshot_date

    def source(self) -> Source:
        return Source.KIS

    def request_stats(self) -> FlowRequestStats:
        stats = self._client.stats
        return FlowRequestStats(
            http_requests=stats.http_requests,
            http_retries=stats.http_retries,
            pages_fetched=stats.pages_fetched,
            rate_limited_responses=stats.rate_limited_responses,
            auth_token_issued=stats.token_issued,
            auth_token_cache_hits=stats.token_cache_hits,
            throttle_waits=stats.throttle_waits,
            throttle_wait_seconds=stats.throttle_wait_seconds,
            status_counts=dict(stats.status_counts),
        )

    def fetch_stock_status(self, ticker: str, market: Market) -> StockStatusFetchResult:
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": str(ticker).zfill(6),
        }
        try:
            response = self._client.get(
                INQUIRE_PRICE_PATH,
                tr_id=INQUIRE_PRICE_TR_ID,
                params=params,
            )
            if response.no_data:
                return StockStatusFetchResult(no_data=True)
            rows = response.rows("output")
            if not rows:
                return StockStatusFetchResult(no_data=True)
            record = parse_stock_status_row(
                rows[0],
                ticker=ticker,
                market=market,
                snapshot_date=self._snapshot_date,
            )
            return StockStatusFetchResult(record=record)
        except (SourceAuthError, SourceQuotaExhaustedError):
            raise
        except Exception as exc:
            logger.warning("KIS stock status fetch failed for %s: %s", ticker, exc)
            return StockStatusFetchResult(error=str(exc))

"""Optional bounded KIS minute-index capture with a source sample time.

KIS domestic-stock-045 documents output2.stck_bsop_date and
output2.stck_cntg_hour. Each request covers exactly one index/sector code.
The returned price is the sampled minute quote, never joined to a later
current-quote response or represented as a completed minute-bar return.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from collector.kr.adapters.kis_intraday import (
    INDEXES,
    SEOUL,
    KisGetClient,
    MarketObservation,
    _numeric_text,
    _request_stat_snapshot,
    _response_hash,
    _rows,
    _source_time,
)

MINUTE_INDEX_PATH = "/uapi/domestic-stock/v1/quotations/inquire-time-indexchartprice"
MINUTE_INDEX_TR_ID = "FHKUP03500200"
MAX_LOGICAL_REQUESTS = 6  # 2 required indices and no more than 4 selected sectors.


@dataclass(frozen=True, slots=True)
class SelectedSector:
    market: str
    code: str
    name: str


def parse_minute_index_response(
    *, kind: str, market: str, code: str, name: str,
    report_date: date, body: dict[str, Any], received_at: datetime,
) -> MarketObservation:
    """Use only the latest same-day output2 row sampled by receive time."""
    if kind not in {"index", "sector"}:
        raise ValueError("minute index kind must be index or sector")
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("received_at needs a timezone")
    received = received_at.astimezone(SEOUL)
    if received.date() != report_date:
        raise ValueError("minute index response is outside report date")
    candidates: list[tuple[datetime, dict[str, Any], str]] = []
    for row in _rows(body, "output2"):
        stamp, status = _source_time(row)
        if stamp is None:
            continue
        observed = datetime.fromisoformat(stamp)
        if observed.date() != report_date or observed > received:
            continue
        price = _numeric_text(row.get("bstp_nmix_prpr"))
        if price is None:
            continue
        candidates.append((observed, row, status))
    if not candidates:
        raise ValueError("no same-day, received-by, finite KIS minute index sample")
    observed, row, status = max(candidates, key=lambda item: item[0])
    return MarketObservation(
        kind=kind, market=market, code=code, name=name,
        price=_numeric_text(row["bstp_nmix_prpr"]),
        change=None, change_percent=None,
        source_observed_at=observed.isoformat(), source_time_status=status,
        received_at=received.isoformat(), source_response_sha256=_response_hash(body),
    )


class KisTimedMarketProvider:
    """Capture two indices and up to four explicitly selected sector codes."""

    def __init__(self, *, client: KisGetClient,
                 selected_sectors: tuple[SelectedSector, ...] = (),
                 now_fn=None, monotonic_fn=None) -> None:
        if len(selected_sectors) > MAX_LOGICAL_REQUESTS - len(INDEXES):
            raise ValueError("selected sector list exceeds bounded request plan")
        codes = [sector.code for sector in selected_sectors]
        if (len(set(codes)) != len(codes) or
                any(not code.isdigit() or len(code) != 4 for code in codes) or
                any(not sector.market or not sector.name for sector in selected_sectors)):
            raise ValueError("selected sector codes must be distinct four-digit codes")
        if set(codes) & {code for _, code in INDEXES}:
            raise ValueError("selected sectors must not repeat broad indices")
        self._client = client
        self._sectors = selected_sectors
        self._now_fn = now_fn or (lambda: datetime.now(SEOUL))
        self._monotonic_fn = monotonic_fn or time.monotonic

    def fetch_snapshot(self, report_date: date) -> dict[str, Any]:
        started_at = self._now_fn()
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("clock timestamps must include a timezone")
        start_tick = self._monotonic_fn()
        stats = getattr(self._client, "stats", None)
        before = _request_stat_snapshot(stats)
        specs = [("index", market, code, market) for market, code in INDEXES]
        specs.extend(("sector", item.market, item.code, item.name) for item in self._sectors)
        observations = []
        for kind, market, code, name in specs:
            response = self._client.get(
                MINUTE_INDEX_PATH,
                tr_id=MINUTE_INDEX_TR_ID,
                params={
                    "FID_COND_MRKT_DIV_CODE": "U",
                    "FID_ETC_CLS_CODE": "0",
                    "FID_INPUT_ISCD": code,
                    "FID_INPUT_HOUR_1": "60",
                    "FID_PW_DATA_INCU_YN": "N",
                },
            )
            received_at = self._now_fn()
            observations.append(parse_minute_index_response(
                kind=kind, market=market, code=code, name=name,
                report_date=report_date, body=response.body, received_at=received_at,
            ))
        received_at = self._now_fn()
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("clock timestamps must include a timezone")
        after = _request_stat_snapshot(stats)
        return {
            "schema_version": 1,
            "market": "KR",
            "status": "source_time_available_pending_calendar",
            "source_endpoint": "domestic-stock-045",
            "bar_semantics": "intraday_sample_not_completed_minute_bar",
            "logical_request_count": len(specs),
            "request_started_at": started_at.isoformat(),
            "received_at": received_at.isoformat(),
            "elapsed_ms_including_retries_and_token_lookup": max(
                0, round((self._monotonic_fn() - start_tick) * 1000)
            ),
            "client_stat_deltas": {
                key: after.get(key, 0) - before.get(key, 0) for key in sorted(after)
            },
            "observations": [
                {
                    "kind": item.kind, "market": item.market, "code": item.code,
                    "name": item.name, "price": item.price,
                    "change": None, "change_percent": None,
                    "source_observed_at": item.source_observed_at,
                    "source_time_status": item.source_time_status,
                    "received_at": item.received_at,
                    "source_response_sha256": item.source_response_sha256,
                }
                for item in observations
            ],
            "publication": {"status": "unresolved", "evidence": []},
        }

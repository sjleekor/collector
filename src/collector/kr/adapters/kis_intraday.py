"""Label-free KIS snapshots for Korean open-session market observations.

This provider only adapts the existing KIS REST client. It does not create
tokens, manage credentials, set a cache path, or publish market data.

The KIS official sample documents ``inquire-index-price`` as
``FHPUP02100000`` and ``inquire-index-category-price`` as ``FHPUP02140000``.
The latter returns all categories for a market in output2, so one observation
slot costs four logical REST calls: two representative indices and two full
sector lists. Publication stays unresolved until the source terms are checked.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Protocol
from zoneinfo import ZoneInfo

INDEX_PATH = "/uapi/domestic-stock/v1/quotations/inquire-index-price"
INDEX_TR_ID = "FHPUP02100000"
SECTOR_PATH = "/uapi/domestic-stock/v1/quotations/inquire-index-category-price"
SECTOR_TR_ID = "FHPUP02140000"
SCREEN_CODE = "20214"
SEOUL = ZoneInfo("Asia/Seoul")

INDEXES = (("KOSPI", "0001"), ("KOSDAQ", "1001"))
SECTOR_MARKETS = (("KRX", "0001", "K"), ("KOSDAQ", "1001", "Q"))


class KisResponseLike(Protocol):
    body: dict[str, Any]


class KisGetClient(Protocol):
    def get(
        self,
        path: str,
        *,
        tr_id: str,
        params: dict[str, str],
        tr_cont: str = "",
    ) -> KisResponseLike: ...


@dataclass(frozen=True, slots=True)
class MarketObservation:
    """One internally retained observation; numeric fields are never public."""

    kind: str
    market: str
    code: str
    name: str
    price: str | None
    change: str | None
    change_percent: str | None
    source_observed_at: str | None
    source_time_status: str
    received_at: str
    source_response_sha256: str


def _rows(body: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = body.get(key)
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _numeric_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    # Keep the source representation where possible; float is only a finite
    # value check and is not used to calculate derived returns.
    return str(value).strip()


def _response_hash(body: dict[str, Any]) -> str:
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _source_time(row: dict[str, Any]) -> tuple[str | None, str]:
    day = str(row.get("stck_bsop_date") or "").strip()
    hour = str(row.get("bsop_hour") or row.get("stck_cntg_hour") or "").strip()
    if len(day) == 8 and day.isdigit() and len(hour) in (4, 6) and hour.isdigit():
        if len(hour) == 4:
            hour += "00"
        try:
            parsed = datetime.strptime(day + hour, "%Y%m%d%H%M%S").replace(tzinfo=SEOUL)
        except ValueError:
            return None, "invalid_source_time"
        return parsed.isoformat(), "source_local_time_assumed_asia_seoul"
    return None, "source_timestamp_not_returned"


def _observation(
    *, kind: str, market: str, code: str, row: dict[str, Any], received_at: datetime
) -> MarketObservation:
    source_time, source_time_status = _source_time(row)
    return MarketObservation(
        kind=kind,
        market=market,
        code=code,
        name=str(row.get("hts_kor_isnm") or market),
        price=_numeric_text(row.get("bstp_nmix_prpr")),
        change=_numeric_text(row.get("bstp_nmix_prdy_vrss")),
        change_percent=_numeric_text(row.get("bstp_nmix_prdy_ctrt")),
        source_observed_at=source_time,
        source_time_status=source_time_status,
        received_at=received_at.isoformat(),
        source_response_sha256="",
    )


def parse_index_response(
    *, market: str, code: str, body: dict[str, Any], received_at: datetime
) -> MarketObservation:
    """Parse one official index response while preserving missing source time."""
    rows = _rows(body, "output")
    if not rows:
        raise ValueError(f"KIS index response has no output for {market} ({code})")
    observation = _observation(
        kind="index", market=market, code=code, row=rows[0], received_at=received_at
    )
    return replace(observation, source_response_sha256=_response_hash(body))


def parse_sector_response(
    *, market: str, body: dict[str, Any], received_at: datetime
) -> list[MarketObservation]:
    """Parse output2 rows from one KIS all-sector response."""
    rows = _rows(body, "output2")
    if not rows:
        raise ValueError(f"KIS sector response has no output2 rows for {market}")
    response_hash = _response_hash(body)
    observations = []
    for row in rows:
        code = str(row.get("bstp_cls_code") or "").strip()
        if not code:
            raise ValueError(f"KIS sector row has no bstp_cls_code for {market}")
        observation = _observation(
            kind="sector", market=market, code=code, row=row, received_at=received_at
        )
        observations.append(replace(observation, source_response_sha256=response_hash))
    return observations


class KisIntradayMarketProvider:
    """Fetch KOSPI/KOSDAQ and all KRX/KOSDAQ sector indices in one slot."""

    def __init__(self, *, client: KisGetClient, now_fn=None, monotonic_fn=None) -> None:
        self._client = client
        self._now_fn = now_fn or (lambda: datetime.now(SEOUL))
        self._monotonic_fn = monotonic_fn or time.monotonic

    def fetch_snapshot(self) -> dict[str, Any]:
        started_at = self._now_fn()
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("clock timestamps must include a timezone")
        started_tick = self._monotonic_fn()
        stats = getattr(self._client, "stats", None)
        before = _request_stat_snapshot(stats)
        observations: list[MarketObservation] = []
        for name, code in INDEXES:
            response = self._client.get(
                INDEX_PATH,
                tr_id=INDEX_TR_ID,
                params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": code},
            )
            response_received_at = self._now_fn()
            if response_received_at.tzinfo is None or response_received_at.utcoffset() is None:
                raise ValueError("clock timestamps must include a timezone")
            observations.append(
                parse_index_response(
                    market=name, code=code, body=response.body, received_at=response_received_at
                )
            )
        for market, base_code, market_class in SECTOR_MARKETS:
            response = self._client.get(
                SECTOR_PATH,
                tr_id=SECTOR_TR_ID,
                params={
                    "FID_COND_MRKT_DIV_CODE": "U",
                    "FID_INPUT_ISCD": base_code,
                    "FID_COND_SCR_DIV_CODE": SCREEN_CODE,
                    "FID_MRKT_CLS_CODE": market_class,
                    "FID_BLNG_CLS_CODE": "0",
                },
            )
            response_received_at = self._now_fn()
            if response_received_at.tzinfo is None or response_received_at.utcoffset() is None:
                raise ValueError("clock timestamps must include a timezone")
            observations.extend(
                parse_sector_response(
                    market=market, body=response.body, received_at=response_received_at
                )
            )
        received_at = self._now_fn()
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("clock timestamps must include a timezone")
        elapsed_ms = max(0, round((self._monotonic_fn() - started_tick) * 1000))
        after = _request_stat_snapshot(stats)
        return {
            "schema_version": 1,
            "market": "KR",
            # The current-index REST response has no documented exchange
            # timestamp. A successful response alone cannot prove that this
            # is an open-session observation rather than a prior close.
            "status": "observation_time_unverified",
            "logical_request_count": 4,
            "request_started_at": started_at.isoformat(),
            "received_at": received_at.isoformat(),
            "elapsed_ms_including_retries_and_token_lookup": elapsed_ms,
            "client_stat_deltas": {
                key: after.get(key, 0) - before.get(key, 0) for key in sorted(after)
            },
            "observations": [
                {
                    "kind": item.kind,
                    "market": item.market,
                    "code": item.code,
                    "name": item.name,
                    "price": item.price,
                    "change": item.change,
                    "change_percent": item.change_percent,
                    "source_observed_at": item.source_observed_at,
                    "source_time_status": item.source_time_status,
                    "received_at": item.received_at,
                    "source_response_sha256": item.source_response_sha256,
                }
                for item in observations
            ],
            "publication": {"status": "unresolved", "evidence": []},
        }


def public_status_only(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return safe status metadata with every raw or derived value removed."""
    return {
        "schema_version": snapshot.get("schema_version"),
        "market": snapshot.get("market"),
        "received_at": snapshot.get("received_at"),
        "status": snapshot.get("status"),
        "session_status": snapshot.get("session_status", "unverified"),
        "index_count": sum(
            item.get("kind") == "index" for item in snapshot.get("observations", [])
        ),
        "sector_count": sum(
            item.get("kind") == "sector" for item in snapshot.get("observations", [])
        ),
        "publication": {"status": "unresolved", "evidence": []},
    }


def _request_stat_snapshot(stats: Any) -> dict[str, int | float]:
    if stats is None:
        return {}
    fields = (
        "http_requests",
        "http_retries",
        "pages_fetched",
        "rate_limited_responses",
        "token_issued",
        "token_cache_hits",
        "throttle_waits",
        "throttle_wait_seconds",
    )
    return {field: getattr(stats, field, 0) for field in fields}


def assess_session_status(
    snapshot: dict[str, Any],
    *,
    calendar_open: bool | None,
    session_open_at: datetime | None = None,
    session_close_at: datetime | None = None,
    slot_at: datetime | None = None,
    max_age_seconds: int | None = None,
) -> str:
    """Fail closed unless both the local calendar and source time are usable."""
    if calendar_open is False:
        return "unavailable_closed_session"
    if calendar_open is not True:
        return "unavailable_calendar_unverified"
    observations = snapshot.get("observations", [])
    if (not isinstance(observations, list) or not observations or
            any(not isinstance(row, dict) or not row.get("source_observed_at")
                for row in observations)):
        return "observation_time_unverified"
    if (
        session_open_at is None
        or session_close_at is None
        or slot_at is None
        or max_age_seconds is None
        or max_age_seconds <= 0
    ):
        return "observation_time_unverified"
    bounds = (session_open_at, session_close_at, slot_at)
    if any(value.tzinfo is None or value.utcoffset() is None for value in bounds):
        return "observation_time_unverified"
    session_open = session_open_at.astimezone(SEOUL)
    session_close = session_close_at.astimezone(SEOUL)
    slot = slot_at.astimezone(SEOUL)
    if (not session_open <= slot < session_close or
            len({session_open.date(), session_close.date(), slot.date()}) != 1):
        return "unavailable_outside_session_window"
    for observation in observations:
        try:
            source_at = datetime.fromisoformat(observation["source_observed_at"])
        except (KeyError, TypeError, ValueError):
            return "observation_time_unverified"
        if source_at.tzinfo is None or source_at.utcoffset() is None:
            return "observation_time_unverified"
        source_at = source_at.astimezone(SEOUL)
        if not session_open <= source_at <= slot:
            return "observation_time_unverified"
        if (slot - source_at).total_seconds() > max_age_seconds:
            return "stale_source_observation"
    return "open_session_observed"

"""HTTP client for FRED series observations API."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

logger = logging.getLogger(__name__)

FRED_SERIES_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


@dataclass(slots=True)
class FredSeriesObservationResult:
    """Normalized FRED ``series/observations`` response."""

    rows: list[dict[str, object]] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None


class FredSeriesObservationsClient:
    """Small client for FRED series observations JSON responses."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = FRED_SERIES_OBSERVATIONS_URL,
        timeout_seconds: float = 20.0,
        urlopen_fn: Callable[..., object] = urlopen,
    ) -> None:
        self._api_key = api_key.strip()
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._urlopen_fn = urlopen_fn

    def fetch_series_observations(
        self,
        *,
        series_id: str,
        observation_start: str,
        observation_end: str,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
        sort_order: str = "asc",
        limit: int = 100000,
        offset: int = 0,
    ) -> FredSeriesObservationResult:
        """Fetch one FRED series observations page."""
        if not self._api_key:
            return FredSeriesObservationResult(error="FRED API key is not configured.")

        try:
            url = self._build_series_observations_url(
                series_id=series_id,
                observation_start=observation_start,
                observation_end=observation_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
                sort_order=sort_order,
                limit=limit,
                offset=offset,
            )
            with self._urlopen_fn(url, timeout=self._timeout_seconds) as response:
                payload = response.read()
            return parse_series_observations_payload(payload)
        except HTTPError as exc:
            retryable = 500 <= exc.code < 600 or exc.code == 429
            return FredSeriesObservationResult(
                error=f"FRED HTTP {exc.code}: {exc.reason}",
                status_code=str(exc.code),
                retryable=retryable,
                retry_after_seconds=_retry_after_seconds(exc),
            )
        except URLError as exc:
            return FredSeriesObservationResult(
                error=f"FRED network error: {exc.reason}",
                retryable=True,
            )
        except Exception as exc:
            logger.exception("Failed to fetch FRED series observations response")
            return FredSeriesObservationResult(error=str(exc))

    def _build_series_observations_url(
        self,
        *,
        series_id: str,
        observation_start: str,
        observation_end: str,
        realtime_start: str | None,
        realtime_end: str | None,
        sort_order: str,
        limit: int,
        offset: int,
    ) -> str:
        params: dict[str, object] = {
            "series_id": series_id.strip(),
            "api_key": self._api_key,
            "file_type": "json",
            "observation_start": observation_start,
            "observation_end": observation_end,
            "sort_order": sort_order,
            "limit": limit,
            "offset": offset,
        }
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end
        return f"{self._base_url}?{urlencode(params)}"


def parse_series_observations_payload(payload: bytes) -> FredSeriesObservationResult:
    """Parse FRED observations JSON payload into a normalized result."""
    parsed = json.loads(payload.decode("utf-8"))
    if not isinstance(parsed, dict):
        return FredSeriesObservationResult(error="FRED returned an unexpected JSON payload.")

    if "error_code" in parsed or "error_message" in parsed:
        code = str(parsed.get("error_code") or "").strip()
        message = str(parsed.get("error_message") or "").strip()
        prefix = f"FRED error {code}: " if code else "FRED error: "
        return FredSeriesObservationResult(
            error=f"{prefix}{message}".strip(),
            status_code=code or None,
        )

    rows = parsed.get("observations")
    if rows is None:
        return FredSeriesObservationResult(error="FRED response missing observations block.")
    if not isinstance(rows, list):
        return FredSeriesObservationResult(error="FRED observations block is not a list.")

    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append(dict(row))

    if not normalized_rows:
        return FredSeriesObservationResult(no_data=True)
    return FredSeriesObservationResult(rows=normalized_rows)


def _retry_after_seconds(exc: HTTPError) -> float | None:
    value = exc.headers.get("Retry-After") if exc.headers else None
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds >= 0 else None

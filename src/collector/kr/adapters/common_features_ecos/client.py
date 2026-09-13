"""HTTP client for Bank of Korea ECOS StatisticSearch API."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import urlopen

logger = logging.getLogger(__name__)

ECOS_STATISTIC_SEARCH_BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
ECOS_NO_DATA_CODES = frozenset({"INFO-200"})


@dataclass(slots=True)
class EcosStatisticSearchResult:
    """Normalized ECOS StatisticSearch response."""

    rows: list[dict[str, object]] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None


class EcosStatisticSearchClient:
    """Small client for ECOS StatisticSearch JSON responses."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = ECOS_STATISTIC_SEARCH_BASE_URL,
        language: str = "kr",
        timeout_seconds: float = 20.0,
        urlopen_fn: Callable[..., object] = urlopen,
    ) -> None:
        self._api_key = api_key.strip()
        self._base_url = base_url.rstrip("/")
        self._language = language
        self._timeout_seconds = timeout_seconds
        self._urlopen_fn = urlopen_fn

    def fetch_statistic_search(
        self,
        *,
        stat_code: str,
        cycle: str,
        start_period: str,
        end_period: str,
        item_codes: Sequence[str] = (),
        start_row: int = 1,
        end_row: int = 10000,
    ) -> EcosStatisticSearchResult:
        """Fetch one ECOS StatisticSearch page."""
        if not self._api_key:
            return EcosStatisticSearchResult(error="ECOS API key is not configured.")
        try:
            url = self._build_statistic_search_url(
                stat_code=stat_code,
                cycle=cycle,
                start_period=start_period,
                end_period=end_period,
                item_codes=item_codes,
                start_row=start_row,
                end_row=end_row,
            )
            with self._urlopen_fn(url, timeout=self._timeout_seconds) as response:
                payload = response.read()
            return parse_statistic_search_payload(payload)
        except HTTPError as exc:
            retryable = 500 <= exc.code < 600 or exc.code == 429
            return EcosStatisticSearchResult(
                error=f"ECOS HTTP {exc.code}: {exc.reason}",
                status_code=str(exc.code),
                retryable=retryable,
            )
        except URLError as exc:
            return EcosStatisticSearchResult(
                error=f"ECOS network error: {exc.reason}",
                retryable=True,
            )
        except Exception as exc:
            logger.exception("Failed to fetch ECOS StatisticSearch response")
            return EcosStatisticSearchResult(error=str(exc))

    def _build_statistic_search_url(
        self,
        *,
        stat_code: str,
        cycle: str,
        start_period: str,
        end_period: str,
        item_codes: Sequence[str],
        start_row: int,
        end_row: int,
    ) -> str:
        segments = [
            self._base_url,
            _path_segment(self._api_key),
            "json",
            _path_segment(self._language),
            str(start_row),
            str(end_row),
            _path_segment(stat_code),
            _path_segment(cycle),
            _path_segment(start_period),
            _path_segment(end_period),
        ]
        segments.extend(_path_segment(item_code) for item_code in item_codes if item_code)
        return "/".join(segments)


def parse_statistic_search_payload(payload: bytes) -> EcosStatisticSearchResult:
    """Parse ECOS StatisticSearch JSON payload into a normalized result."""
    parsed = json.loads(payload.decode("utf-8"))
    if not isinstance(parsed, dict):
        return EcosStatisticSearchResult(error="ECOS returned an unexpected JSON payload.")

    result_block = parsed.get("RESULT")
    if isinstance(result_block, dict):
        code = str(result_block.get("CODE", ""))
        message = str(result_block.get("MESSAGE", ""))
        if code in ECOS_NO_DATA_CODES:
            return EcosStatisticSearchResult(
                no_data=True,
                status_code=code,
            )
        return EcosStatisticSearchResult(
            error=f"ECOS error {code}: {message}".strip(),
            status_code=code,
        )

    statistic_search = parsed.get("StatisticSearch")
    if not isinstance(statistic_search, dict):
        return EcosStatisticSearchResult(error="ECOS response missing StatisticSearch block.")

    rows = statistic_search.get("row", [])
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        return EcosStatisticSearchResult(error="ECOS StatisticSearch row block is not a list.")

    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append(dict(row))

    if not normalized_rows:
        return EcosStatisticSearchResult(no_data=True)
    return EcosStatisticSearchResult(rows=normalized_rows)


def _path_segment(value: object) -> str:
    return quote(str(value).strip(), safe="")

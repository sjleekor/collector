"""SEIBro HTTP 클라이언트.

응답은 바이트 그대로 돌려줍니다(원문 보존). 요청 사이 간격은 0.6초로, 조사 때
``pull.py``가 쓴 값입니다. 계수기는 실제 HTTP 횟수입니다.
"""

from __future__ import annotations

import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

import requests

from collector.kr.adapters.seibro_distribution.request import (
    COUNT_ACTION,
    ENDPOINT_URL,
    LIST_ACTION,
    REFERER_URL,
)

USER_AGENT = "Mozilla/5.0 (research; personal)"
RETRYABLE_STATUS_CODES = frozenset({500, 502, 503, 504})
_ACTION_RE = re.compile(r'action="([^"]+)"')


class SeibroMalformedResponseError(RuntimeError):
    """응답이 기대한 XML 모양이 아닐 때."""


class SeibroRequestError(RuntimeError):
    """재시도를 다 쓰고도 요청이 실패했을 때."""


@dataclass(slots=True)
class SeibroCounters:
    """한 번 실행의 HTTP 계수기."""

    http_requests: int = 0
    http_retries: int = 0
    http_errors: int = 0
    throttle_waits: int = 0
    throttle_wait_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {k: v for k, v in asdict(self).items() if v}


@dataclass(frozen=True, slots=True)
class SeibroRawResponse:
    """응답 원문 한 건과 그것을 만든 요청의 기록."""

    body: bytes
    fetched_at: datetime
    status_code: int
    action: str
    range_start: date | None = None
    range_end: date | None = None
    page: int | None = None


def check_envelope(body: bytes) -> None:
    """바이트가 XML이고 뿌리가 있는지 봅니다. 아니면 예외."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise SeibroMalformedResponseError(f"response is not XML: {exc}") from exc
    if root.tag in ("html", "HTML") or root.tag.endswith("}html"):
        raise SeibroMalformedResponseError("response is an HTML page, not XML data")


class SeibroClient:
    """요청 간격·재시도·계수기를 가진 SEIBro POST 클라이언트."""

    def __init__(
        self,
        session: requests.Session | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        min_interval_seconds: float = 0.6,
        timeout_seconds: float = 30.0,
        max_attempts: int = 3,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session = session or requests.Session()
        self._sleep_fn = sleep_fn
        self._min_interval = max(0.0, min_interval_seconds)
        self._timeout = timeout_seconds
        self._max_attempts = max(1, max_attempts)
        self._monotonic = monotonic_fn
        self._last_request_at: float | None = None
        self.counters = SeibroCounters()

    def _pace(self) -> None:
        if self._last_request_at is not None and self._min_interval > 0:
            wait = self._min_interval - (self._monotonic() - self._last_request_at)
            if wait > 0:
                self.counters.throttle_waits += 1
                self.counters.throttle_wait_seconds = round(
                    self.counters.throttle_wait_seconds + wait, 3
                )
                self._sleep_fn(wait)
        self._last_request_at = self._monotonic()

    def post(
        self,
        xml: str,
        *,
        range_start: date | None = None,
        range_end: date | None = None,
        page: int | None = None,
    ) -> SeibroRawResponse:
        """요청 XML을 보내고 원문 응답을 돌려줍니다.

        Raises:
            SeibroRequestError: 재시도를 다 썼는데도 HTTP 200이 아닐 때.
            SeibroMalformedResponseError: 200인데 XML 데이터가 아닐 때.
        """
        match = _ACTION_RE.search(xml)
        action = match.group(1) if match else ""
        if action not in (LIST_ACTION, COUNT_ACTION):
            raise ValueError(f"unknown SEIBro action in request: {action!r}")
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/xml; charset=UTF-8",
            "Referer": REFERER_URL,
        }
        last_error = ""
        for attempt in range(1, self._max_attempts + 1):
            self._pace()
            self.counters.http_requests += 1
            if attempt > 1:
                self.counters.http_retries += 1
            try:
                response = self._session.post(
                    ENDPOINT_URL,
                    data=xml.encode("utf-8"),
                    headers=headers,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                self.counters.http_errors += 1
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self._max_attempts:
                    self._sleep_fn(min(2.0 * attempt, 10.0))
                    continue
                break
            if response.status_code == 200:
                body = bytes(response.content)
                check_envelope(body)
                return SeibroRawResponse(
                    body=body,
                    fetched_at=datetime.now(UTC),
                    status_code=response.status_code,
                    action=action,
                    range_start=range_start,
                    range_end=range_end,
                    page=page,
                )
            self.counters.http_errors += 1
            last_error = f"HTTP {response.status_code}"
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self._max_attempts:
                self._sleep_fn(min(2.0 * attempt, 10.0))
                continue
            break
        raise SeibroRequestError(f"SEIBro request failed ({action}): {last_error}")

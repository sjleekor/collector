"""DIS HTTP 클라이언트 — 응답은 바이트 그대로 돌려준다(원문 보존).

**끊긴 응답이 핵심이다.** 연구용 사본을 받을 때 Python ``urllib``는 응답이 중간에 끊겨
(``IncompleteRead``, 본문 52~54바이트) 같은 날짜에서 세 번 실패했고, 첫 판은 그것을 빈 달로
보고 앞날짜로 넘어가 15개 달이 빠졌다. curl은 온전히 받았다. 그래서 여기서는

1. 200이어도 본문이 ``</root>``로 끝나지 않으면 **끊긴 응답**으로 보고,
2. **같은 요청을 다시** 보내며(앞날짜로 넘어가지 않는다), 횟수는 ``max_attempts``로 묶는다.
3. 끝내 못 받으면 ``DisRequestError``를 던진다 — 호출자가 그것을 빈 날로 세면 안 된다.

``Accept-Encoding: gzip``은 ``requests``가 기본으로 붙이고(curl ``--compressed``와 같다),
``urllib``의 끊김이 압축 없는 응답에서 났을 가능성을 줄이려는 것이다(추정). 실제 서버에서
처음 돌릴 때 끊김 횟수(``counters.truncated``)를 본다.

요청 사이 간격은 3초다 — 연구용 사본 409건을 받을 때 쓴 값이다.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import requests

from collector.kr.adapters.kofia_dis_fee.request import ENDPOINT_URL, REFERER_URL, USER_AGENT

COMPLETE_SUFFIX = b"</root>"
RETRYABLE_STATUS_CODES = frozenset({500, 502, 503, 504})
DEFAULT_MIN_INTERVAL = 3.0
DEFAULT_RETRY_SLEEP = 10.0


class DisRequestError(RuntimeError):
    """재시도를 다 쓰고도 온전한 응답을 못 받았을 때."""


@dataclass(slots=True)
class DisCounters:
    """한 번 실행의 HTTP 계수기."""

    http_requests: int = 0
    http_retries: int = 0
    http_errors: int = 0
    truncated: int = 0
    throttle_waits: int = 0
    throttle_wait_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {k: v for k, v in asdict(self).items() if v}


@dataclass(frozen=True, slots=True)
class DisRawResponse:
    body: bytes
    fetched_at: datetime
    status_code: int


def is_complete(body: bytes) -> bool:
    """본문이 ``</root>``로 끝나는가(공백 무시)."""
    return body.rstrip().endswith(COMPLETE_SUFFIX)


class DisClient:
    """요청 간격·같은 요청 재시도·계수기를 가진 DIS POST 클라이언트."""

    def __init__(
        self,
        session: requests.Session | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        min_interval_seconds: float = DEFAULT_MIN_INTERVAL,
        retry_sleep_seconds: float = DEFAULT_RETRY_SLEEP,
        timeout_seconds: float = 120.0,
        max_attempts: int = 3,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session = session or requests.Session()
        self._sleep = sleep_fn
        self._min_interval = max(0.0, min_interval_seconds)
        self._retry_sleep = max(0.0, retry_sleep_seconds)
        self._timeout = timeout_seconds
        self._max_attempts = max(1, max_attempts)
        self._monotonic = monotonic_fn
        self._last_request_at: float | None = None
        self.counters = DisCounters()

    def _pace(self) -> None:
        if self._last_request_at is not None and self._min_interval > 0:
            wait = self._min_interval - (self._monotonic() - self._last_request_at)
            if wait > 0:
                self.counters.throttle_waits += 1
                self.counters.throttle_wait_seconds = round(
                    self.counters.throttle_wait_seconds + wait, 3
                )
                self._sleep(wait)
        self._last_request_at = self._monotonic()

    def post(self, xml: str) -> DisRawResponse:
        """요청 XML을 보내고 온전한 원문 응답을 돌려준다.

        Raises:
            DisRequestError: ``max_attempts``번 모두 실패(연결 오류·5xx·끊긴 응답)했거나
                재시도할 수 없는 상태 코드를 받았을 때.
        """
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "text/xml",
            "Referer": REFERER_URL,
        }
        last = ""
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
                body = bytes(response.content)
                status = response.status_code
            except requests.RequestException as exc:
                self.counters.http_errors += 1
                last = f"{type(exc).__name__}: {str(exc)[:120]}"
            else:
                if status == 200 and is_complete(body):
                    return DisRawResponse(body, datetime.now(UTC), status)
                if status == 200:
                    self.counters.truncated += 1
                    last = f"truncated response ({len(body)} bytes, no </root>)"
                else:
                    self.counters.http_errors += 1
                    last = f"HTTP {status}"
                    if status not in RETRYABLE_STATUS_CODES:
                        break
            if attempt < self._max_attempts:
                self._sleep(self._retry_sleep)
        raise DisRequestError(f"DIS request failed after retries: {last}")

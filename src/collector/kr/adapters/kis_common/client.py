"""Shared KIS Developers REST client.

Two things here are not incidental.

**The counters are real HTTP counts.** ``krx_security_flow_raw``'s existing
audit records ``requests_attempted``, which is a count of *logical* work items
— one investor "request" is four KRX endpoints, and none of the retries or
logins appear anywhere.  That number could neither explain the 2026-08 block
nor audit a published quota.  Every actual call, retry and page is counted
here, at the only layer that knows about them.

**Auth and quota failures stop the run, they are not per-item errors.** A
collector that treats them as item errors walks its whole target list against
a server already saying no.  For KIS the recovery attempt also costs a token
issuance, which notifies the account holder.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import requests

from collector.kr.adapters.kis_common.token import KisTokenProvider
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError
from collector.kr.util.rate_limit import TokenBucket

logger = logging.getLogger(__name__)

# ``rt_cd`` "0" is success; anything else carries a ``msg_cd`` explaining why.
KIS_SUCCESS_CODE = "0"

# 초당 거래건수 초과.  Retryable: the bucket simply got ahead of the server.
KIS_RATE_LIMIT_MSG_CODES = frozenset({"EGW00201"})

# Token/app-key rejections.  Not retryable without a new token.
KIS_AUTH_MSG_CODES = frozenset({"EGW00121", "EGW00123", "EGW00133"})

# "조회할 자료가 없습니다" and friends — a real answer, not a failure.
KIS_NO_DATA_MSG_FRAGMENTS: tuple[str, ...] = (
    "조회할 자료가 없습니다",
    "데이터가 없습니다",
    "자료가 없습니다",
)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


@dataclass(slots=True)
class KisResponse:
    """One decoded KIS response plus the continuation header."""

    body: dict[str, Any]
    rt_cd: str
    msg_cd: str
    msg1: str
    tr_cont: str = ""

    @property
    def ok(self) -> bool:
        return self.rt_cd == KIS_SUCCESS_CODE

    @property
    def no_data(self) -> bool:
        message = self.msg1 or ""
        return any(fragment in message for fragment in KIS_NO_DATA_MSG_FRAGMENTS)

    def rows(self, key: str) -> list[dict[str, Any]]:
        value = self.body.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            return [value]
        return []


@dataclass(slots=True)
class KisRequestStats:
    """Actual HTTP work done, as opposed to logical work items."""

    http_requests: int = 0
    http_retries: int = 0
    pages_fetched: int = 0
    rate_limited_responses: int = 0
    token_issued: int = 0
    token_cache_hits: int = 0
    throttle_waits: int = 0
    throttle_wait_seconds: float = 0.0
    status_counts: dict[str, int] = field(default_factory=dict)

    def as_counts(self) -> dict[str, int]:
        """Flatten into ``ingestion_runs.counts``-shaped integers."""
        counts = {
            "http_requests": self.http_requests,
            "http_retries": self.http_retries,
            "http_pages_fetched": self.pages_fetched,
            "http_rate_limited": self.rate_limited_responses,
            "token_issued": self.token_issued,
            "token_cache_hits": self.token_cache_hits,
            "throttle_waits": self.throttle_waits,
            "throttle_wait_seconds": int(self.throttle_wait_seconds),
        }
        for status, count in sorted(self.status_counts.items()):
            counts[f"http_status_{status}"] = count
        return counts


class KisResponseError(RuntimeError):
    """Raised when KIS returns a malformed or unusable response."""


class KisClient:
    """GET client for KIS domestic-stock quotation endpoints."""

    def __init__(
        self,
        *,
        token_provider: KisTokenProvider,
        app_key: str,
        app_secret: str,
        base_url: str,
        bucket: TokenBucket,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
        max_attempts: int = 3,
        base_backoff_seconds: float = 1.0,
        max_consecutive_rate_limits: int = 10,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._token_provider = token_provider
        self._app_key = app_key.strip()
        self._app_secret = app_secret.strip()
        self._base_url = base_url.rstrip("/")
        self._bucket = bucket
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max(1, max_attempts)
        self._base_backoff_seconds = base_backoff_seconds
        self._max_consecutive_rate_limits = max_consecutive_rate_limits
        self._sleep_fn = sleep_fn
        self._consecutive_rate_limits = 0
        self._token_refreshed = False
        self.stats = KisRequestStats()

    def get(
        self,
        path: str,
        *,
        tr_id: str,
        params: dict[str, str],
        tr_cont: str = "",
    ) -> KisResponse:
        """Fetch one KIS page, retrying transient HTTP and rate-limit answers.

        Raises:
            SourceAuthError: credentials or token rejected and unrecoverable.
            SourceQuotaExhaustedError: rate-limited past the point of pacing.
            KisResponseError: transport or decoding failure after retries.
        """
        last_error: str = ""
        for attempt in range(1, self._max_attempts + 1):
            if attempt > 1:
                self.stats.http_retries += 1
            response = self._request_once(path, tr_id=tr_id, params=params, tr_cont=tr_cont)
            if isinstance(response, KisResponse):
                self._consecutive_rate_limits = 0
                self.stats.pages_fetched += 1
                return response

            last_error = response.detail
            if not response.retryable or attempt == self._max_attempts:
                break
            delay = self._base_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "KIS request %s (%s) failed on attempt %d/%d; retrying in %.1fs: %s",
                path,
                tr_id,
                attempt,
                self._max_attempts,
                delay,
                response.detail,
            )
            self._sleep_fn(delay)

        raise KisResponseError(f"KIS request {path} ({tr_id}) failed: {last_error}")

    def _request_once(
        self,
        path: str,
        *,
        tr_id: str,
        params: dict[str, str],
        tr_cont: str,
    ) -> KisResponse | _TransientFailure:
        self._bucket.acquire()
        self.stats.throttle_waits = self._bucket.waits
        self.stats.throttle_wait_seconds = self._bucket.total_wait_seconds

        token = self._token_provider.token()
        self.stats.http_requests += 1
        try:
            http_response = self._session.get(
                f"{self._base_url}{path}",
                params=params,
                headers=self._headers(token, tr_id=tr_id, tr_cont=tr_cont),
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as exc:
            self._record_status("transport_error")
            return _TransientFailure(detail=str(exc), retryable=True)

        self._record_status(str(http_response.status_code))
        self._sync_token_stats()

        # The body is read *before* the status code is judged. KIS reports
        # "초당 거래건수를 초과하였습니다" (EGW00201) as **HTTP 500**, not 429 —
        # confirmed live on 2026-08-16. Branching on the status alone files
        # every throttle rejection as a generic server error, which leaves the
        # rate-limit counter at zero and the quota breaker unable to ever fire.
        payload = _decode_json_object(http_response)
        message_code = str(payload.get("msg_cd", "")) if payload is not None else ""
        detail_text = http_response.text[:200]

        if message_code in KIS_RATE_LIMIT_MSG_CODES or http_response.status_code == 429:
            return self._handle_rate_limit(
                f"HTTP {http_response.status_code} {message_code}: {detail_text}"
            )
        if message_code in KIS_AUTH_MSG_CODES or http_response.status_code in (401, 403):
            return self._handle_auth_rejection(
                f"HTTP {http_response.status_code} {message_code}: {detail_text}"
            )
        if http_response.status_code in RETRYABLE_STATUS_CODES:
            return _TransientFailure(
                detail=f"HTTP {http_response.status_code}: {detail_text}",
                retryable=True,
            )
        if http_response.status_code != 200:
            return _TransientFailure(
                detail=f"HTTP {http_response.status_code}: {detail_text}",
                retryable=False,
            )
        if payload is None:
            return _TransientFailure(detail="response body was not a JSON object", retryable=True)

        response = KisResponse(
            body=payload,
            rt_cd=str(payload.get("rt_cd", "")),
            msg_cd=message_code,
            msg1=str(payload.get("msg1", "")).strip(),
            tr_cont=str(http_response.headers.get("tr_cont", "")).strip(),
        )
        if response.ok or response.no_data:
            return response
        return _TransientFailure(
            detail=f"rt_cd={response.rt_cd} msg_cd={response.msg_cd} msg1={response.msg1}",
            retryable=False,
        )

    def _handle_rate_limit(self, detail: str) -> _TransientFailure:
        self.stats.rate_limited_responses += 1
        self._consecutive_rate_limits += 1
        if self._consecutive_rate_limits >= self._max_consecutive_rate_limits:
            raise SourceQuotaExhaustedError(
                f"KIS rate-limited {self._consecutive_rate_limits} times in a row ({detail}). "
                "Stopping; lower KIS_REQUESTS_PER_SECOND or re-run later."
            )
        return _TransientFailure(detail=detail, retryable=True)

    def _handle_auth_rejection(self, detail: str) -> _TransientFailure:
        """Refresh the token exactly once, then give up on the whole run.

        Reissuing notifies the account holder, so one attempt is the budget.
        """
        if self._token_refreshed:
            raise SourceAuthError(f"KIS rejected our credentials after a token refresh: {detail}")
        logger.warning("KIS rejected the cached token (%s); refreshing once", detail)
        self._token_refreshed = True
        self._token_provider.token(force_refresh=True)
        self._sync_token_stats()
        return _TransientFailure(detail=detail, retryable=True)

    def _headers(self, token: str, *, tr_id: str, tr_cont: str) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        if tr_cont:
            headers["tr_cont"] = tr_cont
        return headers

    def _record_status(self, status: str) -> None:
        self.stats.status_counts[status] = self.stats.status_counts.get(status, 0) + 1

    def _sync_token_stats(self) -> None:
        self.stats.token_issued = self._token_provider.issued_count
        self.stats.token_cache_hits = self._token_provider.cache_hit_count


@dataclass(frozen=True, slots=True)
class _TransientFailure:
    """Internal marker for a failed attempt inside :meth:`KisClient.get`."""

    detail: str
    retryable: bool


def _decode_json_object(http_response) -> dict[str, Any] | None:
    """Return the JSON body if it is an object, else ``None``.

    Error bodies matter as much as successful ones here, so decoding never
    depends on the status code.
    """
    try:
        payload = http_response.json()
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None

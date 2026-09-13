"""Shared KRX Open API REST client.

Four decisions here are worth stating, because each of them was wrong at some
point in the path this adapter replaces.

**A 401 is two different failures and the body says which.**  Measured on
2026-08-18: a missing or bad key returns ``{"respMsg": "Unauthorized Key"}``,
while a good key hitting an endpoint that has no 이용 신청 approval returns
``{"respMsg": "Unauthorized API Call"}``.  Collapsing both into "auth failed"
would send someone hunting for a bad key when the real fix is a portal
application, so the two are separated and named.

**Keys rotate, but exhaustion is not assumed to be per-key.**  KRX publishes
10,000 requests/day and does *not* say whether that budget belongs to the key
or to the account.  Rotation is therefore a best effort, and the run still has
to end cleanly when every key reports exhaustion — which is the same shape as
OpenDART's exit 75.

**Counters are real HTTP counts.**  The scraping path recorded logical work
items, which could neither audit a published quota nor explain the block when
it came.  Requests, retries and rotations are counted at the only layer that
sees them.

**Pacing is a token bucket, not the KRX scraping throttle.**  ``HumanThrottle``
exists to look human to a site that forbids automation.  This is a published
quota on an endpoint we are authorised to call, so the correct behaviour is to
run up to the line and never past it.  Ten consecutive unpaced requests drew
no rejection on 2026-08-18, so the default is deliberately loose.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import Any

import requests

from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError
from collector.kr.util.rate_limit import TokenBucket

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://data-dbg.krx.co.kr/svc/apis"

#: Array key every documented endpoint wraps its rows in.
RESPONSE_ROWS_KEY = "OutBlock_1"

#: A good key on an endpoint that has not been approved yet.  Nothing about
#: retrying or rotating helps: the fix is a 이용 신청 on the portal.
UNAPPROVED_ENDPOINT_MESSAGE = "Unauthorized API Call"

#: The key itself is missing, malformed or revoked.
UNAUTHORIZED_KEY_MESSAGE = "Unauthorized Key"

#: Message fragments that mean the daily quota is spent.  KRX does not publish
#: the exact wording, so this is matched loosely and an unrecognised 429 is
#: treated as exhaustion too.
QUOTA_MESSAGE_FRAGMENTS: tuple[str, ...] = (
    "Limit Exceeded",
    "Quota",
    "일일 이용량",
    "호출 한도",
)

RETRYABLE_STATUS_CODES = frozenset({500, 502, 503, 504})


class KrxOpenApiEndpointNotApprovedError(RuntimeError):
    """Raised when the key is valid but the endpoint has no 이용 신청 approval.

    Deliberately not a :class:`SourceAuthError`: the credentials are fine, and
    telling an operator to check the key would send them the wrong way.
    """


@dataclass(slots=True)
class KrxOpenApiCounters:
    """Real HTTP counters for one run, recorded into ``ingestion_runs.counts``."""

    http_requests: int = 0
    http_retries: int = 0
    http_errors: int = 0
    http_rate_limited: int = 0
    key_rotations: int = 0
    throttle_waits: int = 0
    throttle_wait_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        """Return the counters keyed for ``ingestion_runs.counts``."""
        return {key: value for key, value in asdict(self).items() if value}


class KrxOpenApiClient:
    """Issues authenticated KRX Open API requests with key rotation.

    Args:
        auth_keys: One or more ``AUTH_KEY`` values.  Rotation moves to the next
            key when the current one reports its quota spent.
        base_url: Service base, without a trailing slash.
        timeout_seconds: Per-request HTTP timeout.
        requests_per_second: Token-bucket rate.  ``0`` disables pacing.
        max_burst_requests: Token-bucket burst size.
        max_attempts: Attempts per request, including the first.
        session: Injected for tests.
        sleep_fn: Injected for tests.
    """

    def __init__(
        self,
        auth_keys: Sequence[str],
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 20.0,
        requests_per_second: float = 2.0,
        max_burst_requests: int = 1,
        max_attempts: int = 3,
        session: requests.Session | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        keys = [key.strip() for key in auth_keys if key and key.strip()]
        if not keys:
            raise ValueError(
                "KRX Open API requires at least one AUTH_KEYS entry; " "set AUTH_KEYS in .env"
            )
        self._keys = keys
        self._key_index = 0
        self._exhausted_keys: set[int] = set()
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max(1, max_attempts)
        self._session = session or requests.Session()
        self._sleep_fn = sleep_fn
        self._bucket = TokenBucket(
            requests_per_second,
            burst=max_burst_requests,
            sleep_fn=sleep_fn,
        )
        self.counters = KrxOpenApiCounters()

    @property
    def key_count(self) -> int:
        """Number of configured keys."""
        return len(self._keys)

    def fetch_rows(self, group: str, endpoint: str, params: dict[str, str]) -> list[dict[str, Any]]:
        """Return ``OutBlock_1`` for one endpoint call.

        An empty list is a real answer, not a failure — a non-trading day
        returns ``rows=0`` rather than the zero-filled rows pykrx produces.

        Args:
            group: Service group, e.g. ``sto`` or ``idx``.
            endpoint: Endpoint name, e.g. ``stk_bydd_trd``.
            params: Query parameters, e.g. ``{"basDd": "20260814"}``.

        Returns:
            The decoded row list, possibly empty.

        Raises:
            SourceAuthError: Every key was rejected.
            SourceQuotaExhaustedError: Every key reported its quota spent.
            KrxOpenApiEndpointNotApprovedError: The endpoint needs 이용 신청.
            RuntimeError: The request failed for any other reason.
        """
        url = f"{self._base_url}/{group}/{endpoint}"
        last_error: str = ""

        for attempt in range(1, self._max_attempts + 1):
            self._bucket.acquire()
            self.counters.http_requests += 1
            if attempt > 1:
                self.counters.http_retries += 1

            try:
                response = self._session.get(
                    url,
                    headers={
                        "AUTH_KEY": self._keys[self._key_index],
                        "Content-Type": "application/json",
                    },
                    params=params,
                    timeout=self._timeout_seconds,
                )
            except requests.RequestException as exc:
                self.counters.http_errors += 1
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self._max_attempts:
                    self._sleep_fn(min(2.0 * attempt, 10.0))
                    continue
                break

            outcome = self._classify(response)
            if outcome == "ok":
                self._record_throttle_waits()
                body = response.json()
                rows = body.get(RESPONSE_ROWS_KEY)
                return list(rows) if isinstance(rows, list) else []

            if outcome == "quota":
                self.counters.http_rate_limited += 1
                if self._rotate_key():
                    continue
                self._record_throttle_waits()
                raise SourceQuotaExhaustedError(
                    f"KRX Open API quota exhausted on all {len(self._keys)} key(s) "
                    f"({group}/{endpoint})"
                )

            self.counters.http_errors += 1
            if outcome == "unapproved":
                self._record_throttle_waits()
                raise KrxOpenApiEndpointNotApprovedError(
                    f"{group}/{endpoint} is not approved for this key. "
                    "Apply for it (이용 신청) at https://openapi.krx.co.kr — "
                    "a valid key alone is not enough."
                )
            if outcome == "auth":
                self._record_throttle_waits()
                raise SourceAuthError(
                    f"KRX Open API rejected the key for {group}/{endpoint}: "
                    f"{self._message(response)}"
                )

            last_error = f"HTTP {response.status_code}: {self._message(response)}"
            if outcome == "retryable" and attempt < self._max_attempts:
                self._sleep_fn(min(2.0 * attempt, 10.0))
                continue
            break

        self._record_throttle_waits()
        raise RuntimeError(f"KRX Open API request failed ({group}/{endpoint}): {last_error}")

    def _classify(self, response: requests.Response) -> str:
        """Map a response to ``ok``/``quota``/``auth``/``unapproved``/... ."""
        message = self._message(response)

        if response.status_code == 200:
            # A 200 can still carry an error envelope: the service answers
            # respCode/respMsg with HTTP 200 for some conditions.
            if self._is_quota(message):
                return "quota"
            return "ok"

        if response.status_code == 401:
            if UNAPPROVED_ENDPOINT_MESSAGE in message:
                return "unapproved"
            return "auth"

        if response.status_code == 429 or self._is_quota(message):
            return "quota"

        if response.status_code in RETRYABLE_STATUS_CODES:
            return "retryable"

        return "error"

    @staticmethod
    def _is_quota(message: str) -> bool:
        return any(fragment in message for fragment in QUOTA_MESSAGE_FRAGMENTS)

    @staticmethod
    def _message(response: requests.Response) -> str:
        """Best-effort ``respMsg`` extraction; falls back to the raw body."""
        try:
            body = response.json()
        except ValueError:
            return (response.text or "")[:200]
        if isinstance(body, dict):
            return str(body.get("respMsg") or body.get("message") or body)[:200]
        return str(body)[:200]

    def _rotate_key(self) -> bool:
        """Move to the next unexhausted key.  ``False`` when none remain."""
        self._exhausted_keys.add(self._key_index)
        for offset in range(1, len(self._keys)):
            candidate = (self._key_index + offset) % len(self._keys)
            if candidate not in self._exhausted_keys:
                self._key_index = candidate
                self.counters.key_rotations += 1
                logger.warning(
                    "KRX Open API key #%d reported its quota spent; rotating to #%d",
                    len(self._exhausted_keys),
                    candidate + 1,
                )
                return True
        return False

    def _record_throttle_waits(self) -> None:
        self.counters.throttle_waits = self._bucket.waits
        self.counters.throttle_wait_seconds = round(self._bucket.total_wait_seconds, 3)

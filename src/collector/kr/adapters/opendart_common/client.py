"""Shared OpenDART request execution and key-rotation logic."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from xml.etree import ElementTree as ET

from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_OK_STATUS = "000"
OPENDART_RATE_LIMIT_STATUS = "020"
OPENDART_KEY_DISABLED_STATUSES = frozenset({"010", "011", "012", "901"})
OPENDART_REQUEST_INVALID_STATUSES = frozenset({"021", "100", "101"})
OPENDART_TRANSIENT_STATUSES = frozenset({"800", "900"})


@dataclass(slots=True)
class OpenDartKeyState:
    """Mutable per-key state used by ``OpenDartRequestExecutor``."""

    alias: str
    api_key: str
    cooldown_until_monotonic: float | None = None
    disabled_reason: str | None = None
    consecutive_failures: int = 0
    last_used_at: datetime | None = None


@dataclass(slots=True)
class OpenDartCallResult:
    """Normalized outcome for one OpenDART HTTP call."""

    payload: bytes | None = None
    parsed_payload: object | None = None
    key_alias: str | None = None
    status_code: str | None = None
    error: str | None = None
    no_data: bool = False
    retryable: bool = False
    retry_after_seconds: float | None = None
    switch_key: bool = False
    disable_key: bool = False
    exhaustion_reason: str | None = None


def format_opendart_status_error(status_code: str, message: str) -> str:
    """Build a compact OpenDART error string."""
    return f"OpenDART error {status_code}: {message}".strip()


def decode_json_payload(payload_bytes: bytes) -> dict[str, object]:
    """Decode a JSON object payload or raise a descriptive error."""
    payload = json.loads(payload_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("OpenDART returned an unexpected JSON payload.")
    return payload


def extract_xml_status(payload_bytes: bytes) -> tuple[str, str] | None:
    """Best-effort extraction of ``status`` / ``message`` from an XML payload."""
    try:
        root = ET.fromstring(payload_bytes.decode("utf-8", errors="ignore"))
    except ET.ParseError:
        return None
    status = (root.findtext(".//status") or "").strip()
    message = (root.findtext(".//message") or "").strip()
    if not status and not message:
        return None
    return status, message


def classify_status(
    *,
    status_code: str,
    message: str,
    no_data_statuses: Collection[str],
    request_invalid_statuses: Collection[str] = OPENDART_REQUEST_INVALID_STATUSES,
    payload: bytes | None = None,
    parsed_payload: object | None = None,
) -> OpenDartCallResult:
    """Classify one OpenDART status code into executor actions."""
    if status_code == OPENDART_OK_STATUS:
        return OpenDartCallResult(
            payload=payload,
            parsed_payload=parsed_payload,
            status_code=status_code,
        )

    if status_code in no_data_statuses:
        return OpenDartCallResult(
            payload=payload,
            parsed_payload=parsed_payload,
            status_code=status_code,
            no_data=True,
        )

    error = format_opendart_status_error(status_code, message)
    if status_code == OPENDART_RATE_LIMIT_STATUS:
        return OpenDartCallResult(
            status_code=status_code,
            error=error,
            retryable=True,
            switch_key=True,
        )

    if status_code in OPENDART_KEY_DISABLED_STATUSES:
        return OpenDartCallResult(
            status_code=status_code,
            error=error,
            switch_key=True,
            disable_key=True,
        )

    if status_code in request_invalid_statuses:
        return OpenDartCallResult(
            status_code=status_code,
            error=error,
            exhaustion_reason="request_invalid",
        )

    if status_code in OPENDART_TRANSIENT_STATUSES:
        return OpenDartCallResult(
            status_code=status_code,
            error=error,
            retryable=True,
            switch_key=True,
        )

    return OpenDartCallResult(
        status_code=status_code,
        error=error,
    )


class OpenDartRequestExecutor:
    """Thread-unsafe OpenDART key pool and request executor."""

    def __init__(
        self,
        api_keys: Sequence[str],
        *,
        cooldown_seconds: float = 60.0,
        transient_cooldown_seconds: float = 5.0,
        rotation_delay_seconds: float = 0.15,
        transient_failure_threshold: int = 3,
        monotonic_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
        urlopen_fn: Callable[..., object] = urlopen,
    ) -> None:
        ordered_keys: list[str] = []
        seen_keys: set[str] = set()
        for key in api_keys:
            normalized = key.strip()
            if normalized and normalized not in seen_keys:
                ordered_keys.append(normalized)
                seen_keys.add(normalized)

        if not ordered_keys:
            raise RuntimeError(
                "At least one OpenDART API key must be configured via "
                "OPENDART_API_KEY or OPENDART_API_KEYS."
            )

        self._key_states = [
            OpenDartKeyState(alias=f"key#{index + 1}", api_key=api_key)
            for index, api_key in enumerate(ordered_keys)
        ]
        self._cooldown_seconds = cooldown_seconds
        self._transient_cooldown_seconds = transient_cooldown_seconds
        self._rotation_delay_seconds = rotation_delay_seconds
        self._transient_failure_threshold = transient_failure_threshold
        self._monotonic_fn = monotonic_fn
        self._sleep_fn = sleep_fn
        self._urlopen_fn = urlopen_fn
        self._round_robin_index = 0

        self._key_rotation_count = 0
        self._key_disable_count = 0
        self._rate_limit_count = 0
        self._successful_key_aliases: set[str] = set()
        self._request_invalid_count = 0
        self._all_rate_limited_count = 0
        self._all_disabled_count = 0
        self._retryable_error_count = 0
        self._terminal_error_count = 0
        self._status_bucket_counts: dict[str, int] = {}

    @property
    def configured_key_count(self) -> int:
        """Return the number of configured keys."""
        return len(self._key_states)

    def snapshot_metrics(self) -> dict[str, int]:
        """Return cumulative executor metrics for the current command run."""
        metrics: dict[str, int] = {
            "key_rotation_count": self._key_rotation_count,
            "key_disable_count": self._key_disable_count,
            "rate_limit_count": self._rate_limit_count,
            "key_effective_use_count": len(self._successful_key_aliases),
            "request_invalid_count": self._request_invalid_count,
            "all_rate_limited_count": self._all_rate_limited_count,
            "all_disabled_count": self._all_disabled_count,
            "retryable_error_count": self._retryable_error_count,
            "terminal_error_count": self._terminal_error_count,
        }
        for status_code, count in self._status_bucket_counts.items():
            metrics[f"status_{status_code}_count"] = count
        return metrics

    def fetch_bytes(
        self,
        *,
        endpoint_url: str,
        params: Mapping[str, str],
        request_label: str,
        parser: Callable[[bytes], OpenDartCallResult],
        timeout_seconds: float = 30.0,
    ) -> OpenDartCallResult:
        """Execute one OpenDART request using the shared key pool."""
        attempted_indices: set[int] = set()
        last_result = OpenDartCallResult(error="OpenDART request did not start.")

        while True:
            key_index = self._select_ready_key_index(excluded_indices=attempted_indices)
            if key_index is None:
                exhausted = self._build_exhausted_result(last_result)
                self._record_outcome(exhausted)
                return exhausted

            attempted_indices.add(key_index)
            if len(attempted_indices) > 1:
                self._key_rotation_count += 1

            key_state = self._key_states[key_index]
            last_result = self._perform_request(
                key_state=key_state,
                endpoint_url=endpoint_url,
                params=params,
                request_label=request_label,
                parser=parser,
                timeout_seconds=timeout_seconds,
            )
            last_result.key_alias = key_state.alias

            if last_result.error is None:
                key_state.consecutive_failures = 0
                key_state.cooldown_until_monotonic = None
                key_state.last_used_at = now_kst()
                if not last_result.no_data:
                    self._successful_key_aliases.add(key_state.alias)
                self._record_outcome(last_result)
                return last_result

            if last_result.disable_key:
                if key_state.disabled_reason is None:
                    self._key_disable_count += 1
                key_state.disabled_reason = last_result.error
                logger.warning(
                    "OpenDART key disabled: request=%s key=%s status=%s reason=%s",
                    request_label,
                    key_state.alias,
                    last_result.status_code,
                    last_result.error,
                )
            elif last_result.status_code == OPENDART_RATE_LIMIT_STATUS:
                self._mark_rate_limited(key_state)
                logger.warning(
                    "OpenDART key rate-limited: request=%s key=%s status=%s",
                    request_label,
                    key_state.alias,
                    last_result.status_code,
                )
            elif last_result.retryable:
                self._mark_transient_failure(key_state)
                logger.warning(
                    "OpenDART transient failure: request=%s key=%s status=%s error=%s",
                    request_label,
                    key_state.alias,
                    last_result.status_code,
                    last_result.error,
                )

            if not last_result.switch_key:
                self._record_outcome(last_result)
                return last_result

            if self._rotation_delay_seconds > 0:
                self._sleep_fn(self._rotation_delay_seconds)

    def _build_exhausted_result(self, last_result: OpenDartCallResult) -> OpenDartCallResult:
        if all(state.disabled_reason is not None for state in self._key_states):
            return OpenDartCallResult(
                error="All OpenDART API keys are disabled.",
                retryable=False,
                exhaustion_reason="all_disabled",
            )

        now_monotonic = self._monotonic_fn()
        remaining_cooldowns = [
            max(0.0, state.cooldown_until_monotonic - now_monotonic)
            for state in self._key_states
            if state.disabled_reason is None and state.cooldown_until_monotonic is not None
        ]
        if all(
            state.disabled_reason is not None
            or (
                state.cooldown_until_monotonic is not None
                and state.cooldown_until_monotonic > now_monotonic
            )
            for state in self._key_states
        ):
            return OpenDartCallResult(
                error="All OpenDART API keys are temporarily rate limited.",
                retryable=True,
                retry_after_seconds=min(remaining_cooldowns) if remaining_cooldowns else None,
                exhaustion_reason="all_rate_limited",
            )

        return last_result

    def _record_outcome(self, result: OpenDartCallResult) -> None:
        """Update aggregate outcome-bucket counters for the run-level snapshot.

        Called once per returned ``fetch_bytes`` result so each request
        contributes to exactly one terminal bucket.
        """
        if result.status_code:
            self._status_bucket_counts[result.status_code] = (
                self._status_bucket_counts.get(result.status_code, 0) + 1
            )

        reason = result.exhaustion_reason
        if reason == "request_invalid":
            self._request_invalid_count += 1
            self._terminal_error_count += 1
            return
        if reason == "all_rate_limited":
            self._all_rate_limited_count += 1
            self._retryable_error_count += 1
            return
        if reason == "all_disabled":
            self._all_disabled_count += 1
            self._terminal_error_count += 1
            return

        if result.error is None:
            return
        if result.retryable:
            self._retryable_error_count += 1
        else:
            self._terminal_error_count += 1

    def _mark_rate_limited(self, key_state: OpenDartKeyState) -> None:
        self._rate_limit_count += 1
        key_state.consecutive_failures = 0
        key_state.cooldown_until_monotonic = self._monotonic_fn() + self._cooldown_seconds

    def _mark_transient_failure(self, key_state: OpenDartKeyState) -> None:
        key_state.consecutive_failures += 1
        if key_state.consecutive_failures >= self._transient_failure_threshold:
            key_state.cooldown_until_monotonic = (
                self._monotonic_fn() + self._transient_cooldown_seconds
            )

    def _select_ready_key_index(self, *, excluded_indices: set[int]) -> int | None:
        key_count = len(self._key_states)
        now_monotonic = self._monotonic_fn()

        for offset in range(key_count):
            index = (self._round_robin_index + offset) % key_count
            if index in excluded_indices:
                continue

            key_state = self._key_states[index]
            if key_state.disabled_reason is not None:
                continue
            if (
                key_state.cooldown_until_monotonic is not None
                and key_state.cooldown_until_monotonic > now_monotonic
            ):
                continue

            self._round_robin_index = (index + 1) % key_count
            return index

        return None

    def _perform_request(
        self,
        *,
        key_state: OpenDartKeyState,
        endpoint_url: str,
        params: Mapping[str, str],
        request_label: str,
        parser: Callable[[bytes], OpenDartCallResult],
        timeout_seconds: float,
    ) -> OpenDartCallResult:
        query = urlencode({"crtfc_key": key_state.api_key, **dict(params)})
        url = f"{endpoint_url}?{query}"
        logger.info(
            "OpenDART request: request=%s key=%s endpoint=%s",
            request_label,
            key_state.alias,
            endpoint_url.rsplit("/", 1)[-1],
        )

        try:
            with self._urlopen_fn(url, timeout=timeout_seconds) as response:
                payload = response.read()
            return parser(payload)
        except HTTPError as exc:
            payload = exc.read()
            if exc.code == 429:
                return OpenDartCallResult(
                    status_code=OPENDART_RATE_LIMIT_STATUS,
                    error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
                    retryable=True,
                    switch_key=True,
                )
            if 500 <= exc.code <= 599:
                return OpenDartCallResult(
                    status_code="800",
                    error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
                    retryable=True,
                    switch_key=True,
                )
            if payload:
                parsed_result = parser(payload)
                if (
                    parsed_result.error is not None
                    or parsed_result.no_data
                    or parsed_result.payload
                ):
                    return parsed_result
            return OpenDartCallResult(
                status_code=str(exc.code),
                error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
            )
        except URLError as exc:
            return OpenDartCallResult(
                status_code="900",
                error=f"OpenDART network error: {exc.reason}",
                retryable=True,
                switch_key=True,
            )
        except Exception as exc:
            return OpenDartCallResult(
                status_code="900",
                error=str(exc),
                retryable=True,
                switch_key=True,
            )

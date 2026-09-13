"""In-memory fake OpenDART executor for provider tests."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from collector.kr.adapters.opendart_common.client import OpenDartCallResult


@dataclass(slots=True)
class FakeOpenDartCall:
    endpoint_url: str
    params: dict[str, str]
    request_label: str
    timeout_seconds: float


class FakeOpenDartExecutor:
    """Queue-driven fake matching ``OpenDartRequestExecutor.fetch_bytes``."""

    def __init__(self, responses: list[bytes | OpenDartCallResult]) -> None:
        self._responses = deque(responses)
        self.calls: list[FakeOpenDartCall] = []
        self.configured_key_count = 2

    def snapshot_metrics(self) -> dict[str, int]:
        return {
            "key_rotation_count": 0,
            "key_disable_count": 0,
            "rate_limit_count": 0,
            "key_effective_use_count": 0,
        }

    def fetch_bytes(
        self,
        *,
        endpoint_url: str,
        params: Mapping[str, str],
        request_label: str,
        parser: Any,
        timeout_seconds: float = 30.0,
    ) -> OpenDartCallResult:
        self.calls.append(
            FakeOpenDartCall(
                endpoint_url=endpoint_url,
                params=dict(params),
                request_label=request_label,
                timeout_seconds=timeout_seconds,
            )
        )
        queued = self._responses.popleft()
        if isinstance(queued, OpenDartCallResult):
            return queued
        return parser(queued)

from __future__ import annotations

import io
from urllib.error import HTTPError, URLError

from collector.kr.adapters.opendart_common.client import (
    OPENDART_REQUEST_INVALID_STATUSES,
    OpenDartCallResult,
    OpenDartRequestExecutor,
    classify_status,
)


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _make_urlopen(queue: list[bytes | BaseException], urls: list[str]):
    def _fake_urlopen(url: str, timeout: float = 30.0) -> _FakeResponse:
        del timeout
        urls.append(url)
        item = queue.pop(0)
        if isinstance(item, BaseException):
            raise item
        return _FakeResponse(item)

    return _fake_urlopen


def _status_parser(payload: bytes) -> OpenDartCallResult:
    status = payload.decode("utf-8")
    return classify_status(
        status_code=status,
        message=f"status={status}",
        no_data_statuses={"013"},
        request_invalid_statuses=OPENDART_REQUEST_INVALID_STATUSES | {"014"},
        payload=payload,
    )


def test_request_executor_rotates_on_rate_limit() -> None:
    urls: list[str] = []
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([b"020", b"000"], urls),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={"corp_code": "00126380"},
        request_label="financial:005930",
        parser=_status_parser,
    )

    assert result.error is None
    assert result.key_alias == "key#2"
    assert "crtfc_key=key-a" in urls[0]
    assert "crtfc_key=key-b" in urls[1]
    metrics = executor.snapshot_metrics()
    assert metrics["key_rotation_count"] == 1
    assert metrics["key_disable_count"] == 0
    assert metrics["rate_limit_count"] == 1
    assert metrics["key_effective_use_count"] == 1
    assert metrics["retryable_error_count"] == 0
    assert metrics["terminal_error_count"] == 0
    assert metrics["status_000_count"] == 1


def test_request_executor_disables_invalid_key_and_uses_next_key() -> None:
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([b"010", b"000"], []),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="share-info:005930",
        parser=_status_parser,
    )

    assert result.error is None
    assert result.key_alias == "key#2"
    assert executor.snapshot_metrics()["key_disable_count"] == 1


def test_request_executor_returns_all_rate_limited_when_every_key_cools_down() -> None:
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([b"020", b"020"], []),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="xbrl:005930",
        parser=_status_parser,
    )

    assert result.exhaustion_reason == "all_rate_limited"
    assert result.retryable is True
    assert result.retry_after_seconds is not None
    assert 59.0 <= result.retry_after_seconds <= 60.0
    assert "rate limited" in (result.error or "").lower()


def test_request_executor_returns_request_invalid_without_rotation() -> None:
    urls: list[str] = []
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([b"100"], urls),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="financial:bad-request",
        parser=_status_parser,
    )

    assert result.exhaustion_reason == "request_invalid"
    assert result.retryable is False
    assert len(urls) == 1


def test_request_executor_maps_http_429_to_rate_limit() -> None:
    urls: list[str] = []
    http_429 = HTTPError(
        url="https://example.com/test.json",
        code=429,
        msg="Too Many Requests",
        hdrs=None,
        fp=io.BytesIO(b""),
    )
    executor = OpenDartRequestExecutor(
        ["key-a"],
        urlopen_fn=_make_urlopen([http_429], urls),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="financial:429",
        parser=_status_parser,
    )

    assert result.exhaustion_reason == "all_rate_limited"
    assert result.retryable is True


def test_request_executor_maps_http_5xx_to_status_800() -> None:
    http_503 = HTTPError(
        url="https://example.com/test.json",
        code=503,
        msg="Service Unavailable",
        hdrs=None,
        fp=io.BytesIO(b""),
    )
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([http_503, b"000"], []),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="financial:5xx",
        parser=_status_parser,
    )

    assert result.error is None
    assert result.key_alias == "key#2"
    metrics = executor.snapshot_metrics()
    assert metrics["key_rotation_count"] == 1
    assert metrics["status_000_count"] == 1


def test_request_executor_maps_urlerror_to_status_900() -> None:
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([URLError("network down"), b"000"], []),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="financial:urlerror",
        parser=_status_parser,
    )

    assert result.error is None
    assert result.key_alias == "key#2"


def test_request_executor_returns_all_disabled_when_every_key_disabled() -> None:
    executor = OpenDartRequestExecutor(
        ["key-a", "key-b"],
        urlopen_fn=_make_urlopen([b"010", b"011"], []),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="xbrl:disabled",
        parser=_status_parser,
    )

    assert result.exhaustion_reason == "all_disabled"
    assert result.retryable is False
    metrics = executor.snapshot_metrics()
    assert metrics["all_disabled_count"] == 1
    assert metrics["terminal_error_count"] == 1


def test_request_executor_records_request_invalid_bucket() -> None:
    executor = OpenDartRequestExecutor(
        ["key-a"],
        urlopen_fn=_make_urlopen([b"100"], []),
        sleep_fn=lambda _: None,
    )

    result = executor.fetch_bytes(
        endpoint_url="https://example.com/test.json",
        params={},
        request_label="financial:invalid",
        parser=_status_parser,
    )

    assert result.exhaustion_reason == "request_invalid"
    metrics = executor.snapshot_metrics()
    assert metrics["request_invalid_count"] == 1
    assert metrics["terminal_error_count"] == 1
    assert metrics["status_100_count"] == 1

"""``KrxOpenApiClient.fetch_raw`` — 원문 바이트와 envelope 검증 (R4 리뷰 R01)."""

from __future__ import annotations

import json
from datetime import UTC

import pytest

from collector.kr.adapters.market_data_krx_openapi.client import (
    KrxOpenApiClient,
    KrxOpenApiMalformedResponseError,
)
from collector.kr.util.pipeline import SourceQuotaExhaustedError


class _Resp:
    def __init__(self, status: int, payload: object, raw: bytes | None = None) -> None:
        self.status_code = status
        self._payload = payload
        self.content = raw if raw is not None else json.dumps(payload).encode("utf-8")
        self.text = self.content.decode("utf-8", "replace")

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise ValueError("not json")
        return self._payload


class _Session:
    def __init__(self, responses: list[_Resp]) -> None:
        self._responses = list(responses)
        self.keys: list[str] = []

    def get(self, url, headers=None, params=None, timeout=None):  # noqa: ANN001
        self.keys.append((headers or {}).get("AUTH_KEY", ""))
        return self._responses.pop(0)


def _client(responses: list[_Resp], keys: tuple[str, ...] = ("secret-a",), attempts: int = 3):
    return KrxOpenApiClient(
        keys,
        session=_Session(responses),
        requests_per_second=0,
        sleep_fn=lambda _s: None,
        max_attempts=attempts,
    )


def test_an_empty_list_is_a_normal_empty_answer() -> None:
    result = _client([_Resp(200, {"OutBlock_1": []})]).fetch_raw(
        "etp", "etf", {"basDd": "20261009"}
    )
    assert result.rows == []
    assert result.status_code == 200
    assert result.fetched_at.tzinfo is UTC
    assert result.params == {"basDd": "20261009"}


@pytest.mark.parametrize(
    "payload",
    [
        {"respCode": "ERROR", "respMsg": "Unexpected provider error"},
        {"OutBlock_1": {"unexpected": True}},
        {"OutBlock_1": [1, 2]},
        ["not", "an", "object"],
    ],
)
def test_a_malformed_200_is_a_failure_after_retries(payload: object) -> None:
    client = _client([_Resp(200, payload) for _ in range(3)])
    with pytest.raises(KrxOpenApiMalformedResponseError):
        client.fetch_raw("etp", "etf", {"basDd": "20261009"})
    assert client.counters.http_requests == 3  # max_attempts 안에서 다시 시도했다
    assert client.counters.http_retries == 2


def test_a_non_json_body_is_malformed() -> None:
    client = _client([_Resp(200, ValueError("x"), raw=b"<html>oops</html>")], attempts=1)
    with pytest.raises(KrxOpenApiMalformedResponseError):
        client.fetch_raw("etp", "etf", {})


def test_a_retry_can_recover_from_a_malformed_response() -> None:
    client = _client([_Resp(200, {"nope": 1}), _Resp(200, {"OutBlock_1": [{"A": "1"}]})])
    assert client.fetch_raw("etp", "etf", {}).rows == [{"A": "1"}]


def test_the_body_bytes_are_returned_exactly_as_received() -> None:
    # 공백·키 순서까지 그대로여야 한다. 다시 직렬화하면 이 바이트가 안 나온다.
    raw = b'{ "OutBlock_1" : [ {"B":"2",  "A":"1"} ] }\n'
    result = _client([_Resp(200, json.loads(raw), raw=raw)]).fetch_raw("etp", "etf", {})
    assert result.body == raw


def test_key_slot_follows_rotation_and_never_holds_the_key() -> None:
    client = _client(
        [
            _Resp(200, {"OutBlock_1": [{"A": "1"}]}),
            _Resp(429, {"respMsg": "Limit Exceeded"}),
            _Resp(200, {"OutBlock_1": []}),
        ],
        keys=("secret-a", "secret-b"),
    )
    first = client.fetch_raw("etp", "etf", {})
    second = client.fetch_raw("etp", "etf", {})
    assert (first.key_slot, second.key_slot) == (1, 2)
    assert client.counters.key_rotations == 1
    assert "secret" not in repr(second)


def test_quota_exhaustion_is_the_same_as_fetch_rows() -> None:
    client = _client([_Resp(429, {"respMsg": "Limit Exceeded"})])
    with pytest.raises(SourceQuotaExhaustedError):
        client.fetch_raw("etp", "etf", {})

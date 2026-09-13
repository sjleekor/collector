from __future__ import annotations

import io
import json
from urllib.error import HTTPError, URLError

from collector.kr.adapters.common_features_ecos.client import EcosStatisticSearchClient


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _payload(value: dict[str, object]) -> bytes:
    return json.dumps(value).encode("utf-8")


def test_ecos_statistic_search_client_builds_url_and_parses_rows() -> None:
    urls: list[str] = []

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        urls.append(url)
        assert timeout == 7.0
        return _FakeResponse(
            _payload(
                {
                    "StatisticSearch": {
                        "list_total_count": 1,
                        "row": [
                            {
                                "STAT_CODE": "722Y001",
                                "TIME": "20240102",
                                "DATA_VALUE": "3.50",
                            }
                        ],
                    }
                }
            )
        )

    client = EcosStatisticSearchClient(
        api_key="demo-key",
        timeout_seconds=7.0,
        urlopen_fn=fake_urlopen,
    )

    result = client.fetch_statistic_search(
        stat_code="722Y001",
        cycle="D",
        start_period="20240102",
        end_period="20240102",
        item_codes=["0101000"],
    )

    assert result.error is None
    assert result.no_data is False
    assert result.rows == [{"STAT_CODE": "722Y001", "TIME": "20240102", "DATA_VALUE": "3.50"}]
    assert urls == [
        (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "demo-key/json/kr/1/10000/722Y001/D/20240102/20240102/0101000"
        )
    ]


def test_ecos_statistic_search_client_reports_no_data_result() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        return _FakeResponse(_payload({"RESULT": {"CODE": "INFO-200", "MESSAGE": "no data"}}))

    result = EcosStatisticSearchClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_statistic_search(
        stat_code="722Y001",
        cycle="D",
        start_period="20240102",
        end_period="20240102",
    )

    assert result.error is None
    assert result.no_data is True
    assert result.status_code == "INFO-200"
    assert result.rows == []


def test_ecos_statistic_search_client_reports_ecos_error_result() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        return _FakeResponse(
            _payload({"RESULT": {"CODE": "ERROR-300", "MESSAGE": "invalid request"}})
        )

    result = EcosStatisticSearchClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_statistic_search(
        stat_code="bad",
        cycle="D",
        start_period="20240102",
        end_period="20240102",
    )

    assert result.no_data is False
    assert result.status_code == "ERROR-300"
    assert result.error == "ECOS error ERROR-300: invalid request"


def test_ecos_statistic_search_client_requires_api_key() -> None:
    called = False

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        nonlocal called
        called = True
        del url, timeout
        return _FakeResponse(b"{}")

    result = EcosStatisticSearchClient(api_key="", urlopen_fn=fake_urlopen).fetch_statistic_search(
        stat_code="722Y001",
        cycle="D",
        start_period="20240102",
        end_period="20240102",
    )

    assert result.error == "ECOS API key is not configured."
    assert called is False


def test_ecos_statistic_search_client_marks_http_5xx_retryable() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        raise HTTPError(
            url=url,
            code=503,
            msg="Service Unavailable",
            hdrs=None,
            fp=io.BytesIO(b""),
        )

    result = EcosStatisticSearchClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_statistic_search(
        stat_code="722Y001",
        cycle="D",
        start_period="20240102",
        end_period="20240102",
    )

    assert result.error == "ECOS HTTP 503: Service Unavailable"
    assert result.retryable is True


def test_ecos_statistic_search_client_marks_urlerror_retryable() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        raise URLError("network down")

    result = EcosStatisticSearchClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_statistic_search(
        stat_code="722Y001",
        cycle="D",
        start_period="20240102",
        end_period="20240102",
    )

    assert "network down" in (result.error or "")
    assert result.retryable is True

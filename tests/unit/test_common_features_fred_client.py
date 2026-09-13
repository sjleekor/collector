from __future__ import annotations

import io
import json
from email.message import Message
from urllib.error import HTTPError, URLError

from collector.kr.adapters.common_features_fred.client import FredSeriesObservationsClient


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


def test_fred_series_observations_client_builds_url_and_parses_rows() -> None:
    urls: list[str] = []

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        urls.append(url)
        assert timeout == 7.0
        return _FakeResponse(
            _payload(
                {
                    "observations": [
                        {
                            "realtime_start": "2026-06-09",
                            "realtime_end": "2026-06-09",
                            "date": "2026-06-08",
                            "value": "4.50",
                        }
                    ]
                }
            )
        )

    client = FredSeriesObservationsClient(
        api_key="demo-key",
        timeout_seconds=7.0,
        urlopen_fn=fake_urlopen,
    )

    result = client.fetch_series_observations(
        series_id="DGS10",
        observation_start="2026-06-01",
        observation_end="2026-06-08",
    )

    assert result.error is None
    assert result.no_data is False
    assert result.rows == [
        {
            "realtime_start": "2026-06-09",
            "realtime_end": "2026-06-09",
            "date": "2026-06-08",
            "value": "4.50",
        }
    ]
    assert urls == [
        (
            "https://api.stlouisfed.org/fred/series/observations?"
            "series_id=DGS10&api_key=demo-key&file_type=json&"
            "observation_start=2026-06-01&observation_end=2026-06-08&"
            "sort_order=asc&limit=100000&offset=0"
        )
    ]


def test_fred_series_observations_client_reports_no_data() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        return _FakeResponse(_payload({"observations": []}))

    result = FredSeriesObservationsClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_series_observations(
        series_id="DGS10",
        observation_start="2026-06-01",
        observation_end="2026-06-08",
    )

    assert result.error is None
    assert result.no_data is True
    assert result.rows == []


def test_fred_series_observations_client_reports_fred_error_result() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        return _FakeResponse(
            _payload({"error_code": 400, "error_message": "Bad Request. Invalid series_id."})
        )

    result = FredSeriesObservationsClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_series_observations(
        series_id="bad",
        observation_start="2026-06-01",
        observation_end="2026-06-08",
    )

    assert result.no_data is False
    assert result.status_code == "400"
    assert result.error == "FRED error 400: Bad Request. Invalid series_id."


def test_fred_series_observations_client_requires_api_key() -> None:
    called = False

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        nonlocal called
        called = True
        del url, timeout
        return _FakeResponse(b"{}")

    result = FredSeriesObservationsClient(
        api_key="",
        urlopen_fn=fake_urlopen,
    ).fetch_series_observations(
        series_id="DGS10",
        observation_start="2026-06-01",
        observation_end="2026-06-08",
    )

    assert result.error == "FRED API key is not configured."
    assert called is False


def test_fred_series_observations_client_marks_http_429_retryable() -> None:
    headers = Message()
    headers["Retry-After"] = "12"

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        raise HTTPError(
            url=url,
            code=429,
            msg="Too Many Requests",
            hdrs=headers,
            fp=io.BytesIO(b""),
        )

    result = FredSeriesObservationsClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_series_observations(
        series_id="DGS10",
        observation_start="2026-06-01",
        observation_end="2026-06-08",
    )

    assert result.error == "FRED HTTP 429: Too Many Requests"
    assert result.retryable is True
    assert result.retry_after_seconds == 12.0


def test_fred_series_observations_client_marks_urlerror_retryable() -> None:
    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        raise URLError("network down")

    result = FredSeriesObservationsClient(
        api_key="demo-key",
        urlopen_fn=fake_urlopen,
    ).fetch_series_observations(
        series_id="DGS10",
        observation_start="2026-06-01",
        observation_end="2026-06-08",
    )

    assert "network down" in (result.error or "")
    assert result.retryable is True

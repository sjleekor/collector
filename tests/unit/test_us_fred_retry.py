"""FRED 5xx 재시도 (2026-10-06). 네트워크 없이 가짜 fredapi로 본다."""

from __future__ import annotations

from io import BytesIO
from urllib.error import HTTPError

import pandas as pd
import pytest

from collector.us.sources import fred


def _http_error(code: int) -> HTTPError:
    return HTTPError("http://x", code, "err", {}, BytesIO(b""))


def _fredapi_style(code: int, message):
    """fredapi가 하듯 HTTPError를 잡은 자리에서 ValueError를 던진 모양(``__context__``)."""
    exc = ValueError(message)
    exc.__context__ = _http_error(code)
    return exc


class _Fake:
    def __init__(self, script):
        self.script = list(script)
        self.calls: list[str] = []

    def get_series_all_releases(self, series_id, **kw):
        self.calls.append("all_releases")
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def get_series_vintage_dates(self, series_id):
        self.calls.append("vintage_dates")
        return [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")]


def _frame():
    return pd.DataFrame(
        {
            "realtime_start": [pd.Timestamp("2020-01-02")],
            "date": [pd.Timestamp("2020-01-01")],
            "value": [1.5],
        }
    )


def _client(monkeypatch, script):
    monkeypatch.setattr(fred.time, "sleep", lambda _s: None)
    client = fred.FredClient.__new__(fred.FredClient)
    client.api_key = "k"
    client.interval_seconds = 0.0
    client._fred = _Fake(script)
    client._last_request_at = 0.0
    client.requests_made = 0
    return client


@pytest.mark.parametrize("message", [None, "Bad Gateway"])
def test_5xx_is_retried_then_succeeds(monkeypatch, message):
    client = _client(monkeypatch, [_fredapi_style(502, message), _frame()])
    rows = client.all_releases("DGS10")
    assert len(rows) == 1 and rows[0][2] == 1.5
    assert client._fred.calls == ["all_releases"] * 2
    assert client.requests_made == 2


def test_message_less_error_without_status_is_retried(monkeypatch):
    client = _client(monkeypatch, [ValueError(None), _frame()])
    assert len(client.all_releases("DGS10")) == 1


def test_persistent_5xx_ends_in_fred_error(monkeypatch):
    err = [_fredapi_style(502, None) for _ in range(fred.RETRY_ATTEMPTS)]
    client = _client(monkeypatch, err)
    with pytest.raises(fred.FredError, match="DGS10|get_series_all_releases"):
        client.all_releases("DGS10")
    assert client.requests_made == fred.RETRY_ATTEMPTS


def test_4xx_is_not_retried(monkeypatch):
    client = _client(monkeypatch, [_fredapi_style(400, "Bad Request. nope")])
    with pytest.raises(fred.FredError, match="DGS10"):
        client.all_releases("DGS10")
    assert client._fred.calls == ["all_releases"]


def test_vintage_cap_goes_to_chunking_without_retry(monkeypatch):
    cap = _fredapi_style(400, "There are 5123 vintage dates exceeds the maximum (2000)")
    client = _client(monkeypatch, [cap, _frame()])
    rows = client.all_releases("DGS10")
    assert len(rows) == 1
    assert client._fred.calls == ["all_releases", "vintage_dates", "all_releases"]

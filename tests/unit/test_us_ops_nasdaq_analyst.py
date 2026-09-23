"""Nasdaq 애널리스트 추정치 — 주 1회 실행의 할 일 계산·이어받기·재시도.

네트워크 없이 돈다 — 가짜 세션으로 :mod:`collector.us.sources.nasdaq_analyst`
클라이언트를 대신한다.
"""

from __future__ import annotations

import datetime as dt
import json

import requests

from collector.lake import DataRoot
from collector.us.ops import nasdaq_analyst as ops
from collector.us.sources import nasdaq_analyst as na
from collector.us.sources.nasdaq import NasdaqClient as _RealNasdaqClient


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


_DOC = {
    "data": {
        "symbol": "x",
        "quarterlyForecast": {"asOf": None, "rows": [{"fiscalEnd": "Sep 2026",
            "consensusEPSForecast": 1.0, "highEPSForecast": 1.1, "lowEPSForecast": 0.9,
            "noOfEstimates": 3, "up": 0, "down": 0}]},
        "yearlyForecast": None,
    },
    "message": None,
    "status": {"rCode": 200, "bCodeMessage": None, "developerMessage": None},
}

_NOT_FOUND = {
    "data": None,
    "message": None,
    "status": {
        "rCode": 400,
        "bCodeMessage": [{"code": 1001, "errorMessage": "Symbol not exists."}],
    },
}


class _FakeSession:
    """심볼별 응답 또는 예외를 미리 정해 둔다. ``fail_times``만큼 먼저 던진다."""

    def __init__(self, docs: dict[str, object], fail_times: dict[str, int] | None = None):
        self.docs = docs
        self.fail_times = dict(fail_times or {})
        self.calls: list[str] = []

    def get(self, url, params=None, headers=None, **kw):
        symbol = url.rsplit("/", 2)[1]
        self.calls.append(symbol)
        remaining = self.fail_times.get(symbol, 0)
        if remaining > 0:
            self.fail_times[symbol] = remaining - 1
            raise requests.exceptions.ConnectTimeout("boom")
        doc = self.docs[symbol]
        return _Resp(doc)


class _Resp:
    def __init__(self, payload):
        self.status_code = 200
        self.payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self.payload


def _patch_client(monkeypatch, session):
    def _client(interval_seconds=None):
        return _RealNasdaqClient(interval_seconds=0, session=session)

    monkeypatch.setattr(na, "NasdaqClient", _client)


# --- 할 일 계산 ----------------------------------------------------------------


def test_dry_run_only_counts(tmp_path):
    root = _lake(tmp_path)
    result = ops.run_weekly(
        root, today=dt.date(2026, 9, 24), symbols=["AAA", "BBB"], dry_run=True
    )
    assert result["universe_symbols"] == 2
    assert result["already_done"] == 0
    assert result["pending_before"] == 2
    assert result["fetched"] == 0
    assert result["ok"] is True


def test_symbols_already_collected_this_week_are_skipped(tmp_path, monkeypatch):
    """이번 주에 이미 받은 심볼은 세션을 아예 건드리지 않는다."""
    root = _lake(tmp_path)
    week = na.week_start(dt.date(2026, 9, 24))
    na.earnings_forecast_path(root, week, "AAA").parent.mkdir(parents=True, exist_ok=True)
    na.earnings_forecast_path(root, week, "AAA").write_text(
        json.dumps({"fetched_at": "2026-09-22T00:00:00+00:00", "response": _DOC})
    )
    session = _FakeSession({"BBB": _DOC})
    _patch_client(monkeypatch, session)

    result = ops.run_weekly(root, today=dt.date(2026, 9, 24), symbols=["AAA", "BBB"])
    assert result["already_done"] == 1
    assert result["pending_before"] == 1
    assert result["fetched"] == 1
    assert session.calls == ["BBB"]
    assert result["ok"] is True


def test_a_different_day_in_the_same_iso_week_reuses_the_partition(tmp_path, monkeypatch):
    root = _lake(tmp_path)
    session = _FakeSession({"AAA": _DOC})
    _patch_client(monkeypatch, session)
    ops.run_weekly(root, today=dt.date(2026, 9, 21), symbols=["AAA"])  # 월요일

    # 같은 주 목요일에 재시도해도 파티션이 같아 다시 안 받는다.
    session2 = _FakeSession({})
    _patch_client(monkeypatch, session2)
    result = ops.run_weekly(root, today=dt.date(2026, 9, 24), symbols=["AAA"])
    assert result["already_done"] == 1
    assert result["fetched"] == 0
    assert session2.calls == []


# --- 예산 ----------------------------------------------------------------------


def test_budget_exhaustion_leaves_the_rest_pending(tmp_path, monkeypatch):
    root = _lake(tmp_path)
    session = _FakeSession({s: _DOC for s in ("A", "B", "C")})
    _patch_client(monkeypatch, session)

    calls = {"n": 0}

    class _Budget:
        def __init__(self, seconds):
            self.seconds = seconds

        def spent(self):
            calls["n"] += 1
            return calls["n"] > 1  # 첫 심볼만 통과시킨다

    monkeypatch.setattr(ops, "_Budget", _Budget)
    result = ops.run_weekly(root, today=dt.date(2026, 9, 24), symbols=["A", "B", "C"])
    assert result["fetched"] == 1
    assert result["pending"] == 2
    assert result["budget_spent"] is True
    assert result["ok"] is True  # 예산 소진은 실패가 아니다


# --- 재시도·실패 ----------------------------------------------------------------


def test_a_transient_failure_is_retried_then_succeeds(tmp_path, monkeypatch):
    root = _lake(tmp_path)
    session = _FakeSession({"AAA": _DOC}, fail_times={"AAA": 2})
    _patch_client(monkeypatch, session)

    result = ops.run_weekly(
        root, today=dt.date(2026, 9, 24), symbols=["AAA"], max_retries=2
    )
    assert result["fetched"] == 1
    assert result["failed"] == []
    assert session.calls == ["AAA", "AAA", "AAA"]  # 처음 둘은 실패, 셋째에 성공


def test_a_permanent_failure_is_recorded_and_not_written(tmp_path, monkeypatch):
    """실패한 심볼은 raw 파일이 안 남아 다음 실행이 자연히 다시 시도한다."""
    root = _lake(tmp_path)
    session = _FakeSession({"AAA": _DOC}, fail_times={"AAA": 99})
    _patch_client(monkeypatch, session)

    result = ops.run_weekly(
        root, today=dt.date(2026, 9, 24), symbols=["AAA"], max_retries=1
    )
    assert result["fetched"] == 0
    assert len(result["failed"]) == 1
    assert result["ok"] is False
    week = na.week_start(dt.date(2026, 9, 24))
    assert not na.earnings_forecast_path(root, week, "AAA").is_file()


def test_a_body_level_not_found_counts_as_fetched_not_failed(tmp_path, monkeypatch):
    """존재하지 않는 심볼도 raw 는 받은 것이다 — 재시도해도 안 바뀐다."""
    root = _lake(tmp_path)
    session = _FakeSession({"ZZZZZ": _NOT_FOUND})
    _patch_client(monkeypatch, session)

    result = ops.run_weekly(root, today=dt.date(2026, 9, 24), symbols=["ZZZZZ"])
    assert result["fetched"] == 1
    assert result["not_found"] == ["ZZZZZ"]
    assert result["failed"] == []
    assert result["ok"] is True

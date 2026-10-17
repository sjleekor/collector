"""``us-nasdaq-earnings-forward run`` — 경로·날짜·덮어쓰기 금지·막힘 처리·종료 코드.

네트워크는 쓰지 않는다.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
import requests

from collector.cli.app import build_parser
from collector.lake import DataRoot
from collector.us.cli import app as us_app
from collector.us.sources import nasdaq_earnings_forward as fwd
from collector.us.sources.nasdaq import NasdaqError

OBSERVED = dt.date(2026, 10, 16)  # 금요일 (시험용 고정값 — 기본값이 아니다)
NOW = dt.datetime(2026, 10, 16, 6, 30, tzinfo=dt.UTC)


class FakeClient:
    def __init__(self, bodies=None, fail=(), timeouts=None):
        self.calls: list[str] = []
        self.bodies = bodies or {}
        self.fail = set(fail)
        # 날짜별로 남은 시간 초과 횟수
        self.timeouts = dict(timeouts or {})

    def get_json(self, url, params):
        d = params["date"]
        self.calls.append(d)
        if self.timeouts.get(d, 0) > 0:
            self.timeouts[d] -= 1
            raise requests.Timeout(f"read timeout {d}")
        if d in self.fail:
            raise NasdaqError(f"403 {d}")
        return self.bodies.get(d, {"data": {"asOf": "x", "rows": [{"symbol": "AAPL"}]}})


@pytest.fixture
def root(tmp_path):
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


def test_path_layout(root):
    p = fwd.forward_path(root, OBSERVED, dt.date(2026, 10, 20))
    assert (
        p == root.raw / "nasdaq/earnings_calendar_forward/observed=2026-10-16/date=2026-10-20.json"
    )


def test_target_dates_weekdays_and_bounds():
    dates = fwd.target_dates(OBSERVED, 14)
    assert dates[0] == dt.date(2026, 10, 19)  # 관측일 자신(금)과 주말은 뺀다
    assert dates[-1] == dt.date(2026, 10, 30)  # T+14 = 10-30 (금)
    assert all(d.weekday() < 5 for d in dates)
    assert OBSERVED not in dates
    assert len(dates) == 10
    assert fwd.target_dates(OBSERVED, 0) == []
    # T+N 이 주말이면 그 앞 평일까지
    assert fwd.target_dates(OBSERVED, 9)[-1] == dt.date(2026, 10, 23)


def test_writes_envelope_with_untouched_body(root):
    body = {"data": {"asOf": "x", "rows": [{"symbol": "A"}, {"symbol": "B"}]}, "extra": 1}
    client = FakeClient(bodies={"2026-10-19": body})
    res = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, client=client)
    assert res["ok"] and res["fetched"] == 1 and res["rows"]["2026-10-19"] == 2
    env = json.loads(fwd.forward_path(root, OBSERVED, dt.date(2026, 10, 19)).read_text())
    assert env["response"] == body
    assert dt.datetime.fromisoformat(env["fetched_at"]).tzinfo is not None


def test_existing_file_is_never_overwritten(root):
    dest = fwd.forward_path(root, OBSERVED, dt.date(2026, 10, 19))
    dest.parent.mkdir(parents=True)
    dest.write_text("OLD")
    client = FakeClient()
    res = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, client=client)
    assert dest.read_text() == "OLD"
    assert "2026-10-19" not in client.calls
    assert res["skipped"] == 1 and res["fetched"] == 0
    # 두 번째 실행은 전부 건너뛴다
    again = FakeClient()
    res2 = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, client=again)
    assert again.calls == [] and res2["skipped"] == 1


def test_null_data_is_zero_rows_not_error(root):
    client = FakeClient(bodies={"2026-10-19": {"data": None}})
    res = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, client=client)
    assert res["ok"] and res["rows"]["2026-10-19"] == 0


def test_missing_data_key_is_error_and_writes_nothing(root):
    client = FakeClient(bodies={"2026-10-19": {"message": "blocked"}})
    res = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, client=client, max_retries=0)
    assert not res["ok"] and len(res["errors"]) == 1
    assert not fwd.forward_path(root, OBSERVED, dt.date(2026, 10, 19)).exists()
    assert client.calls == ["2026-10-19"] and fwd.read_block(root) is not None


def test_http_error_stops_run_and_writes_block(root):
    client = FakeClient(fail={"2026-10-20"})
    res = fwd.run_forward(root, horizon_days=7, observed=OBSERVED, client=client, now=NOW)
    assert not res["ok"]
    # 실패한 날짜를 다시 하지 않고, 그 뒤 날짜도 받지 않는다
    assert client.calls == ["2026-10-19", "2026-10-20"]
    assert fwd.forward_path(root, OBSERVED, dt.date(2026, 10, 19)).is_file()
    assert not fwd.forward_path(root, OBSERVED, dt.date(2026, 10, 21)).exists()
    block = fwd.read_block(root)
    assert block["consecutive"] == 1 and block["cooldown_days"] == 2
    assert dt.datetime.fromisoformat(block["until"]) == NOW + dt.timedelta(days=2)
    assert res["blocked_until"] == block["until"] and "403" in res["errors"][0]


def test_within_cooldown_makes_no_request(root):
    fwd.run_forward(
        root, horizon_days=3, observed=OBSERVED, client=FakeClient(fail={"2026-10-19"}), now=NOW
    )
    later = FakeClient()
    res = fwd.run_forward(
        root,
        horizon_days=14,
        observed=dt.date(2026, 10, 17),
        client=later,
        now=NOW + dt.timedelta(days=1),
    )
    assert later.calls == []
    assert res["ok"] and res["skipped_reason"] == "blocked" and res["dates_requested"] == 0


def test_after_cooldown_success_clears_block(root):
    fwd.run_forward(
        root, horizon_days=3, observed=OBSERVED, client=FakeClient(fail={"2026-10-19"}), now=NOW
    )
    res = fwd.run_forward(
        root,
        horizon_days=3,
        observed=dt.date(2026, 10, 19),
        client=FakeClient(),
        now=NOW + dt.timedelta(days=3),
    )
    assert res["ok"] and res["block_cleared"] and res["blocked_until"] is None
    assert fwd.read_block(root) is None


def test_repeated_failure_escalates_cooldown(root):
    when = NOW
    expected = [2, 4, 8, 16, 30, 30]
    for days in expected:
        fwd.run_forward(
            root,
            horizon_days=3,
            observed=OBSERVED,
            client=FakeClient(fail={"2026-10-19"}),
            now=when,
        )
        block = fwd.read_block(root)
        assert block["cooldown_days"] == days
        when = dt.datetime.fromisoformat(block["until"])
    assert block["consecutive"] == len(expected)


def test_timeout_is_retried_once_then_blocks(root):
    once = FakeClient(timeouts={"2026-10-19": 1})
    res = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, client=once, now=NOW)
    assert res["ok"] and once.calls.count("2026-10-19") == 2 and fwd.read_block(root) is None

    always = FakeClient(timeouts={"2026-10-20": 99})
    res = fwd.run_forward(
        root, horizon_days=7, observed=dt.date(2026, 10, 19), client=always, now=NOW
    )
    assert not res["ok"] and always.calls.count("2026-10-20") == 2
    assert "Timeout" in res["errors"][0] and fwd.read_block(root)["consecutive"] == 1


def test_ignore_block_runs_anyway(root):
    fwd.run_forward(
        root, horizon_days=3, observed=OBSERVED, client=FakeClient(fail={"2026-10-19"}), now=NOW
    )
    manual = FakeClient()
    res = fwd.run_forward(
        root,
        horizon_days=3,
        observed=dt.date(2026, 10, 17),
        client=manual,
        now=NOW + dt.timedelta(hours=1),
        ignore_block=True,
    )
    assert manual.calls == ["2026-10-19", "2026-10-20"]
    assert res["ok"] and fwd.read_block(root) is None


def test_dry_run_reports_block_without_request(root):
    fwd.run_forward(
        root, horizon_days=3, observed=OBSERVED, client=FakeClient(fail={"2026-10-19"}), now=NOW
    )
    res = fwd.run_forward(root, horizon_days=3, observed=OBSERVED, dry_run=True, now=NOW)
    assert res["blocked_until"] == fwd.read_block(root)["until"]


def test_dry_run_makes_no_request(root):
    res = fwd.run_forward(root, horizon_days=14, observed=OBSERVED, dry_run=True)
    assert res["would_fetch"] == 10 and not (root.raw / "nasdaq").exists()


def _parse(*argv):
    return build_parser().parse_args(["us-nasdaq-earnings-forward", "run", *argv])


def test_cli_defaults_and_overrides():
    a = _parse()
    assert a.handler is us_app._handle_nasdaq_earnings_forward_run
    assert a.horizon_days == 14 and a.observed_date is None and a.dry_run is False
    assert a.max_retries == 1 and a.ignore_block is False
    assert _parse("--ignore-block").ignore_block is True
    b = _parse(
        "--horizon-days",
        "100",
        "--observed-date",
        "2026-10-16",
        "--dry-run",
        "--lake-root",
        "/x",
        "--interval-seconds",
        "0.1",
    )
    assert b.horizon_days == 100 and b.observed_date == "2026-10-16"
    assert b.dry_run and b.lake_root == "/x" and b.interval_seconds == 0.1


@pytest.mark.parametrize("ok,code", [(True, None), (False, 1)])
def test_handler_exit_code(tmp_path, monkeypatch, capsys, ok, code):
    captured = {}

    def fake(root, **kw):
        captured.update(kw)
        return {"ok": ok}

    monkeypatch.setattr(fwd, "run_forward", fake)
    args = _parse(
        "--horizon-days", "7", "--observed-date", "2026-10-16", "--lake-root", str(tmp_path)
    )
    if code is None:
        args.handler(args)
    else:
        with pytest.raises(SystemExit) as exc:
            args.handler(args)
        assert exc.value.code == code
    assert captured["horizon_days"] == 7 and captured["observed"] == dt.date(2026, 10, 16)
    assert captured["ignore_block"] is False

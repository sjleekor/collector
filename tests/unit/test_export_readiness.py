"""KR export start gate: data freshness plus ingestion_runs evidence."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from collector.kr.domain.enums import RunStatus, RunType, Source
from collector.kr.domain.models import IngestionRun
from collector.kr.service import export_readiness as er
from collector.kr.util.time import KST

K = date(2026, 10, 1)  # Thursday
PREV = date(2026, 9, 30)
AS_OF = datetime(2026, 10, 2, 4, 10, tzinfo=KST)  # D = 10-02 04:10
EVENING_END = datetime(2026, 10, 1, 21, 0, tzinfo=KST)
DART_END = datetime(2026, 10, 2, 4, 3, tzinfo=KST)
RECEIPT_END = datetime(2026, 10, 1, 23, 44, tzinfo=KST)


class FakeStorage:
    def __init__(
        self,
        *,
        latest: date | None = K,
        counts: dict[date, int] | None = None,
        flow_latest: date | None = K,
        runs: dict[RunType, IngestionRun] | None = None,
    ) -> None:
        self.latest = latest
        self.counts = {PREV: 2800, K: 2795} if counts is None else counts
        self.flow_latest = flow_latest
        self.runs = runs if runs is not None else _good_runs()

    def get_latest_daily_price_date(self, tickers=None):
        return self.latest

    def get_daily_ohlcv_session_counts(self, since, until):
        return {d: n for d, n in self.counts.items() if since <= d <= until}

    def get_krx_security_flow_metric_max_dates(self, metric_codes, sources):
        assert Source.KRX in sources
        if self.flow_latest is None:
            return {}
        return {m: self.flow_latest for m in metric_codes}

    def get_recent_ingestion_runs(self, run_type, limit=20):
        run = self.runs.get(run_type)
        return [run] if run else []


def _run(rt: RunType, ended: datetime, status: RunStatus = RunStatus.SUCCESS) -> IngestionRun:
    return IngestionRun(
        run_id=f"{rt.value}-1",
        run_type=rt,
        started_at=ended - timedelta(minutes=3),
        ended_at=ended,
        status=status,
    )


def _good_runs() -> dict[RunType, IngestionRun]:
    runs = {}
    for rt in er.DEFAULT_REQUIRED_RUN_TYPES:
        if rt in er.DART_RUN_TYPES:
            ended = DART_END
        elif rt in er.DAILY_2330_RUN_TYPES:
            ended = RECEIPT_END
        else:
            ended = EVENING_END
        runs[rt] = _run(rt, ended)
    return runs


def _cfg(**kw) -> er.ReadinessConfig:
    return er.ReadinessConfig(feature_asof_date=K, as_of=AS_OF, **kw)


def _check(doc: dict, name: str, run_type: str | None = None) -> dict:
    for c in doc["checks"]:
        if c["check"] == name and (run_type is None or c.get("run_type") == run_type):
            return c
    raise AssertionError(name)


def test_ready() -> None:
    doc = er.evaluate_export_readiness(FakeStorage(), _cfg())
    assert doc["verdict"] == "ready"
    assert doc["reasons"] == []
    assert doc["dart_chain_ended_at"] == DART_END.isoformat()
    assert er.exit_code_for(doc["verdict"]) == 0
    assert doc["parameters"]["expected_since"]["xbrl_parse"].startswith("2026-10-02T04:00")
    assert doc["parameters"]["expected_since"]["dart_filing_receipt_sync"].startswith(
        "2026-10-01T23:30"
    )
    assert doc["parameters"]["expected_since"]["daily_backfill"].startswith("2026-10-01T18:30")


def test_dart_chain_late_is_not_yet() -> None:
    storage = FakeStorage()
    # Yesterday's chain is the latest record: the 04:00 run has not finished.
    storage.runs[RunType.XBRL_PARSE] = _run(
        RunType.XBRL_PARSE, datetime(2026, 10, 1, 4, 6, tzinfo=KST)
    )
    doc = er.evaluate_export_readiness(storage, _cfg())
    assert doc["verdict"] == "not_yet"
    assert _check(doc, "ingestion_run", "xbrl_parse")["status"] == "not_yet"
    assert er.exit_code_for(doc["verdict"]) == er.EXIT_NOT_YET


def test_running_chain_is_not_yet() -> None:
    storage = FakeStorage()
    running = IngestionRun(
        run_id="x",
        run_type=RunType.DART_SHARE_INFO_SYNC,
        started_at=datetime(2026, 10, 2, 4, 20, tzinfo=KST),
        ended_at=None,
        status=RunStatus.RUNNING,
    )
    storage.runs[RunType.DART_SHARE_INFO_SYNC] = running
    assert er.evaluate_export_readiness(storage, _cfg())["verdict"] == "not_yet"


@pytest.mark.parametrize("status", [RunStatus.PARTIAL, RunStatus.FAILED])
def test_partial_or_failed_blocks(status: RunStatus) -> None:
    storage = FakeStorage()
    storage.runs[RunType.DART_SHARE_INFO_SYNC] = _run(
        RunType.DART_SHARE_INFO_SYNC, DART_END, status
    )
    doc = er.evaluate_export_readiness(storage, _cfg())
    assert doc["verdict"] == "blocked"
    assert er.exit_code_for(doc["verdict"]) == er.EXIT_BLOCKED
    assert any("dart_share_info_sync" in r for r in doc["reasons"])


def test_old_failure_is_just_stale_not_blocked() -> None:
    storage = FakeStorage()
    storage.runs[RunType.DART_CORP_SYNC] = _run(
        RunType.DART_CORP_SYNC, datetime(2026, 10, 1, 4, 3, tzinfo=KST), RunStatus.FAILED
    )
    assert er.evaluate_export_readiness(storage, _cfg())["verdict"] == "not_yet"


def test_stale_prices_not_yet() -> None:
    doc = er.evaluate_export_readiness(FakeStorage(latest=PREV, counts={PREV: 2800}), _cfg())
    assert doc["verdict"] == "not_yet"
    assert _check(doc, "daily_ohlcv")["status"] == "not_yet"


def test_thin_session_not_yet() -> None:
    doc = er.evaluate_export_readiness(FakeStorage(counts={PREV: 2800, K: 1500}), _cfg())
    price = _check(doc, "daily_ohlcv")
    assert doc["verdict"] == "not_yet"
    assert price["ratio"] == round(1500 / 2800, 6)


def test_ratio_is_a_parameter() -> None:
    storage = FakeStorage(counts={PREV: 2800, K: 2600})
    assert er.evaluate_export_readiness(storage, _cfg())["verdict"] == "not_yet"
    assert er.evaluate_export_readiness(storage, _cfg(min_ticker_ratio=0.9))["verdict"] == "ready"


def test_prices_past_k_blocked() -> None:
    doc = er.evaluate_export_readiness(
        FakeStorage(latest=date(2026, 10, 2), counts={PREV: 2800, K: 2795}), _cfg()
    )
    assert doc["verdict"] == "blocked"


def test_flow_behind_not_yet_and_lag_allowed() -> None:
    storage = FakeStorage(flow_latest=PREV)
    assert er.evaluate_export_readiness(storage, _cfg())["verdict"] == "not_yet"
    ok = er.evaluate_export_readiness(
        storage, _cfg(flow_max_lag_trading_days=1, holidays=frozenset())
    )
    assert ok["verdict"] == "ready"


def test_missing_run_record_not_yet() -> None:
    storage = FakeStorage()
    del storage.runs[RunType.KIS_FLOW_SYNC]
    doc = er.evaluate_export_readiness(storage, _cfg())
    assert doc["verdict"] == "not_yet"
    assert any("no ingestion_runs record" in r for r in doc["reasons"])


def test_blocked_outranks_not_yet() -> None:
    storage = FakeStorage(latest=PREV, counts={PREV: 2800})
    storage.runs[RunType.XBRL_PARSE] = _run(RunType.XBRL_PARSE, DART_END, RunStatus.FAILED)
    assert er.evaluate_export_readiness(storage, _cfg())["verdict"] == "blocked"


def test_overrides_and_required_set() -> None:
    cfg = _cfg(
        required_run_types=(RunType.DAILY_BACKFILL,),
        run_since_overrides=((RunType.DAILY_BACKFILL, datetime(2026, 10, 1, 22, 0)),),
    )
    doc = er.evaluate_export_readiness(FakeStorage(), cfg)
    # Evening run ended 21:00, before the overridden 22:00 (naive = KST).
    assert doc["verdict"] == "not_yet"
    assert doc["parameters"]["required_run_types"] == ["daily_backfill"]


def test_cli_handler_writes_evidence_and_exit_code(monkeypatch, tmp_path, capsys) -> None:
    import json

    from collector.kr.cli import app
    from collector.kr.infra.db_postgres import repositories

    monkeypatch.setattr(repositories, "PostgresStorage", lambda dsn: FakeStorage())
    monkeypatch.setattr(app, "get_settings", lambda: type("S", (), {"db_dsn": "x"})())
    out = tmp_path / "ev.json"

    def run(argv):
        args = app.build_parser().parse_args(argv)
        args.handler(args)

    with pytest.raises(SystemExit) as exc:
        run(
            [
                "ops",
                "kr-export-readiness",
                "--feature-asof-date",
                "2026-10-01",
                "--as-of",
                "2026-10-02T04:10:00",
                "--output",
                str(out),
            ]
        )
    assert exc.value.code == 0
    doc = json.loads(out.read_text())
    assert doc["verdict"] == "ready" and doc["feature_asof_date"] == "2026-10-01"

    with pytest.raises(SystemExit) as bad:
        run(["ops", "kr-export-readiness", "--feature-asof-date", "nope"])
    assert bad.value.code == 2


def test_filing_receipts_before_k_2330_not_yet() -> None:
    storage = FakeStorage()
    storage.runs[RunType.DART_FILING_RECEIPT_SYNC] = _run(
        RunType.DART_FILING_RECEIPT_SYNC, datetime(2026, 10, 1, 23, 20, tzinfo=KST)
    )
    doc = er.evaluate_export_readiness(storage, _cfg())
    assert doc["verdict"] == "not_yet"
    assert _check(doc, "ingestion_run", "dart_filing_receipt_sync")["status"] == "not_yet"


def test_filing_receipts_at_2344_do_not_need_the_morning_chain_time() -> None:
    # Receipts finished K 23:44, well before D 04:00; still ready.
    doc = er.evaluate_export_readiness(FakeStorage(), _cfg())
    receipt = _check(doc, "ingestion_run", "dart_filing_receipt_sync")
    assert receipt["status"] == "ok" and receipt["ended_at"].startswith("2026-10-01T23:44")

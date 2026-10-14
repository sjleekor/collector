"""19:00 KIS 수급 잡이 같은 inquire-price 응답에서 상태 플래그도 쌓는다 (10월 계획 4.2-b).

fixture만 쓴다. 실제 KIS·DB 호출 없음.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from collector.kr.adapters.flows_kis import KisFlowProvider
from collector.kr.adapters.kis_common.client import KisClient
from collector.kr.adapters.kis_common.token import KisAccessToken, KisTokenProvider
from collector.kr.domain.enums import (
    ListingStatus,
    Market,
    RunStatus,
    RunType,
    Source,
    UniverseScope,
)
from collector.kr.domain.models import (
    FlowRequestStats,
    IngestionRun,
    SecurityFlowFetchResult,
    SecurityFlowLine,
    Stock,
    StockStatusRecord,
    UpsertResult,
)
from collector.kr.service import sync_kis_flows as service
from collector.kr.service.sync_kis_flows import sync_kis_security_flows
from collector.kr.util.pipeline import SourceAuthError
from collector.kr.util.rate_limit import TokenBucket

KST = timezone(timedelta(hours=9))
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "kis_status"
SNAPSHOT = date(2026, 10, 8)
SESSION = date(2026, 10, 8)


def _fixture(name: str) -> dict[str, Any]:
    payload = json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    payload.pop("_fixture_note", None)
    return payload


# --- provider over the real KisClient with a fake HTTP session -----------------


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.status_code = 200
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.text = json.dumps(payload)

    def json(self) -> dict[str, Any]:
        return self._payload


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append({"url": url, "params": dict(params)})
        if not self._responses:
            raise AssertionError("unexpected extra KIS request")
        return self._responses.pop(0)


class _Cache:
    path = "stub"

    def load(self):
        return KisAccessToken("cached-token", datetime.now(KST) + timedelta(hours=12))

    def store(self, token) -> None:  # pragma: no cover
        raise AssertionError("a warm cache must not trigger an issuance")


def _provider(
    responses: list[_Response], *, status_snapshot_date: date | None = SNAPSHOT
) -> tuple[KisFlowProvider, _Session]:
    session = _Session(responses)
    token_provider = KisTokenProvider(
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        cache=_Cache(),
        session=session,
    )
    client = KisClient(
        token_provider=token_provider,
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        bucket=TokenBucket(0),
        session=session,
        sleep_fn=lambda _seconds: None,
    )
    return KisFlowProvider(client=client, status_snapshot_date=status_snapshot_date), session


def test_one_response_yields_foreign_holding_and_status_with_one_http_request() -> None:
    provider, session = _provider([_Response(_fixture("inquire_price_managed"))])

    result = provider.fetch_foreign_holding("000040", Market.KOSPI, SESSION)

    assert len(session.calls) == 1
    assert provider.request_stats().http_requests == 1
    assert result.error is None and len(result.records) == 1
    assert result.records[0].value == Decimal("2736287683")
    assert result.status_error is None
    assert result.status_record is not None
    assert result.status_record.mang_issu_cls_code == "Y"
    assert result.status_record.snapshot_date == SNAPSHOT
    assert result.status_record.ticker == "000040"


def test_without_a_snapshot_date_status_is_not_parsed() -> None:
    provider, _ = _provider(
        [_Response(_fixture("inquire_price_managed"))], status_snapshot_date=None
    )

    result = provider.fetch_foreign_holding("000040", Market.KOSPI, SESSION)

    assert result.records and result.status_record is None and result.status_error is None


def test_a_missing_flag_field_keeps_the_foreign_holding_and_reports_the_status_error() -> None:
    payload = _fixture("inquire_price_normal")
    del payload["output"]["temp_stop_yn"]
    provider, _ = _provider([_Response(payload)])

    result = provider.fetch_foreign_holding("005930", Market.KOSPI, SESSION)

    assert result.error is None and len(result.records) == 1
    assert result.status_record is None
    assert result.status_error and "temp_stop_yn" in result.status_error


def test_a_missing_foreign_field_keeps_the_status_row() -> None:
    payload = _fixture("inquire_price_normal")
    del payload["output"]["frgn_hldn_qty"]
    provider, _ = _provider([_Response(payload)])

    result = provider.fetch_foreign_holding("005930", Market.KOSPI, SESSION)

    assert result.error and "frgn_hldn_qty" in result.error
    assert result.status_record is not None and result.status_error is None


# --- service --------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _newest_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service, "_latest_trading_day", lambda: SESSION)
    monkeypatch.setattr(service, "get_trading_days", lambda start, end: [SESSION])


def _stock(ticker: str) -> Stock:
    return Stock(
        ticker=ticker,
        market=Market.KOSPI,
        name=f"name-{ticker}",
        status=ListingStatus.ACTIVE,
        last_seen_date=SESSION,
        source=Source.KRX,
    )


def _status(ticker: str, **flags: str) -> StockStatusRecord:
    return StockStatusRecord(
        snapshot_date=SNAPSHOT,
        ticker=ticker,
        market=Market.KOSPI,
        mang_issu_cls_code=flags.get("mang_issu_cls_code", "N"),
        temp_stop_yn=flags.get("temp_stop_yn", "N"),
        mrkt_warn_cls_code=flags.get("mrkt_warn_cls_code", "00"),
        observed_at=datetime(2026, 10, 8, 19, 5, tzinfo=KST),
        raw_payload={},
    )


def _flow(ticker: str) -> SecurityFlowLine:
    return SecurityFlowLine(
        trade_date=SESSION,
        ticker=ticker,
        market=Market.KOSPI,
        metric_code="foreign_holding_shares",
        metric_name="metric",
        value=Decimal("1"),
        unit="shares",
        source=Source.KIS,
        fetched_at=datetime(2026, 10, 8, 19, 5, tzinfo=KST),
        raw_payload={},
    )


class FakeProvider:
    def __init__(
        self,
        *,
        status_error: dict[str, str] | None = None,
        no_status: set[str] | None = None,
        http_error: set[str] | None = None,
        raises: dict[str, Exception] | None = None,
        flags: dict[str, dict[str, str]] | None = None,
    ) -> None:
        self.status_error = status_error or {}
        self.no_status = no_status or set()
        self.http_error = http_error or set()
        self.raises = raises or {}
        self.flags = flags or {}
        self.calls: list[str] = []

    def source(self) -> Source:
        return Source.KIS

    def supported_flow_groups(self) -> tuple[str, ...]:
        return ("foreign_holding", "investor", "shorting")

    def unsupported_metric_codes(self) -> list[str]:
        return []

    def request_stats(self) -> FlowRequestStats:
        return FlowRequestStats(http_requests=len(self.calls))

    def fetch_foreign_holding(self, ticker, market, trade_date):
        self.calls.append(ticker)
        if ticker in self.raises:
            raise self.raises[ticker]
        if ticker in self.http_error:
            return SecurityFlowFetchResult(error="HTTP 500")
        return SecurityFlowFetchResult(
            records=[_flow(ticker)],
            status_record=None
            if ticker in self.no_status or ticker in self.status_error
            else _status(ticker, **self.flags.get(ticker, {})),
            status_error=self.status_error.get(ticker),
        )


class FakeStorage:
    def __init__(self, stocks: list[Stock], *, status_write_fails: bool = False) -> None:
        self.stocks = stocks
        self.status_write_fails = status_write_fails
        self.runs: list[IngestionRun] = []
        self.flow_rows: list[SecurityFlowLine] = []
        self.status_batches: list[list[StockStatusRecord]] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(
            IngestionRun(
                run_id=run.run_id,
                run_type=run.run_type,
                started_at=run.started_at,
                ended_at=run.ended_at,
                status=run.status,
                params=dict(run.params or {}),
                counts=dict(run.counts or {}),
                error_summary=run.error_summary,
            )
        )

    def get_active_stocks(self, market=None):
        return list(self.stocks)

    def get_stocks(self, market=None, tickers=None):
        return list(self.stocks)

    def get_dart_corp_master(self, active_only=True, tickers=None, include_delisted=False):
        return []

    def upsert_krx_security_flow_raw(self, records):
        self.flow_rows.extend(records)
        return UpsertResult(updated=len(records))

    def get_krx_security_flow_ticker_metric_coverage(self, **kwargs):
        return {}

    def get_recent_ingestion_runs(self, run_type, limit=20):
        return []

    def upsert_kis_stock_status(self, records):
        if self.status_write_fails:
            raise RuntimeError("db down")
        self.status_batches.append(list(records))
        return UpsertResult(inserted=len(records))

    @property
    def last_run(self) -> IngestionRun:
        return self.runs[-1]

    @property
    def status_rows(self) -> list[StockStatusRecord]:
        return [row for batch in self.status_batches for row in batch]


def _run(storage: FakeStorage, provider: FakeProvider, **kwargs):
    return sync_kis_security_flows(
        provider,
        storage,
        start=SESSION,
        end=SESSION,
        scope=UniverseScope.CURRENT,
        enabled_flow_groups=["foreign_holding"],
        retry_max_attempts=1,
        guard_backoff_seconds=0.0,
        **kwargs,
    )


def _stocks(*tickers: str) -> list[Stock]:
    return [_stock(ticker) for ticker in tickers]


def test_one_job_stores_flow_rows_and_status_rows_with_no_extra_requests() -> None:
    storage = FakeStorage(_stocks("000040", "005930", "000660"))
    provider = FakeProvider(flags={"000040": {"mang_issu_cls_code": "Y"}})

    result = _run(storage, provider, capture_status=True)

    assert provider.calls == ["000040", "000660", "005930"]  # 종목당 정확히 한 번
    assert result.requests_attempted == 3
    assert result.http_counts["http_requests"] == 3
    assert result.rows_upserted == 3 and len(storage.flow_rows) == 3
    assert result.status_rows_inserted == 3 and len(storage.status_rows) == 3
    assert {row.run_id for row in storage.status_rows} == {storage.last_run.run_id}
    assert result.status_flagged_managed == 1
    assert result.status_errors == {}
    run = storage.last_run
    assert run.run_type is RunType.KIS_FLOW_SYNC and run.status is RunStatus.SUCCESS
    assert run.counts["status_rows_inserted"] == 3
    assert run.counts["status_flagged_managed"] == 1
    assert run.params["capture_status"] is True


def test_status_is_ignored_when_capture_is_off() -> None:
    storage = FakeStorage(_stocks("005930"))

    result = _run(storage, FakeProvider(), capture_status=False)

    assert result.rows_upserted == 1
    assert storage.status_batches == [] and "status_rows_inserted" not in storage.last_run.counts


def test_status_parse_failure_keeps_the_flow_rows_and_marks_the_run_partial() -> None:
    storage = FakeStorage(_stocks("000040", "005930"))
    provider = FakeProvider(status_error={"005930": "response has no temp_stop_yn"})

    result = _run(storage, provider, capture_status=True)

    assert result.rows_upserted == 2  # 수급은 계속 쌓인다
    assert [row.ticker for row in storage.status_rows] == ["000040"]
    assert set(result.status_errors) == {"005930"}
    assert result.errors == {}  # 수급 요청 오류로 세지 않는다
    run = storage.last_run
    assert run.status is RunStatus.PARTIAL
    assert run.params["status_failed_request_keys"] == ["005930"]
    assert run.counts["status_error_count"] == 1


def test_a_failed_request_counts_as_a_missing_status_row() -> None:
    storage = FakeStorage(_stocks("000040", "005930"))
    provider = FakeProvider(http_error={"005930"})

    result = _run(storage, provider, capture_status=True)

    assert result.status_attempted == 2
    assert set(result.status_errors) == {"005930"}
    assert "HTTP 500" in result.status_errors["005930"]
    assert set(result.errors) == {"foreign_holding:005930"}


def test_status_store_failure_does_not_stop_flow_collection() -> None:
    storage = FakeStorage(_stocks("000040", "005930"), status_write_fails=True)

    result = _run(storage, FakeProvider(), capture_status=True)

    assert result.aborted_reason is None
    assert result.rows_upserted == 2  # 수급 두 행은 저장됐다
    assert result.status_rows_inserted == 0
    assert set(result.status_errors) == {"000040", "005930"}
    assert all("store failed" in message for message in result.status_errors.values())
    run = storage.last_run
    assert run.status is RunStatus.PARTIAL
    assert run.params["status_failed_request_keys"] == ["000040", "005930"]


def test_status_rows_are_written_in_batches() -> None:
    storage = FakeStorage(_stocks(*[f"{n:06d}" for n in range(1, 8)]))

    _run(storage, FakeProvider(), capture_status=True, status_flush_every=3)

    assert [len(batch) for batch in storage.status_batches] == [3, 3, 1]


def test_an_abort_keeps_the_status_rows_already_fetched() -> None:
    storage = FakeStorage(_stocks("000040", "000660", "005930"))
    provider = FakeProvider(raises={"005930": SourceAuthError("token rejected")})

    result = _run(storage, provider, capture_status=True)

    assert result.aborted_reason == "SourceAuthError"
    assert [row.ticker for row in storage.status_rows] == ["000040", "000660"]
    assert storage.last_run.status is RunStatus.FAILED
    assert storage.last_run.counts["status_rows_inserted"] == 2


def test_status_is_only_taken_from_the_foreign_holding_group() -> None:
    class InvestorProvider(FakeProvider):
        def fetch_investor_net_volume(self, ticker, market, start, end):
            return SecurityFlowFetchResult(status_record=_status(ticker))

    storage = FakeStorage(_stocks("005930"))
    result = sync_kis_security_flows(
        InvestorProvider(),
        storage,
        start=SESSION,
        end=SESSION,
        enabled_flow_groups=["investor"],
        retry_max_attempts=1,
        guard_backoff_seconds=0.0,
        capture_status=True,
    )

    assert result.status_attempted == 0 and storage.status_batches == []

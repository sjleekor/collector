"""KIS 종목 상태 플래그 스냅샷(4.2-b): 파서, 어댑터, 서비스.

fixture는 tests/fixtures/kis_status/. 필드 이름은 2026-09-27 probe에서 왔고, 값은 손으로 썼다
(양성 사례 둘은 합성이다). 실제 KIS는 부르지 않는다.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from collector.kr.adapters.flows_kis.parsers import KisFieldError
from collector.kr.adapters.kis_common.client import KisClient
from collector.kr.adapters.kis_common.token import KisAccessToken, KisTokenProvider
from collector.kr.adapters.kis_stock_status import (
    KisStockStatusProvider,
    parse_stock_status_row,
)
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
    Stock,
    StockStatusFetchResult,
    StockStatusRecord,
    UpsertResult,
)
from collector.kr.service.sync_kis_stock_status import (
    is_market_warning,
    sync_kis_stock_status,
)
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError
from collector.kr.util.rate_limit import TokenBucket

KST = timezone(timedelta(hours=9))
TODAY = date(2026, 10, 8)
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "kis_status"


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


# --- parser ------------------------------------------------------------------


def test_normal_row_is_all_clear() -> None:
    row = _fixture("inquire_price_normal")["output"]
    record = parse_stock_status_row(row, ticker="5930", market=Market.KOSPI, snapshot_date=TODAY)

    assert record.ticker == "005930"
    assert (record.mang_issu_cls_code, record.temp_stop_yn, record.mrkt_warn_cls_code) == (
        "N",
        "N",
        "00",
    )
    assert record.snapshot_date == TODAY
    assert record.source is Source.KIS
    assert record.iscd_stat_cls_code == "55"


@pytest.mark.parametrize(
    ("name", "field", "expected"),
    [
        ("inquire_price_managed", "mang_issu_cls_code", "Y"),
        ("inquire_price_temp_stop", "temp_stop_yn", "Y"),
        ("inquire_price_market_warning", "mrkt_warn_cls_code", "02"),
    ],
)
def test_positive_rows_keep_the_raw_code(name: str, field: str, expected: str) -> None:
    row = _fixture(name)["output"]
    record = parse_stock_status_row(row, ticker="000040", market=Market.KOSPI, snapshot_date=TODAY)
    assert getattr(record, field) == expected


def test_raw_payload_holds_only_the_status_fields() -> None:
    row = _fixture("inquire_price_managed")["output"]
    record = parse_stock_status_row(row, ticker="000040", market=Market.KOSPI, snapshot_date=TODAY)

    assert record.raw_payload["mang_issu_cls_code"] == "Y"
    assert record.raw_payload["marg_rate"] == "100.00"
    assert "stck_prpr" not in record.raw_payload
    assert "per" not in record.raw_payload


@pytest.mark.parametrize("missing", ["mang_issu_cls_code", "temp_stop_yn", "mrkt_warn_cls_code"])
def test_a_missing_required_field_fails_the_ticker(missing: str) -> None:
    # 필드가 사라진 응답을 NULL로 저장하면 "플래그 없음"으로 읽힌다.
    row = dict(_fixture("inquire_price_normal")["output"])
    del row[missing]
    with pytest.raises(KisFieldError, match=missing):
        parse_stock_status_row(row, ticker="005930", market=Market.KOSPI, snapshot_date=TODAY)


def test_a_blank_flag_is_not_filed_as_clean() -> None:
    row = {**_fixture("inquire_price_normal")["output"], "mang_issu_cls_code": "  "}
    with pytest.raises(KisFieldError, match="empty"):
        parse_stock_status_row(row, ticker="005930", market=Market.KOSPI, snapshot_date=TODAY)


def test_optional_fields_may_be_absent() -> None:
    row = {
        key: value
        for key, value in _fixture("inquire_price_normal")["output"].items()
        if key not in ("invt_caful_yn", "short_over_yn", "sltr_yn", "iscd_stat_cls_code")
    }
    record = parse_stock_status_row(row, ticker="005930", market=Market.KOSPI, snapshot_date=TODAY)
    assert record.invt_caful_yn is None
    assert record.iscd_stat_cls_code is None


def test_market_warning_helper() -> None:
    assert not is_market_warning("00")
    assert not is_market_warning("")
    assert is_market_warning("01")
    assert is_market_warning("03")


# --- adapter over the real KisClient with a fake HTTP session -----------------


class _Response:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self.status_code = status_code
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
        self.calls.append({"url": url, "params": dict(params), "headers": dict(headers)})
        if not self._responses:
            raise AssertionError("unexpected extra KIS request")
        return self._responses.pop(0)


class _Cache:
    path = "stub"

    def __init__(self) -> None:
        self.token = KisAccessToken("cached-token", datetime.now(KST) + timedelta(hours=12))

    def load(self):
        return self.token

    def store(self, token) -> None:  # pragma: no cover - the cache is warm
        raise AssertionError("a warm cache must not trigger an issuance")


def _provider(responses: list[_Response]) -> tuple[KisStockStatusProvider, _Session]:
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
    return KisStockStatusProvider(client=client, snapshot_date=TODAY), session


def test_provider_issues_one_inquire_price_call_per_ticker() -> None:
    provider, session = _provider([_Response(_fixture("inquire_price_managed"))])

    result = provider.fetch_stock_status("000040", Market.KOSPI)

    assert result.error is None and result.record is not None
    assert result.record.mang_issu_cls_code == "Y"
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"].endswith("/uapi/domestic-stock/v1/quotations/inquire-price")
    assert call["params"] == {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "000040"}
    assert call["headers"]["tr_id"] == "FHKST01010100"
    assert provider.request_stats().http_requests == 1
    # 토큰은 캐시에서 왔다
    assert provider.request_stats().auth_token_issued == 0


def test_provider_reports_a_field_rename_as_an_error_not_as_clean() -> None:
    payload = _fixture("inquire_price_normal")
    del payload["output"]["mang_issu_cls_code"]
    provider, _ = _provider([_Response(payload)])

    result = provider.fetch_stock_status("005930", Market.KOSPI)

    assert result.record is None
    assert result.error and "mang_issu_cls_code" in result.error


def test_provider_treats_no_data_message_as_no_data() -> None:
    body = {"rt_cd": "1", "msg_cd": "X", "msg1": "조회할 자료가 없습니다.", "output": {}}
    provider, _ = _provider([_Response(body)])

    result = provider.fetch_stock_status("999999", Market.KOSPI)

    assert result.no_data and result.record is None and result.error is None


# --- service -------------------------------------------------------------------


def _stock(ticker: str, market: Market = Market.KOSPI) -> Stock:
    return Stock(
        ticker=ticker,
        market=market,
        name=f"name-{ticker}",
        status=ListingStatus.ACTIVE,
        last_seen_date=TODAY,
        source=Source.KRX,
    )


def _record(ticker: str, **flags: str) -> StockStatusRecord:
    return StockStatusRecord(
        snapshot_date=TODAY,
        ticker=ticker,
        market=Market.KOSPI,
        mang_issu_cls_code=flags.get("mang_issu_cls_code", "N"),
        temp_stop_yn=flags.get("temp_stop_yn", "N"),
        mrkt_warn_cls_code=flags.get("mrkt_warn_cls_code", "00"),
        observed_at=datetime(2026, 10, 8, 22, 20, tzinfo=KST),
        raw_payload={},
    )


class FakeProvider:
    def __init__(
        self,
        *,
        results: dict[str, StockStatusFetchResult] | None = None,
        raises: dict[str, Exception] | None = None,
    ) -> None:
        self.results = results or {}
        self.raises = raises or {}
        self.calls: list[str] = []

    def source(self) -> Source:
        return Source.KIS

    def request_stats(self) -> FlowRequestStats:
        return FlowRequestStats(http_requests=len(self.calls))

    def fetch_stock_status(self, ticker: str, market: Market) -> StockStatusFetchResult:
        self.calls.append(ticker)
        if ticker in self.raises:
            raise self.raises[ticker]
        if ticker in self.results:
            return self.results[ticker]
        return StockStatusFetchResult(record=_record(ticker))


class FakeStorage:
    def __init__(
        self, stocks: list[Stock], stored: set[str] | None = None, *, write_fails: bool = False
    ) -> None:
        self.stocks = stocks
        self.stored = set(stored or ())
        self.runs: list[IngestionRun] = []
        self.batches: list[list[StockStatusRecord]] = []
        self.write_fails = write_fails

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

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        return list(self.stocks)

    def get_stocks(self, market=None, tickers=None) -> list[Stock]:
        return list(self.stocks)

    def get_dart_corp_master(self, active_only=True, tickers=None, include_delisted=False):
        return []

    def get_kis_stock_status_tickers(self, snapshot_date: date) -> set[str]:
        return set(self.stored)

    def upsert_kis_stock_status(self, records: list[StockStatusRecord]) -> UpsertResult:
        if self.write_fails:
            raise RuntimeError("db down")
        self.batches.append(list(records))
        fresh = [record for record in records if record.ticker not in self.stored]
        self.stored.update(record.ticker for record in fresh)
        return UpsertResult(inserted=len(fresh))

    @property
    def last_run(self) -> IngestionRun:
        return self.runs[-1]


def _sync(storage: FakeStorage, provider: FakeProvider, **kwargs):
    return sync_kis_stock_status(
        provider,
        storage,
        snapshot_date=TODAY,
        scope=UniverseScope.CURRENT,
        retry_max_attempts=kwargs.pop("retry_max_attempts", 1),
        guard_backoff_seconds=0.0,
        **kwargs,
    )


def test_first_run_stores_one_row_per_target() -> None:
    storage = FakeStorage([_stock("000040"), _stock("005930"), _stock("035720", Market.KOSDAQ)])
    provider = FakeProvider()

    result = _sync(storage, provider)

    assert provider.calls == ["000040", "005930", "035720"]
    assert result.rows_inserted == 3
    assert storage.stored == {"000040", "005930", "035720"}
    assert result.errors == {} and result.aborted_reason is None
    assert storage.last_run.status is RunStatus.SUCCESS
    assert storage.last_run.run_type is RunType.KIS_STOCK_STATUS_SYNC
    assert storage.last_run.counts["rows_upserted"] == 3
    assert storage.last_run.counts["requests_attempted"] == 3
    assert all(
        record.run_id == storage.last_run.run_id for batch in storage.batches for record in batch
    )


def test_rerun_asks_only_for_tickers_not_stored_today() -> None:
    # 재처리 경로: 같은 명령을 다시 돌리면 구멍만 받는다.
    storage = FakeStorage(
        [_stock("000040"), _stock("005930"), _stock("035720")], stored={"000040", "005930"}
    )
    provider = FakeProvider()

    result = _sync(storage, provider)

    assert provider.calls == ["035720"]
    assert result.already_stored == 2
    assert result.requests_attempted == 1
    assert storage.last_run.params["planned_requests"] == 1


def test_a_fully_stored_day_sends_no_request() -> None:
    storage = FakeStorage([_stock("005930")], stored={"005930"})
    provider = FakeProvider()

    result = _sync(storage, provider)

    assert provider.calls == []
    assert result.requests_attempted == 0
    assert storage.last_run.status is RunStatus.SUCCESS


def test_failed_tickers_are_recorded_and_the_run_is_partial() -> None:
    storage = FakeStorage([_stock("000040"), _stock("005930"), _stock("035720")])
    provider = FakeProvider(results={"005930": StockStatusFetchResult(error="HTTP 500")})

    result = _sync(storage, provider)

    assert result.rows_inserted == 2
    assert result.errors == {"005930": "HTTP 500"}
    assert storage.last_run.status is RunStatus.PARTIAL
    assert storage.last_run.params["failed_request_keys"] == ["005930"]

    # 두 번째 실행: 실패 종목이 풀리면 그것만 받는다
    provider2 = FakeProvider()
    result2 = _sync(storage, provider2)
    assert provider2.calls == ["005930"]
    assert result2.rows_inserted == 1 and result2.errors == {}
    assert storage.stored == {"000040", "005930", "035720"}


def test_no_data_is_counted_and_listed_but_not_an_error() -> None:
    storage = FakeStorage([_stock("000040"), _stock("999999")])
    provider = FakeProvider(results={"999999": StockStatusFetchResult(no_data=True)})

    result = _sync(storage, provider)

    assert result.no_data_requests == 1
    assert result.errors == {}
    assert storage.last_run.status is RunStatus.SUCCESS
    assert storage.last_run.params["no_data_request_keys"] == ["999999"]
    assert "999999" not in storage.stored


def test_flag_counts_land_in_the_run_counts() -> None:
    storage = FakeStorage([_stock("000040"), _stock("111111"), _stock("222222"), _stock("005930")])
    provider = FakeProvider(
        results={
            "000040": StockStatusFetchResult(record=_record("000040", mang_issu_cls_code="Y")),
            "111111": StockStatusFetchResult(record=_record("111111", temp_stop_yn="Y")),
            "222222": StockStatusFetchResult(record=_record("222222", mrkt_warn_cls_code="02")),
        }
    )

    result = _sync(storage, provider)

    assert (result.flagged_managed, result.flagged_temp_stop, result.flagged_market_warning) == (
        1,
        1,
        1,
    )
    counts = storage.last_run.counts
    assert counts["flagged_managed"] == 1
    assert counts["flagged_temp_stop"] == 1
    assert counts["flagged_market_warning"] == 1


def test_consecutive_failures_abort_and_keep_what_was_fetched() -> None:
    tickers = [f"{n:06d}" for n in range(1, 11)]
    storage = FakeStorage([_stock(t) for t in tickers])
    provider = FakeProvider(
        results={t: StockStatusFetchResult(error="HTTP 500") for t in tickers[3:]}
    )

    result = _sync(storage, provider, max_consecutive_failures=3, flush_every=1000)

    assert result.aborted_reason == "SourceBlockedError"
    assert storage.stored == set(tickers[:3])  # 앞 셋은 abort 때 flush됐다
    assert storage.last_run.status is RunStatus.FAILED
    assert storage.last_run.params["failed_request_keys"] == tickers[3:6]
    assert len(provider.calls) == 6  # 나머지 넷은 부르지 않았다


@pytest.mark.parametrize(
    ("exc", "name"),
    [
        (SourceAuthError("token rejected"), "SourceAuthError"),
        (SourceQuotaExhaustedError("rate limited"), "SourceQuotaExhaustedError"),
    ],
)
def test_auth_and_quota_abort_without_retrying(exc: Exception, name: str) -> None:
    storage = FakeStorage([_stock("000040"), _stock("005930"), _stock("035720")])
    provider = FakeProvider(raises={"005930": exc})

    result = _sync(storage, provider, retry_max_attempts=3)

    assert result.aborted_reason == name
    assert provider.calls == ["000040", "005930"]  # 재시도·후속 종목 없음
    assert storage.stored == {"000040"}
    assert storage.last_run.status is RunStatus.FAILED


def test_rows_are_flushed_in_batches() -> None:
    tickers = [f"{n:06d}" for n in range(1, 8)]
    storage = FakeStorage([_stock(t) for t in tickers])

    _sync(storage, FakeProvider(), flush_every=3)

    assert [len(batch) for batch in storage.batches] == [3, 3, 1]


def test_a_storage_failure_fails_the_run() -> None:
    storage = FakeStorage([_stock("005930")], write_fails=True)

    result = _sync(storage, FakeProvider())

    assert result.aborted_reason == "RuntimeError"
    assert storage.last_run.status is RunStatus.FAILED
    assert "pipeline" in result.errors


def test_no_targets_fails_the_run() -> None:
    storage = FakeStorage([])

    result = _sync(storage, FakeProvider())

    assert result.aborted_reason == "RuntimeError"
    assert storage.last_run.status is RunStatus.FAILED

import io
import zipfile
from datetime import date

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.adapters.opendart_corp.provider import (
    OpenDartCorpCodeProvider,
    parse_corp_code_zip_bytes,
)
from collector.kr.domain.enums import ListingStatus, Market, RunStatus, RunType, Source
from collector.kr.domain.models import (
    DartCorp,
    DartCorpCodeResult,
    IngestionRun,
    Stock,
    UpsertResult,
)
from collector.kr.service.sync_dart_corp import sync_dart_corp_master
from collector.kr.util.time import now_kst
from tests.helpers.fake_opendart_executor import FakeOpenDartExecutor


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _make_urlopen(queue: list[bytes], urls: list[str]):
    def _fake_urlopen(url: str, timeout: float = 30.0) -> _FakeResponse:
        del timeout
        urls.append(url)
        return _FakeResponse(queue.pop(0))

    return _fake_urlopen


def _make_corp_code_zip(xml_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("CORPCODE.xml", xml_text)
    return buffer.getvalue()


def test_parse_corp_code_zip_bytes() -> None:
    payload = _make_corp_code_zip("""
        <result>
          <list>
            <corp_code>00126380</corp_code>
            <corp_name>삼성전자</corp_name>
            <stock_code>005930</stock_code>
            <modify_date>20240401</modify_date>
          </list>
          <list>
            <corp_code>00987654</corp_code>
            <corp_name>비상장예시</corp_name>
            <stock_code></stock_code>
            <modify_date>20240315</modify_date>
          </list>
        </result>
        """)

    records = parse_corp_code_zip_bytes(payload)

    assert len(records) == 2
    assert records[0].corp_code == "00126380"
    assert records[0].corp_name == "삼성전자"
    assert records[0].ticker == "005930"
    assert records[0].modify_date == date(2024, 4, 1)
    assert records[0].source == Source.OPENDART
    assert records[1].ticker is None
    assert records[1].modify_date == date(2024, 3, 15)


def test_open_dart_corp_code_provider_uses_executor_payload() -> None:
    provider = OpenDartCorpCodeProvider(
        request_executor=FakeOpenDartExecutor([_make_corp_code_zip("""
                    <result>
                      <list>
                        <corp_code>00126380</corp_code>
                        <corp_name>삼성전자</corp_name>
                        <stock_code>005930</stock_code>
                        <modify_date>20240401</modify_date>
                      </list>
                    </result>
                    """)])
    )

    result = provider.fetch_corp_codes()

    assert result.error is None
    assert len(result.records) == 1
    assert result.records[0].ticker == "005930"


def test_open_dart_corp_code_provider_maps_body_error_as_error() -> None:
    provider = OpenDartCorpCodeProvider(
        request_executor=FakeOpenDartExecutor(
            [
                (
                    "<result><status>010</status>"
                    "<message>등록되지 않은 키입니다.</message></result>"
                ).encode(),
            ]
        )
    )

    result = provider.fetch_corp_codes()

    assert result.records == []
    assert result.error == "OpenDART error 010: 등록되지 않은 키입니다."


class MockCorpCodeProvider:
    def fetch_corp_codes(self) -> DartCorpCodeResult:
        fetched_at = now_kst()
        return DartCorpCodeResult(
            records=[
                DartCorp(
                    corp_code="00126380",
                    corp_name="삼성전자",
                    ticker="005930",
                    market=None,
                    stock_name="삼성전자",
                    modify_date=date(2024, 4, 1),
                    is_active=False,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                ),
                DartCorp(
                    corp_code="00999999",
                    corp_name="상장폐지예시",
                    ticker="999999",
                    market=None,
                    stock_name="상장폐지예시",
                    modify_date=date(2024, 4, 1),
                    is_active=False,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                ),
            ]
        )


class MockStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.saved_records: list[DartCorp] = []
        self.last_successful_corp_run: IngestionRun | None = None

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_last_successful_run(self, run_type: RunType) -> IngestionRun | None:
        if run_type == RunType.DART_CORP_SYNC:
            return self.last_successful_corp_run
        return None

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        stocks = [
            Stock(
                ticker="005930",
                market=Market.KOSPI,
                name="삼성전자",
                status=ListingStatus.ACTIVE,
                last_seen_date=date(2024, 4, 19),
                source=Source.FDR,
            ),
            Stock(
                ticker="000660",
                market=Market.KOSPI,
                name="SK하이닉스",
                status=ListingStatus.ACTIVE,
                last_seen_date=date(2024, 4, 19),
                source=Source.FDR,
            ),
        ]
        if market is None:
            return stocks
        return [stock for stock in stocks if stock.market == market]

    def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
        self.saved_records = records
        return UpsertResult(updated=len(records))


def test_sync_dart_corp_master_matches_active_tickers_and_reports_gaps() -> None:
    storage = MockStorage()
    result = sync_dart_corp_master(provider=MockCorpCodeProvider(), storage=storage)

    assert result.error is None
    assert result.total_records == 2
    assert result.matched_active_tickers == 1
    assert result.unmatched_active_tickers == ["000660"]
    assert result.unmatched_dart_tickers == ["999999"]

    matched_record = next(record for record in storage.saved_records if record.ticker == "005930")
    assert matched_record.market == Market.KOSPI
    assert matched_record.stock_name == "삼성전자"
    assert matched_record.is_active is True

    assert storage.runs[0].run_type == RunType.DART_CORP_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_dart_corp_master_skips_when_previous_success_recorded() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        status=RunStatus.SUCCESS,
    )
    provider = _ExhaustedCorpProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert result.error is None
    assert provider.calls == 0
    assert storage.saved_records == []
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert storage.runs[-1].counts["skipped_existing"] == 1


def test_sync_dart_corp_master_force_re_runs_even_with_previous_success() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        status=RunStatus.SUCCESS,
    )

    result = sync_dart_corp_master(
        provider=MockCorpCodeProvider(),
        storage=storage,
        force=True,
    )

    assert result.error is None
    assert result.total_records == 2
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert storage.runs[-1].counts.get("skipped_existing") is None


class _ExhaustedCorpProvider:
    """Minimal provider that returns an ``all_disabled`` exhaustion once."""

    def __init__(self) -> None:
        self.calls = 0

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        self.calls += 1
        return DartCorpCodeResult(
            error="All OpenDART API keys are disabled.",
            status_code=None,
            retryable=False,
            exhaustion_reason="all_disabled",
        )


def test_sync_dart_corp_master_records_exhaustion_reason_on_failure() -> None:
    storage = MockStorage()
    provider = _ExhaustedCorpProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert result.error is not None
    assert "exhaustion_reason=all_disabled" in result.error
    assert storage.runs[-1].status == RunStatus.FAILED
    assert "exhaustion_reason=all_disabled" in (storage.runs[-1].error_summary or "")
    # Non-retryable exhaustion should not re-enter the provider.
    assert provider.calls == 1


def test_sync_dart_corp_master_records_executor_metrics_on_failure() -> None:
    storage = MockStorage()
    provider = OpenDartCorpCodeProvider(
        request_executor=OpenDartRequestExecutor(
            ["key-a"],
            urlopen_fn=_make_urlopen(
                [
                    (
                        "<result><status>010</status>"
                        "<message>등록되지 않은 키입니다.</message></result>"
                    ).encode()
                ],
                [],
            ),
            sleep_fn=lambda _: None,
        )
    )

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert result.error is not None
    assert storage.runs[-1].status == RunStatus.FAILED
    assert storage.runs[-1].counts == {
        "key_rotation_count": 0,
        "key_disable_count": 1,
        "rate_limit_count": 0,
        "key_effective_use_count": 0,
        "request_invalid_count": 0,
        "all_rate_limited_count": 0,
        "all_disabled_count": 1,
        "retryable_error_count": 0,
        "terminal_error_count": 1,
    }

from datetime import date

import pytest

from collector.kr.adapters.opendart_filings.provider import (
    OpenDartFilingReceiptProvider,
    parse_filing_receipt_page,
)
from collector.kr.domain.enums import Market, RunStatus, Source
from collector.kr.domain.models import (
    DartCorp,
    DartFilingReceiptLine,
    DartFilingReceiptResult,
    IngestionRun,
    UpsertResult,
)
from collector.kr.service.sync_dart_filings import sync_dart_filings
from collector.kr.util.time import now_kst
from tests.helpers.fake_opendart_executor import FakeOpenDartExecutor


def _sample_corp() -> DartCorp:
    return DartCorp(
        corp_code="00126380",
        corp_name="삼성전자",
        ticker="005930",
        market=Market.KOSPI,
        stock_name="삼성전자",
        modify_date=date(2026, 3, 10),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def _provider(
    executor: FakeOpenDartExecutor,
    sleeps: list[float] | None = None,
    page_delay_seconds: float = 0.2,
) -> OpenDartFilingReceiptProvider:
    """Build a provider that records its page delays instead of sleeping."""
    recorded = sleeps if sleeps is not None else []
    return OpenDartFilingReceiptProvider(
        request_executor=executor,
        page_delay_seconds=page_delay_seconds,
        sleep_fn=recorded.append,
    )


def _page_payload(
    *, page_no: int, total_page: int, total_count: int, rcept_nos: list[str]
) -> bytes:
    import json

    return json.dumps(
        {
            "status": "000",
            "message": "정상",
            "page_no": page_no,
            "total_page": total_page,
            "total_count": total_count,
            "list": [
                {
                    "corp_code": "00126380",
                    "corp_name": "삼성전자",
                    "stock_code": "005930",
                    "corp_cls": "Y",
                    "report_nm": "사업보고서",
                    "rcept_no": rcept_no,
                    "flr_nm": "삼성전자",
                    "rcept_dt": "20260310",
                    "rm": "",
                }
                for rcept_no in rcept_nos
            ],
        }
    ).encode()


def test_parse_filing_receipt_page() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "page_no": 1,
        "total_page": 1,
        "total_count": 1,
        "list": [
            {
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_code": "005930",
                "corp_cls": "Y",
                "report_nm": "사업보고서",
                "rcept_no": "20260310002820",
                "flr_nm": "삼성전자",
                "rcept_dt": "20260310",
                "rm": "",
            }
        ],
    }

    records, page_no, total_page, total_count = parse_filing_receipt_page(payload, corp)

    assert page_no == 1
    assert total_page == 1
    assert total_count == 1
    assert len(records) == 1
    assert records[0].rcept_dt == date(2026, 3, 10)
    assert records[0].report_nm == "사업보고서"


def test_provider_single_page() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        [_page_payload(page_no=1, total_page=1, total_count=1, rcept_nos=["r1"])]
    )
    provider = _provider(executor)

    result = provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    assert result.error is None
    assert len(result.records) == 1
    assert len(executor.calls) == 1


def test_provider_paginates_across_pages() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        [
            _page_payload(page_no=1, total_page=2, total_count=2, rcept_nos=["r1"]),
            _page_payload(page_no=2, total_page=2, total_count=2, rcept_nos=["r2"]),
        ]
    )
    provider = _provider(executor)

    result = provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    assert result.error is None
    assert [record.rcept_no for record in result.records] == ["r1", "r2"]
    assert len(executor.calls) == 2
    assert executor.calls[1].params["page_no"] == "2"


def test_provider_sleeps_between_pages_but_not_after_the_last() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        [
            _page_payload(page_no=1, total_page=3, total_count=3, rcept_nos=["r1"]),
            _page_payload(page_no=2, total_page=3, total_count=3, rcept_nos=["r2"]),
            _page_payload(page_no=3, total_page=3, total_count=3, rcept_nos=["r3"]),
        ]
    )
    sleeps: list[float] = []
    provider = _provider(executor, sleeps, page_delay_seconds=0.5)

    result = provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    assert result.error is None
    assert len(executor.calls) == 3
    # Three pages leave two gaps; the window returns without a trailing sleep.
    assert sleeps == [0.5, 0.5]


def test_provider_does_not_sleep_after_a_failing_page() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        [
            _page_payload(page_no=1, total_page=3, total_count=3, rcept_nos=["r1"]),
            b'{"status":"800","message":"server error"}',
        ]
    )
    sleeps: list[float] = []
    provider = _provider(executor, sleeps, page_delay_seconds=0.5)

    provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    # Only the gap before the failing page 2 — the error path returns at once.
    assert sleeps == [0.5]


def test_provider_page_delay_can_be_disabled() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        [
            _page_payload(page_no=1, total_page=2, total_count=2, rcept_nos=["r1"]),
            _page_payload(page_no=2, total_page=2, total_count=2, rcept_nos=["r2"]),
        ]
    )
    sleeps: list[float] = []
    provider = _provider(executor, sleeps, page_delay_seconds=0.0)

    provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    assert sleeps == []


def test_provider_maps_no_data_on_first_page() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        ['{"status":"013","message":"조회된 데이타가 없습니다."}'.encode()]
    )
    provider = _provider(executor)

    result = provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    assert result.no_data is True
    assert result.records == []
    assert result.error is None


def test_provider_drops_partial_records_on_mid_pagination_error() -> None:
    corp = _sample_corp()
    executor = FakeOpenDartExecutor(
        [
            _page_payload(page_no=1, total_page=2, total_count=2, rcept_nos=["r1"]),
            b'{"status":"800","message":"server error"}',
        ]
    )
    provider = _provider(executor)

    result = provider.fetch_filing_receipts(corp, date(2026, 1, 1), date(2026, 12, 31))

    assert result.error is not None
    assert result.records == []


class MockFilingReceiptProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, date, date]] = []

    def fetch_filing_receipts(
        self,
        corp: DartCorp,
        bgn_de: date,
        end_de: date,
    ) -> DartFilingReceiptResult:
        self.calls.append((corp.corp_code, bgn_de, end_de))
        return DartFilingReceiptResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bgn_de=bgn_de,
            end_de=end_de,
            records=[
                DartFilingReceiptLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    corp_name="삼성전자",
                    stock_code="005930",
                    corp_cls="Y",
                    report_nm="사업보고서",
                    rcept_no=f"r-{bgn_de.year}",
                    flr_nm="삼성전자",
                    rcept_dt=bgn_de,
                    rm="",
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={},
                )
            ],
        )


class MockFilingReceiptStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.rows: list[DartFilingReceiptLine] = []
        self.existing_years: set[tuple[str, int]] = set()

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        records = [_sample_corp()]
        if tickers is None:
            return records
        return [record for record in records if record.ticker in tickers]

    def get_existing_dart_filing_receipt_years(
        self,
        years: list[int],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int]]:
        return {
            key
            for key in self.existing_years
            if key[1] in years and (corp_codes is None or key[0] in corp_codes)
        }

    def upsert_dart_filing_receipt_raw(
        self,
        records: list[DartFilingReceiptLine],
    ) -> UpsertResult:
        self.rows.extend(records)
        return UpsertResult(updated=len(records))


def test_sync_dart_filings_counts_results() -> None:
    storage = MockFilingReceiptStorage()
    provider = MockFilingReceiptProvider()

    result = sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=[2025],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        today=date(2026, 8, 10),
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert result.rows_upserted == 1
    assert len(provider.calls) == 1
    assert provider.calls[0][1] == date(2025, 1, 1)
    assert provider.calls[0][2] == date(2025, 12, 31)
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_dart_filings_skips_existing_past_year() -> None:
    storage = MockFilingReceiptStorage()
    storage.existing_years.add(("00126380", 2024))
    provider = MockFilingReceiptProvider()

    result = sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=[2024],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        today=date(2026, 8, 10),
    )

    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert provider.calls == []


def test_sync_dart_filings_never_skips_current_year() -> None:
    storage = MockFilingReceiptStorage()
    storage.existing_years.add(("00126380", 2026))
    provider = MockFilingReceiptProvider()

    result = sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=[2026],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        today=date(2026, 8, 10),
    )

    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert provider.calls[0][2] == date(2026, 8, 10)


def test_sync_dart_filings_limits_current_year_to_incremental_lookback() -> None:
    storage = MockFilingReceiptStorage()
    provider = MockFilingReceiptProvider()

    result = sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=[2026],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        today=date(2026, 8, 10),
        lookback_days=14,
    )

    assert result.errors == {}
    assert provider.calls[0][1] == date(2026, 7, 27)
    assert provider.calls[0][2] == date(2026, 8, 10)
    assert storage.runs[-1].params["lookback_days"] == 14


def test_sync_dart_filings_keeps_past_year_as_full_window_with_lookback() -> None:
    storage = MockFilingReceiptStorage()
    provider = MockFilingReceiptProvider()

    sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=[2025],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        today=date(2026, 8, 10),
        lookback_days=14,
    )

    assert provider.calls[0][1] == date(2025, 1, 1)
    assert provider.calls[0][2] == date(2025, 12, 31)


def test_sync_dart_filings_rejects_negative_lookback() -> None:
    storage = MockFilingReceiptStorage()
    provider = MockFilingReceiptProvider()

    with pytest.raises(ValueError, match="lookback_days must be >= 0"):
        sync_dart_filings(
            filing_receipt_provider=provider,
            storage=storage,
            years=[2026],
            tickers=["005930"],
            rate_limit_seconds=0.0,
            today=date(2026, 8, 10),
            lookback_days=-1,
        )

    assert provider.calls == []
    assert storage.runs == []


def test_sync_dart_filings_force_bypasses_existing_check() -> None:
    storage = MockFilingReceiptStorage()
    storage.existing_years.add(("00126380", 2024))
    provider = MockFilingReceiptProvider()

    result = sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=[2024],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        force=True,
        today=date(2026, 8, 10),
    )

    assert result.requests_attempted == 1
    assert len(provider.calls) == 1

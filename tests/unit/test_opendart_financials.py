from datetime import date
from decimal import Decimal

from collector.kr.adapters.opendart_financials.provider import (
    OpenDartFinancialStatementProvider,
    parse_fnltt_singl_acnt_all_response,
)
from collector.kr.domain.enums import Market, RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartFinancialStatementResult,
    IngestionRun,
    UpsertResult,
)
from collector.kr.service.sync_dart_financials import sync_dart_financial_statements
from collector.kr.util.time import now_kst
from tests.helpers.fake_opendart_executor import FakeOpenDartExecutor
from tests.helpers.fake_slice_storage import FakeSliceStorageMixin


def _sample_corp() -> DartCorp:
    return DartCorp(
        corp_code="00126380",
        corp_name="삼성전자",
        ticker="005930",
        market=Market.KOSPI,
        stock_name="삼성전자",
        modify_date=date(2024, 4, 1),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def test_parse_fnltt_singl_acnt_all_response_success() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20240312000123",
                "sj_div": "BS",
                "sj_nm": "재무상태표",
                "account_id": "ifrs-full_Assets",
                "account_nm": "자산총계",
                "account_detail": "",
                "thstrm_nm": "제 56 기말",
                "thstrm_amount": "455,905,980,000,000",
                "thstrm_add_amount": "",
                "frmtrm_nm": "제 55 기말",
                "frmtrm_amount": "448,424,507,000,000",
                "frmtrm_q_nm": "",
                "frmtrm_q_amount": "",
                "frmtrm_add_amount": "",
                "bfefrmtrm_nm": "제 54 기말",
                "bfefrmtrm_amount": "426,621,158,000,000",
                "ord": "1",
                "currency": "KRW",
            }
        ],
    }

    result = parse_fnltt_singl_acnt_all_response(payload, corp, 2025, "11011", "CFS")

    assert result.error is None
    assert result.no_data is False
    assert len(result.records) == 1
    line = result.records[0]
    assert line.ticker == "005930"
    assert line.sj_div == "BS"
    assert line.account_id == "ifrs-full_Assets"
    assert line.thstrm_amount == Decimal("455905980000000")
    assert line.frmtrm_amount == Decimal("448424507000000")
    assert line.bfefrmtrm_amount == Decimal("426621158000000")
    assert line.ord == 1


def test_parse_fnltt_singl_acnt_all_response_empty_list() -> None:
    corp = _sample_corp()
    payload = {"status": "000", "message": "정상"}

    result = parse_fnltt_singl_acnt_all_response(payload, corp, 2025, "11011", "CFS")

    assert result.error is None
    assert result.records == []


def test_open_dart_financial_provider_maps_rate_limited_result() -> None:
    corp = _sample_corp()
    provider = OpenDartFinancialStatementProvider(
        request_executor=FakeOpenDartExecutor(
            [
                '{"status":"020","message":"요청 제한을 초과하였습니다."}'.encode(),
            ]
        )
    )

    result = provider.fetch_financial_statement(corp, 2025, "11011", "CFS")

    assert result.error == "OpenDART error 020: 요청 제한을 초과하였습니다."
    assert result.status_code == "020"
    assert result.retryable is True


def test_open_dart_financial_provider_maps_no_data_result() -> None:
    corp = _sample_corp()
    provider = OpenDartFinancialStatementProvider(
        request_executor=FakeOpenDartExecutor(
            [
                '{"status":"013","message":"조회된 데이타가 없습니다."}'.encode(),
            ]
        )
    )

    result = provider.fetch_financial_statement(corp, 2025, "11011", "CFS")

    assert result.no_data is True
    assert result.error is None
    assert result.status_code == "013"
    assert result.retryable is False


def test_open_dart_financial_provider_maps_file_missing_as_request_invalid() -> None:
    corp = _sample_corp()
    provider = OpenDartFinancialStatementProvider(
        request_executor=FakeOpenDartExecutor(
            [
                '{"status":"014","message":"파일이 존재하지 않습니다."}'.encode(),
            ]
        )
    )

    result = provider.fetch_financial_statement(corp, 2025, "11011", "CFS")

    assert result.no_data is False
    assert result.error == "OpenDART error 014: 파일이 존재하지 않습니다."
    assert result.status_code == "014"
    assert result.retryable is False
    assert result.exhaustion_reason == "request_invalid"


class MockFinancialProvider:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        self.calls += 1
        if fs_div == "OFS":
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                no_data=True,
            )
        return DartFinancialStatementResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
            records=[
                DartFinancialStatementLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    fs_div=fs_div,
                    sj_div="IS",
                    sj_nm="손익계산서",
                    account_id="ifrs-full_Revenue",
                    account_nm="매출액",
                    account_detail="",
                    thstrm_nm="제 56 기",
                    thstrm_amount=Decimal("300"),
                    thstrm_add_amount=Decimal("1200"),
                    frmtrm_nm="제 55 기",
                    frmtrm_amount=Decimal("250"),
                    frmtrm_q_nm="",
                    frmtrm_q_amount=None,
                    frmtrm_add_amount=Decimal("1000"),
                    bfefrmtrm_nm="제 54 기",
                    bfefrmtrm_amount=Decimal("900"),
                    ord=1,
                    currency="KRW",
                    rcept_no="20240312000123",
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"account_nm": "매출액"},
                )
            ],
        )


class MockFinancialStorage(FakeSliceStorageMixin):
    def __init__(self) -> None:
        super().__init__()
        self.runs: list[IngestionRun] = []
        self.upserts: list[DartFinancialStatementLine] = []
        self.existing_financial_requests: set[tuple[str, int, str, str]] = set()

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

    def get_existing_dart_financial_statement_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        fs_divs: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        return {
            key
            for key in self.existing_financial_requests
            if key[1] in bsns_years
            and key[2] in reprt_codes
            and key[3] in fs_divs
            and (corp_codes is None or key[0] in corp_codes)
        }

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        self.upserts.extend(records)
        return UpsertResult(updated=len(records))


class _RetryTrackingFinancialProvider:
    """Provider that counts calls per key and configurably varies result flags."""

    def __init__(
        self,
        *,
        retryable_attempts: int,
        final_error: str = "done",
        final_retryable: bool = False,
        final_exhaustion_reason: str | None = None,
    ) -> None:
        self._retryable_attempts = retryable_attempts
        self._final_error = final_error
        self._final_retryable = final_retryable
        self._final_exhaustion_reason = final_exhaustion_reason
        self.calls = 0

    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        self.calls += 1
        if self.calls <= self._retryable_attempts:
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error="rate limited",
                status_code="020",
                retryable=True,
            )
        return DartFinancialStatementResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
            error=self._final_error,
            retryable=self._final_retryable,
            exhaustion_reason=self._final_exhaustion_reason,
        )


def test_sync_dart_financial_statements_retries_on_retryable_flag() -> None:
    storage = MockFinancialStorage()
    provider = _RetryTrackingFinancialProvider(retryable_attempts=1)

    sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert provider.calls == 2


def test_sync_dart_financial_statements_does_not_retry_request_invalid() -> None:
    storage = MockFinancialStorage()
    provider = _RetryTrackingFinancialProvider(
        retryable_attempts=0,
        final_error="OpenDART error 100: bad field",
        final_exhaustion_reason="request_invalid",
    )

    sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert provider.calls == 1


def test_sync_dart_financial_statements_counts_success_and_no_data() -> None:
    storage = MockFinancialStorage()
    provider = MockFinancialProvider()
    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS", "OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 2
    assert result.requests_skipped == 0
    assert provider.calls == 2
    assert result.rows_upserted == 1
    assert result.no_data_requests == 1
    assert len(storage.upserts) == 1
    assert storage.runs[0].run_type == RunType.DART_FINANCIAL_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_dart_financial_statements_skips_existing_raw_request() -> None:
    storage = MockFinancialStorage()
    storage.existing_financial_requests.add(("00126380", 2025, "11011", "CFS"))
    provider = MockFinancialProvider()

    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert provider.calls == 0
    assert storage.upserts == []


def test_sync_dart_financial_statements_force_bypasses_existing_check() -> None:
    storage = MockFinancialStorage()
    storage.existing_financial_requests.add(("00126380", 2025, "11011", "CFS"))
    provider = MockFinancialProvider()

    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        force=True,
    )

    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert provider.calls == 1


def test_sync_dart_financial_statements_persists_and_skips_no_data() -> None:
    storage = MockFinancialStorage()
    provider = MockFinancialProvider()

    first = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        scope=UniverseScope.HISTORICAL,
    )
    second = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        scope=UniverseScope.HISTORICAL,
    )

    assert first.no_data_requests == 1
    assert second.requests_attempted == 0
    assert second.requests_skipped == 1
    assert provider.calls == 1
    state = storage.slice_states[(Source.OPENDART, "fnlttSinglAcntAll", "00126380:2025:11011:OFS")]
    assert state.status.value == "no_data"

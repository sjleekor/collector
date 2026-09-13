from dataclasses import dataclass
from datetime import date

import pytest

from collector.kr.domain.enums import Market, RunStatus, RunType, Source
from collector.kr.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartFinancialStatementResult,
    IngestionRun,
    UpsertResult,
)
from collector.kr.service.sync_dart_financials import sync_dart_financial_statements
from collector.kr.util.pipeline import (
    NON_RETRYABLE_EXCEPTIONS,
    OpenDartKeyExhaustedError,
    SourceAuthError,
    SourceBlockedError,
    SourceQuotaExhaustedError,
    call_with_retry,
    complete_run,
    should_retry_opendart_result,
)
from collector.kr.util.time import now_kst
from tests.helpers.fake_slice_storage import FakeSliceStorageMixin


@dataclass(slots=True)
class RetryProbe:
    error: str | None = None


class RecordingStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)


class PartialFinancialProvider:
    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        if fs_div == "OFS":
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error="temporary upstream failure",
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
                    thstrm_amount=None,
                    thstrm_add_amount=None,
                    frmtrm_nm="",
                    frmtrm_amount=None,
                    frmtrm_q_nm="",
                    frmtrm_q_amount=None,
                    frmtrm_add_amount=None,
                    bfefrmtrm_nm="",
                    bfefrmtrm_amount=None,
                    ord=1,
                    currency="KRW",
                    rcept_no="20260419000001",
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"account_nm": "매출액"},
                )
            ],
        )


class ExhaustedFinancialProvider:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        del corp, bsns_year, reprt_code, fs_div
        self.calls += 1
        return DartFinancialStatementResult(
            error="All OpenDART API keys are temporarily rate limited.",
            retryable=True,
            exhaustion_reason="all_rate_limited",
        )


class PartialFinancialStorage(RecordingStorage, FakeSliceStorageMixin):
    def __init__(self) -> None:
        RecordingStorage.__init__(self)
        FakeSliceStorageMixin.__init__(self)
        self.upserts: list[DartFinancialStatementLine] = []

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        corp = DartCorp(
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
        return [corp]

    def get_existing_dart_financial_statement_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        fs_divs: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        return set()

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        self.upserts.extend(records)
        return UpsertResult(updated=len(records))


def test_call_with_retry_retries_result_errors_until_success() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def operation() -> RetryProbe:
        attempts["count"] += 1
        if attempts["count"] < 3:
            return RetryProbe(error="try again")
        return RetryProbe(error=None)

    result = call_with_retry(
        operation,
        request_label="retry-probe",
        sleep_fn=sleeps.append,
    )

    assert result.error is None
    assert attempts["count"] == 3
    assert sleeps == [0.5, 1.0]


def test_call_with_retry_returns_last_result_after_max_attempts() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def operation() -> RetryProbe:
        attempts["count"] += 1
        return RetryProbe(error="keeps failing")

    result = call_with_retry(
        operation,
        request_label="exhausting-probe",
        max_attempts=3,
        sleep_fn=sleeps.append,
    )

    assert result.error == "keeps failing"
    assert attempts["count"] == 3
    assert sleeps == [0.5, 1.0]


@dataclass(slots=True)
class _OpenDartProbe:
    error: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


def test_should_retry_opendart_result_retries_on_retryable_flag() -> None:
    assert should_retry_opendart_result(_OpenDartProbe(error="e", retryable=True)) is True


def test_should_retry_opendart_result_stops_on_all_rate_limited() -> None:
    probe = _OpenDartProbe(error="e", exhaustion_reason="all_rate_limited")
    assert should_retry_opendart_result(probe) is False


def test_should_retry_opendart_result_does_not_retry_request_invalid() -> None:
    probe = _OpenDartProbe(error="e", exhaustion_reason="request_invalid")
    assert should_retry_opendart_result(probe) is False


def test_should_retry_opendart_result_does_not_retry_all_disabled() -> None:
    probe = _OpenDartProbe(error="e", exhaustion_reason="all_disabled")
    assert should_retry_opendart_result(probe) is False


def test_call_with_retry_respects_should_retry_result_predicate() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def operation() -> RetryProbe:
        attempts["count"] += 1
        return RetryProbe(error="stop immediately")

    result = call_with_retry(
        operation,
        request_label="no-retry-probe",
        sleep_fn=sleeps.append,
        should_retry_result=lambda _: False,
    )

    assert result.error == "stop immediately"
    assert attempts["count"] == 1
    assert sleeps == []


def test_call_with_retry_uses_result_retry_after_seconds() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def operation() -> _OpenDartProbe:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return _OpenDartProbe(
                error="all keys cooling down",
                retryable=True,
                retry_after_seconds=12.5,
            )
        return _OpenDartProbe(error=None)

    result = call_with_retry(
        operation,
        request_label="retry-after-probe",
        sleep_fn=sleeps.append,
        should_retry_result=should_retry_opendart_result,
    )

    assert result.error is None
    assert attempts["count"] == 2
    assert sleeps == [12.5]


def test_complete_run_marks_partial_and_extends_counts() -> None:
    storage = RecordingStorage()
    run = IngestionRun(run_type=RunType.DART_FINANCIAL_SYNC, started_at=now_kst())

    complete_run(
        storage,  # type: ignore[arg-type]
        run,
        counts={"requests_attempted": 5, "rows_upserted": 4},
        errors={"005930:2025:11011:OFS": "temporary upstream failure"},
        partial_subject="financial sync requests",
    )

    assert storage.runs[-1].status == RunStatus.PARTIAL
    assert storage.runs[-1].counts == {
        "requests_attempted": 5,
        "rows_upserted": 4,
        "error_count": 1,
        "partial_failure_count": 1,
        "completed_request_count": 4,
    }
    assert "financial sync requests had errors" in (storage.runs[-1].error_summary or "")


def test_sync_dart_financial_statements_marks_partial_run() -> None:
    storage = PartialFinancialStorage()

    result = sync_dart_financial_statements(
        provider=PartialFinancialProvider(),
        storage=storage,  # type: ignore[arg-type]
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS", "OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.requests_attempted == 2
    assert result.rows_upserted == 1
    assert result.errors == {"005930:2025:11011:OFS": "temporary upstream failure"}
    assert storage.runs[-1].status == RunStatus.PARTIAL
    assert storage.runs[-1].counts == {
        "targets_processed": 1,
        "requests_attempted": 2,
        "requests_skipped": 0,
        "rows_upserted": 1,
        "no_data_requests": 0,
        "slices_skipped_no_data": 0,
        "error_count": 1,
        "partial_failure_count": 1,
        "completed_request_count": 1,
    }


def test_sync_dart_financial_statements_stops_on_all_keys_rate_limited() -> None:
    storage = PartialFinancialStorage()
    provider = ExhaustedFinancialProvider()

    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,  # type: ignore[arg-type]
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS", "OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert provider.calls == 1
    assert result.opendart_exhaustion_reason == "all_rate_limited"
    assert result.errors == {"pipeline": "All OpenDART API keys are temporarily rate limited."}
    assert storage.runs[-1].status == RunStatus.FAILED


def test_call_with_retry_never_retries_a_terminal_source_condition() -> None:
    # None of these are transient: the source is out of quota, has refused our
    # credentials, or has started blocking. Retrying multiplies the condition —
    # and for KIS an auth retry can cost a token issuance, which notifies the
    # account holder once per attempt.
    for error in (
        SourceAuthError("credentials rejected"),
        SourceQuotaExhaustedError("quota spent"),
        SourceBlockedError("source refusing"),
        OpenDartKeyExhaustedError("all keys rate limited"),
    ):
        calls = 0

        def operation(error=error):
            nonlocal calls
            calls += 1
            raise error

        try:
            call_with_retry(
                operation,
                request_label="probe",
                max_attempts=3,
                sleep_fn=lambda _seconds: pytest.fail("must not sleep before re-raising"),
            )
        except type(error):
            pass
        else:  # pragma: no cover - the call must raise
            pytest.fail(f"{type(error).__name__} was swallowed")

        assert calls == 1, f"{type(error).__name__} was retried"


def test_the_non_retryable_set_is_not_silently_narrowed() -> None:
    assert set(NON_RETRYABLE_EXCEPTIONS) == {
        SourceAuthError,
        SourceBlockedError,
        SourceQuotaExhaustedError,
        OpenDartKeyExhaustedError,
    }

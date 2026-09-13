from datetime import UTC, date, datetime

from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.models import IngestionRun
from collector.kr.service.dart_target_plan import build_dart_target_plan


class MockDartPlanStorage:
    def __init__(self, runs: list[IngestionRun] | None = None) -> None:
        self.runs = runs or []

    def get_recent_ingestion_runs(self, run_type: RunType, limit: int = 20) -> list[IngestionRun]:
        return [run for run in self.runs if run.run_type == run_type][:limit]


def test_dart_target_plan_skips_reports_before_availability_window() -> None:
    plan = build_dart_target_plan(
        MockDartPlanStorage(),  # type: ignore[arg-type]
        run_type=RunType.DART_FINANCIAL_SYNC,
        active_corp_count=100,
        requests_per_corp_target=1,
        lookback_years=0,
        reprt_codes=["11011", "11012", "11013", "11014"],
        as_of=date(2026, 6, 13),
    )

    assert plan.allowed_year_report_pairs == {(2026, "11013")}
    assert plan.skipped_year_report_pairs["2026:11011"] == "not_yet_available"
    assert plan.estimated_request_count == 100


def test_dart_target_plan_reads_recent_no_data_request_keys() -> None:
    run = IngestionRun(
        run_type=RunType.DART_FINANCIAL_SYNC,
        started_at=datetime(2026, 6, 12, tzinfo=UTC),
        status=RunStatus.SUCCESS,
        params={"no_data_request_keys": ["005930:2026:11013:CFS"]},
    )

    plan = build_dart_target_plan(
        MockDartPlanStorage([run]),  # type: ignore[arg-type]
        run_type=RunType.DART_FINANCIAL_SYNC,
        active_corp_count=1,
        requests_per_corp_target=1,
        lookback_years=0,
        reprt_codes=["11013"],
        as_of=date(2026, 6, 13),
    )

    assert plan.negative_cache_request_keys == {"005930:2026:11013:CFS"}

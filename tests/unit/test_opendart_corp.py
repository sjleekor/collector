import io
import zipfile
from datetime import date, datetime, timedelta
from typing import Any

import pytest

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
from collector.kr.util.time import KST, now_kst
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


def _real_fetch_run(days_ago: int, *, at: datetime | None = None) -> IngestionRun:
    ended = at if at is not None else now_kst() - timedelta(days=days_ago)
    return IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        started_at=ended - timedelta(minutes=1),
        ended_at=ended,
        status=RunStatus.SUCCESS,
        counts={"fetched_records": 3000},
    )


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
        self.positive_count_requests: list[str | None] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_last_successful_run(
        self, run_type: RunType, *, positive_count: str | None = None
    ) -> IngestionRun | None:
        self.positive_count_requests.append(positive_count)
        if run_type != RunType.DART_CORP_SYNC:
            return None
        run = self.last_successful_corp_run
        if run is not None and positive_count is not None:
            value = run.counts.get(positive_count)
            if not isinstance(value, int) or value <= 0:
                return None
        return run

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


def test_sync_dart_corp_master_skips_when_recent_real_fetch_recorded() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(1)
    provider = _ExhaustedCorpProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert result.error is None
    assert provider.calls == 0
    assert storage.saved_records == []
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert storage.runs[-1].counts["skipped_existing"] == 1


def test_sync_dart_corp_master_force_re_runs_even_with_previous_success() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(1)

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


class _CountingProvider(MockCorpCodeProvider):
    def __init__(self) -> None:
        self.calls = 0

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        self.calls += 1
        return super().fetch_corp_codes()


class _RaisingProvider:
    def fetch_corp_codes(self) -> DartCorpCodeResult:
        raise ConnectionError("boom")


def _maintenance_provider() -> OpenDartCorpCodeProvider:
    payload = "<result><status>800</status><message>시스템 점검 중입니다.</message></result>"
    return OpenDartCorpCodeProvider(
        request_executor=OpenDartRequestExecutor(
            ["key-a"],
            urlopen_fn=_make_urlopen([payload.encode()] * 10, []),
            sleep_fn=lambda _: None,
        )
    )


def test_corp_code_provider_maps_status_800_to_error() -> None:
    result = _maintenance_provider().fetch_corp_codes()

    assert result.records == []
    assert result.error is not None
    assert "800" in result.error


def test_sync_skips_with_real_fetch_6_days_ago() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(6)
    provider = _CountingProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert provider.calls == 0
    assert result.skipped is True
    assert result.last_real_fetch_age_days == 6
    assert storage.runs[-1].counts["skipped_existing"] == 1
    assert storage.runs[-1].counts["last_real_fetch_age_days"] == 6
    assert storage.runs[-1].params["max_age_days"] == 7
    assert storage.runs[-1].params["stale_fail_days"] == 14
    assert storage.positive_count_requests == ["fetched_records"]


def test_sync_refetches_with_real_fetch_7_days_ago() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(7)
    provider = _CountingProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert provider.calls == 1
    assert result.error is None
    assert result.skipped is False
    assert storage.runs[-1].counts["fetched_records"] == 2


def test_sync_refetches_when_only_skipped_runs_are_recent() -> None:
    """Regression: 125 of 127 runs were skips, so the master was never refreshed."""
    storage = MockStorage()
    # Only the real fetch (30 days ago) is visible to positive_count lookups; the
    # fake mimics the SQL filter by returning the real run when asked.
    storage.last_successful_corp_run = _real_fetch_run(30)
    provider = _CountingProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert provider.calls == 1
    assert result.error is None
    assert storage.runs[-1].counts.get("skipped_existing") is None


def test_sync_ignores_skip_run_without_positive_fetched_records() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        started_at=now_kst(),
        ended_at=now_kst(),
        status=RunStatus.SUCCESS,
        counts={"skipped_existing": 1, "fetched_records": 0},
    )
    provider = _CountingProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert provider.calls == 1
    assert result.error is None


def test_sync_fetches_when_no_real_fetch_exists() -> None:
    storage = MockStorage()
    provider = _CountingProvider()

    result = sync_dart_corp_master(provider=provider, storage=storage)

    assert provider.calls == 1
    assert result.error is None


def test_sync_force_fetches_even_one_day_after_real_fetch() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(1)
    provider = _CountingProvider()

    sync_dart_corp_master(provider=provider, storage=storage, force=True)

    assert provider.calls == 1
    assert storage.positive_count_requests == []


def test_sync_keeps_master_when_refresh_fails_within_stale_window() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(9)

    result = sync_dart_corp_master(provider=_maintenance_provider(), storage=storage)

    assert result.error is None
    assert result.refresh_warning is not None
    assert "exhaustion_reason" in result.refresh_warning
    assert result.last_real_fetch_age_days == 9
    assert storage.saved_records == []
    last = storage.runs[-1]
    assert last.status == RunStatus.SUCCESS
    assert last.counts["refresh_failed"] == 1
    assert last.counts["fetched_records"] == 0
    assert last.counts["last_real_fetch_age_days"] == 9
    assert "retryable_error_count" in last.counts
    assert last.error_summary == result.refresh_warning


def test_sync_keeps_master_when_provider_raises_within_stale_window() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(9)

    result = sync_dart_corp_master(provider=_RaisingProvider(), storage=storage)

    assert result.error is None
    assert result.refresh_warning is not None and "boom" in result.refresh_warning
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert storage.runs[-1].counts["refresh_failed"] == 1


def test_sync_fails_when_refresh_fails_after_stale_window() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(20)

    result = sync_dart_corp_master(provider=_maintenance_provider(), storage=storage)

    assert result.error is not None
    assert result.refresh_warning is None
    assert storage.runs[-1].status == RunStatus.FAILED


def test_sync_fails_when_refresh_fails_without_previous_real_fetch() -> None:
    storage = MockStorage()

    result = sync_dart_corp_master(provider=_ExhaustedCorpProvider(), storage=storage)

    assert result.error is not None
    assert storage.runs[-1].status == RunStatus.FAILED


def test_sync_fails_on_raising_provider_without_previous_real_fetch() -> None:
    storage = MockStorage()

    result = sync_dart_corp_master(provider=_RaisingProvider(), storage=storage)

    assert result.error is not None and "boom" in result.error
    assert storage.runs[-1].status == RunStatus.FAILED


def test_sync_force_does_not_fall_back_on_refresh_failure() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(9)

    result = sync_dart_corp_master(provider=_ExhaustedCorpProvider(), storage=storage, force=True)

    assert result.error is not None
    assert storage.runs[-1].status == RunStatus.FAILED


def test_sync_db_step_failure_is_not_replaced_by_fallback() -> None:
    class _FailingUpsertStorage(MockStorage):
        def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
            raise RuntimeError("db down")

    storage = _FailingUpsertStorage()
    storage.last_successful_corp_run = _real_fetch_run(9)

    result = sync_dart_corp_master(provider=MockCorpCodeProvider(), storage=storage)

    assert result.error == "db down"
    assert storage.runs[-1].status == RunStatus.FAILED


def test_sync_age_is_counted_in_kst_calendar_days() -> None:
    """04:00:30 seven days ago vs 04:00:02 now is still 7 days, not 6.99."""
    storage = MockStorage()
    now = now_kst()
    seven_days_ago = (
        (now - timedelta(days=7))
        .astimezone(KST)
        .replace(hour=4, minute=0, second=30, microsecond=0)
    )
    storage.last_successful_corp_run = _real_fetch_run(0, at=seven_days_ago)
    provider = _CountingProvider()

    sync_dart_corp_master(provider=provider, storage=storage, max_age_days=7)

    # Pin the comparison: the run starts at "now", whatever time of day that is,
    # and the age is the date difference.
    assert provider.calls == 1
    assert storage.runs[0].started_at.date() - seven_days_ago.date() == timedelta(days=7)


def test_sync_max_age_zero_fetches_every_time() -> None:
    storage = MockStorage()
    storage.last_successful_corp_run = _real_fetch_run(0)
    provider = _CountingProvider()

    sync_dart_corp_master(provider=provider, storage=storage, max_age_days=0)

    assert provider.calls == 1


# --- Postgres positive_count SQL -------------------------------------------


def _capture_last_run_sql(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> tuple[str, tuple]:
    from contextlib import contextmanager

    from collector.kr.infra.db_postgres import repositories

    executed: list[tuple[str, tuple]] = []

    class _Cur:
        def __enter__(self) -> "_Cur":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple) -> None:
            executed.append((sql, params))

        def fetchone(self) -> None:
            return None

    class _Conn:
        def cursor(self, *_a: object, **_k: object) -> _Cur:
            return _Cur()

    @contextmanager
    def fake_get_connection(_dsn: str):
        yield _Conn()

    monkeypatch.setattr(repositories, "get_connection", fake_get_connection)
    storage = repositories.PostgresStorage("postgresql://unused")
    assert storage.get_last_successful_run(RunType.DART_CORP_SYNC, **kwargs) is None
    return executed[0]


def test_postgres_last_successful_run_default_has_no_count_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql, params = _capture_last_run_sql(monkeypatch)

    assert "counts->>" not in sql
    assert params == (RunType.DART_CORP_SYNC.value, RunStatus.SUCCESS.value)


def test_postgres_last_successful_run_positive_count_filter_is_cast_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql, params = _capture_last_run_sql(monkeypatch, positive_count="fetched_records")

    assert "CASE" in sql and "~ '^[0-9]{1,18}$'" in sql and "::bigint" in sql
    assert params == (
        RunType.DART_CORP_SYNC.value,
        RunStatus.SUCCESS.value,
        "fetched_records",
        "fetched_records",
    )
    assert sql.index("::bigint") < sql.index("ORDER BY")


def test_postgres_upsert_dart_corp_master_keeps_market_when_incoming_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import contextmanager

    from collector.kr.infra.db_postgres import repositories

    captured: list[str] = []

    class _Cur:
        rowcount = 0

        def __enter__(self) -> "_Cur":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    class _Conn:
        def cursor(self, *_a: object, **_k: object) -> _Cur:
            return _Cur()

    @contextmanager
    def fake_get_connection(_dsn: str):
        yield _Conn()

    def fake_execute_counted(_cur: object, sql: str, *_a: object, **_k: object) -> None:
        captured.append(sql)

    monkeypatch.setattr(repositories, "get_connection", fake_get_connection)
    monkeypatch.setattr(repositories, "_execute_values_counted", fake_execute_counted)

    storage = repositories.PostgresStorage("postgresql://unused")
    storage.upsert_dart_corp_master(
        [
            DartCorp(
                corp_code="00999999",
                corp_name="상장폐지예시",
                ticker="999999",
                market=None,
                stock_name="상장폐지예시",
                modify_date=date(2024, 4, 1),
                is_active=False,
                source=Source.OPENDART,
                fetched_at=now_kst(),
            )
        ]
    )

    sql = captured[0]
    assert "market = COALESCE(EXCLUDED.market, dart_corp_master.market)" in sql
    assert "is_active = EXCLUDED.is_active" in sql

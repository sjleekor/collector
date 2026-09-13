from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from collector.kr.domain.enums import RunStatus, RunType, Source
from collector.kr.domain.models import (
    CommonFeatureFetchResult,
    CommonFeatureObservation,
    CommonFeatureSeries,
    IngestionRun,
    UpsertResult,
)
from collector.kr.service.sync_common_features import sync_common_features


def _krx_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _series(
    series_id: str,
    source: Source = Source.PYKRX,
    policy: str = "next_krx_session",
    source_timezone: str = "Asia/Seoul",
    endpoint_params: dict[str, object] | None = None,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=series_id,
        category="market",
        frequency="D",
        name_kr=series_id,
        unit="point",
        availability_policy=policy,
        source_timezone=source_timezone,
        endpoint_params=endpoint_params or {},
    )


def _observation(
    series_id: str,
    source: Source = Source.PYKRX,
    observation_date: date = date(2026, 6, 8),
    value: str = "2910.42",
) -> CommonFeatureObservation:
    return CommonFeatureObservation(
        source=source,
        series_id=series_id,
        observation_date=observation_date,
        frequency="D",
        fetched_at=datetime(2026, 6, 8, 18, 30, tzinfo=UTC),
        value_numeric=Decimal(value),
        raw_payload={"value": value},
    )


class MockCommonFeatureProvider:
    def __init__(
        self,
        source: Source,
        results: dict[str, CommonFeatureFetchResult],
    ) -> None:
        self._source = source
        self.results = results
        self.calls: list[tuple[str, date, date]] = []

    def source(self) -> Source:
        return self._source

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        self.calls.append((series.series_id, start, end))
        dated_key = f"{series.series_id}:{start.isoformat()}"
        return self.results.get(dated_key, self.results[series.series_id])


class MockCommonFeatureStorage:
    def __init__(
        self,
        series: list[CommonFeatureSeries],
        observation_counts: dict[str, int] | None = None,
        observation_rows: dict[str, list[CommonFeatureObservation]] | None = None,
    ) -> None:
        self.series = series
        self.observation_counts = observation_counts or {}
        self.observation_rows = observation_rows or {}
        self.observations: list[CommonFeatureObservation] = []
        self.upsert_batches: list[list[CommonFeatureObservation]] = []
        self.runs: list[IngestionRun] = []
        self.series_query: tuple[list[Source] | None, list[str] | None, bool] | None = None
        self.count_queries: list[
            tuple[list[str] | None, date | None, date | None, Source | None]
        ] = []
        self.observation_queries: list[
            tuple[list[str] | None, date | None, date | None, Source | None, date | None]
        ] = []
        self.observation_date_queries: list[tuple[Source, str, date, date]] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_common_feature_series(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureSeries]:
        self.series_query = (sources, series_ids, active_only)
        return list(self.series)

    def upsert_common_feature_observations(
        self,
        records: list[CommonFeatureObservation],
    ) -> UpsertResult:
        self.upsert_batches.append(list(records))
        self.observations.extend(records)
        return UpsertResult(updated=len(records))

    def count_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
    ) -> dict[str, int]:
        self.count_queries.append((series_ids, start, end, source))
        if series_ids:
            return {
                series_id: self.observation_counts.get(series_id, 0) for series_id in series_ids
            }
        return dict(self.observation_counts)

    def get_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
        available_from_end: date | None = None,
    ) -> list[CommonFeatureObservation]:
        self.observation_queries.append((series_ids, start, end, source, available_from_end))
        selected_series_ids = series_ids or list(self.observation_rows)
        rows = [
            row
            for series_id in selected_series_ids
            for row in self.observation_rows.get(series_id, [])
        ]
        if start:
            rows = [row for row in rows if row.observation_date >= start]
        if end:
            rows = [row for row in rows if row.observation_date <= end]
        if source:
            rows = [row for row in rows if row.source == source]
        if available_from_end:
            rows = [
                row
                for row in rows
                if row.available_from_date is not None
                and row.available_from_date <= available_from_end
            ]
        return rows

    def get_common_feature_observation_max_dates(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
    ) -> dict[str, date]:
        selected_series_ids = series_ids or [series.series_id for series in self.series]
        return {
            series_id: max(row.observation_date for row in self.observation_rows.get(series_id, []))
            for series_id in selected_series_ids
            if self.observation_rows.get(series_id)
        }

    def get_common_feature_observation_dates(
        self,
        *,
        source: Source,
        series_id: str,
        start: date,
        end: date,
    ) -> set[date]:
        self.observation_date_queries.append((source, series_id, start, end))
        return {
            row.observation_date
            for row in self.observation_rows.get(series_id, [])
            if row.source == source and start <= row.observation_date <= end
        }


def test_sync_common_features_writes_observations_with_service_availability() -> None:
    storage = MockCommonFeatureStorage([_series("market_kospi")])
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {"market_kospi": CommonFeatureFetchResult(records=[_observation("market_kospi")])},
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 1
    assert result.requests_attempted == 1
    assert result.rows_upserted == 1
    assert storage.observations[0].available_from_date == date(2026, 6, 9)
    assert storage.observations[0].unit == "point"
    assert storage.runs[-1].run_type == RunType.COMMON_FEATURE_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert storage.runs[-1].counts["rows_upserted"] == 1


def test_sync_common_features_records_partial_run_for_series_error() -> None:
    storage = MockCommonFeatureStorage([_series("market_kospi"), _series("market_kosdaq")])
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {
            "market_kospi": CommonFeatureFetchResult(records=[_observation("market_kospi")]),
            "market_kosdaq": CommonFeatureFetchResult(error="provider failed"),
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.rows_upserted == 1
    assert result.requests_attempted == 2
    assert result.errors == {"market_kosdaq": "provider failed"}
    assert storage.runs[-1].status == RunStatus.PARTIAL
    assert storage.runs[-1].counts["partial_failure_count"] == 1


def test_sync_common_features_applies_source_and_series_filters() -> None:
    storage = MockCommonFeatureStorage(
        [
            _series("market_kospi", source=Source.PYKRX),
            _series(
                "global_sp500",
                source=Source.FDR,
                policy="same_krx_session_morning",
                source_timezone="America/New_York",
            ),
            _series(
                "global_vix",
                source=Source.FDR,
                policy="same_krx_session_morning",
                source_timezone="America/New_York",
            ),
        ]
    )
    pykrx_provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {"market_kospi": CommonFeatureFetchResult(records=[_observation("market_kospi")])},
    )
    fdr_provider = MockCommonFeatureProvider(
        Source.FDR,
        {
            "global_sp500": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "global_sp500",
                        source=Source.FDR,
                        observation_date=date(2026, 6, 5),
                    )
                ]
            )
        },
    )

    result = sync_common_features(
        providers=[pykrx_provider, fdr_provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
        sources=[Source.FDR],
        series_ids=["global_sp500"],
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 1
    assert pykrx_provider.calls == []
    assert fdr_provider.calls == [("global_sp500", date(2026, 6, 5), date(2026, 6, 8))]
    assert storage.series_query == ([Source.FDR], ["global_sp500"], True)
    assert storage.observations[0].source == Source.FDR
    assert storage.observations[0].available_from_date == date(2026, 6, 8)


def test_sync_common_features_can_include_inactive_series_for_explicit_smoke() -> None:
    storage = MockCommonFeatureStorage([_series("rate_kr_gov3y", source=Source.ECOS)])
    provider = MockCommonFeatureProvider(
        Source.ECOS,
        {
            "rate_kr_gov3y": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "rate_kr_gov3y",
                        source=Source.ECOS,
                        observation_date=date(2026, 6, 8),
                        value="3.25",
                    )
                ]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        sources=[Source.ECOS],
        series_ids=["rate_kr_gov3y"],
        active_only=False,
        force=True,
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 1
    assert provider.calls == [("rate_kr_gov3y", date(2026, 6, 8), date(2026, 6, 8))]
    assert storage.series_query == ([Source.ECOS], ["rate_kr_gov3y"], False)
    assert storage.runs[-1].params["active_only"] is False
    assert storage.observations[0].source == Source.ECOS


def test_sync_common_features_skips_series_with_existing_coverage() -> None:
    storage = MockCommonFeatureStorage(
        [_series("market_kospi")],
        observation_counts={"market_kospi": 1},
    )
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {"market_kospi": CommonFeatureFetchResult(records=[_observation("market_kospi")])},
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert result.rows_upserted == 0
    assert provider.calls == []
    assert storage.count_queries == [
        (["market_kospi"], date(2026, 6, 8), date(2026, 6, 8), Source.PYKRX)
    ]
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_common_features_skips_ecos_daily_series_with_calendar_aware_coverage() -> None:
    start = date(2025, 6, 9)
    end = date(2026, 6, 10)
    storage = MockCommonFeatureStorage(
        [
            _series("rate_kr_gov3y", source=Source.ECOS),
            _series("rate_kr_gov10y", source=Source.ECOS),
        ],
        observation_counts={
            "rate_kr_gov3y": 247,
            "rate_kr_gov10y": 247,
        },
        observation_rows={
            "rate_kr_gov3y": [
                _observation("rate_kr_gov3y", source=Source.ECOS, observation_date=start),
                _observation("rate_kr_gov3y", source=Source.ECOS, observation_date=end),
            ],
            "rate_kr_gov10y": [
                _observation("rate_kr_gov10y", source=Source.ECOS, observation_date=start),
                _observation("rate_kr_gov10y", source=Source.ECOS, observation_date=end),
            ],
        },
    )
    provider = MockCommonFeatureProvider(Source.ECOS, {})

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=start,
        end=end,
        sources=[Source.ECOS],
        series_ids=["rate_kr_gov3y", "rate_kr_gov10y"],
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 2
    assert result.requests_attempted == 0
    assert result.requests_skipped == 2
    assert result.rows_upserted == 0
    assert provider.calls == []
    assert storage.count_queries == [
        (["rate_kr_gov3y"], start, end, Source.ECOS),
        (["rate_kr_gov10y"], start, end, Source.ECOS),
    ]
    assert storage.observation_queries == [
        (["rate_kr_gov3y"], start, end, Source.ECOS, None),
        (["rate_kr_gov10y"], start, end, Source.ECOS, None),
    ]
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_common_features_force_fetches_even_with_existing_coverage() -> None:
    storage = MockCommonFeatureStorage(
        [_series("market_kospi")],
        observation_counts={"market_kospi": 1},
    )
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {"market_kospi": CommonFeatureFetchResult(records=[_observation("market_kospi")])},
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        force=True,
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert result.rows_upserted == 1
    assert provider.calls == [("market_kospi", date(2026, 6, 8), date(2026, 6, 8))]
    assert storage.count_queries == []


def test_sync_common_features_incremental_lookback_bypasses_existing_coverage() -> None:
    storage = MockCommonFeatureStorage(
        [_series("market_kospi")],
        observation_counts={"market_kospi": 3},
        observation_rows={
            "market_kospi": [_observation("market_kospi", observation_date=date(2026, 6, 9))]
        },
    )
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {
            "market_kospi": CommonFeatureFetchResult(
                records=[_observation("market_kospi", observation_date=date(2026, 6, 10))]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=None,
        end=date(2026, 6, 10),
        sources=[Source.PYKRX],
        incremental=True,
        lookback_days=3,
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert provider.calls == [("market_kospi", date(2026, 6, 7), date(2026, 6, 10))]


def test_sync_common_features_krx_market_breadth_upserts_each_missing_day() -> None:
    series = _series(
        "market_kosdaq_advancers_krx",
        source=Source.KRX,
        endpoint_params={"kind": "market_breadth", "mktId": "KSQ", "metric": "advancers"},
    )
    storage = MockCommonFeatureStorage(
        [series],
        observation_rows={
            "market_kosdaq_advancers_krx": [
                _observation(
                    "market_kosdaq_advancers_krx",
                    source=Source.KRX,
                    observation_date=date(2026, 6, 8),
                )
            ]
        },
    )
    provider = MockCommonFeatureProvider(
        Source.KRX,
        {
            "market_kosdaq_advancers_krx": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "market_kosdaq_advancers_krx",
                        source=Source.KRX,
                        observation_date=date(2026, 6, 9),
                    )
                ]
            ),
            "market_kosdaq_advancers_krx:2026-06-10": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "market_kosdaq_advancers_krx",
                        source=Source.KRX,
                        observation_date=date(2026, 6, 10),
                    )
                ]
            ),
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 10),
        sources=[Source.KRX],
        series_ids=["market_kosdaq_advancers_krx"],
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.requests_skipped == 1
    assert result.requests_attempted == 2
    assert result.rows_upserted == 2
    assert provider.calls == [
        ("market_kosdaq_advancers_krx", date(2026, 6, 9), date(2026, 6, 9)),
        ("market_kosdaq_advancers_krx", date(2026, 6, 10), date(2026, 6, 10)),
    ]
    assert len(storage.upsert_batches) == 2
    assert [batch[0].observation_date for batch in storage.upsert_batches] == [
        date(2026, 6, 9),
        date(2026, 6, 10),
    ]
    assert storage.observation_date_queries == [
        (
            Source.KRX,
            "market_kosdaq_advancers_krx",
            date(2026, 6, 8),
            date(2026, 6, 10),
        )
    ]
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_common_features_krx_market_breadth_continues_after_daily_error() -> None:
    series = _series(
        "market_kosdaq_advancers_krx",
        source=Source.KRX,
        endpoint_params={"kind": "market_breadth", "mktId": "KSQ", "metric": "advancers"},
    )
    storage = MockCommonFeatureStorage([series])
    provider = MockCommonFeatureProvider(
        Source.KRX,
        {
            "market_kosdaq_advancers_krx": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "market_kosdaq_advancers_krx",
                        source=Source.KRX,
                        observation_date=date(2026, 6, 8),
                    )
                ]
            ),
            "market_kosdaq_advancers_krx:2026-06-09": CommonFeatureFetchResult(
                error="KRX blocked"
            ),
            "market_kosdaq_advancers_krx:2026-06-10": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "market_kosdaq_advancers_krx",
                        source=Source.KRX,
                        observation_date=date(2026, 6, 10),
                    )
                ]
            ),
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 10),
        sources=[Source.KRX],
        series_ids=["market_kosdaq_advancers_krx"],
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_attempted == 3
    assert result.rows_upserted == 2
    assert result.errors == {"KRX:market_kosdaq_advancers_krx:2026-06-09": "KRX blocked"}
    assert provider.calls == [
        ("market_kosdaq_advancers_krx", date(2026, 6, 8), date(2026, 6, 8)),
        ("market_kosdaq_advancers_krx", date(2026, 6, 9), date(2026, 6, 9)),
        ("market_kosdaq_advancers_krx", date(2026, 6, 10), date(2026, 6, 10)),
    ]
    assert storage.runs[-1].status == RunStatus.PARTIAL


def test_sync_common_features_krx_market_breadth_force_refetches_existing_days() -> None:
    series = _series(
        "market_kosdaq_advancers_krx",
        source=Source.KRX,
        endpoint_params={"kind": "market_breadth", "mktId": "KSQ", "metric": "advancers"},
    )
    storage = MockCommonFeatureStorage(
        [series],
        observation_rows={
            "market_kosdaq_advancers_krx": [
                _observation(
                    "market_kosdaq_advancers_krx",
                    source=Source.KRX,
                    observation_date=date(2026, 6, 8),
                )
            ]
        },
    )
    provider = MockCommonFeatureProvider(
        Source.KRX,
        {
            "market_kosdaq_advancers_krx": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "market_kosdaq_advancers_krx",
                        source=Source.KRX,
                        observation_date=date(2026, 6, 8),
                    )
                ]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        sources=[Source.KRX],
        series_ids=["market_kosdaq_advancers_krx"],
        force=True,
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_skipped == 0
    assert result.requests_attempted == 1
    assert provider.calls == [
        ("market_kosdaq_advancers_krx", date(2026, 6, 8), date(2026, 6, 8))
    ]
    assert storage.observation_date_queries == []


def test_sync_common_features_records_missing_provider_as_partial() -> None:
    storage = MockCommonFeatureStorage([_series("rate_kr_gov3y", source=Source.ECOS)])

    result = sync_common_features(
        providers=[],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert result.rows_upserted == 0
    assert "rate_kr_gov3y" in result.errors
    assert "source ECOS" in result.errors["rate_kr_gov3y"]
    assert storage.runs[-1].status == RunStatus.PARTIAL

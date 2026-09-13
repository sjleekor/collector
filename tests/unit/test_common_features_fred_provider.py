from __future__ import annotations

from datetime import date
from decimal import Decimal

from collector.kr.adapters.common_features_fred.client import FredSeriesObservationResult
from collector.kr.adapters.common_features_fred.provider import FredCommonFeatureProvider
from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureSeries


def _series(
    *,
    series_id: str = "rate_us10y",
    source_series_key: str = "DGS10",
    endpoint_params: dict[str, object] | None = None,
    source: Source = Source.FRED,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=source_series_key,
        category="rate",
        frequency="D",
        name_kr=series_id,
        unit="pct",
        endpoint_params=endpoint_params or {},
        availability_policy="same_krx_session_morning",
        source_timezone="America/New_York",
    )


class FakeFredClient:
    def __init__(self, result: FredSeriesObservationResult) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    def fetch_series_observations(self, **kwargs: object) -> FredSeriesObservationResult:
        self.calls.append(kwargs)
        return self.result


def test_fred_common_feature_provider_uses_endpoint_series_id() -> None:
    client = FakeFredClient(
        FredSeriesObservationResult(
            rows=[
                {
                    "realtime_start": "2026-06-09",
                    "realtime_end": "2026-06-09",
                    "date": "2026-06-08",
                    "value": "4.50",
                }
            ]
        )
    )

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(endpoint_params={"series_id": "DGS10"}),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert client.calls == [
        {
            "series_id": "DGS10",
            "observation_start": "2026-06-01",
            "observation_end": "2026-06-08",
        }
    ]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source == Source.FRED
    assert record.series_id == "rate_us10y"
    assert record.observation_date == date(2026, 6, 8)
    assert record.period_end_date == date(2026, 6, 8)
    assert record.release_date is None
    assert record.available_from_date is None
    assert record.vintage == ""
    assert record.value_numeric == Decimal("4.50")
    assert record.unit == "pct"
    assert record.frequency == "D"
    assert record.raw_payload["series_id"] == "DGS10"
    assert record.raw_payload["row"] == {
        "realtime_start": "2026-06-09",
        "realtime_end": "2026-06-09",
        "date": "2026-06-08",
        "value": "4.50",
    }


def test_fred_common_feature_provider_falls_back_to_source_series_key() -> None:
    client = FakeFredClient(
        FredSeriesObservationResult(
            rows=[
                {
                    "date": "2026-06-08",
                    "value": "3.95",
                }
            ]
        )
    )

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(source_series_key="DGS2"),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert client.calls[0]["series_id"] == "DGS2"
    assert result.records[0].value_numeric == Decimal("3.95")


def test_fred_common_feature_provider_filters_rows_and_missing_values() -> None:
    client = FakeFredClient(
        FredSeriesObservationResult(
            rows=[
                {"date": "2026-05-29", "value": "4.40"},
                {"date": "2026-06-02", "value": "."},
                {"date": "2026-06-03", "value": ""},
                {"date": "2026-06-08", "value": "4.50"},
                {"date": "2026-06-09", "value": "4.55"},
            ]
        )
    )

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 8), Decimal("4.50"))
    ]


def test_fred_common_feature_provider_reports_no_data_when_all_values_invalid() -> None:
    client = FakeFredClient(
        FredSeriesObservationResult(
            rows=[
                {"date": "2026-06-02", "value": "."},
                {"date": "2026-06-03", "value": "nan"},
            ]
        )
    )

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_fred_common_feature_provider_propagates_client_error() -> None:
    client = FakeFredClient(
        FredSeriesObservationResult(
            error="FRED HTTP 503: Service Unavailable",
            retryable=True,
            retry_after_seconds=10.0,
        )
    )

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert result.error == "FRED HTTP 503: Service Unavailable"
    assert result.retryable is True
    assert result.retry_after_seconds == 10.0


def test_fred_common_feature_provider_reports_no_data_result() -> None:
    client = FakeFredClient(FredSeriesObservationResult(no_data=True))

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_fred_common_feature_provider_rejects_non_fred_series() -> None:
    client = FakeFredClient(FredSeriesObservationResult(rows=[]))

    result = FredCommonFeatureProvider(client=client).fetch_series(
        series=_series(source=Source.ECOS),
        start=date(2026, 6, 1),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert "cannot fetch source ECOS" in result.error

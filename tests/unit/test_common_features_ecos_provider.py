from __future__ import annotations

from datetime import date
from decimal import Decimal

from collector.kr.adapters.common_features_ecos.client import EcosStatisticSearchResult
from collector.kr.adapters.common_features_ecos.provider import EcosCommonFeatureProvider
from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureSeries


def _series(
    *,
    series_id: str = "rate_kr_base",
    source: Source = Source.ECOS,
    frequency: str = "D",
    endpoint_params: dict[str, object] | None = None,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key="722Y001",
        category="rate",
        frequency=frequency,
        name_kr=series_id,
        unit="%",
        endpoint_params=endpoint_params
        or {
            "stat_code": "722Y001",
            "cycle": frequency,
            "item_code1": "0101000",
        },
        availability_policy="manual_lag_days",
        manual_lag_days=1,
    )


class FakeEcosClient:
    def __init__(self, result: EcosStatisticSearchResult) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    def fetch_statistic_search(self, **kwargs: object) -> EcosStatisticSearchResult:
        self.calls.append(kwargs)
        return self.result


def test_ecos_common_feature_provider_converts_daily_rows_to_observations() -> None:
    client = FakeEcosClient(
        EcosStatisticSearchResult(
            rows=[
                {
                    "STAT_CODE": "722Y001",
                    "ITEM_CODE1": "0101000",
                    "TIME": "20240102",
                    "DATA_VALUE": "3.50",
                    "UNIT_NAME": "%",
                }
            ]
        )
    )

    result = EcosCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2024, 1, 2),
        end=date(2024, 1, 12),
    )

    assert result.error is None
    assert result.no_data is False
    assert client.calls == [
        {
            "stat_code": "722Y001",
            "cycle": "D",
            "start_period": "20240102",
            "end_period": "20240112",
            "item_codes": ["0101000"],
        }
    ]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source == Source.ECOS
    assert record.series_id == "rate_kr_base"
    assert record.observation_date == date(2024, 1, 2)
    assert record.period_end_date == date(2024, 1, 2)
    assert record.release_date is None
    assert record.available_from_date is None
    assert record.value_numeric == Decimal("3.50")
    assert record.value_text == ""
    assert record.unit == "%"
    assert record.frequency == "D"
    assert record.raw_payload["row"]["DATA_VALUE"] == "3.50"


def test_ecos_common_feature_provider_converts_monthly_time_to_period_end() -> None:
    client = FakeEcosClient(
        EcosStatisticSearchResult(
            rows=[
                {
                    "TIME": "202401",
                    "DATA_VALUE": "112.4",
                    "UNIT_NAME": "index",
                }
            ]
        )
    )

    result = EcosCommonFeatureProvider(client=client).fetch_series(
        series=_series(
            series_id="macro_cpi",
            frequency="M",
            endpoint_params={
                "stat_code": "901Y009",
                "cycle": "M",
                "item_codes": ["0"],
            },
        ),
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert result.error is None
    assert client.calls[0]["start_period"] == "202401"
    assert client.calls[0]["end_period"] == "202401"
    assert client.calls[0]["item_codes"] == ["0"]
    assert result.records[0].observation_date == date(2024, 1, 31)
    assert result.records[0].period_end_date == date(2024, 1, 31)
    assert result.records[0].value_numeric == Decimal("112.4")


def test_ecos_common_feature_provider_reports_client_error() -> None:
    client = FakeEcosClient(
        EcosStatisticSearchResult(
            error="ECOS HTTP 503: Service Unavailable",
            retryable=True,
            retry_after_seconds=10.0,
        )
    )

    result = EcosCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    assert result.records == []
    assert result.error == "ECOS HTTP 503: Service Unavailable"
    assert result.retryable is True
    assert result.retry_after_seconds == 10.0


def test_ecos_common_feature_provider_reports_no_data_for_invalid_numeric_rows() -> None:
    client = FakeEcosClient(
        EcosStatisticSearchResult(
            rows=[
                {
                    "TIME": "20240102",
                    "DATA_VALUE": "NaN",
                    "UNIT_NAME": "%",
                }
            ]
        )
    )

    result = EcosCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_ecos_common_feature_provider_rejects_non_ecos_series() -> None:
    result = EcosCommonFeatureProvider(
        client=FakeEcosClient(EcosStatisticSearchResult())
    ).fetch_series(
        series=_series(source=Source.FDR),
        start=date(2024, 1, 2),
        end=date(2024, 1, 2),
    )

    assert result.records == []
    assert "cannot fetch source FDR" in result.error

from datetime import date
from decimal import Decimal

import pandas as pd

from collector.kr.adapters.common_features_fdr import provider as provider_module
from collector.kr.adapters.common_features_fdr.provider import FdrCommonFeatureProvider
from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureSeries


def _series(
    *,
    series_id: str = "global_sp500",
    source_series_key: str = "fallback-symbol",
    endpoint_params: dict[str, object] | None = None,
    source: Source = Source.FDR,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=source_series_key,
        category="global_index",
        frequency="D",
        name_kr=series_id,
        unit="index_point",
        endpoint_params=endpoint_params or {},
        availability_policy="same_krx_session_morning",
        source_timezone="America/New_York",
    )


class FakeFdr:
    def __init__(self, frame: pd.DataFrame | None = None, error: Exception | None = None) -> None:
        self.frame = frame
        self.error = error
        self.calls: list[tuple[str, str, str]] = []

    def DataReader(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        self.calls.append((symbol, start, end))
        if self.error is not None:
            raise self.error
        return self.frame if self.frame is not None else pd.DataFrame()


def test_fdr_common_feature_provider_uses_endpoint_symbol(monkeypatch) -> None:
    frame = pd.DataFrame(
        [{"Open": 6000.0, "High": 6020.0, "Low": 5980.0, "Close": 6010.25, "Volume": 1000}],
        index=pd.to_datetime(["2026-06-05"]),
    )
    fake_fdr = FakeFdr(frame)
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"symbol": "US500"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert fake_fdr.calls == [("US500", "2026-06-05", "2026-06-09")]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source == Source.FDR
    assert record.series_id == "global_sp500"
    assert record.observation_date == date(2026, 6, 5)
    assert record.period_end_date == date(2026, 6, 5)
    assert record.release_date is None
    assert record.available_from_date is None
    assert record.value_numeric == Decimal("6010.25")
    assert record.unit == "index_point"
    assert record.frequency == "D"
    assert record.raw_payload["symbol"] == "US500"
    assert record.raw_payload["row"]["Close"] == 6010.25


def test_fdr_common_feature_provider_falls_back_to_source_series_key(monkeypatch) -> None:
    frame = pd.DataFrame(
        [{"Close": 42.5}],
        index=pd.to_datetime(["2026-06-05"]),
    )
    fake_fdr = FakeFdr(frame)
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(source_series_key="VIX"),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert fake_fdr.calls == [("VIX", "2026-06-05", "2026-06-09")]
    assert result.records[0].value_numeric == Decimal("42.5")


def test_fdr_common_feature_provider_filters_rows_outside_requested_range(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {"Close": 6000.0},
            {"Close": 6010.25},
            {"Close": 6020.0},
        ],
        index=pd.to_datetime(["2026-06-04", "2026-06-05", "2026-06-09"]),
    )
    fake_fdr = FakeFdr(frame)
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"symbol": "US500"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 5), Decimal("6010.25"))
    ]


def test_fdr_common_feature_provider_reports_no_data(monkeypatch) -> None:
    fake_fdr = FakeFdr(pd.DataFrame())
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"symbol": "US500"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_fdr_common_feature_provider_skips_non_finite_close_values(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {"Close": float("nan"), "Volume": float("nan")},
            {"Close": float("inf"), "Volume": 1000},
            {"Close": 6010.25, "Volume": 2000},
        ],
        index=pd.to_datetime(["2026-06-04", "2026-06-05", "2026-06-08"]),
    )
    fake_fdr = FakeFdr(frame)
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"symbol": "US500"}),
        start=date(2026, 6, 4),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is False
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 8), Decimal("6010.25"))
    ]
    assert result.records[0].value_text == ""
    assert result.records[0].raw_payload["row"]["Close"] == 6010.25


def test_fdr_common_feature_provider_reports_no_data_when_all_close_values_invalid(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        [{"Close": float("nan")}, {"Close": float("-inf")}],
        index=pd.to_datetime(["2026-06-05", "2026-06-08"]),
    )
    fake_fdr = FakeFdr(frame)
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"symbol": "US500"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_fdr_common_feature_provider_reports_fdr_exception(monkeypatch) -> None:
    fake_fdr = FakeFdr(error=RuntimeError("FDR unavailable"))
    monkeypatch.setattr(provider_module, "fdr", fake_fdr)

    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"symbol": "US500"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert result.error == "FDR unavailable"


def test_fdr_common_feature_provider_rejects_non_fdr_series() -> None:
    result = FdrCommonFeatureProvider().fetch_series(
        series=_series(source=Source.PYKRX),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert "cannot fetch source PYKRX" in result.error

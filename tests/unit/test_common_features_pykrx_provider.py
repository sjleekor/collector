from datetime import date
from decimal import Decimal

import pandas as pd

from collector.kr.adapters.common_features_pykrx import provider as provider_module
from collector.kr.adapters.common_features_pykrx.provider import PykrxCommonFeatureProvider
from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureSeries


def _series(
    *,
    source_series_key: str = "fallback-code",
    endpoint_params: dict[str, object] | None = None,
    source: Source = Source.PYKRX,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id="market_kospi",
        source=source,
        source_series_key=source_series_key,
        category="market_index",
        frequency="D",
        name_kr="KOSPI",
        unit="index_point",
        endpoint_params=endpoint_params or {},
        availability_policy="next_krx_session",
    )


class FakePykrxStock:
    def __init__(self, frame: pd.DataFrame | None = None, error: Exception | None = None) -> None:
        self.frame = frame
        self.error = error
        self.calls: list[tuple[str, str, str]] = []

    def get_index_ohlcv_by_date(
        self,
        start: str,
        end: str,
        index_code: str,
    ) -> pd.DataFrame:
        self.calls.append((start, end, index_code))
        if self.error is not None:
            raise self.error
        return self.frame if self.frame is not None else pd.DataFrame()


def test_pykrx_common_feature_provider_uses_endpoint_index_code(monkeypatch) -> None:
    frame = pd.DataFrame(
        [{"시가": 2900.0, "고가": 2920.0, "저가": 2890.0, "종가": 2910.42, "거래량": 1000}],
        index=pd.to_datetime(["2026-06-08"]),
    )
    fake_stock = FakePykrxStock(frame)
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"index_code": "1001"}),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert fake_stock.calls == [("20260608", "20260608", "1001")]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source == Source.PYKRX
    assert record.series_id == "market_kospi"
    assert record.observation_date == date(2026, 6, 8)
    assert record.period_end_date == date(2026, 6, 8)
    assert record.release_date is None
    assert record.available_from_date is None
    assert record.value_numeric == Decimal("2910.42")
    assert record.unit == "index_point"
    assert record.frequency == "D"
    assert record.raw_payload["index_code"] == "1001"
    assert record.raw_payload["row"]["종가"] == 2910.42


def test_pykrx_common_feature_provider_falls_back_to_source_series_key(monkeypatch) -> None:
    frame = pd.DataFrame(
        [{"종가": 910.12}],
        index=pd.to_datetime(["2026-06-08"]),
    )
    fake_stock = FakePykrxStock(frame)
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(source_series_key="2001"),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert fake_stock.calls == [("20260608", "20260608", "2001")]
    assert result.records[0].value_numeric == Decimal("910.12")


def test_pykrx_common_feature_provider_filters_rows_outside_requested_range(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {"종가": 2900.0},
            {"종가": 2910.42},
            {"종가": 2920.0},
        ],
        index=pd.to_datetime(["2026-06-04", "2026-06-05", "2026-06-09"]),
    )
    fake_stock = FakePykrxStock(frame)
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"index_code": "1001"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 5), Decimal("2910.42"))
    ]


def test_pykrx_common_feature_provider_reports_no_data(monkeypatch) -> None:
    fake_stock = FakePykrxStock(pd.DataFrame())
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"index_code": "1001"}),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_pykrx_common_feature_provider_skips_non_finite_close_values(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {"종가": float("nan"), "거래량": float("nan")},
            {"종가": float("inf"), "거래량": 1000},
            {"종가": 2910.42, "거래량": 2000},
        ],
        index=pd.to_datetime(["2026-06-04", "2026-06-05", "2026-06-08"]),
    )
    fake_stock = FakePykrxStock(frame)
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"index_code": "1001"}),
        start=date(2026, 6, 4),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is False
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 8), Decimal("2910.42"))
    ]
    assert result.records[0].value_text == ""
    assert result.records[0].raw_payload["row"]["종가"] == 2910.42


def test_pykrx_common_feature_provider_reports_no_data_when_all_close_values_invalid(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        [{"종가": float("nan")}, {"종가": float("-inf")}],
        index=pd.to_datetime(["2026-06-05", "2026-06-08"]),
    )
    fake_stock = FakePykrxStock(frame)
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"index_code": "1001"}),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_pykrx_common_feature_provider_reports_pykrx_exception(monkeypatch) -> None:
    fake_stock = FakePykrxStock(error=RuntimeError("pykrx unavailable"))
    monkeypatch.setattr(provider_module, "get_pykrx_stock_module", lambda: fake_stock)

    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(endpoint_params={"index_code": "1001"}),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert result.error == "pykrx unavailable"


def test_pykrx_common_feature_provider_rejects_non_pykrx_series() -> None:
    result = PykrxCommonFeatureProvider().fetch_series(
        series=_series(source=Source.FDR),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert "cannot fetch source FDR" in result.error

import logging
from datetime import date
from decimal import Decimal

from collector.kr.adapters.common_features_krx.provider import (
    KRX_INDEX_OHLCV_BLD,
    KRX_MARKET_BREADTH_BLD,
    KrxCommonFeatureProvider,
)
from collector.kr.adapters.krx_common.client import KrxMdcRow
from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureSeries


def _series(
    *,
    series_id: str = "market_kospi_krx",
    source_series_key: str = "1001",
    endpoint_params: dict[str, object] | None = None,
    source: Source = Source.KRX,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=source_series_key,
        category="market_index",
        frequency="D",
        name_kr=series_id,
        unit="index_point",
        endpoint_params=endpoint_params or {},
        availability_policy="next_krx_session",
    )


class FakeKrxClient:
    def __init__(
        self,
        rows: list[KrxMdcRow] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.error = error
        self.calls: list[dict[str, object]] = []

    def post_rows(
        self,
        bld: str,
        params: dict[str, object],
        *,
        output_key: str,
    ) -> list[KrxMdcRow]:
        self.calls.append({"bld": bld, "params": dict(params), "output_key": output_key})
        if self.error is not None:
            raise self.error
        return self.rows


def test_krx_common_feature_provider_uses_endpoint_index_params() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(
                row={
                    "TRD_DD": "2026/06/08",
                    "CLSPRC_IDX": "2,910.42",
                    "ACC_TRDVOL": "1,000",
                },
                request={"indIdx": "1", "indIdx2": "001", "strtDd": "20260608"},
            )
        ]
    )

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(
            endpoint_params={
                "bld": KRX_INDEX_OHLCV_BLD,
                "output_key": "output",
                "index_code": "1001",
                "indIdx": "1",
                "indIdx2": "001",
            }
        ),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert client.calls == [
        {
            "bld": KRX_INDEX_OHLCV_BLD,
            "params": {
                "indIdx": "1",
                "indIdx2": "001",
                "strtDd": "20260608",
                "endDd": "20260608",
            },
            "output_key": "output",
        }
    ]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source == Source.KRX
    assert record.series_id == "market_kospi_krx"
    assert record.observation_date == date(2026, 6, 8)
    assert record.period_end_date == date(2026, 6, 8)
    assert record.release_date is None
    assert record.available_from_date is None
    assert record.value_numeric == Decimal("2910.42")
    assert record.unit == "index_point"
    assert record.frequency == "D"
    assert record.raw_payload["bld"] == KRX_INDEX_OHLCV_BLD
    assert record.raw_payload["index_code"] == "1001"
    assert record.raw_payload["indIdx"] == "1"
    assert record.raw_payload["indIdx2"] == "001"
    assert record.raw_payload["row"]["CLSPRC_IDX"] == "2,910.42"


def test_krx_common_feature_provider_falls_back_to_split_source_series_key() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(
                row={"TRD_DD": "20260608", "CLSPRC_IDX": "910.12"},
                request={"indIdx": "2", "indIdx2": "001"},
            )
        ]
    )

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(
            series_id="market_kosdaq_krx",
            source_series_key="2001",
        ),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert client.calls[0]["params"]["indIdx"] == "2"
    assert client.calls[0]["params"]["indIdx2"] == "001"
    assert result.records[0].value_numeric == Decimal("910.12")


def test_krx_common_feature_provider_filters_rows_outside_requested_range() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(row={"TRD_DD": "2026/06/04", "CLSPRC_IDX": "2880.00"}, request={}),
            KrxMdcRow(row={"TRD_DD": "2026/06/05", "CLSPRC_IDX": "2910.42"}, request={}),
            KrxMdcRow(row={"TRD_DD": "2026/06/09", "CLSPRC_IDX": "2920.00"}, request={}),
        ]
    )

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 5), Decimal("2910.42"))
    ]


def test_krx_common_feature_provider_reports_no_data() -> None:
    client = FakeKrxClient([])

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is True
    assert result.records == []


def test_krx_common_feature_provider_skips_invalid_close_values() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(row={"TRD_DD": "2026/06/05", "CLSPRC_IDX": "-"}, request={}),
            KrxMdcRow(row={"TRD_DD": "2026/06/08", "CLSPRC_IDX": "2910.42"}, request={}),
        ]
    )

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.no_data is False
    assert [(record.observation_date, record.value_numeric) for record in result.records] == [
        (date(2026, 6, 8), Decimal("2910.42"))
    ]


def test_krx_common_feature_provider_aggregates_market_breadth() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(
                row={
                    "ISU_SRT_CD": "000001",
                    "FLUC_TP_CD": "1",
                    "ACC_TRDVAL": "1,000",
                    "ACC_TRDVOL": "10",
                    "MKT_ID": "STK",
                },
                request={"mktId": "STK", "trdDd": "20260608"},
            ),
            KrxMdcRow(
                row={
                    "ISU_SRT_CD": "000002",
                    "FLUC_TP_CD": "4",
                    "ACC_TRDVAL": "2,000",
                    "ACC_TRDVOL": "20",
                    "MKT_ID": "STK",
                },
                request={"mktId": "STK", "trdDd": "20260608"},
            ),
            KrxMdcRow(
                row={
                    "ISU_SRT_CD": "000003",
                    "FLUC_TP_CD": "2",
                    "ACC_TRDVAL": "3,000",
                    "ACC_TRDVOL": "30",
                    "MKT_ID": "STK",
                },
                request={"mktId": "STK", "trdDd": "20260608"},
            ),
            KrxMdcRow(
                row={
                    "ISU_SRT_CD": "000004",
                    "FLUC_TP_CD": "3",
                    "ACC_TRDVAL": "-",
                    "ACC_TRDVOL": "-",
                    "MKT_ID": "STK",
                },
                request={"mktId": "STK", "trdDd": "20260608"},
            ),
        ]
    )

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(
            series_id="market_kospi_advancers_krx",
            source_series_key="MDCSTAT01501:STK:advancers",
            endpoint_params={
                "kind": "market_breadth",
                "bld": KRX_MARKET_BREADTH_BLD,
                "output_key": "OutBlock_1",
                "mktId": "STK",
                "metric": "advancers",
            },
        ),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert client.calls == [
        {
            "bld": KRX_MARKET_BREADTH_BLD,
            "params": {"mktId": "STK", "trdDd": "20260608"},
            "output_key": "OutBlock_1",
        }
    ]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.observation_date == date(2026, 6, 8)
    assert record.value_numeric == Decimal("2")
    assert record.raw_payload["kind"] == "market_breadth"
    assert record.raw_payload["metric"] == "advancers"
    assert record.raw_payload["row_count"] == 4
    assert record.raw_payload["fluc_counts"] == {"1": 1, "2": 1, "3": 1, "4": 1}


def test_krx_common_feature_provider_aggregates_market_turnover_value() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(row={"FLUC_TP_CD": "1", "ACC_TRDVAL": "1,000"}, request={}),
            KrxMdcRow(row={"FLUC_TP_CD": "2", "ACC_TRDVAL": "2,500"}, request={}),
            KrxMdcRow(row={"FLUC_TP_CD": "3", "ACC_TRDVAL": "-"}, request={}),
        ]
    )

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(
            series_id="market_kospi_turnover_value_krx",
            source_series_key="MDCSTAT01501:STK:total_turnover_value",
            endpoint_params={
                "kind": "market_breadth",
                "mktId": "STK",
                "metric": "total_turnover_value",
            },
        ),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.error is None
    assert result.records[0].value_numeric == Decimal("3500")


def test_krx_common_feature_provider_logs_market_breadth_progress(caplog) -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(row={"FLUC_TP_CD": "1", "ACC_TRDVAL": "1,000"}, request={}),
            KrxMdcRow(row={"FLUC_TP_CD": "2", "ACC_TRDVAL": "2,500"}, request={}),
        ]
    )
    caplog.set_level(logging.INFO, logger="collector.kr.adapters.common_features_krx.provider")

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(
            series_id="market_kospi_advancers_krx",
            endpoint_params={
                "kind": "market_breadth",
                "mktId": "STK",
                "metric": "advancers",
            },
        ),
        start=date(2026, 1, 1),
        end=date(2026, 3, 31),
    )

    messages = [record.getMessage() for record in caplog.records]
    assert result.error is None
    assert len(client.calls) > 50
    assert any("KRX market breadth fetch started" in message for message in messages)
    assert any(
        "KRX market breadth progress" in message and "processed=50/" in message
        for message in messages
    )
    assert any("KRX market breadth fetch completed" in message for message in messages)


def test_krx_common_feature_provider_reuses_market_breadth_rows_for_same_day() -> None:
    client = FakeKrxClient(
        [
            KrxMdcRow(row={"FLUC_TP_CD": "1", "ACC_TRDVAL": "1,000"}, request={}),
            KrxMdcRow(row={"FLUC_TP_CD": "2", "ACC_TRDVAL": "2,500"}, request={}),
        ]
    )
    provider = KrxCommonFeatureProvider(client=client)

    advancers = provider.fetch_series(
        series=_series(
            series_id="market_kospi_advancers_krx",
            endpoint_params={
                "kind": "market_breadth",
                "mktId": "STK",
                "metric": "advancers",
            },
        ),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )
    turnover = provider.fetch_series(
        series=_series(
            series_id="market_kospi_turnover_value_krx",
            endpoint_params={
                "kind": "market_breadth",
                "mktId": "STK",
                "metric": "total_turnover_value",
            },
        ),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert advancers.records[0].value_numeric == Decimal("1")
    assert turnover.records[0].value_numeric == Decimal("3500")
    assert len(client.calls) == 1


def test_krx_common_feature_provider_reports_error() -> None:
    client = FakeKrxClient(error=RuntimeError("KRX unavailable"))

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert result.error == "KRX unavailable"


def test_krx_common_feature_provider_rejects_non_krx_series() -> None:
    client = FakeKrxClient()

    result = KrxCommonFeatureProvider(client=client).fetch_series(
        series=_series(source=Source.PYKRX),
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
    )

    assert result.records == []
    assert "cannot fetch source PYKRX" in result.error

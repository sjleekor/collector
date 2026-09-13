from datetime import date

from collector.kr.definitions.common_features import DAILY_MAX_STALE_BUSINESS_DAYS
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureSeries,
    UpsertResult,
)
from collector.kr.service.default_common_feature_catalog import (
    default_common_feature_catalog,
    default_common_feature_series,
    seed_common_feature_catalog,
)


class MockCommonFeatureSeedStorage:
    def __init__(self) -> None:
        self.series: list[CommonFeatureSeries] = []
        self.catalog: list[CommonFeatureCatalogEntry] = []

    def upsert_common_feature_series(
        self,
        records: list[CommonFeatureSeries],
    ) -> UpsertResult:
        self.series = records
        return UpsertResult(updated=len(records))

    def upsert_common_feature_catalog(
        self,
        records: list[CommonFeatureCatalogEntry],
    ) -> UpsertResult:
        self.catalog = records
        return UpsertResult(updated=len(records))


def test_default_common_feature_series_ids_are_unique() -> None:
    series = default_common_feature_series()
    series_ids = [item.series_id for item in series]

    assert len(series_ids) == len(set(series_ids))


def test_default_common_feature_catalog_codes_are_unique() -> None:
    catalog = default_common_feature_catalog()
    feature_codes = [item.feature_code for item in catalog]

    assert len(feature_codes) == len(set(feature_codes))


def test_default_common_feature_catalog_inputs_exist_in_series_seed() -> None:
    series_ids = {item.series_id for item in default_common_feature_series()}

    missing_inputs = sorted(
        series_id
        for feature in default_common_feature_catalog()
        for series_id in feature.input_series_ids
        if series_id not in series_ids
    )

    assert missing_inputs == []


def test_default_common_feature_series_policies_match_pit_contract() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}

    assert series_by_id["market_kospi"].availability_policy == "next_krx_session"
    assert series_by_id["market_kospi"].source_timezone == "Asia/Seoul"
    assert series_by_id["market_kospi_krx"].availability_policy == "next_krx_session"
    assert series_by_id["market_kospi_krx"].source_timezone == "Asia/Seoul"
    assert series_by_id["market_kospi_advancers_krx"].availability_policy == ("next_krx_session")
    assert series_by_id["market_kospi_advancers_krx"].source_timezone == "Asia/Seoul"
    assert series_by_id["market_kosdaq"].availability_policy == "next_krx_session"
    assert series_by_id["market_kosdaq_krx"].availability_policy == "next_krx_session"
    assert series_by_id["market_kosdaq_advancers_krx"].availability_policy == ("next_krx_session")
    assert series_by_id["global_sp500"].availability_policy == "same_krx_session_morning"
    assert series_by_id["global_sp500"].source_timezone == "America/New_York"
    assert series_by_id["global_nasdaq"].source_timezone == "America/New_York"
    assert series_by_id["global_vix"].source_timezone == "America/New_York"
    assert series_by_id["fx_usdkrw_ecos"].availability_policy == "next_krx_session"
    assert series_by_id["fx_usdkrw_ecos"].source_timezone == "Asia/Seoul"
    assert series_by_id["rate_us2y"].availability_policy == "same_krx_session_morning"
    assert series_by_id["rate_us2y"].source_timezone == "America/New_York"
    assert series_by_id["rate_us10y"].availability_policy == "same_krx_session_morning"
    assert series_by_id["rate_us10y"].source_timezone == "America/New_York"
    assert series_by_id["commodity_wti_fred"].source_timezone == "America/New_York"
    assert series_by_id["rate_kr_gov3y"].availability_policy == "next_krx_session"
    assert series_by_id["rate_kr_gov10y"].availability_policy == "next_krx_session"
    assert series_by_id["rate_kr_gov10y"].source_timezone == "Asia/Seoul"
    assert series_by_id["macro_cpi"].availability_policy == "manual_lag_days"
    assert series_by_id["macro_cpi"].manual_lag_days == 20


def test_default_common_feature_series_declares_stale_limits() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    daily = (
        "market_kospi",
        "market_kospi_krx",
        "market_kospi_advancers_krx",
        "market_kospi_turnover_value_krx",
        "market_kosdaq",
        "market_kosdaq_krx",
        "market_kosdaq_advancers_krx",
        "market_kosdaq_turnover_value_krx",
        "market_kospi200",
        "market_kospi200_krx",
        "global_sp500",
        "global_nasdaq",
        "global_vix",
        "commodity_wti",
        "rate_us2y",
        "rate_us10y",
        "rate_kr_gov3y",
        "rate_kr_gov10y",
    )
    for series_id in daily:
        limit = series_by_id[series_id].max_stale_business_days
        assert limit == DAILY_MAX_STALE_BUSINESS_DAYS, series_id

    # Sources that publish on their own calendar keep their own limits.
    assert series_by_id["commodity_wti_fred"].max_stale_business_days == 10
    assert series_by_id["fx_usdkrw"].max_stale_business_days == 10
    assert series_by_id["fx_usdkrw_ecos"].max_stale_business_days == 10
    assert series_by_id["macro_cpi"].max_stale_business_days == 45
    assert series_by_id["macro_m2"].max_stale_business_days == 90


def test_default_common_feature_catalog_contains_phase1_mvp_features() -> None:
    feature_codes = {item.feature_code for item in default_common_feature_catalog()}

    assert "market_kospi_close" in feature_codes
    assert "market_kospi_ret_1d" in feature_codes
    assert "market_kospi_krx_close" in feature_codes
    assert "market_kospi_krx_ret_1d" in feature_codes
    assert "global_sp500_ret_1d" in feature_codes
    assert "global_vix_level" in feature_codes
    assert "fx_usdkrw_level" in feature_codes
    assert "commodity_wti_ret_20d" in feature_codes
    assert "commodity_wti_spot_ret_20d" in feature_codes
    assert "commodity_wti_spot_level" in feature_codes
    assert "rate_us10y_level" in feature_codes


def test_default_common_feature_krx_direct_market_indexes_are_active_after_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    assert series_by_id["market_kospi"].source == Source.PYKRX
    assert series_by_id["market_kospi"].active is False
    assert series_by_id["market_kosdaq"].source == Source.PYKRX
    assert series_by_id["market_kosdaq"].active is False
    assert series_by_id["market_kospi200"].source == Source.PYKRX
    assert series_by_id["market_kospi200"].active is False

    kospi = series_by_id["market_kospi_krx"]
    assert kospi.source == Source.KRX
    assert kospi.active is True
    assert kospi.source_series_key == "1001"
    assert kospi.endpoint_params == {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
        "output_key": "output",
        "index_code": "1001",
        "indIdx": "1",
        "indIdx2": "001",
    }

    kosdaq = series_by_id["market_kosdaq_krx"]
    assert kosdaq.source == Source.KRX
    assert kosdaq.active is True
    assert kosdaq.source_series_key == "2001"
    assert kosdaq.endpoint_params["indIdx"] == "2"
    assert kosdaq.endpoint_params["indIdx2"] == "001"

    kospi200 = series_by_id["market_kospi200_krx"]
    assert kospi200.source == Source.KRX
    assert kospi200.active is True
    assert kospi200.source_series_key == "1028"
    assert kospi200.endpoint_params["indIdx"] == "1"
    assert kospi200.endpoint_params["indIdx2"] == "028"

    assert catalog_by_code["market_kospi_close"].active is True
    assert catalog_by_code["market_kospi_close"].input_series_ids == ("market_kospi_krx",)
    assert catalog_by_code["market_kospi_ret_1d"].active is True
    assert catalog_by_code["market_kospi_ret_1d"].input_series_ids == ("market_kospi_krx",)
    assert catalog_by_code["market_kospi_ret_5d"].active is True
    assert catalog_by_code["market_kospi_ret_5d"].input_series_ids == ("market_kospi_krx",)
    assert catalog_by_code["market_kospi_ret_20d"].active is True
    assert catalog_by_code["market_kospi_ret_20d"].input_series_ids == ("market_kospi_krx",)
    assert catalog_by_code["market_kosdaq_ret_1d"].active is True
    assert catalog_by_code["market_kosdaq_ret_1d"].input_series_ids == ("market_kosdaq_krx",)
    assert catalog_by_code["market_kospi200_ret_1d"].active is True
    assert catalog_by_code["market_kospi200_ret_1d"].input_series_ids == ("market_kospi200_krx",)

    assert catalog_by_code["market_kospi_krx_close"].active is False
    assert catalog_by_code["market_kospi_krx_close"].input_series_ids == ("market_kospi_krx",)
    assert catalog_by_code["market_kospi_krx_ret_1d"].active is False
    assert catalog_by_code["market_kospi_krx_ret_1d"].input_series_ids == ("market_kospi_krx",)
    assert catalog_by_code["market_kospi_krx_ret_5d"].active is False
    assert catalog_by_code["market_kospi_krx_ret_20d"].active is False
    assert catalog_by_code["market_kosdaq_krx_ret_1d"].active is False
    assert catalog_by_code["market_kosdaq_krx_ret_1d"].input_series_ids == ("market_kosdaq_krx",)
    assert catalog_by_code["market_kospi200_krx_ret_1d"].active is False
    assert catalog_by_code["market_kospi200_krx_ret_1d"].input_series_ids == (
        "market_kospi200_krx",
    )


def test_default_common_feature_krx_breadth_features_are_active_after_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    expected_series = {
        "market_kospi_advancers_krx": ("STK", "advancers", "count"),
        "market_kospi_decliners_krx": ("STK", "decliners", "count"),
        "market_kospi_unchanged_krx": ("STK", "unchanged", "count"),
        "market_kospi_turnover_value_krx": ("STK", "total_turnover_value", "KRW"),
        "market_kosdaq_advancers_krx": ("KSQ", "advancers", "count"),
        "market_kosdaq_decliners_krx": ("KSQ", "decliners", "count"),
        "market_kosdaq_unchanged_krx": ("KSQ", "unchanged", "count"),
        "market_kosdaq_turnover_value_krx": ("KSQ", "total_turnover_value", "KRW"),
    }

    for series_id, (market_id, metric, unit) in expected_series.items():
        series = series_by_id[series_id]
        assert series.source == Source.KRX
        assert series.active is True
        assert series.unit == unit
        assert series.endpoint_params["kind"] == "market_breadth"
        assert series.endpoint_params["bld"] == "dbms/MDC/STAT/standard/MDCSTAT01501"
        assert series.endpoint_params["output_key"] == "OutBlock_1"
        assert series.endpoint_params["mktId"] == market_id
        assert series.endpoint_params["metric"] == metric

    expected_features = {
        "market_kospi_advancers_count": "market_kospi_advancers_krx",
        "market_kospi_decliners_count": "market_kospi_decliners_krx",
        "market_kospi_unchanged_count": "market_kospi_unchanged_krx",
        "market_kospi_turnover_value": "market_kospi_turnover_value_krx",
        "market_kosdaq_advancers_count": "market_kosdaq_advancers_krx",
        "market_kosdaq_decliners_count": "market_kosdaq_decliners_krx",
        "market_kosdaq_unchanged_count": "market_kosdaq_unchanged_krx",
        "market_kosdaq_turnover_value": "market_kosdaq_turnover_value_krx",
    }

    for feature_code, series_id in expected_features.items():
        feature = catalog_by_code[feature_code]
        assert feature.active is True
        assert feature.transform_code == "level"
        assert feature.input_series_ids == (series_id,)


def test_default_common_feature_industry_index_candidates_remain_inactive() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    expected_series = {
        "industry_krx_semiconductor_krx": ("KRX", "5044", "5", "044"),
        "industry_kospi_electronics_krx": ("KOSPI", "1013", "1", "013"),
        "industry_kospi_financials_krx": ("KOSPI", "1021", "1", "021"),
        "industry_kosdaq_pharma_krx": ("KOSDAQ", "2066", "2", "066"),
    }

    for series_id, (market, index_code, ind_idx, ind_idx2) in expected_series.items():
        series = series_by_id[series_id]
        assert series.source == Source.KRX
        assert series.active is False
        assert series.category == "industry_index"
        assert series.unit == "index_point"
        assert series.market == market
        assert series.availability_policy == "next_krx_session"
        assert series.source_timezone == "Asia/Seoul"
        assert series.max_stale_business_days == DAILY_MAX_STALE_BUSINESS_DAYS
        assert series.endpoint_params == {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
            "output_key": "output",
            "index_code": index_code,
            "indIdx": ind_idx,
            "indIdx2": ind_idx2,
        }

    expected_features = {
        "industry_krx_semiconductor_level": (
            "industry_krx_semiconductor_krx",
            "level",
            "index_point",
        ),
        "industry_krx_semiconductor_ret_1d": (
            "industry_krx_semiconductor_krx",
            "ret_1d",
            "pct",
        ),
        "industry_kospi_electronics_level": (
            "industry_kospi_electronics_krx",
            "level",
            "index_point",
        ),
        "industry_kospi_electronics_ret_1d": (
            "industry_kospi_electronics_krx",
            "ret_1d",
            "pct",
        ),
        "industry_kospi_financials_level": (
            "industry_kospi_financials_krx",
            "level",
            "index_point",
        ),
        "industry_kospi_financials_ret_1d": (
            "industry_kospi_financials_krx",
            "ret_1d",
            "pct",
        ),
        "industry_kosdaq_pharma_level": (
            "industry_kosdaq_pharma_krx",
            "level",
            "index_point",
        ),
        "industry_kosdaq_pharma_ret_1d": (
            "industry_kosdaq_pharma_krx",
            "ret_1d",
            "pct",
        ),
    }

    for feature_code, (series_id, transform_code, unit) in expected_features.items():
        feature = catalog_by_code[feature_code]
        assert feature.active is False
        assert feature.category == "industry_index"
        assert feature.transform_code == transform_code
        assert feature.unit == unit
        assert feature.input_series_ids == (series_id,)


def test_default_common_feature_fx_usdkrw_uses_ecos_after_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    fallback = series_by_id["fx_usdkrw"]
    assert fallback.source == Source.FDR
    assert fallback.active is True

    ecos = series_by_id["fx_usdkrw_ecos"]
    assert ecos.source == Source.ECOS
    assert ecos.active is True
    assert ecos.source_series_key == "731Y001"
    assert ecos.history_start_date.isoformat() == "1964-05-04"
    assert ecos.endpoint_params == {
        "stat_code": "731Y001",
        "cycle": "D",
        "item_code1": "0000001",
    }

    active_level = catalog_by_code["fx_usdkrw_level"]
    assert active_level.active is True
    assert active_level.input_series_ids == ("fx_usdkrw_ecos",)

    active_return = catalog_by_code["fx_usdkrw_ret_5d"]
    assert active_return.active is True
    assert active_return.input_series_ids == ("fx_usdkrw_ecos",)

    ecos_level = catalog_by_code["fx_usdkrw_ecos_level"]
    assert ecos_level.active is False
    assert ecos_level.input_series_ids == ("fx_usdkrw_ecos",)

    ecos_return = catalog_by_code["fx_usdkrw_ecos_ret_5d"]
    assert ecos_return.active is False
    assert ecos_return.input_series_ids == ("fx_usdkrw_ecos",)


def test_default_common_feature_catalog_multi_input_roles_are_well_formed() -> None:
    catalog = default_common_feature_catalog()

    malformed = [
        feature.feature_code
        for feature in catalog
        if feature.input_roles and len(feature.input_roles) != len(feature.input_series_ids)
    ]

    assert malformed == []


def test_default_common_feature_fred_rate_candidates_are_active_after_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    us2y = series_by_id["rate_us2y"]
    assert us2y.source == Source.FRED
    assert us2y.active is True
    assert us2y.source_series_key == "DGS2"
    assert us2y.history_start_date.isoformat() == "1976-06-01"
    assert us2y.endpoint_params == {"series_id": "DGS2"}

    us10y = series_by_id["rate_us10y"]
    assert us10y.source == Source.FRED
    assert us10y.active is True
    assert us10y.source_series_key == "DGS10"
    assert us10y.history_start_date.isoformat() == "1962-01-02"
    assert us10y.endpoint_params == {"series_id": "DGS10"}

    assert catalog_by_code["rate_us2y_level"].active is True
    assert catalog_by_code["rate_us2y_level"].input_series_ids == ("rate_us2y",)
    assert catalog_by_code["rate_us10y_level"].active is True
    assert catalog_by_code["rate_us10y_level"].input_series_ids == ("rate_us10y",)

    spread = catalog_by_code["rate_us_term_spread_10y_2y"]
    assert spread.active is True
    assert spread.transform_code == "spread"
    assert spread.input_series_ids == ("rate_us10y", "rate_us2y")
    assert spread.input_roles == ("spread_long", "spread_short")
    assert spread.series_by_role() == {
        "spread_long": "rate_us10y",
        "spread_short": "rate_us2y",
    }


def test_default_common_feature_fred_wti_spot_is_active_as_parallel_feature() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    wti = series_by_id["commodity_wti_fred"]
    assert wti.source == Source.FRED
    assert wti.active is True
    assert wti.source_series_key == "DCOILWTICO"
    assert wti.history_start_date.isoformat() == "1986-01-02"
    assert wti.endpoint_params == {"series_id": "DCOILWTICO"}

    assert catalog_by_code["commodity_wti_ret_20d"].active is True
    assert catalog_by_code["commodity_wti_ret_20d"].input_series_ids == ("commodity_wti",)
    assert catalog_by_code["commodity_wti_spot_ret_20d"].active is True
    assert catalog_by_code["commodity_wti_spot_ret_20d"].input_series_ids == ("commodity_wti_fred",)
    assert catalog_by_code["commodity_wti_fred_ret_20d"].active is False
    assert catalog_by_code["commodity_wti_fred_ret_20d"].input_series_ids == ("commodity_wti_fred",)


def test_default_common_feature_rate_kr_gov3y_is_active_after_pr4j() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    rate = series_by_id["rate_kr_gov3y"]
    assert rate.source == Source.ECOS
    assert rate.active is True
    assert rate.source_series_key == "817Y002"
    assert rate.endpoint_params == {
        "stat_code": "817Y002",
        "cycle": "D",
        "item_code1": "010200000",
    }

    rate_feature = catalog_by_code["rate_kr_gov3y_level"]
    assert rate_feature.active is True
    assert rate_feature.input_series_ids == ("rate_kr_gov3y",)


def test_default_common_feature_gov10y_and_term_spread_are_active_after_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    rate = series_by_id["rate_kr_gov10y"]
    assert rate.source == Source.ECOS
    assert rate.active is True
    assert rate.source_series_key == "817Y002"
    assert rate.endpoint_params == {
        "stat_code": "817Y002",
        "cycle": "D",
        "item_code1": "010210000",
    }

    rate_feature = catalog_by_code["rate_kr_gov10y_level"]
    assert rate_feature.active is True
    assert rate_feature.input_series_ids == ("rate_kr_gov10y",)

    spread_feature = catalog_by_code["rate_kr_term_spread_10y_3y"]
    assert spread_feature.active is True
    assert spread_feature.transform_code == "spread"
    assert spread_feature.input_series_ids == ("rate_kr_gov10y", "rate_kr_gov3y")
    assert spread_feature.input_roles == ("spread_long", "spread_short")
    assert spread_feature.series_by_role() == {
        "spread_long": "rate_kr_gov10y",
        "spread_short": "rate_kr_gov3y",
    }


def test_default_common_feature_macro_cpi_is_active_after_conservative_policy_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    cpi = series_by_id["macro_cpi"]
    assert cpi.source == Source.ECOS
    assert cpi.active is True
    assert cpi.source_series_key == "901Y009"
    assert cpi.frequency == "M"
    assert cpi.unit == "2020=100"
    assert cpi.availability_policy == "manual_lag_days"
    assert cpi.manual_lag_days == 20
    assert cpi.endpoint_params == {
        "stat_code": "901Y009",
        "cycle": "M",
        "item_code1": "0",
    }

    assert catalog_by_code["macro_cpi_level"].active is True
    assert catalog_by_code["macro_cpi_level"].frequency == "M"
    assert catalog_by_code["macro_cpi_level"].unit == "2020=100"
    assert catalog_by_code["macro_cpi_level"].transform_code == "level"
    assert catalog_by_code["macro_cpi_level"].input_series_ids == ("macro_cpi",)

    assert catalog_by_code["macro_cpi_yoy_latest"].active is True
    assert catalog_by_code["macro_cpi_yoy_latest"].frequency == "M"
    assert catalog_by_code["macro_cpi_yoy_latest"].unit == "pct"
    assert catalog_by_code["macro_cpi_yoy_latest"].transform_code == "yoy"
    assert catalog_by_code["macro_cpi_yoy_latest"].input_series_ids == ("macro_cpi",)

    assert catalog_by_code["macro_cpi_mom_latest"].active is True
    assert catalog_by_code["macro_cpi_mom_latest"].frequency == "M"
    assert catalog_by_code["macro_cpi_mom_latest"].unit == "pct"
    assert catalog_by_code["macro_cpi_mom_latest"].transform_code == "mom"
    assert catalog_by_code["macro_cpi_mom_latest"].input_series_ids == ("macro_cpi",)


def test_default_common_feature_macro_monthly_candidates_are_active_after_validation() -> None:
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    expected_series = {
        "macro_ppi": (
            "404Y014",
            "macro_price",
            "2020=100",
            date(1965, 1, 1),
            45,
            {"stat_code": "404Y014", "cycle": "M", "item_code1": "*AA"},
        ),
        "macro_m2": (
            "161Y005",
            "macro_money",
            "KRW_bn",
            date(2003, 10, 1),
            90,
            {"stat_code": "161Y005", "cycle": "M", "item_code1": "BBHS00"},
        ),
        "macro_consumer_sentiment": (
            "511Y002",
            "macro_sentiment",
            "index",
            date(2008, 7, 1),
            45,
            {
                "stat_code": "511Y002",
                "cycle": "M",
                "item_code1": "FME",
                "item_code2": "99988",
            },
        ),
    }

    for (
        series_id,
        (source_key, category, unit, history_start, max_stale_days, endpoint_params),
    ) in expected_series.items():
        series = series_by_id[series_id]
        assert series.source == Source.ECOS
        assert series.active is True
        assert series.source_series_key == source_key
        assert series.category == category
        assert series.frequency == "M"
        assert series.unit == unit
        assert series.availability_policy == "manual_lag_days"
        assert series.manual_lag_days == 20
        assert series.history_start_date == history_start
        assert series.max_stale_business_days == max_stale_days
        assert series.endpoint_params == endpoint_params

    expected_features = {
        "macro_ppi_level": ("macro_ppi", "macro_price", "2020=100", "level"),
        "macro_ppi_yoy_latest": ("macro_ppi", "macro_price", "pct", "yoy"),
        "macro_ppi_mom_latest": ("macro_ppi", "macro_price", "pct", "mom"),
        "macro_m2_level": ("macro_m2", "macro_money", "KRW_bn", "level"),
        "macro_m2_yoy_latest": ("macro_m2", "macro_money", "pct", "yoy"),
        "macro_m2_mom_latest": ("macro_m2", "macro_money", "pct", "mom"),
        "macro_consumer_sentiment_level": (
            "macro_consumer_sentiment",
            "macro_sentiment",
            "index",
            "level",
        ),
    }

    for feature_code, (series_id, category, unit, transform_code) in expected_features.items():
        feature = catalog_by_code[feature_code]
        assert feature.active is True
        assert feature.category == category
        assert feature.frequency == "M"
        assert feature.unit == unit
        assert feature.transform_code == transform_code
        assert feature.input_series_ids == (series_id,)


def test_seed_common_feature_catalog_upserts_series_only() -> None:
    # Decision 7: seed only the common_feature_series table; the model-facing
    # catalog is now code-only (read directly by the DuckDB marts).
    storage = MockCommonFeatureSeedStorage()

    result = seed_common_feature_catalog(storage)  # type: ignore[arg-type]

    assert result.series_upsert.updated == len(default_common_feature_series())
    assert result.catalog_upsert.updated == 0  # catalog no longer upserted
    assert [item.series_id for item in storage.series][:3] == [
        "market_kospi",
        "market_kosdaq",
        "market_kospi200",
    ]
    assert storage.catalog == []  # catalog table not seeded


# --------------------------------------------------------------------------
# N8 — employment series (ECOS 901Y027)
# --------------------------------------------------------------------------

N8_SERIES = ("macro_unemployment_rate", "macro_employment_rate", "macro_employed_persons")


def _series_by_id() -> dict[str, object]:
    return {series.series_id: series for series in default_common_feature_series()}


def test_the_employment_series_pin_item_code2() -> None:
    # The trap this exists for, measured live 2026-08-18: requesting I61BC
    # alone returned 14 rows for 7 months — 원계열 AND 계절조정 under one
    # ITEM_CODE1, two values per month. Without item_code2 both would land on
    # the same (series_id, observation_date) and one would arbitrarily win.
    by_id = _series_by_id()

    for series_id in N8_SERIES:
        params = by_id[series_id].endpoint_params
        assert params["stat_code"] == "901Y027"
        assert params["item_code2"] == "I28A", series_id


def test_the_employment_series_take_the_unadjusted_variant() -> None:
    # Seasonal adjustment is revised after the fact, so today's adjusted 2019
    # value is not what was published in 2019. That is a leak, not a detail.
    by_id = _series_by_id()

    assert all(by_id[series_id].endpoint_params["item_code2"] == "I28A" for series_id in N8_SERIES)
    assert all("I28B" not in str(by_id[series_id].endpoint_params) for series_id in N8_SERIES)


def test_the_employment_series_reflect_the_duplicate_measure_gate() -> None:
    # N8-5 keeps the two rate series and drops employed persons: its level
    # correlation with macro_m2 was 0.9045 after availability alignment.
    by_id = _series_by_id()

    assert by_id["macro_unemployment_rate"].active is True
    assert by_id["macro_employment_rate"].active is True
    assert by_id["macro_employed_persons"].active is False


def test_the_employment_series_use_the_same_conservative_availability_lag() -> None:
    # Same policy as the other monthly ECOS macro series; a new series must not
    # quietly introduce a second PIT convention.
    by_id = _series_by_id()
    cpi = by_id["macro_cpi"]

    for series_id in N8_SERIES:
        series = by_id[series_id]
        assert series.availability_policy == cpi.availability_policy
        assert series.manual_lag_days == cpi.manual_lag_days
        assert series.max_stale_business_days == cpi.max_stale_business_days


def test_the_daily_stale_limit_clears_the_longest_krx_closure() -> None:
    # The limit exists to catch a stalled collector, so it has to clear every
    # legitimate closure and nothing more. Measured over every session in the
    # lake (2007-2026), the longest is six business days — 2017-09-29 to
    # 2017-10-10, the Chuseok break a temporary public holiday stretched to ten
    # calendar days. At 5 it did not clear, and nine series went NULL on a day
    # whose data was fine.
    longest_observed_closure_business_days = 6

    assert DAILY_MAX_STALE_BUSINESS_DAYS > longest_observed_closure_business_days
    # ...and not so generous that a dead collector goes unnoticed for weeks.
    assert DAILY_MAX_STALE_BUSINESS_DAYS <= 10


def test_default_common_feature_wti_spot_level_is_registered_for_macro_betas() -> None:
    """The catalog carried WTI only as a 20-day return, from which a per-session
    oil shock cannot be recovered — ``macro_beta_wti`` regresses residual stock
    returns on ``ln(wti_t / wti_t-1)``, so it needs the level.

    See ``docs/dev/20260829_macro_features/01_design/02_stage1a_exposure_beta_families.md``
    §2.2/§2.5. Reading the FRED spot series (not the FDR CL=F futures fallback)
    is deliberate: it is the one the spot return feature already uses.
    """
    series_by_id = {item.series_id: item for item in default_common_feature_series()}
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    level = catalog_by_code["commodity_wti_spot_level"]
    assert level.active is True
    assert level.transform_code == "level"
    assert level.category == "commodity"
    assert level.frequency == "D"
    assert level.input_series_ids == ("commodity_wti_fred",)
    # Every other level feature reports its source series' own unit.
    assert level.unit == series_by_id["commodity_wti_fred"].unit

    # The pairing in §2.2 rests on this policy: the value on KRX session t is
    # the NY t-1 close, which is what makes beta_wti a spillover exposure.
    assert series_by_id["commodity_wti_fred"].availability_policy == "same_krx_session_morning"


def test_default_common_feature_wti_spot_level_leaves_the_existing_wti_features_alone() -> None:
    catalog_by_code = {item.feature_code: item for item in default_common_feature_catalog()}

    assert catalog_by_code["commodity_wti_ret_20d"].input_series_ids == ("commodity_wti",)
    assert catalog_by_code["commodity_wti_spot_ret_20d"].input_series_ids == ("commodity_wti_fred",)
    assert catalog_by_code["commodity_wti_spot_ret_20d"].transform_code == "ret_20d"
    assert catalog_by_code["commodity_wti_fred_ret_20d"].active is False

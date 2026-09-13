from datetime import UTC, date, datetime
from decimal import Decimal

from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureBuildResult,
    CommonFeatureCatalogEntry,
    CommonFeatureCoverageReport,
    CommonFeatureDailyFact,
    CommonFeatureObservation,
    CommonFeatureReadinessReport,
    CommonFeatureSeries,
)


def test_common_feature_series_defaults_match_catalog_contract() -> None:
    series = CommonFeatureSeries(
        series_id="market_kospi",
        source=Source.PYKRX,
        source_series_key="1001",
        category="market_index",
        frequency="D",
        name_kr="KOSPI",
    )

    assert series.availability_policy == "release_date"
    assert series.manual_lag_days == 0
    assert series.source_timezone == "Asia/Seoul"
    assert series.max_stale_business_days == 5
    assert series.active is True
    assert series.endpoint_params == {}


def test_common_feature_observation_can_represent_provider_raw_before_availability() -> None:
    fetched_at = datetime(2026, 6, 8, 18, 30, tzinfo=UTC)
    observation = CommonFeatureObservation(
        source=Source.PYKRX,
        series_id="market_kospi",
        observation_date=date(2026, 6, 8),
        frequency="D",
        fetched_at=fetched_at,
        value_numeric=Decimal("2910.42"),
        raw_payload={"close": "2910.42"},
    )

    assert observation.available_from_date is None
    assert observation.value_numeric == Decimal("2910.42")
    assert observation.raw_payload["close"] == "2910.42"


def test_common_feature_catalog_entry_tracks_input_series() -> None:
    entry = CommonFeatureCatalogEntry(
        feature_code="rate_kr_term_spread_10y_3y",
        feature_name_kr="국고채 10년-3년 스프레드",
        category="rate",
        unit="pctp",
        transform_code="spread",
        input_series_ids=("rate_kr_gov10y", "rate_kr_gov3y"),
    )

    assert entry.frequency == "D"
    assert entry.input_series_ids == ("rate_kr_gov10y", "rate_kr_gov3y")
    assert entry.active is True


def test_common_feature_catalog_entry_defaults_roles_to_primary() -> None:
    entry = CommonFeatureCatalogEntry(
        feature_code="market_kospi_close",
        feature_name_kr="KOSPI 종가",
        category="market_index",
        input_series_ids=("market_kospi",),
    )

    assert entry.input_roles == ()
    assert entry.roles() == ("primary",)
    assert entry.series_by_role() == {"primary": "market_kospi"}


def test_common_feature_catalog_entry_tracks_explicit_input_roles() -> None:
    entry = CommonFeatureCatalogEntry(
        feature_code="rate_kr_term_spread_10y_3y",
        feature_name_kr="국고채 10년-3년 스프레드",
        category="rate",
        unit="pctp",
        transform_code="spread",
        input_series_ids=("rate_kr_gov10y", "rate_kr_gov3y"),
        input_roles=("spread_long", "spread_short"),
    )

    assert entry.roles() == ("spread_long", "spread_short")
    assert entry.series_by_role() == {
        "spread_long": "rate_kr_gov10y",
        "spread_short": "rate_kr_gov3y",
    }


def test_common_feature_daily_fact_defaults_do_not_share_lists() -> None:
    generated_at = datetime(2026, 6, 8, 18, 40, tzinfo=UTC)
    first = CommonFeatureDailyFact(
        feature_date=date(2026, 6, 9),
        feature_code="market_kospi_close",
        asof_available_date=date(2026, 6, 9),
        generated_at=generated_at,
    )
    second = CommonFeatureDailyFact(
        feature_date=date(2026, 6, 10),
        feature_code="market_kospi_close",
        asof_available_date=date(2026, 6, 10),
        generated_at=generated_at,
    )

    first.source_series_ids.append("market_kospi")

    assert first.source_series_ids == ["market_kospi"]
    assert second.source_series_ids == []


def test_common_feature_build_result_defaults_do_not_share_mutable_values() -> None:
    first = CommonFeatureBuildResult()
    second = CommonFeatureBuildResult()

    first.errors["market_kospi_close"] = "failed"
    first.upsert.updated = 1

    assert first.errors == {"market_kospi_close": "failed"}
    assert second.errors == {}
    assert first.upsert.updated == 1
    assert second.upsert.updated == 0


def test_common_feature_coverage_report_defaults_do_not_share_mutable_values() -> None:
    first = CommonFeatureCoverageReport()
    second = CommonFeatureCoverageReport()

    first.errors["global_vix_level"] = "failed"

    assert first.errors == {"global_vix_level": "failed"}
    assert second.errors == {}
    assert first.rows == []
    assert second.rows == []


def test_common_feature_readiness_report_defaults_do_not_share_mutable_values() -> None:
    first = CommonFeatureReadinessReport()
    second = CommonFeatureReadinessReport()

    first.errors["rate_kr_gov3y_level"] = "failed"

    assert first.errors == {"rate_kr_gov3y_level": "failed"}
    assert second.errors == {}
    assert first.rows == []
    assert second.rows == []

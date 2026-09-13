"""The staleness gate over the raw freshness report.

``ops freshness-report`` prints and exits 0 no matter what it finds, so putting
it on a schedule produces a green job every day whether or not anything was
collected. That is exactly what happened on 2026-08-14: sj2-server was down
across the 18:30 KRX chain and the 20:30 common sync, every raw table stayed a
day behind, no run failed because no run started, and nothing surfaced it.

A missed run leaves no trace in ``ingestion_runs``. The only evidence is the data
itself, so the gate asks whether the newest row is as new as the calendar says it
should be.
"""

from __future__ import annotations

from datetime import date

import pytest

from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureSeries
from collector.kr.service import freshness
from collector.kr.service.freshness import (
    FLOW_SOURCES,
    TRADING_DAY_SOURCES,
    CommonSeriesFreshness,
    FreshnessReport,
    build_freshness_report,
    evaluate_staleness,
)
from collector.kr.service.sync_krx_flows import (
    DISCONTINUED_FLOW_METRICS,
    SHORTING_BALANCE_METRIC,
    active_flow_metrics,
)

# 2026-08-15 is a Saturday; the sessions before it are Fri 08-14 and Thu 08-13.
SATURDAY = date(2026, 8, 15)
FRIDAY = date(2026, 8, 14)
THURSDAY = date(2026, 8, 13)
TUESDAY = date(2026, 8, 18)

NO_HOLIDAYS: set[date] = set()


def _report(
    *,
    price: date | None = FRIDAY,
    market_cap: date | None = FRIDAY,
    flows: dict[str, date | None] | None = None,
    common: list[CommonSeriesFreshness] | None = None,
) -> FreshnessReport:
    return FreshnessReport(
        price_latest_date=price,
        market_cap_latest_date=market_cap,
        flow_group_latest_dates={"investor_net_buy": FRIDAY} if flows is None else flows,
        common_series=common or [],
    )


def _evaluate(report: FreshnessReport, **kwargs):
    kwargs.setdefault("as_of", SATURDAY)
    kwargs.setdefault("holidays", NO_HOLIDAYS)
    return evaluate_staleness(report, **kwargs)


def test_current_data_produces_no_findings() -> None:
    assert _evaluate(_report()) == []


def test_reproduces_the_2026_08_14_outage() -> None:
    # Same-day domains are one session behind, which is what the host being down
    # over the evening window looked like. Thursday market cap is still valid
    # on Friday because that source is T+1.
    report = _report(
        price=THURSDAY,
        market_cap=THURSDAY,
        flows={"investor_net_buy": THURSDAY, "short_selling": THURSDAY},
        common=[
            CommonSeriesFreshness("market_kospi_close_krx", Source.KRX, THURSDAY),
            CommonSeriesFreshness(
                "fx_usdkrw_fdr",
                Source.FDR,
                THURSDAY,
                max_stale_business_days=7,
            ),
        ],
    )

    findings = _evaluate(report)

    assert {finding.domain for finding in findings} == {
        "daily_ohlcv",
        "krx_security_flow_raw:investor_net_buy",
        "krx_security_flow_raw:short_selling",
        "common:market_kospi_close_krx",
    }
    assert all(finding.required_at_or_after == FRIDAY for finding in findings)
    assert all(finding.cadence == "trading" for finding in findings)


def test_weekend_does_not_move_the_threshold_past_the_last_session() -> None:
    # Run the same check on Sunday: Friday is still the newest session, so data
    # that was current on Saturday must not turn stale overnight.
    assert _evaluate(_report(), as_of=date(2026, 8, 16)) == []


def test_a_holiday_run_does_not_demand_data_from_a_closed_market() -> None:
    # 08-17 is the substitute holiday for Liberation Day. The newest session is
    # still Friday, and asking for Monday's data would be asking for data that
    # cannot exist.
    monday = date(2026, 8, 17)

    assert _evaluate(_report(), as_of=monday, holidays={monday}) == []


def test_an_empty_table_is_stale_rather_than_skipped() -> None:
    findings = _evaluate(_report(price=None))

    assert [finding.domain for finding in findings] == ["daily_ohlcv"]
    assert findings[0].latest is None


def test_market_cap_is_gated_alongside_prices() -> None:
    # A separate T+1 budget is not an exemption. Empty is still stale.
    findings = _evaluate(_report(market_cap=None))

    assert [finding.domain for finding in findings] == ["daily_market_cap"]


def test_market_cap_has_a_separate_t_plus_one_budget() -> None:
    # 08-17 is a holiday. At Tuesday's close the T+1 source may legitimately
    # end on Friday while same-day domains must already contain Tuesday.
    report = _report(
        price=TUESDAY,
        market_cap=FRIDAY,
        flows={"investor_net_buy": TUESDAY},
    )

    assert _evaluate(report, as_of=TUESDAY, holidays={date(2026, 8, 17)}) == []


def test_market_cap_two_published_sessions_behind_is_stale() -> None:
    report = _report(
        price=TUESDAY,
        market_cap=THURSDAY,
        flows={"investor_net_buy": TUESDAY},
    )

    findings = _evaluate(report, as_of=TUESDAY, holidays={date(2026, 8, 17)})

    assert [finding.domain for finding in findings] == ["daily_market_cap"]
    assert findings[0].required_at_or_after == FRIDAY


def test_release_lagged_sources_get_a_calendar_budget() -> None:
    # FRED and ECOS publish on their own schedule. Holding them to the trading
    # calendar would flag them every day, and a gate that always fires is a gate
    # nobody reads.
    common = [
        CommonSeriesFreshness("us_cpi_fred", Source.FRED, date(2026, 8, 5)),
        CommonSeriesFreshness("kr_base_rate_ecos", Source.ECOS, date(2026, 8, 5)),
    ]

    assert _evaluate(_report(common=common)) == []

    stale = [
        CommonSeriesFreshness("us_cpi_fred", Source.FRED, date(2026, 7, 1)),
    ]
    findings = _evaluate(_report(common=stale))
    assert [finding.domain for finding in findings] == ["common:us_cpi_fred"]
    assert findings[0].cadence == "calendar"


def test_catalog_budget_handles_daily_and_monthly_source_release_lag() -> None:
    # The production catalog distinguishes daily overseas observations from
    # monthly macro releases. A single 14-calendar-day limit makes one of those
    # groups permanently fail.
    as_of = date(2026, 8, 28)
    common = [
        CommonSeriesFreshness(
            "global_sp500_fdr",
            Source.FDR,
            date(2026, 8, 26),
            frequency="D",
            max_stale_business_days=7,
        ),
        CommonSeriesFreshness(
            "macro_cpi",
            Source.ECOS,
            date(2026, 7, 31),
            frequency="M",
            max_stale_business_days=45,
        ),
        CommonSeriesFreshness(
            "macro_m2",
            Source.ECOS,
            date(2026, 6, 30),
            frequency="M",
            max_stale_business_days=90,
        ),
    ]

    current_report = _report(
        price=as_of,
        market_cap=as_of,
        flows={"investor_net_buy": as_of},
        common=common,
    )
    assert _evaluate(current_report, as_of=as_of) == []

    stale = [
        CommonSeriesFreshness(
            "global_sp500_fdr",
            Source.FDR,
            date(2026, 8, 19),
            max_stale_business_days=7,
        )
    ]
    stale_report = _report(
        price=as_of,
        market_cap=as_of,
        flows={"investor_net_buy": as_of},
        common=stale,
    )
    findings = _evaluate(stale_report, as_of=as_of)

    assert [finding.domain for finding in findings] == ["common:global_sp500_fdr"]
    assert findings[0].required_at_or_after == date(2026, 8, 20)
    assert findings[0].cadence == "business"


def test_a_wider_trading_budget_tolerates_one_missed_session() -> None:
    report = _report(price=THURSDAY, flows={"investor_net_buy": THURSDAY})

    assert _evaluate(report, max_lag_trading_days=2) == []


@pytest.mark.parametrize("lag", [0, -1])
def test_a_nonsensical_budget_falls_back_to_the_latest_session(lag: int) -> None:
    # 0 would otherwise index sessions[-0] == sessions[0], i.e. 60 days back,
    # silently turning the gate off.
    report = _report(price=THURSDAY, flows={"investor_net_buy": THURSDAY})

    findings = _evaluate(report, max_lag_trading_days=lag)

    assert {finding.required_at_or_after for finding in findings} == {FRIDAY}


def test_describe_names_the_domain_and_both_dates() -> None:
    findings = _evaluate(_report(price=THURSDAY, flows={}))

    message = findings[0].describe()
    assert "daily_ohlcv" in message
    assert "2026-08-13" in message
    assert "2026-08-14" in message


def test_the_flow_cursor_asks_about_both_krx_and_kis() -> None:
    # `flows` moved from KRX scraping to KIS on 2026-08. A KRX-scoped cursor
    # would report the flow domain as frozen on changeover day and fire this
    # gate every morning on data that is in fact current.
    class RecordingStorage:
        def __init__(self) -> None:
            self.flow_sources: tuple[Source, ...] = ()

        def get_krx_security_flow_metric_max_dates(self, metric_codes, sources):
            self.flow_sources = tuple(sources)
            return {metric: FRIDAY for metric in metric_codes}

        def get_common_feature_series(self, active_only=True):
            return []

        def get_common_feature_observation_max_dates(self, series_ids):
            return {}

        def get_table_bsns_year_range(self, table_name):
            return None

        def get_latest_daily_price_date(self):
            return FRIDAY

        def get_latest_market_cap_date(self):
            return FRIDAY

        def get_running_ingestion_runs(self, limit=20):
            return []

    storage = RecordingStorage()
    report = build_freshness_report(storage)

    assert storage.flow_sources == FLOW_SOURCES
    assert Source.KRX in storage.flow_sources
    assert Source.KIS in storage.flow_sources
    assert report.flow_group_latest_dates["investor"] == FRIDAY


class _FlowStorage:
    """Storage stub whose flow metric dates are set per metric code."""

    def __init__(self, metric_dates: dict[str, date]) -> None:
        self._metric_dates = metric_dates

    def get_krx_security_flow_metric_max_dates(self, metric_codes, sources):
        return {
            metric: self._metric_dates[metric]
            for metric in metric_codes
            if metric in self._metric_dates
        }

    def get_common_feature_series(self, active_only=True):
        return []

    def get_common_feature_observation_max_dates(self, series_ids):
        return {}

    def get_table_bsns_year_range(self, table_name):
        return None

    def get_latest_daily_price_date(self):
        return FRIDAY

    def get_latest_market_cap_date(self):
        return FRIDAY

    def get_running_ingestion_runs(self, limit=20):
        return []


def test_report_carries_each_common_series_catalog_budget() -> None:
    series = CommonFeatureSeries(
        series_id="macro_cpi",
        source=Source.ECOS,
        source_series_key="901Y009",
        category="macro",
        frequency="M",
        name_kr="소비자물가지수",
        max_stale_business_days=45,
    )

    class CommonStorage(_FlowStorage):
        def get_common_feature_series(self, active_only=True):
            return [series]

        def get_common_feature_observation_max_dates(self, series_ids):
            return {series.series_id: date(2026, 7, 31)}

    report = build_freshness_report(CommonStorage({}))

    assert report.common_series == [
        CommonSeriesFreshness(
            series_id="macro_cpi",
            source=Source.ECOS,
            latest_observation_date=date(2026, 7, 31),
            frequency="M",
            max_stale_business_days=45,
        )
    ]


def _shorting_storage(balance: date) -> _FlowStorage:
    """KIS-collected shorting metrics current, the KRX-only balance frozen."""
    return _FlowStorage(
        {
            "foreign_holding_shares": FRIDAY,
            "institution_net_buy_volume": FRIDAY,
            "individual_net_buy_volume": FRIDAY,
            "foreign_net_buy_volume": FRIDAY,
            "short_selling_volume": FRIDAY,
            "short_selling_value": FRIDAY,
            SHORTING_BALANCE_METRIC: balance,
        }
    )


def test_the_krx_only_balance_metric_is_declared_discontinued() -> None:
    # KIS covers six of the seven flow metrics. If the seventh stays in the
    # budget, decommissioning KRX (K-5) makes this gate fire every morning.
    assert SHORTING_BALANCE_METRIC in DISCONTINUED_FLOW_METRICS
    assert SHORTING_BALANCE_METRIC not in active_flow_metrics("shorting")
    assert set(active_flow_metrics("shorting")) == {"short_selling_volume", "short_selling_value"}


def test_a_group_takes_its_date_from_the_metrics_still_collected() -> None:
    # The group date is a minimum, so before this change one frozen metric
    # became the group's date no matter how current the other two were.
    report = build_freshness_report(_shorting_storage(balance=date(2026, 8, 1)))

    assert report.flow_group_latest_dates["shorting"] == FRIDAY


def test_turning_krx_off_does_not_make_the_shorting_gate_fire_daily() -> None:
    # The regression this exists for: KRX is decommissioned, the balance
    # freezes on its last scraped date, and the other two keep arriving.
    report = build_freshness_report(_shorting_storage(balance=date(2026, 8, 1)))

    assert _evaluate(report) == []


def test_the_gate_still_fires_when_a_collected_shorting_metric_lags() -> None:
    # Excluding the discontinued metric must not excuse the whole group.
    storage = _shorting_storage(balance=date(2026, 8, 1))
    storage._metric_dates["short_selling_volume"] = THURSDAY

    findings = _evaluate(build_freshness_report(storage))

    assert [finding.domain for finding in findings] == ["krx_security_flow_raw:shorting"]


def test_a_discontinued_metric_keeps_its_last_date_in_the_report() -> None:
    # Dropping it from the budget must not drop it from view — otherwise the
    # group merely stops moving and nobody can tell why.
    report = build_freshness_report(_shorting_storage(balance=date(2026, 8, 1)))

    assert report.flow_metric_latest_dates[SHORTING_BALANCE_METRIC] == date(2026, 8, 1)
    assert SHORTING_BALANCE_METRIC in report.discontinued_flow_metrics
    assert "KIS" in report.discontinued_flow_metrics[SHORTING_BALANCE_METRIC]


def test_an_unrecognised_group_is_still_gated() -> None:
    # "Has no active metrics" and "is not a group we know" look identical from
    # the lookup. Only the first may switch a gate off; a renamed or misspelt
    # group must stay loud.
    findings = _evaluate(_report(flows={"not_a_real_group": THURSDAY}))

    assert [finding.domain for finding in findings] == ["krx_security_flow_raw:not_a_real_group"]


def test_a_fully_discontinued_group_is_not_gated_even_though_it_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No group is fully discontinued today. Pin the behaviour anyway: a group
    # with nothing left to collect has no lag to measure, and reporting it as
    # stale forever is the failure mode this whole change is about.
    monkeypatch.setitem(freshness.DISCONTINUED_FLOW_METRICS, "foreign_holding_shares", "test")

    report = build_freshness_report(_shorting_storage(balance=FRIDAY))

    assert report.flow_group_latest_dates["foreign_holding"] is None
    assert _evaluate(report) == []


def test_kis_is_treated_as_a_trading_day_source() -> None:
    # KIS publishes on KRX sessions, so it gets the trading-day budget rather
    # than the catalog-level business-day budget that ECOS/FDR/FRED need.
    assert Source.KIS in TRADING_DAY_SOURCES
    assert Source.FDR not in TRADING_DAY_SOURCES

    stale = [
        CommonSeriesFreshness(
            series_id="kospi_flow", source=Source.KIS, latest_observation_date=THURSDAY
        )
    ]
    findings = _evaluate(_report(common=stale))
    assert [finding.cadence for finding in findings] == ["trading"]

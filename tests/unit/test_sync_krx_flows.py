import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from collector.kr.cli import app as cli_app
from collector.kr.cli.app import build_parser
from collector.kr.domain.enums import ListingStatus, Market, RunStatus, RunType, Source
from collector.kr.domain.models import (
    IngestionRun,
    SecurityFlowFetchResult,
    SecurityFlowLine,
    Stock,
    UpsertResult,
)
from collector.kr.service.sync_krx_flows import (
    resolve_incremental_flow_range,
    sync_krx_security_flows,
)

KST = timezone(timedelta(hours=9))


class MockFlowProvider:
    def __init__(self, source: Source = Source.KRX) -> None:
        self._source = source
        self.investor_calls = 0
        self.investor_bulk_calls = 0
        self.shorting_calls = 0
        self.shorting_trading_bulk_calls = 0
        self.shorting_balance_bulk_calls = 0
        self.foreign_calls = 0

    def source(self) -> Source:
        return self._source

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        del end
        self.investor_calls += 1
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=start,
                    ticker=ticker,
                    market=market,
                    metric_code="institution_net_buy_volume",
                    metric_name="기관 순매수 수량",
                    value=Decimal("10"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def fetch_investor_net_volume_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        self.investor_bulk_calls += 1
        ticker = tickers[0] if tickers else "005930"
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="institution_net_buy_volume",
                    metric_name="기관 순매수 수량",
                    value=Decimal("10"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                ),
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="individual_net_buy_volume",
                    metric_name="개인 순매수 수량",
                    value=Decimal("20"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                ),
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="foreign_net_buy_volume",
                    metric_name="외국인 순매수 수량",
                    value=Decimal("30"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                ),
            ]
        )

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        del end
        self.shorting_calls += 1
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=start,
                    ticker=ticker,
                    market=market,
                    metric_code="short_selling_volume",
                    metric_name="공매도 거래량",
                    value=Decimal("20"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def fetch_shorting_trading_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        self.shorting_trading_bulk_calls += 1
        ticker = tickers[0] if tickers else "005930"
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="short_selling_volume",
                    metric_name="공매도 거래량",
                    value=Decimal("20"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                ),
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="short_selling_value",
                    metric_name="공매도 거래대금",
                    value=Decimal("2000"),
                    unit="KRW",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                ),
            ]
        )

    def fetch_shorting_balance_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        self.shorting_balance_bulk_calls += 1
        ticker = tickers[0] if tickers else "005930"
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="short_selling_balance_quantity",
                    metric_name="공매도 잔고 수량",
                    value=Decimal("30"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        self.foreign_calls += 1
        ticker = tickers[0] if tickers else "005930"
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="foreign_holding_shares",
                    metric_name="외국인 보유주식수",
                    value=Decimal("30"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def unsupported_metric_codes(self) -> list[str]:
        return ["borrow_balance_quantity"]


class MockFlowStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.records: list[SecurityFlowLine] = []
        self.foreign_counts: dict[tuple[date, str], int] = {}
        self.daily_market_counts: dict[str, dict[tuple[date, str], int]] = {}
        self.shorting_counts: dict[str, int] = {}
        self.count_sources: list[Source] = []
        self.latest_price_date: date | None = date(2026, 4, 17)
        self.metric_max_dates: dict[str, date] = {
            "foreign_holding_shares": date(2026, 4, 17),
            "institution_net_buy_volume": date(2026, 4, 17),
            "individual_net_buy_volume": date(2026, 4, 17),
            "foreign_net_buy_volume": date(2026, 4, 17),
            "short_selling_volume": date(2026, 4, 17),
            "short_selling_value": date(2026, 4, 17),
            "short_selling_balance_quantity": date(2026, 4, 17),
        }

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        stocks = [
            Stock(
                ticker="005930",
                market=Market.KOSPI,
                name="삼성전자",
                status=ListingStatus.ACTIVE,
                last_seen_date=date(2026, 4, 17),
                source=Source.KRX,
            )
        ]
        if market is None:
            return stocks
        return [stock for stock in stocks if stock.market == market]

    def upsert_krx_security_flow_raw(self, records: list[SecurityFlowLine]) -> UpsertResult:
        self.records.extend(records)
        return UpsertResult(updated=len(records))

    def count_krx_security_flow_daily_market_tickers(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_code: str,
        source: Source,
    ) -> dict[tuple[date, str], int]:
        del start, end, tickers
        self.count_sources.append(source)
        if metric_code != "foreign_holding_shares":
            return self.daily_market_counts.get(metric_code, {})
        return self.foreign_counts

    def count_krx_security_flow_ticker_metric_dates(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        source: Source,
    ) -> dict[str, int]:
        del start, end, tickers
        self.count_sources.append(source)
        return self.shorting_counts

    def get_daily_price_date_range(
        self,
        tickers: list[str] | None = None,
    ) -> tuple[date, date] | None:
        del tickers
        return date(2026, 4, 16), date(2026, 4, 17)

    def get_latest_daily_price_date(
        self,
        tickers: list[str] | None = None,
    ) -> date | None:
        del tickers
        return self.latest_price_date

    def get_krx_security_flow_metric_max_dates(
        self,
        metric_codes: list[str],
        sources: tuple[Source, ...],
    ) -> dict[str, date]:
        del sources
        return {
            metric_code: latest_date
            for metric_code, latest_date in self.metric_max_dates.items()
            if metric_code in metric_codes
        }


def test_resolve_incremental_flow_range_uses_lagging_metric_group() -> None:
    resolved = resolve_incremental_flow_range(
        latest_price_date=date(2026, 6, 10),
        metric_latest_dates={
            "foreign_holding_shares": date(2026, 6, 10),
            "institution_net_buy_volume": date(2026, 5, 21),
            "individual_net_buy_volume": date(2026, 5, 21),
            "foreign_net_buy_volume": date(2026, 5, 21),
            "short_selling_volume": date(2026, 5, 25),
            "short_selling_value": date(2026, 5, 25),
            "short_selling_balance_quantity": date(2026, 5, 25),
        },
        lookback_days=14,
    )

    assert resolved.start == date(2026, 5, 22)
    assert resolved.end == date(2026, 6, 10)
    assert resolved.latest_flow_date == date(2026, 5, 21)
    assert resolved.group_latest_dates["investor"] == date(2026, 5, 21)
    assert resolved.group_lag_days["investor"] == 20


def test_resolve_incremental_flow_range_always_rescans_lookback_window() -> None:
    resolved = resolve_incremental_flow_range(
        latest_price_date=date(2026, 6, 10),
        metric_latest_dates={
            "foreign_holding_shares": date(2026, 6, 10),
            "institution_net_buy_volume": date(2026, 6, 10),
            "individual_net_buy_volume": date(2026, 6, 10),
            "foreign_net_buy_volume": date(2026, 6, 10),
            "short_selling_volume": date(2026, 6, 10),
            "short_selling_value": date(2026, 6, 10),
            "short_selling_balance_quantity": date(2026, 6, 10),
        },
        lookback_days=14,
    )

    assert resolved.start == date(2026, 5, 27)
    assert resolved.end == date(2026, 6, 10)


def test_resolve_incremental_flow_range_rejects_missing_baseline() -> None:
    with pytest.raises(ValueError, match="missing baseline"):
        resolve_incremental_flow_range(
            latest_price_date=date(2026, 6, 10),
            metric_latest_dates={"foreign_holding_shares": date(2026, 6, 10)},
            lookback_days=14,
        )


def test_resolve_incremental_flow_range_excludes_groups() -> None:
    resolved = resolve_incremental_flow_range(
        latest_price_date=date(2026, 6, 10),
        metric_latest_dates={
            "foreign_holding_shares": date(2026, 6, 10),
            "institution_net_buy_volume": date(2026, 5, 21),
            "individual_net_buy_volume": date(2026, 5, 21),
            "foreign_net_buy_volume": date(2026, 5, 21),
            "short_selling_volume": date(2026, 6, 9),
            "short_selling_value": date(2026, 6, 9),
            "short_selling_balance_quantity": date(2026, 6, 9),
        },
        lookback_days=14,
        exclude_groups=["investor"],
    )

    assert resolved.excluded_groups == ["investor"]
    assert "investor" not in resolved.group_latest_dates
    assert resolved.latest_flow_date == date(2026, 6, 9)
    assert resolved.start == date(2026, 5, 27)


def test_sync_krx_security_flows_writes_rows_and_pending_metrics() -> None:
    storage = MockFlowStorage()
    provider = MockFlowProvider()

    result = sync_krx_security_flows(
        provider=provider,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 4
    assert result.rows_upserted == 7
    assert result.pending_metrics == ["borrow_balance_quantity"]
    assert len(storage.records) == 7
    assert provider.investor_calls == 0
    assert provider.investor_bulk_calls == 1
    assert provider.shorting_calls == 0
    assert provider.shorting_trading_bulk_calls == 1
    assert provider.shorting_balance_bulk_calls == 1
    assert storage.runs[0].run_type == RunType.KRX_FLOW_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert result.phase_counts["foreign_holding"].requests_attempted == 1
    assert result.phase_counts["foreign_holding"].rows_upserted == 1
    assert result.phase_counts["investor_bulk"].requests_attempted == 1
    assert result.phase_counts["investor_bulk"].rows_upserted == 3
    assert result.phase_counts["shorting_bulk"].requests_attempted == 2
    assert result.phase_counts["shorting_bulk"].rows_upserted == 3
    assert storage.runs[-1].counts["foreign_holding_requests_attempted"] == 1
    assert storage.runs[-1].counts["foreign_holding_rows_upserted"] == 1
    assert storage.runs[-1].counts["investor_bulk_requests_attempted"] == 1
    assert storage.runs[-1].counts["investor_bulk_rows_upserted"] == 3
    assert storage.runs[-1].counts["shorting_bulk_requests_attempted"] == 2
    assert storage.runs[-1].counts["shorting_bulk_rows_upserted"] == 3


def test_sync_krx_security_flows_logs_progress(caplog: pytest.LogCaptureFixture) -> None:
    storage = MockFlowStorage()
    caplog.set_level(logging.INFO, logger="collector.kr.service.sync_krx_flows")

    result = sync_krx_security_flows(
        provider=MockFlowProvider(),  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
        progress_log_interval_seconds=0.0,
        progress_log_every_items=1,
    )

    assert result.errors == {}
    messages = [record.getMessage() for record in caplog.records]
    assert any("Flow sync started:" in message for message in messages)
    assert any("Flow sync existing coverage loaded:" in message for message in messages)
    assert any("Flow sync phase started: phase=foreign_holding" in message for message in messages)
    assert any(
        "Flow sync progress: phase=foreign_holding processed=1/1" in message for message in messages
    )
    assert any("Flow sync phase started: phase=investor_bulk" in message for message in messages)
    assert any(
        "Flow sync progress: phase=investor_bulk processed=1/1" in message for message in messages
    )
    assert any("Flow sync phase started: phase=shorting_bulk" in message for message in messages)
    assert any(
        "Flow sync progress: phase=shorting_bulk processed=2/2" in message for message in messages
    )


def test_sync_krx_security_flows_skips_complete_existing_requests() -> None:
    storage = MockFlowStorage()
    storage.foreign_counts[(date(2026, 4, 17), Market.KOSPI.value)] = 1
    for metric_code in [
        "institution_net_buy_volume",
        "individual_net_buy_volume",
        "foreign_net_buy_volume",
        "short_selling_volume",
        "short_selling_value",
        "short_selling_balance_quantity",
    ]:
        storage.daily_market_counts[metric_code] = {(date(2026, 4, 17), Market.KOSPI.value): 1}
    provider = MockFlowProvider()

    result = sync_krx_security_flows(
        provider=provider,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 4
    assert result.rows_upserted == 0
    assert len(storage.records) == 0
    assert provider.foreign_calls == 0
    assert provider.investor_calls == 0
    assert provider.investor_bulk_calls == 0
    assert provider.shorting_calls == 0
    assert provider.shorting_trading_bulk_calls == 0
    assert provider.shorting_balance_bulk_calls == 0
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert result.phase_counts["foreign_holding"].requests_skipped == 1
    assert result.phase_counts["investor_bulk"].requests_skipped == 1
    assert result.phase_counts["shorting_bulk"].requests_skipped == 2
    assert storage.runs[-1].counts["foreign_holding_requests_skipped"] == 1
    assert storage.runs[-1].counts["investor_bulk_requests_skipped"] == 1
    assert storage.runs[-1].counts["shorting_bulk_requests_skipped"] == 2


def test_sync_krx_security_flows_uses_provider_source_for_existing_counts() -> None:
    storage = MockFlowStorage()
    provider = MockFlowProvider(source=Source.KRX)

    result = sync_krx_security_flows(
        provider=provider,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert storage.count_sources == [
        Source.KRX,
        Source.KRX,
        Source.KRX,
        Source.KRX,
        Source.KRX,
        Source.KRX,
        Source.KRX,
    ]
    assert storage.records
    assert {record.source for record in storage.records} == {Source.KRX}
    assert storage.runs[0].params["provider_source"] == Source.KRX.value


def test_sync_krx_security_flows_records_incremental_params() -> None:
    storage = MockFlowStorage()

    result = sync_krx_security_flows(
        provider=MockFlowProvider(),  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 10),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
        run_params_extra={
            "incremental": True,
            "resolved_start": "2026-04-10",
            "resolved_end": "2026-04-17",
            "lookback_days": 14,
            "group_latest_dates": {"investor": "2026-04-09"},
        },
    )

    assert result.errors == {}
    params = storage.runs[0].params
    assert params["incremental"] is True
    assert params["resolved_start"] == "2026-04-10"
    assert params["resolved_end"] == "2026-04-17"
    assert params["lookback_days"] == 14
    assert params["group_latest_dates"] == {"investor": "2026-04-09"}


def test_sync_krx_security_flows_honors_enabled_flow_groups() -> None:
    storage = MockFlowStorage()
    provider = MockFlowProvider()

    result = sync_krx_security_flows(
        provider=provider,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
        enabled_flow_groups=["foreign_holding"],
    )

    assert result.errors == {}
    assert result.requests_attempted == 1
    assert provider.foreign_calls == 1
    assert provider.investor_calls == 0
    assert provider.investor_bulk_calls == 0
    assert provider.shorting_calls == 0
    assert provider.shorting_trading_bulk_calls == 0
    assert provider.shorting_balance_bulk_calls == 0
    assert storage.count_sources == [Source.KRX]
    assert storage.runs[0].params["enabled_flow_groups"] == ["foreign_holding"]
    assert result.phase_counts["foreign_holding"].requests_attempted == 1
    assert result.phase_counts["investor_bulk"].requests_attempted == 0
    assert result.phase_counts["shorting_bulk"].requests_attempted == 0


def test_flows_sync_parser_supports_price_range_mode_without_provider_selection() -> None:
    args = build_parser().parse_args(["flows", "sync", "--use-price-range"])

    assert args.use_price_range is True
    assert args.incremental is False
    assert args.start is None
    assert args.end is None
    assert args.progress_log_interval_seconds == 30.0
    assert args.progress_log_every_items == 100
    assert not hasattr(args, "provider")


def test_flows_sync_parser_supports_progress_log_options() -> None:
    args = build_parser().parse_args(
        [
            "flows",
            "sync",
            "--progress-log-interval-seconds",
            "5",
            "--progress-log-every-items",
            "10",
        ]
    )

    assert args.progress_log_interval_seconds == 5.0
    assert args.progress_log_every_items == 10


def test_flows_sync_parser_supports_human_throttle_options() -> None:
    args = build_parser().parse_args(
        [
            "flows",
            "sync",
            "--http-min-delay-seconds",
            "1.5",
            "--http-max-delay-seconds",
            "4.0",
            "--long-rest-every",
            "15",
            "--ordered-requests",
        ]
    )

    assert args.http_min_delay_seconds == 1.5
    assert args.http_max_delay_seconds == 4.0
    assert args.long_rest_every == 15
    assert args.ordered_requests is True


def test_flows_sync_parser_supports_timeout_seconds_suffix() -> None:
    args = build_parser().parse_args(["flows", "sync", "--timeout-seconds", "150s"])

    assert args.timeout_seconds == 150.0


def test_flows_sync_parser_supports_incremental_options() -> None:
    args = build_parser().parse_args(
        [
            "flows",
            "sync",
            "--incremental",
            "--lookback-days",
            "14",
            "--max-auto-range-days",
            "30",
            "--allow-large-range",
            "--exclude-groups",
            "shorting",
        ]
    )

    assert args.incremental is True
    assert args.lookback_days == 14
    assert args.max_auto_range_days == 30
    assert args.allow_large_range is True
    assert args.exclude_groups == "shorting"


def test_flows_sync_handler_applies_exclude_groups_to_explicit_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeStorage:
        def __init__(self, dsn: str) -> None:
            captured["dsn"] = dsn

    class FakeProvider:
        def __init__(self, **kwargs: object) -> None:
            captured["provider_kwargs"] = kwargs

        def source(self) -> Source:
            return Source.KRX

    def fake_sync_krx_security_flows(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(
            errors={},
            targets_processed=0,
            requests_attempted=0,
            requests_skipped=0,
            rows_upserted=0,
            no_data_requests=0,
            pending_metrics=[],
        )

    monkeypatch.setattr(
        cli_app,
        "get_settings",
        lambda: SimpleNamespace(
            db_dsn="postgresql://example",
            krx_mdc_timeout_seconds=150.0,
            krx_logical_rate_limit_seconds=8.0,
            krx_min_delay_seconds=1.5,
            krx_max_delay_seconds=4.0,
            krx_long_rest_every=15,
            krx_long_rest_min_seconds=30.0,
            krx_long_rest_max_seconds=90.0,
            krx_auth_cooldown_seconds=10.0,
            krx_error_backoff_min_seconds=45.0,
            krx_error_backoff_max_seconds=180.0,
            krx_id="id",
            krx_pw="pw",
        ),
    )
    monkeypatch.setattr("collector.kr.infra.db_postgres.repositories.PostgresStorage", FakeStorage)
    monkeypatch.setattr(
        "collector.kr.adapters.flows_krx.provider.KrxDirectFlowProvider",
        FakeProvider,
    )
    monkeypatch.setattr(
        "collector.kr.service.sync_krx_flows.sync_krx_security_flows",
        fake_sync_krx_security_flows,
    )

    args = build_parser().parse_args(
        [
            "flows",
            "sync",
            "--start",
            "2026-05-22",
            "--end",
            "2026-06-10",
            "--exclude-groups",
            "foreign_holding",
        ]
    )

    args.handler(args)

    assert captured["enabled_flow_groups"] == ["investor", "shorting"]


def test_flows_sync_parser_rejects_provider_selection() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["flows", "sync", "--provider", "krx"])

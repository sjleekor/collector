"""Unit tests for the KIS flow sync service.

These cover the six conditions that had to be met before ``flows`` could move
off KRX scraping — the cursor, the per-ticker checkpoint, the no-data
tombstone, real HTTP counts, the global auth/quota breaker, and the
source-aware view of what "current" means.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from collector.kr.domain.enums import (
    ListingStatus,
    Market,
    RunStatus,
    RunType,
    Source,
    UniverseScope,
)
from collector.kr.domain.models import (
    FlowRequestStats,
    IngestionRun,
    SecurityFlowFetchResult,
    SecurityFlowLine,
    Stock,
    UpsertResult,
)
from collector.kr.service.sync_kis_flows import (
    FLOW_CURSOR_SOURCES,
    KIS_FLOW_METRIC_GROUPS,
    resolve_kis_flow_plan,
    sync_kis_security_flows,
)
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError

KST = timezone(timedelta(hours=9))

# 2026-08-03 .. 2026-08-14 has these sessions (2026-08-15 is 광복절).
SESSIONS = [
    date(2026, 8, 3),
    date(2026, 8, 4),
    date(2026, 8, 5),
    date(2026, 8, 6),
    date(2026, 8, 7),
    date(2026, 8, 10),
    date(2026, 8, 11),
    date(2026, 8, 12),
    date(2026, 8, 13),
    date(2026, 8, 14),
]
LATEST = SESSIONS[-1]


def _stock(ticker: str, market: Market = Market.KOSPI) -> Stock:
    return Stock(
        ticker=ticker,
        market=market,
        name=f"name-{ticker}",
        status=ListingStatus.ACTIVE,
        last_seen_date=LATEST,
        source=Source.KRX,
    )


class FakeProvider:
    def __init__(
        self,
        *,
        results: dict[str, SecurityFlowFetchResult] | None = None,
        raises: dict[str, Exception] | None = None,
    ) -> None:
        self.results = results or {}
        self.raises = raises or {}
        self.calls: list[tuple[str, str, date, date]] = []
        self.http_requests = 0

    def source(self) -> Source:
        return Source.KIS

    def supported_flow_groups(self) -> tuple[str, ...]:
        return ("foreign_holding", "investor", "shorting")

    def unsupported_metric_codes(self) -> list[str]:
        return ["short_selling_balance_quantity"]

    def request_stats(self) -> FlowRequestStats:
        return FlowRequestStats(
            http_requests=self.http_requests,
            http_retries=1,
            pages_fetched=self.http_requests,
            auth_token_cache_hits=1,
            status_counts={"200": self.http_requests},
        )

    def _answer(self, group: str, ticker: str, start: date, end: date):
        self.calls.append((group, ticker, start, end))
        self.http_requests += 1
        key = f"{group}:{ticker}"
        if key in self.raises:
            raise self.raises[key]
        if key in self.results:
            return self.results[key]
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=end,
                    ticker=ticker,
                    market=Market.KOSPI,
                    metric_code=KIS_FLOW_METRIC_GROUPS[group][0],
                    metric_name="metric",
                    value=Decimal("1"),
                    unit="shares",
                    source=Source.KIS,
                    fetched_at=datetime(2026, 8, 16, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def fetch_foreign_holding(self, ticker, market, trade_date):
        return self._answer("foreign_holding", ticker, trade_date, trade_date)

    def fetch_investor_net_volume(self, ticker, market, start, end):
        return self._answer("investor", ticker, start, end)

    def fetch_shorting_metrics(self, ticker, market, start, end):
        return self._answer("shorting", ticker, start, end)


class FakeStorage:
    def __init__(
        self,
        *,
        stocks: list[Stock] | None = None,
        coverage: dict[tuple[str, str], tuple[int, date]] | None = None,
        recent_runs: list[IngestionRun] | None = None,
    ) -> None:
        self.stocks = stocks if stocks is not None else [_stock("005930")]
        self.coverage = coverage or {}
        self.recent_runs = recent_runs or []
        self.runs: list[IngestionRun] = []
        self.records: list[SecurityFlowLine] = []
        self.coverage_calls: list[dict] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        if market is None:
            return list(self.stocks)
        return [stock for stock in self.stocks if stock.market == market]

    def get_stocks(self, market=None, tickers=None) -> list[Stock]:
        return list(self.stocks)

    def get_dart_corp_master(self, active_only=True, tickers=None, include_delisted=False):
        return []

    def upsert_krx_security_flow_raw(self, records: list[SecurityFlowLine]) -> UpsertResult:
        self.records.extend(records)
        return UpsertResult(updated=len(records))

    def get_krx_security_flow_ticker_metric_coverage(
        self, start, end, tickers, metric_codes, sources
    ):
        self.coverage_calls.append(
            {
                "start": start,
                "end": end,
                "tickers": list(tickers),
                "metric_codes": list(metric_codes),
                "sources": tuple(sources),
            }
        )
        return dict(self.coverage)

    def get_recent_ingestion_runs(self, run_type: RunType, limit: int = 20) -> list[IngestionRun]:
        return [run for run in self.recent_runs if run.run_type is run_type][:limit]


def _full_coverage(ticker: str, groups: list[str]) -> dict[tuple[str, str], tuple[int, date]]:
    return {
        (ticker, metric): (len(SESSIONS), LATEST)
        for group in groups
        for metric in KIS_FLOW_METRIC_GROUPS[group]
    }


def _run(storage: FakeStorage, provider: FakeProvider, **kwargs):
    return sync_kis_security_flows(
        provider,
        storage,
        start=SESSIONS[0],
        end=LATEST,
        scope=UniverseScope.CURRENT,
        # Real sleeping is not the behaviour under test; the retry and backoff
        # policies have their own coverage in test_retry.py.
        retry_max_attempts=kwargs.pop("retry_max_attempts", 1),
        guard_backoff_seconds=kwargs.pop("guard_backoff_seconds", 0.0),
        **kwargs,
    )


# --- condition 1: the cursor is source-scoped -------------------------------


def test_the_cursor_reads_only_kis_rows() -> None:
    # KRX can finish first on the same day. Its row must not make the KIS item
    # look complete, or the daily KIS snapshot job skips the whole universe.
    storage = FakeStorage()
    _run(storage, FakeProvider())

    assert storage.coverage_calls
    assert storage.coverage_calls[0]["sources"] == FLOW_CURSOR_SOURCES
    assert storage.coverage_calls[0]["sources"] == (Source.KIS,)


def test_kis_history_lets_the_next_kis_run_resume_rather_than_refetch() -> None:
    # KIS filled everything up to 2026-08-12; it should ask only for the rest.
    stored_through = SESSIONS[-3]
    coverage = {
        (ticker_metric[0], ticker_metric[1]): (SESSIONS.index(stored_through) + 1, stored_through)
        for ticker_metric in [("005930", metric) for metric in KIS_FLOW_METRIC_GROUPS["investor"]]
    }
    plan = resolve_kis_flow_plan(
        targets=[_stock("005930")],
        trading_days=SESSIONS,
        coverage=coverage,
        enabled_groups=["investor"],
        no_data_keys=set(),
        latest_trading_day=LATEST,
    )

    assert len(plan.items) == 1
    assert plan.items[0].start == SESSIONS[-2]
    assert plan.items[0].end == LATEST


# --- condition 2: the checkpoint is per ticker ------------------------------


def test_a_ticker_that_is_already_current_is_not_requested() -> None:
    storage = FakeStorage(coverage=_full_coverage("005930", ["investor", "shorting"]))
    provider = FakeProvider()
    result = _run(storage, provider, enabled_flow_groups=["investor", "shorting"])

    assert provider.calls == []
    assert result.requests_attempted == 0
    assert result.requests_skipped == 2


def test_a_hole_behind_the_cursor_refetches_the_whole_window() -> None:
    # Latest date is current but three sessions are missing behind it. Trusting
    # MAX(trade_date) alone is exactly how a per-ticker collector hides gaps.
    coverage = {
        ("005930", metric): (len(SESSIONS) - 3, LATEST)
        for metric in KIS_FLOW_METRIC_GROUPS["investor"]
    }
    plan = resolve_kis_flow_plan(
        targets=[_stock("005930")],
        trading_days=SESSIONS,
        coverage=coverage,
        enabled_groups=["investor"],
        no_data_keys=set(),
        latest_trading_day=LATEST,
    )

    assert len(plan.items) == 1
    assert plan.items[0].start == SESSIONS[0]


def test_failures_are_recorded_per_ticker_not_only_counted() -> None:
    storage = FakeStorage(stocks=[_stock("005930"), _stock("000660"), _stock("035720")])
    provider = FakeProvider(
        results={"investor:000660": SecurityFlowFetchResult(error="upstream 500")}
    )
    result = _run(storage, provider, enabled_flow_groups=["investor"])

    finished = storage.runs[-1]
    assert finished.status is RunStatus.PARTIAL
    assert finished.params["failed_request_keys"] == ["investor:000660"]
    assert "investor:000660" in result.errors
    assert result.rows_upserted == 2


# --- condition 3: the no-data tombstone -------------------------------------


def test_no_data_is_written_to_the_run_so_the_next_run_can_skip_it() -> None:
    storage = FakeStorage(stocks=[_stock("005930"), _stock("000660")])
    provider = FakeProvider(results={"investor:000660": SecurityFlowFetchResult(no_data=True)})
    _run(storage, provider, enabled_flow_groups=["investor"])

    finished = storage.runs[-1]
    assert finished.params["no_data_request_keys"] == ["investor:000660"]


def test_a_recent_tombstone_suppresses_the_request() -> None:
    tombstone = IngestionRun(
        run_type=RunType.KIS_FLOW_SYNC,
        started_at=datetime.now(KST) - timedelta(days=1),
        status=RunStatus.SUCCESS,
        params={"no_data_request_keys": ["investor:000660"]},
    )
    storage = FakeStorage(
        stocks=[_stock("005930"), _stock("000660")],
        recent_runs=[tombstone],
    )
    provider = FakeProvider()
    result = _run(storage, provider, enabled_flow_groups=["investor"])

    assert [call[1] for call in provider.calls] == ["005930"]
    assert result.requests_skipped == 1


def test_a_stale_tombstone_expires() -> None:
    # A suspended name can resume; the tombstone is about today, not forever.
    tombstone = IngestionRun(
        run_type=RunType.KIS_FLOW_SYNC,
        started_at=datetime.now(KST) - timedelta(days=90),
        status=RunStatus.SUCCESS,
        params={"no_data_request_keys": ["investor:000660"]},
    )
    storage = FakeStorage(
        stocks=[_stock("005930"), _stock("000660")],
        recent_runs=[tombstone],
    )
    provider = FakeProvider()
    _run(storage, provider, enabled_flow_groups=["investor"], no_data_ttl_days=7)

    assert sorted(call[1] for call in provider.calls) == ["000660", "005930"]


# --- condition 4: real HTTP counts ------------------------------------------


def test_run_counts_carry_real_http_not_just_logical_requests() -> None:
    storage = FakeStorage(stocks=[_stock("005930"), _stock("000660")])
    result = _run(storage, FakeProvider(), enabled_flow_groups=["investor"])

    counts = storage.runs[-1].counts
    assert counts["requests_attempted"] == 2
    assert counts["http_requests"] == 2
    assert counts["http_retries"] == 1
    assert counts["http_status_200"] == 2
    assert counts["auth_token_cache_hits"] == 1
    assert result.http_counts["http_requests"] == 2


# --- condition 5: the global auth / quota breaker ---------------------------


@pytest.mark.parametrize(
    "error",
    [SourceAuthError("token rejected"), SourceQuotaExhaustedError("rate limited")],
)
def test_auth_and_quota_failures_fail_the_run_instead_of_continuing(error: Exception) -> None:
    storage = FakeStorage(stocks=[_stock("005930"), _stock("000660"), _stock("035720")])
    # Targets run in ticker order, so this is the very first request.
    provider = FakeProvider(raises={"investor:000660": error})
    result = _run(storage, provider, enabled_flow_groups=["investor"])

    assert result.aborted_reason == type(error).__name__
    assert storage.runs[-1].status is RunStatus.FAILED
    # It stopped instead of spending the rest of the list on a server saying no,
    # and it did not retry: an auth retry can cost a token issuance.
    assert len(provider.calls) == 1
    assert storage.runs[-1].counts["http_requests"] == 1


def test_sustained_per_item_failures_trip_the_consecutive_guard() -> None:
    storage = FakeStorage(stocks=[_stock(f"{index:06d}") for index in range(1, 21)])
    provider = FakeProvider(
        results={
            f"investor:{index:06d}": SecurityFlowFetchResult(error="refused")
            for index in range(1, 21)
        }
    )
    result = _run(
        storage,
        provider,
        enabled_flow_groups=["investor"],
        max_consecutive_failures=3,
    )

    assert result.aborted_reason == "SourceBlockedError"
    assert storage.runs[-1].status is RunStatus.FAILED
    # Three items, then the run stops — not the 20 the target list would cost.
    # With the production retry budget this is 3 x 3 requests; either way the
    # point is that it stops rather than working through a refusing source.
    assert len(provider.calls) == 3


# --- condition 6: source-aware scope, and what KIS cannot cover -------------


def test_foreign_holding_is_refused_for_anything_but_the_newest_session() -> None:
    # inquire-price has no business date. Filing a live figure under an old day
    # would be a silent lie, so the group is skipped with a stated reason.
    plan = resolve_kis_flow_plan(
        targets=[_stock("005930")],
        trading_days=SESSIONS[:-1],
        coverage={},
        enabled_groups=["foreign_holding", "investor"],
        no_data_keys=set(),
        latest_trading_day=LATEST,
    )

    assert [item.group for item in plan.items] == ["investor"]
    assert "foreign_holding" in plan.skipped_groups
    assert "live snapshot" in plan.skipped_groups["foreign_holding"]


def test_foreign_holding_runs_for_the_newest_session() -> None:
    plan = resolve_kis_flow_plan(
        targets=[_stock("005930")],
        trading_days=SESSIONS,
        coverage={},
        enabled_groups=["foreign_holding"],
        no_data_keys=set(),
        latest_trading_day=LATEST,
    )
    assert [item.group for item in plan.items] == ["foreign_holding"]
    assert plan.items[0].end == LATEST


def test_the_krx_only_balance_metric_is_reported_as_pending() -> None:
    storage = FakeStorage()
    result = _run(storage, FakeProvider(), enabled_flow_groups=["shorting"])

    assert result.pending_metrics == ["short_selling_balance_quantity"]
    assert storage.runs[-1].counts["pending_metric_count"] == 1
    assert all(record.metric_code != "short_selling_balance_quantity" for record in storage.records)


def test_the_kis_group_map_never_claims_a_metric_kis_cannot_fill() -> None:
    from collector.kr.service.sync_krx_flows import FLOW_METRIC_GROUPS

    kis_metrics = {metric for metrics in KIS_FLOW_METRIC_GROUPS.values() for metric in metrics}
    krx_metrics = {metric for metrics in FLOW_METRIC_GROUPS.values() for metric in metrics}

    assert kis_metrics < krx_metrics
    assert krx_metrics - kis_metrics == {"short_selling_balance_quantity"}


def test_an_unsupported_group_is_rejected_before_any_request() -> None:
    provider = FakeProvider()
    with pytest.raises(ValueError, match="short_selling_balance"):
        sync_kis_security_flows(
            provider,
            FakeStorage(),
            start=SESSIONS[0],
            end=LATEST,
            enabled_flow_groups=["short_selling_balance"],
        )
    assert provider.calls == []


def test_a_clean_run_is_success_with_rows_written() -> None:
    storage = FakeStorage(stocks=[_stock("005930"), _stock("000660")])
    result = _run(storage, FakeProvider(), enabled_flow_groups=["investor", "shorting"])

    assert storage.runs[-1].status is RunStatus.SUCCESS
    assert result.requests_attempted == 4
    assert result.rows_upserted == 4
    assert all(record.source is Source.KIS for record in storage.records)
    assert storage.runs[-1].run_type is RunType.KIS_FLOW_SYNC


def test_a_second_run_on_the_same_day_requests_nothing() -> None:
    # Including foreign holding. It is a live snapshot, so it can never hold a
    # full window from one run; judging it by the windowed rule made every run
    # re-request every ticker for days that endpoint cannot fill.
    coverage = {
        **_full_coverage("005930", ["investor", "shorting"]),
        ("005930", "foreign_holding_shares"): (1, LATEST),
    }
    plan = resolve_kis_flow_plan(
        targets=[_stock("005930")],
        trading_days=SESSIONS,
        coverage=coverage,
        enabled_groups=["foreign_holding", "investor", "shorting"],
        no_data_keys=set(),
        latest_trading_day=LATEST,
    )

    assert plan.items == []
    assert plan.skipped_current == 3


def test_yesterdays_snapshot_does_not_satisfy_today() -> None:
    coverage = {("005930", "foreign_holding_shares"): (1, SESSIONS[-2])}
    plan = resolve_kis_flow_plan(
        targets=[_stock("005930")],
        trading_days=SESSIONS,
        coverage=coverage,
        enabled_groups=["foreign_holding"],
        no_data_keys=set(),
        latest_trading_day=LATEST,
    )

    assert len(plan.items) == 1
    assert plan.items[0].start == LATEST


def test_an_aborted_run_still_records_which_tickers_were_left_behind() -> None:
    # A checkpoint that only survives a clean finish is not a checkpoint. An
    # abort is exactly when you need to know what to re-run.
    storage = FakeStorage(stocks=[_stock(f"{index:06d}") for index in range(1, 21)])
    provider = FakeProvider(
        results={
            f"investor:{index:06d}": SecurityFlowFetchResult(error="refused")
            for index in range(1, 21)
        }
    )
    _run(storage, provider, enabled_flow_groups=["investor"], max_consecutive_failures=3)

    finished = storage.runs[-1]
    assert finished.status is RunStatus.FAILED
    assert finished.params["failed_request_keys"] == [
        "investor:000001",
        "investor:000002",
        "investor:000003",
    ]


def test_an_aborted_run_still_records_no_data_tombstones() -> None:
    storage = FakeStorage(stocks=[_stock("000660"), _stock("005930"), _stock("035720")])
    provider = FakeProvider(
        results={
            "investor:000660": SecurityFlowFetchResult(no_data=True),
            "investor:005930": SecurityFlowFetchResult(no_data=True),
        },
        raises={"investor:035720": SourceQuotaExhaustedError("quota spent")},
    )
    _run(storage, provider, enabled_flow_groups=["investor"])

    finished = storage.runs[-1]
    assert finished.status is RunStatus.FAILED
    assert finished.params["no_data_request_keys"] == [
        "investor:000660",
        "investor:005930",
    ]

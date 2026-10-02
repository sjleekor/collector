"""Read-only readiness check that gates the KR raw export.

The nightly serving path exports raw tables to parquet after the collection
chains. A fixed start time cannot prove the collection finished: the 04:00
OpenDART chain has started at 05:20 and 04:50, and a run that failed and was
retried finishes later still. This check turns "is the input ready for feature
date K" into evidence the export step can wait on.

Two kinds of evidence are combined, because neither alone is enough:

* **Data**: ``daily_ohlcv`` holds K with close to the previous session's ticker
  count, and the flow tables reach K. This is what ``ops freshness-report``
  already measures; the logic is reused.
* **Run records**: for each required ``run_type`` the *latest* ``ingestion_runs``
  row ended after an expected-since time and is ``success``. Cronicle's exit
  code is not used: ``dart_share_info_sync`` has ended ``partial`` with exit 0.

The DART chain cannot prove "K-day filings arrived" (a day with no new filings
ends ``success`` after zero attempts). The verdict therefore only claims that the
chain *completed*; the end of the chain is the day's ``xbrl_parse.ended_at``.

Verdicts: ``ready`` / ``not_yet`` (keep waiting) / ``blocked`` (something
failed or the input has moved past K; waiting will not help unless the run is
retried). The exit-code mapping lives in the CLI handler.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Protocol

from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.models import IngestionRun
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.service.freshness import flow_latest_dates
from collector.kr.service.sync_krx_flows import FLOW_METRIC_GROUPS, active_flow_metrics
from collector.kr.util.time import KST

VERDICT_READY = "ready"
VERDICT_NOT_YET = "not_yet"
VERDICT_BLOCKED = "blocked"

EXIT_READY = 0
EXIT_BLOCKED = 1
#: EX_TEMPFAIL, the code the source-lock wrapper already uses for "try later".
EXIT_NOT_YET = 75

EVIDENCE_SCHEMA = "kr_export_readiness/v1"

#: Run types whose latest record must be fresh and ``success``.
DEFAULT_REQUIRED_RUN_TYPES: tuple[RunType, ...] = (
    RunType.DAILY_BACKFILL,
    RunType.KRX_FLOW_SYNC,
    RunType.KIS_FLOW_SYNC,
    RunType.DART_FILING_RECEIPT_SYNC,
    RunType.DART_CORP_SYNC,
    RunType.DART_FINANCIAL_SYNC,
    RunType.DART_SHARE_INFO_SYNC,
    RunType.XBRL_PARSE,
)

#: Run types written by the 04:00 OpenDART chain (corp -> financials -> share_info ->
#: xbrl_parse; morning of the as-of date). The filing-receipt sync is NOT in this
#: chain: it is its own Cronicle event, see ``DAILY_2330_RUN_TYPES``.
DART_RUN_TYPES: frozenset[RunType] = frozenset(
    {
        RunType.DART_CORP_SYNC,
        RunType.DART_FINANCIAL_SYNC,
        RunType.DART_SHARE_INFO_SYNC,
        RunType.XBRL_PARSE,
    }
)

#: ``sdc_daily_opendart_filings`` runs every day at 23:30 KST (~12-15 min), independent
#: of the 04:00 chain. Source: my/milestones/kr/modeling/dev/20260731_raw_features/
#: 01_feature_candidate/00_status.md §8.5 and 02_data_expansion_plan/10_work_breakdown.md
#: (deploy/prod/README.md does not list this event; confirm against the Cronicle export).
#: Daily means a weekend run after a Friday K also satisfies it.
DAILY_2330_RUN_TYPES: frozenset[RunType] = frozenset({RunType.DART_FILING_RECEIPT_SYNC})

#: The run whose end marks the end of the DART chain.
DART_CHAIN_END_RUN_TYPE = RunType.XBRL_PARSE

#: Mon-Fri evening KRX chain starts 18:30 KST (deploy/prod/README.md).
DEFAULT_EVENING_START = time(18, 30)
#: Daily OpenDART chain starts 04:00 KST. It has started later (05:20, 04:50),
#: which only delays ``ended_at``; it never makes a good run look older than this.
DEFAULT_DART_START = time(4, 0)
DEFAULT_FILING_RECEIPT_START = time(23, 30)

#: Minimum K ticker count as a share of the previous session's.
#:
#: Why 0.97: the universe changes by a handful of listings and delistings per
#: day (well under 1% of roughly 2,800 tickers), so a complete session sits
#: near 1.00. A collection that stopped part-way — a market missing, a range
#: guard hit — loses far more than 3%. 0.97 leaves room for a legitimately busy
#: delisting day while still catching half-written sessions. It is a starting
#: value, not a fitted one: calibrate against the historical per-session count
#: ratio on the server and pass ``--min-ticker-ratio``.
DEFAULT_MIN_TICKER_RATIO = 0.97
DEFAULT_FLOW_MAX_LAG_TRADING_DAYS = 0
#: How far back to look for the previous session when counting tickers.
PRICE_COUNT_LOOKBACK_DAYS = 30


class ReadinessStorage(Protocol):
    """The slice of storage this check reads. ``PostgresStorage`` implements it."""

    def get_latest_daily_price_date(self, tickers: list[str] | None = None) -> date | None: ...

    def get_daily_ohlcv_session_counts(self, since: date, until: date) -> dict[date, int]: ...

    def get_krx_security_flow_metric_max_dates(
        self, metric_codes: list[str], sources: Any
    ) -> dict[str, date]: ...

    def get_recent_ingestion_runs(
        self, run_type: RunType, limit: int = 20
    ) -> list[IngestionRun]: ...


@dataclass(frozen=True, slots=True)
class ReadinessConfig:
    feature_asof_date: date
    as_of: datetime
    min_ticker_ratio: float = DEFAULT_MIN_TICKER_RATIO
    flow_max_lag_trading_days: int = DEFAULT_FLOW_MAX_LAG_TRADING_DAYS
    required_run_types: tuple[RunType, ...] = DEFAULT_REQUIRED_RUN_TYPES
    #: Per-run-type overrides of the expected-since time.
    run_since_overrides: tuple[tuple[RunType, datetime], ...] = ()
    holidays: frozenset[date] | None = None


def _aware_kst(value: datetime) -> datetime:
    return value.replace(tzinfo=KST) if value.tzinfo is None else value.astimezone(KST)


def default_expected_since(run_type: RunType, k: date, as_of: datetime) -> datetime:
    """KR-serving default: K 18:30 evening chain, K 23:30 filing receipts, D 04:00 DART chain."""
    if run_type in DAILY_2330_RUN_TYPES:
        return datetime.combine(k, DEFAULT_FILING_RECEIPT_START, tzinfo=KST)
    if run_type in DART_RUN_TYPES:
        return datetime.combine(_aware_kst(as_of).date(), DEFAULT_DART_START, tzinfo=KST)
    return datetime.combine(k, DEFAULT_EVENING_START, tzinfo=KST)


def _iso(value: datetime | date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _check_prices(storage: ReadinessStorage, cfg: ReadinessConfig) -> dict[str, Any]:
    k = cfg.feature_asof_date
    latest = storage.get_latest_daily_price_date()
    counts = storage.get_daily_ohlcv_session_counts(
        k - timedelta(days=PRICE_COUNT_LOOKBACK_DAYS), k
    )
    k_count = counts.get(k)
    previous = max((d for d in counts if d < k), default=None)
    previous_count = counts.get(previous) if previous else None
    ratio = k_count / previous_count if k_count is not None and previous_count else None

    status = "ok"
    reasons: list[str] = []
    if latest is None or latest < k:
        status = VERDICT_NOT_YET
        reasons.append(f"daily_ohlcv max trade_date={_iso(latest)} is older than K={k.isoformat()}")
    elif latest > k:
        status = VERDICT_BLOCKED
        reasons.append(
            f"daily_ohlcv max trade_date={_iso(latest)} is past K={k.isoformat()}; "
            "the export would include later data than the feature date"
        )
    else:
        if k_count is None:
            status = VERDICT_NOT_YET
            reasons.append(f"no daily_ohlcv rows for K={k.isoformat()}")
        elif previous_count is None:
            # No previous session in the lookback: the ratio cannot be judged.
            reasons.append("no previous session in lookback; ticker-ratio check skipped")
        elif ratio is not None and ratio < cfg.min_ticker_ratio:
            status = VERDICT_NOT_YET
            reasons.append(
                f"K ticker count {k_count} is {ratio:.4f} of previous session "
                f"{previous_count} (< {cfg.min_ticker_ratio})"
            )
    return {
        "check": "daily_ohlcv",
        "status": status,
        "max_trade_date": _iso(latest),
        "k_ticker_count": k_count,
        "previous_session": _iso(previous),
        "previous_ticker_count": previous_count,
        "ratio": round(ratio, 6) if ratio is not None else None,
        "min_ticker_ratio": cfg.min_ticker_ratio,
        "reasons": reasons,
    }


def _flow_required_date(cfg: ReadinessConfig) -> date:
    lag = max(0, cfg.flow_max_lag_trading_days)
    if lag == 0:
        return cfg.feature_asof_date
    k = cfg.feature_asof_date
    sessions = get_trading_days(
        k - timedelta(days=60), k, holidays=set(cfg.holidays) if cfg.holidays else None
    )
    if len(sessions) > lag:
        return sessions[-1 - lag]
    return sessions[0] if sessions else k


def _check_flows(storage: ReadinessStorage, cfg: ReadinessConfig) -> dict[str, Any]:
    required = _flow_required_date(cfg)
    _, group_latest = flow_latest_dates(storage)  # type: ignore[arg-type]
    groups: dict[str, dict[str, Any]] = {}
    status = "ok"
    reasons: list[str] = []
    for group, latest in sorted(group_latest.items()):
        if group in FLOW_METRIC_GROUPS and not active_flow_metrics(group):
            groups[group] = {"latest": _iso(latest), "status": "skipped_discontinued"}
            continue
        group_status = "ok"
        if latest is None or latest < required:
            group_status = VERDICT_NOT_YET
            status = VERDICT_NOT_YET
            reasons.append(
                f"flow group {group} latest={_iso(latest)} is older than " f"{required.isoformat()}"
            )
        groups[group] = {"latest": _iso(latest), "status": group_status}
    return {
        "check": "flows",
        "status": status,
        "required_at_or_after": required.isoformat(),
        "flow_max_lag_trading_days": cfg.flow_max_lag_trading_days,
        "groups": groups,
        "reasons": reasons,
    }


def _expected_since(run_type: RunType, cfg: ReadinessConfig) -> datetime:
    for override_type, since in cfg.run_since_overrides:
        if override_type == run_type:
            return _aware_kst(since)
    return default_expected_since(run_type, cfg.feature_asof_date, cfg.as_of)


def _check_run(
    storage: ReadinessStorage, run_type: RunType, cfg: ReadinessConfig
) -> dict[str, Any]:
    since = _expected_since(run_type, cfg)
    runs = storage.get_recent_ingestion_runs(run_type, limit=1)
    row: dict[str, Any] = {
        "check": "ingestion_run",
        "run_type": run_type.value,
        "expected_since": since.isoformat(),
    }
    if not runs:
        return {
            **row,
            "status": VERDICT_NOT_YET,
            "run_id": None,
            "run_status": None,
            "reasons": [f"{run_type.value}: no ingestion_runs record"],
        }
    run = runs[0]
    ended = _aware_kst(run.ended_at) if run.ended_at else None
    row.update(
        run_id=run.run_id,
        run_status=run.status.value,
        started_at=_iso(run.started_at),
        ended_at=_iso(ended),
        counts=run.counts,
        error_summary=run.error_summary,
    )
    if run.status == RunStatus.RUNNING or ended is None:
        return {
            **row,
            "status": VERDICT_NOT_YET,
            "reasons": [f"{run_type.value}: latest run {run.run_id} is still running"],
        }
    if ended < since:
        return {
            **row,
            "status": VERDICT_NOT_YET,
            "reasons": [
                f"{run_type.value}: latest run ended {ended.isoformat()} "
                f"before expected {since.isoformat()} (status={run.status.value})"
            ],
        }
    if run.status != RunStatus.SUCCESS:
        return {
            **row,
            "status": VERDICT_BLOCKED,
            "reasons": [
                f"{run_type.value}: latest run {run.run_id} ended {run.status.value}"
                + (f" ({run.error_summary})" if run.error_summary else "")
            ],
        }
    return {**row, "status": "ok", "reasons": []}


def evaluate_export_readiness(storage: ReadinessStorage, cfg: ReadinessConfig) -> dict[str, Any]:
    """Run every check and return the JSON-serialisable evidence document."""
    checks: list[dict[str, Any]] = [_check_prices(storage, cfg), _check_flows(storage, cfg)]
    run_checks = [_check_run(storage, rt, cfg) for rt in cfg.required_run_types]
    checks.extend(run_checks)

    statuses = {c["status"] for c in checks}
    if VERDICT_BLOCKED in statuses:
        verdict = VERDICT_BLOCKED
    elif VERDICT_NOT_YET in statuses:
        verdict = VERDICT_NOT_YET
    else:
        verdict = VERDICT_READY
    reasons = [r for c in checks for r in c["reasons"]]

    dart_end = next(
        (c.get("ended_at") for c in run_checks if c["run_type"] == DART_CHAIN_END_RUN_TYPE.value),
        None,
    )
    return {
        "schema": EVIDENCE_SCHEMA,
        "verdict": verdict,
        "reasons": reasons,
        "feature_asof_date": cfg.feature_asof_date.isoformat(),
        "as_of": _aware_kst(cfg.as_of).isoformat(),
        "dart_chain_ended_at": dart_end,
        "parameters": {
            "min_ticker_ratio": cfg.min_ticker_ratio,
            "flow_max_lag_trading_days": cfg.flow_max_lag_trading_days,
            "required_run_types": [rt.value for rt in cfg.required_run_types],
            "expected_since": {
                rt.value: _expected_since(rt, cfg).isoformat() for rt in cfg.required_run_types
            },
        },
        "checks": checks,
    }


def exit_code_for(verdict: str) -> int:
    if verdict == VERDICT_READY:
        return EXIT_READY
    if verdict == VERDICT_NOT_YET:
        return EXIT_NOT_YET
    return EXIT_BLOCKED

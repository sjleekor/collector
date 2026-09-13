"""Use-case: Sync security-flow raw rows from KIS, one ticker at a time.

This is not a port of :mod:`sync_krx_flows` with a different provider. KRX
answers ``(date, market) -> every ticker``; KIS answers ``(ticker) -> a date
range``. Everything downstream of that difference has to change:

* **The cursor is source-scoped.** KRX and KIS can run in parallel, so a KRX
  row must not make a KIS work item look complete. The first transition run may
  refetch the bounded lookback window; after that, only KIS rows advance the
  KIS cursor.
* **The checkpoint is per ticker.** A KRX failure leaves a market-day hole that
  aggregate counters describe. A KIS failure leaves a hole in one name, and the
  run has to say which one — a sample of three error keys cannot.
* **No-data is recorded, not just counted.** A per-ticker collector asks the
  same dead question every night otherwise.
* **The audit counts real HTTP.** ``requests_attempted`` is logical work; the
  provider reports what actually left, which is what a published quota needs.
* **Auth and quota failures abort.** Continuing spends the whole target list on
  a server saying no, and for KIS each retry can cost a token issuance, which
  notifies the account holder.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta

from collector.kr.domain.enums import (
    ListingStatus,
    Market,
    RunStatus,
    RunType,
    Source,
    UniverseScope,
)
from collector.kr.domain.models import (
    IngestionRun,
    KrxFlowPhaseCounts,
    SecurityFlowFetchResult,
    Stock,
    UpsertResult,
)
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.ports.flows import TickerFlowProvider
from collector.kr.ports.storage import Storage
from collector.kr.service.collection_targets import (
    resolve_dart_targets,
    resolve_price_targets,
)
from collector.kr.util.pipeline import (
    ConsecutiveFailureGuard,
    SourceAuthError,
    SourceBlockedError,
    SourceQuotaExhaustedError,
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    recent_no_data_request_keys,
    record_no_data_request_keys,
)
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

FOREIGN_HOLDING_GROUP = "foreign_holding"
INVESTOR_GROUP = "investor"
SHORTING_GROUP = "shorting"

# The metric codes KIS actually fills, by group. Deliberately a subset of
# ``sync_krx_flows.FLOW_METRIC_GROUPS``: ``short_selling_balance_quantity`` is
# a KRX-only series, and pretending otherwise would make the cursor for that
# metric advance on data nobody collected.
KIS_FLOW_METRIC_GROUPS: dict[str, tuple[str, ...]] = {
    FOREIGN_HOLDING_GROUP: ("foreign_holding_shares",),
    INVESTOR_GROUP: (
        "institution_net_buy_volume",
        "individual_net_buy_volume",
        "foreign_net_buy_volume",
    ),
    SHORTING_GROUP: ("short_selling_volume", "short_selling_value"),
}

# Only rows written by this collector can complete its work. Reading KRX here
# made the daily KIS foreign-holding job skip every ticker whenever the KRX job
# had already stored the same trading day.
FLOW_CURSOR_SOURCES: tuple[Source, ...] = (Source.KIS,)

DEFAULT_LOOKBACK_DAYS = 10
DEFAULT_NO_DATA_TTL_DAYS = 7
DEFAULT_MAX_CONSECUTIVE_FAILURES = 5
# Each retry spends quota on a source that already answered. Three attempts
# is the repo-wide default; the KIS path exposes it because a per-ticker
# collector multiplies it by the whole universe.
DEFAULT_RETRY_MAX_ATTEMPTS = 3
DEFAULT_GUARD_BACKOFF_SECONDS = 1.0
DEFAULT_PROGRESS_LOG_EVERY_ITEMS = 200
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 30.0
# Above the whole universe times every group, so in practice nothing is cut.
# The truncation count is recorded anyway — a silently shortened list reads as
# "that is all of them".
MAX_CHECKPOINT_KEYS = 20000


@dataclass(slots=True)
class KisFlowSyncResult:
    """Outcome of syncing security-flow raw rows from KIS."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    #: Requests that reached KIS and got well-formed rows, none of them inside
    #: the requested window — the ticker did not trade during it. Halts are the
    #: common cause. Deliberately NOT counted as no-data: the tombstone key has
    #: no date in it, so tombstoning this would suspend the ticker for the whole
    #: TTL, and a halted ticker is exactly one whose resumption must be caught.
    #: Counted separately because otherwise such a run reports "attempted 129,
    #: rows 0, no_data 0, errors 0" and reads as if nothing happened.
    rows_outside_window: int = 0
    phase_counts: dict[str, KrxFlowPhaseCounts] = field(default_factory=dict)
    pending_metrics: list[str] = field(default_factory=list)
    skipped_groups: dict[str, str] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    http_counts: dict[str, int] = field(default_factory=dict)
    aborted_reason: str | None = None


@dataclass(frozen=True, slots=True)
class KisFlowWorkItem:
    """One ticker's outstanding window for one metric group."""

    ticker: str
    market: Market
    group: str
    start: date
    end: date

    @property
    def request_key(self) -> str:
        return f"{self.group}:{self.ticker}"


@dataclass(frozen=True, slots=True)
class KisFlowPlan:
    """What a run intends to do, resolvable without touching KIS."""

    items: list[KisFlowWorkItem]
    targets: int
    skipped_current: int
    skipped_no_data: int
    skipped_groups: dict[str, str]
    start: date
    end: date

    def group_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in self.items:
            counts[item.group] = counts.get(item.group, 0) + 1
        return counts


def resolve_kis_flow_plan(
    *,
    targets: list[Stock],
    trading_days: list[date],
    coverage: dict[tuple[str, str], tuple[int, date]],
    enabled_groups: list[str],
    no_data_keys: set[str],
    latest_trading_day: date | None,
) -> KisFlowPlan:
    """Turn stored coverage into the per-ticker windows still outstanding.

    Pure: no storage, no HTTP. That is what makes ``--plan-only`` honest and
    the skip logic testable without a provider.
    """
    if not trading_days:
        raise ValueError("Cannot plan a KIS flow sync: the range holds no trading days.")

    start = trading_days[0]
    end = trading_days[-1]
    session_index = {session: position for position, session in enumerate(trading_days)}

    items: list[KisFlowWorkItem] = []
    skipped_current = 0
    skipped_no_data = 0
    skipped_groups: dict[str, str] = {}

    # ``inquire-price`` returns a live figure with no business date, so the
    # only date it can honestly be filed under is the newest session. Asking
    # for a historical window would silently stamp today's holdings onto an old
    # day, which is worse than not collecting it.
    foreign_enabled = FOREIGN_HOLDING_GROUP in enabled_groups
    if foreign_enabled and (latest_trading_day is None or end != latest_trading_day):
        skipped_groups[FOREIGN_HOLDING_GROUP] = (
            "foreign holding is a live snapshot with no business date; it can only be "
            f"stored for the newest session ({latest_trading_day}), not {end}"
        )
        foreign_enabled = False

    for stock in targets:
        for group in enabled_groups:
            if group == FOREIGN_HOLDING_GROUP and not foreign_enabled:
                continue
            metric_codes = KIS_FLOW_METRIC_GROUPS[group]
            if group == FOREIGN_HOLDING_GROUP:
                group_start = _snapshot_group_start(
                    ticker=stock.ticker,
                    metric_codes=metric_codes,
                    coverage=coverage,
                    newest_session=end,
                )
            else:
                group_start = _group_start(
                    ticker=stock.ticker,
                    metric_codes=metric_codes,
                    coverage=coverage,
                    trading_days=trading_days,
                    session_index=session_index,
                    default_start=start,
                )
            if group_start is None:
                skipped_current += 1
                continue
            item = KisFlowWorkItem(
                ticker=stock.ticker,
                market=stock.market,
                group=group,
                start=group_start,
                end=end,
            )
            if item.request_key in no_data_keys:
                skipped_no_data += 1
                continue
            items.append(item)

    return KisFlowPlan(
        items=items,
        targets=len(targets),
        skipped_current=skipped_current,
        skipped_no_data=skipped_no_data,
        skipped_groups=skipped_groups,
        start=start,
        end=end,
    )


def _group_start(
    *,
    ticker: str,
    metric_codes: tuple[str, ...],
    coverage: dict[tuple[str, str], tuple[int, date]],
    trading_days: list[date],
    session_index: dict[date, int],
    default_start: date,
) -> date | None:
    """The oldest session this ticker/group still needs, or ``None`` if current.

    Two conditions have to hold before a ticker is skipped: its newest stored
    session is the newest session in the window, *and* it has a row for every
    session in between. Checking only the newest date would declare a ticker
    complete while a hole sat behind it — which is exactly the failure mode a
    per-ticker collector introduces.
    """
    window_sessions = len(trading_days)
    earliest_needed: date | None = None

    for metric_code in metric_codes:
        stored = coverage.get((ticker, metric_code))
        if stored is None:
            return default_start
        session_count, latest = stored
        if session_count >= window_sessions and latest == trading_days[-1]:
            continue
        if latest not in session_index or session_count < session_index[latest] + 1:
            # There is a gap behind the cursor; refetch the whole window.
            return default_start
        resume_from = _next_session(trading_days, latest)
        if resume_from is None:
            continue
        earliest_needed = (
            resume_from if earliest_needed is None else min(earliest_needed, resume_from)
        )

    return earliest_needed


def _snapshot_group_start(
    *,
    ticker: str,
    metric_codes: tuple[str, ...],
    coverage: dict[tuple[str, str], tuple[int, date]],
    newest_session: date,
) -> date | None:
    """Completeness for a live-snapshot group is "do we have today's row".

    Foreign holding comes from ``inquire-price``, which only ever answers about
    right now. Judging it by the same session-count rule as the windowed groups
    would mark it incomplete forever — it can never hold a full window from one
    run — and every run would re-request every ticker for days it is incapable
    of filling.
    """
    for metric_code in metric_codes:
        stored = coverage.get((ticker, metric_code))
        if stored is None or stored[1] < newest_session:
            return newest_session
    return None


def _next_session(trading_days: list[date], anchor: date) -> date | None:
    for session in trading_days:
        if session > anchor:
            return session
    return None


def load_kis_flow_targets(
    storage: Storage,
    tickers: list[str] | None,
    scope: UniverseScope,
) -> list[Stock]:
    """Resolve collection targets, mirroring the KRX flow service's fallback."""
    stocks = resolve_price_targets(storage, scope)
    if tickers is not None:
        ticker_filter = set(tickers)
        stocks = [stock for stock in stocks if stock.ticker in ticker_filter]
    if stocks:
        return sorted(stocks, key=lambda stock: stock.ticker)

    dart_rows = resolve_dart_targets(storage, scope, tickers)
    return sorted(
        (
            Stock(
                ticker=row.ticker or "",
                market=row.market,
                name=row.stock_name or row.corp_name,
                status=ListingStatus.ACTIVE,
                last_seen_date=row.modify_date or today_kst(),
                source=Source.OPENDART,
            )
            for row in dart_rows
            if row.ticker and row.market is not None
        ),
        key=lambda stock: stock.ticker,
    )


def sync_kis_security_flows(
    provider: TickerFlowProvider,
    storage: Storage,
    *,
    start: date,
    end: date,
    tickers: list[str] | None = None,
    enabled_flow_groups: list[str] | None = None,
    scope: UniverseScope = UniverseScope.CURRENT,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    no_data_ttl_days: int = DEFAULT_NO_DATA_TTL_DAYS,
    retry_max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS,
    guard_backoff_seconds: float = DEFAULT_GUARD_BACKOFF_SECONDS,
    progress_log_every_items: int = DEFAULT_PROGRESS_LOG_EVERY_ITEMS,
    progress_log_interval_seconds: float = DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
    run_params_extra: dict[str, object] | None = None,
) -> KisFlowSyncResult:
    """Synchronise investor/short-selling/foreign-holding raw rows from KIS."""
    provider_source = provider.source()
    supported_groups = set(provider.supported_flow_groups())
    enabled_groups = sorted(set(enabled_flow_groups or supported_groups) & supported_groups)
    unknown_groups = sorted(set(enabled_flow_groups or []) - supported_groups)
    if unknown_groups:
        supported = ", ".join(sorted(supported_groups))
        raise ValueError(
            f"Unknown or unsupported KIS flow group(s): {', '.join(unknown_groups)} "
            f"(supported: {supported})"
        )
    if not enabled_groups:
        raise ValueError("Cannot run a KIS flow sync with every metric group disabled.")

    run_params: dict[str, object] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "tickers": tickers,
        "provider_source": provider_source.value,
        "enabled_flow_groups": enabled_groups,
        "universe_scope": scope.value,
        "max_consecutive_failures": max_consecutive_failures,
        "no_data_ttl_days": no_data_ttl_days,
        "retry_max_attempts": retry_max_attempts,
        "cursor_sources": [source.value for source in FLOW_CURSOR_SOURCES],
    }
    if run_params_extra:
        run_params.update(run_params_extra)

    run = IngestionRun(
        run_type=RunType.KIS_FLOW_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params=run_params,
    )
    storage.record_run(run)

    result = KisFlowSyncResult(pending_metrics=provider.unsupported_metric_codes())
    for group in enabled_groups:
        result.phase_counts[group] = KrxFlowPhaseCounts()

    guard = ConsecutiveFailureGuard(
        max_consecutive_failures,
        label="KIS flow sync",
        backoff_seconds=guard_backoff_seconds,
        logger_instance=logger,
    )
    # Declared outside the try so an abort can still persist them: a run that
    # stopped early is precisely when knowing which tickers were left behind
    # matters, and a checkpoint that only survives success is not a checkpoint.
    no_data_keys_seen: set[str] = set()
    failed_keys: list[str] = []

    try:
        targets = load_kis_flow_targets(storage, tickers, scope)
        if not targets:
            raise RuntimeError("No stocks found for the requested KIS flow sync.")
        result.targets_processed = len(targets)

        trading_days = get_trading_days(start, end)
        if not trading_days:
            raise RuntimeError("No trading days found in the requested range.")

        plan = build_kis_flow_plan(
            storage=storage,
            targets=targets,
            trading_days=trading_days,
            enabled_groups=enabled_groups,
            no_data_ttl_days=no_data_ttl_days,
        )
        result.skipped_groups = dict(plan.skipped_groups)
        result.requests_skipped = plan.skipped_current + plan.skipped_no_data
        run.params["planned_requests"] = len(plan.items)
        run.params["planned_by_group"] = plan.group_counts()
        run.params["skipped_current"] = plan.skipped_current
        run.params["skipped_no_data_tombstone"] = plan.skipped_no_data
        if plan.skipped_groups:
            run.params["skipped_groups"] = dict(plan.skipped_groups)
            for group, reason in sorted(plan.skipped_groups.items()):
                logger.warning("KIS flow group %s skipped: %s", group, reason)

        logger.info(
            "KIS flow sync started: range=%s..%s targets=%d trading_days=%d "
            "planned=%d skipped_current=%d skipped_no_data=%d groups=%s",
            plan.start.isoformat(),
            plan.end.isoformat(),
            len(targets),
            len(trading_days),
            len(plan.items),
            plan.skipped_current,
            plan.skipped_no_data,
            ",".join(enabled_groups),
        )

        started_at = time.monotonic()
        last_log_at = started_at
        last_log_processed = 0

        for processed, item in enumerate(plan.items, start=1):
            phase_counts = result.phase_counts.setdefault(item.group, KrxFlowPhaseCounts())
            result.requests_attempted += 1
            phase_counts.requests_attempted += 1

            fetch_result = call_with_retry(
                lambda item=item: _fetch(provider, item),
                request_label=item.request_key,
                max_attempts=retry_max_attempts,
                logger_instance=logger,
            )

            if fetch_result.error:
                logger.warning(
                    "KIS flow fetch failed for %s: %s", item.request_key, fetch_result.error
                )
                result.errors[item.request_key] = fetch_result.error
                phase_counts.error_count += 1
                failed_keys.append(item.request_key)
                guard.record_failure(fetch_result.error)
            else:
                guard.record_success()
                if fetch_result.records:
                    upsert = storage.upsert_krx_security_flow_raw(fetch_result.records)
                    result.upsert.updated += upsert.updated
                    result.upsert.errors += upsert.errors
                    result.rows_upserted += upsert.updated
                    phase_counts.rows_upserted += upsert.updated
                if fetch_result.no_data:
                    result.no_data_requests += 1
                    phase_counts.no_data_requests += 1
                    no_data_keys_seen.add(item.request_key)
                elif not fetch_result.records:
                    result.rows_outside_window += 1

            now = time.monotonic()
            should_log = processed == len(plan.items)
            if progress_log_every_items > 0 and processed - last_log_processed >= (
                progress_log_every_items
            ):
                should_log = True
            if progress_log_interval_seconds > 0 and now - last_log_at >= (
                progress_log_interval_seconds
            ):
                should_log = True
            if should_log:
                logger.info(
                    "KIS flow sync progress: processed=%d/%d rows=%d no_data=%d errors=%d "
                    "http_requests=%d elapsed=%.1fs current=%s",
                    processed,
                    len(plan.items),
                    result.rows_upserted,
                    result.no_data_requests,
                    len(result.errors),
                    provider.request_stats().http_requests,
                    now - started_at,
                    item.request_key,
                )
                last_log_at = now
                last_log_processed = processed

        result.http_counts = provider.request_stats().as_counts()
        _record_checkpoint(run, no_data_keys=no_data_keys_seen, failed_keys=failed_keys)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
                pending_metric_count=len(result.pending_metrics),
                **_flatten_phase_counts(result.phase_counts, enabled_groups),
                **result.http_counts,
            ),
            errors=result.errors,
            partial_subject="KIS flow requests",
        )
        return result
    except (SourceAuthError, SourceQuotaExhaustedError, SourceBlockedError) as exc:
        # Not a per-item error: every remaining item would fail the same way.
        result.http_counts = _safe_http_counts(provider)
        result.aborted_reason = type(exc).__name__
        run.counts = build_run_counts(
            targets_processed=result.targets_processed,
            requests_attempted=result.requests_attempted,
            requests_skipped=result.requests_skipped,
            rows_upserted=result.rows_upserted,
            no_data_requests=result.no_data_requests,
            **result.http_counts,
        )
        run.params["aborted_reason"] = result.aborted_reason
        _record_checkpoint(run, no_data_keys=no_data_keys_seen, failed_keys=failed_keys)
        logger.error("KIS flow sync aborted (%s): %s", result.aborted_reason, exc)
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("KIS flow sync failed")
        result.http_counts = _safe_http_counts(provider)
        _record_checkpoint(run, no_data_keys=no_data_keys_seen, failed_keys=failed_keys)
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def _record_checkpoint(
    run: IngestionRun,
    *,
    no_data_keys: set[str],
    failed_keys: list[str],
) -> None:
    """Persist which tickers found nothing and which ones failed.

    Under KIS the failure unit is a per-ticker hole, so aggregate counters and
    a three-key error sample cannot say what to re-run. Both lists are written
    on abort as well as on success.
    """
    record_no_data_request_keys(run, no_data_keys)
    if failed_keys:
        ordered = sorted(set(failed_keys))
        run.params["failed_request_keys"] = ordered[:MAX_CHECKPOINT_KEYS]
        dropped = max(0, len(ordered) - MAX_CHECKPOINT_KEYS)
        if dropped:
            run.params["failed_request_keys_truncated"] = dropped


def build_kis_flow_plan(
    *,
    storage: Storage,
    targets: list[Stock],
    trading_days: list[date],
    enabled_groups: list[str],
    no_data_ttl_days: int,
) -> KisFlowPlan:
    metric_codes = sorted(
        {metric for group in enabled_groups for metric in KIS_FLOW_METRIC_GROUPS[group]}
    )
    coverage = storage.get_krx_security_flow_ticker_metric_coverage(
        start=trading_days[0],
        end=trading_days[-1],
        tickers=[stock.ticker for stock in targets],
        metric_codes=metric_codes,
        sources=FLOW_CURSOR_SOURCES,
    )
    no_data_keys = recent_no_data_request_keys(
        storage,
        run_type=RunType.KIS_FLOW_SYNC,
        as_of=today_kst(),
        ttl_days=no_data_ttl_days,
    )
    return resolve_kis_flow_plan(
        targets=targets,
        trading_days=trading_days,
        coverage=coverage,
        enabled_groups=enabled_groups,
        no_data_keys=no_data_keys,
        latest_trading_day=_latest_trading_day(),
    )


def _latest_trading_day() -> date | None:
    today = today_kst()
    sessions = get_trading_days(today - timedelta(days=30), today)
    return sessions[-1] if sessions else None


def _fetch(provider: TickerFlowProvider, item: KisFlowWorkItem) -> SecurityFlowFetchResult:
    if item.group == FOREIGN_HOLDING_GROUP:
        return provider.fetch_foreign_holding(
            ticker=item.ticker,
            market=item.market,
            trade_date=item.end,
        )
    if item.group == INVESTOR_GROUP:
        return provider.fetch_investor_net_volume(
            ticker=item.ticker,
            market=item.market,
            start=item.start,
            end=item.end,
        )
    return provider.fetch_shorting_metrics(
        ticker=item.ticker,
        market=item.market,
        start=item.start,
        end=item.end,
    )


def _flatten_phase_counts(
    phase_counts: dict[str, KrxFlowPhaseCounts],
    groups: list[str],
) -> dict[str, int]:
    flattened: dict[str, int] = {}
    for group in groups:
        counts = phase_counts.get(group, KrxFlowPhaseCounts())
        flattened.update(
            {
                f"{group}_requests_attempted": counts.requests_attempted,
                f"{group}_requests_skipped": counts.requests_skipped,
                f"{group}_rows_upserted": counts.rows_upserted,
                f"{group}_no_data_requests": counts.no_data_requests,
                f"{group}_error_count": counts.error_count,
            }
        )
    return flattened


def _safe_http_counts(provider: TickerFlowProvider) -> dict[str, int]:
    try:
        return provider.request_stats().as_counts()
    except Exception:  # pragma: no cover - stats must never mask the real error
        logger.warning("Could not read KIS request stats while finalising the run")
        return {}

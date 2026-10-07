"""Use-case: snapshot every ticker's KIS status flags once a day.

``inquire-price`` answers "what is this ticker's status right now", with no
business date and no history, so the only way to have a history is to ask every
day and keep the answer. That is this table (``kis_stock_status_daily``), and
the history starts the day it starts — nothing here reconstructs the past.

Design points, all of them about the one-request-per-ticker shape:

* **The key is the day we asked** (``snapshot_date`` = today, KST). A flag read
  on a holiday describes the last session, but filing it under that session
  would be a guess; ``observed_at`` carries the exact time.
* **Re-running is the retry path.** The plan is "targets minus tickers already
  stored for today", so a run that failed on 40 tickers is repaired by running
  the same command again; it spends requests only on the holes. Rows are
  inserted ``ON CONFLICT DO NOTHING`` — the first observation of a day stays.
* **Which tickers failed is recorded**, in ``ingestion_runs.params`` as
  ``failed_request_keys`` (and ``no_data_request_keys``). A sample of three error
  messages cannot say what to re-run.
* **Auth and quota failures abort**, like the KIS flow sync: every remaining
  ticker would fail the same way, and a retry can cost a token issuance, which
  notifies the account holder.
* **Rows are written in batches while running**, not at the end, so an abort
  keeps what was already fetched.
"""

from __future__ import annotations

import dataclasses
import logging
import time
from dataclasses import dataclass, field
from datetime import date

from collector.kr.domain.enums import RunStatus, RunType, UniverseScope
from collector.kr.domain.models import (
    IngestionRun,
    StockStatusRecord,
    UpsertResult,
)
from collector.kr.ports.stock_status import StockStatusProvider
from collector.kr.ports.storage import Storage
from collector.kr.service.sync_kis_flows import load_kis_flow_targets
from collector.kr.util.pipeline import (
    ConsecutiveFailureGuard,
    SourceAuthError,
    SourceBlockedError,
    SourceQuotaExhaustedError,
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    record_no_data_request_keys,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONSECUTIVE_FAILURES = 5
DEFAULT_RETRY_MAX_ATTEMPTS = 2
DEFAULT_GUARD_BACKOFF_SECONDS = 1.0
DEFAULT_FLUSH_EVERY = 100
DEFAULT_PROGRESS_LOG_EVERY_ITEMS = 200
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 30.0
# Above the whole universe, so in practice nothing is cut; the truncation count
# is still recorded because a silently shortened list reads as "that is all".
MAX_CHECKPOINT_KEYS = 20000


@dataclass(slots=True)
class KisStockStatusSyncResult:
    """Outcome of one status snapshot run."""

    snapshot_date: date | None = None
    targets: int = 0
    already_stored: int = 0
    requests_attempted: int = 0
    rows_inserted: int = 0
    no_data_requests: int = 0
    flagged_managed: int = 0
    flagged_temp_stop: int = 0
    flagged_market_warning: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    http_counts: dict[str, int] = field(default_factory=dict)
    aborted_reason: str | None = None
    upsert: UpsertResult = field(default_factory=UpsertResult)


def is_market_warning(code: str) -> bool:
    """``mrkt_warn_cls_code`` other than ``00`` means a warning grade is set."""
    return code not in ("", "00")


def sync_kis_stock_status(
    provider: StockStatusProvider,
    storage: Storage,
    *,
    snapshot_date: date,
    tickers: list[str] | None = None,
    scope: UniverseScope = UniverseScope.CURRENT,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    retry_max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS,
    guard_backoff_seconds: float = DEFAULT_GUARD_BACKOFF_SECONDS,
    flush_every: int = DEFAULT_FLUSH_EVERY,
    progress_log_every_items: int = DEFAULT_PROGRESS_LOG_EVERY_ITEMS,
    progress_log_interval_seconds: float = DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
) -> KisStockStatusSyncResult:
    """Fetch and store today's status flags for every ticker not yet stored."""
    run = IngestionRun(
        run_type=RunType.KIS_STOCK_STATUS_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "snapshot_date": snapshot_date.isoformat(),
            "tickers": tickers,
            "provider_source": provider.source().value,
            "universe_scope": scope.value,
            "max_consecutive_failures": max_consecutive_failures,
            "retry_max_attempts": retry_max_attempts,
        },
    )
    storage.record_run(run)

    result = KisStockStatusSyncResult(snapshot_date=snapshot_date)
    guard = ConsecutiveFailureGuard(
        max_consecutive_failures,
        label="KIS stock status sync",
        backoff_seconds=guard_backoff_seconds,
        logger_instance=logger,
    )
    # Outside the try so an abort can still persist them.
    no_data_keys: set[str] = set()
    failed_keys: list[str] = []
    pending: list[StockStatusRecord] = []

    def flush() -> None:
        if not pending:
            return
        upsert = storage.upsert_kis_stock_status(list(pending))
        result.upsert.inserted += upsert.inserted
        result.upsert.errors += upsert.errors
        result.rows_inserted += upsert.inserted
        pending.clear()

    try:
        targets = load_kis_flow_targets(storage, tickers, scope)
        if not targets:
            raise RuntimeError("No stocks found for the requested KIS stock status sync.")
        result.targets = len(targets)

        stored = storage.get_kis_stock_status_tickers(snapshot_date)
        todo = [stock for stock in targets if stock.ticker not in stored]
        result.already_stored = len(targets) - len(todo)
        run.params["targets"] = len(targets)
        run.params["already_stored"] = result.already_stored
        run.params["planned_requests"] = len(todo)
        logger.info(
            "KIS stock status sync started: snapshot_date=%s targets=%d already_stored=%d "
            "planned=%d",
            snapshot_date.isoformat(),
            len(targets),
            result.already_stored,
            len(todo),
        )

        started_at = time.monotonic()
        last_log_at = started_at
        last_log_processed = 0

        for processed, stock in enumerate(todo, start=1):
            request_key = stock.ticker
            result.requests_attempted += 1
            fetch = call_with_retry(
                lambda stock=stock: provider.fetch_stock_status(stock.ticker, stock.market),
                request_label=request_key,
                max_attempts=retry_max_attempts,
                logger_instance=logger,
            )

            if fetch.error:
                logger.warning("KIS stock status failed for %s: %s", request_key, fetch.error)
                result.errors[request_key] = fetch.error
                failed_keys.append(request_key)
                guard.record_failure(fetch.error)
            else:
                guard.record_success()
                if fetch.record is not None:
                    record = dataclasses.replace(fetch.record, run_id=run.run_id)
                    pending.append(record)
                    result.flagged_managed += record.mang_issu_cls_code == "Y"
                    result.flagged_temp_stop += record.temp_stop_yn == "Y"
                    result.flagged_market_warning += is_market_warning(record.mrkt_warn_cls_code)
                    if len(pending) >= flush_every:
                        flush()
                elif fetch.no_data:
                    result.no_data_requests += 1
                    no_data_keys.add(request_key)

            now = time.monotonic()
            should_log = processed == len(todo)
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
                    "KIS stock status progress: processed=%d/%d inserted=%d no_data=%d "
                    "errors=%d http_requests=%d elapsed=%.1fs current=%s",
                    processed,
                    len(todo),
                    result.rows_inserted + len(pending),
                    result.no_data_requests,
                    len(result.errors),
                    provider.request_stats().http_requests,
                    now - started_at,
                    request_key,
                )
                last_log_at = now
                last_log_processed = processed

        flush()
        result.http_counts = provider.request_stats().as_counts()
        _record_checkpoint(run, no_data_keys=no_data_keys, failed_keys=failed_keys)
        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.already_stored,
                rows_upserted=result.rows_inserted,
                no_data_requests=result.no_data_requests,
                flagged_managed=result.flagged_managed,
                flagged_temp_stop=result.flagged_temp_stop,
                flagged_market_warning=result.flagged_market_warning,
                **result.http_counts,
            ),
            errors=result.errors,
            partial_subject="KIS stock status requests",
        )
        return result
    except (SourceAuthError, SourceQuotaExhaustedError, SourceBlockedError) as exc:
        _finish_aborted(storage, run, result, provider, flush, no_data_keys, failed_keys, exc)
        return result
    except Exception as exc:
        logger.exception("KIS stock status sync failed")
        _finish_aborted(storage, run, result, provider, flush, no_data_keys, failed_keys, exc)
        result.aborted_reason = result.aborted_reason or type(exc).__name__
        return result


def _finish_aborted(storage, run, result, provider, flush, no_data_keys, failed_keys, exc) -> None:
    """Keep what was fetched, record what was left, and fail the run."""
    try:
        flush()
    except Exception:  # pragma: no cover - never mask the real error
        logger.exception("Could not flush pending status rows while aborting")
    try:
        result.http_counts = provider.request_stats().as_counts()
    except Exception:  # pragma: no cover
        logger.warning("Could not read KIS request stats while finalising the run")
    result.aborted_reason = type(exc).__name__
    run.counts = build_run_counts(
        targets_processed=result.targets,
        requests_attempted=result.requests_attempted,
        requests_skipped=result.already_stored,
        rows_upserted=result.rows_inserted,
        no_data_requests=result.no_data_requests,
        **result.http_counts,
    )
    run.params["aborted_reason"] = result.aborted_reason
    _record_checkpoint(run, no_data_keys=no_data_keys, failed_keys=failed_keys)
    logger.error("KIS stock status sync aborted (%s): %s", result.aborted_reason, exc)
    fail_run(storage, run, exc)
    result.errors["pipeline"] = str(exc)


def _record_checkpoint(
    run: IngestionRun,
    *,
    no_data_keys: set[str],
    failed_keys: list[str],
) -> None:
    record_no_data_request_keys(run, no_data_keys)
    if failed_keys:
        ordered = sorted(set(failed_keys))
        run.params["failed_request_keys"] = ordered[:MAX_CHECKPOINT_KEYS]
        dropped = max(0, len(ordered) - MAX_CHECKPOINT_KEYS)
        if dropped:
            run.params["failed_request_keys_truncated"] = dropped

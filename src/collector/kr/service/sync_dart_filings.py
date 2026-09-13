"""Use-case: Sync OpenDART disclosure-receipt history raw rows."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, UniverseScope
from collector.kr.domain.models import DartFilingReceiptSyncResult, IngestionRun
from collector.kr.ports.filing_receipt import FilingReceiptProvider
from collector.kr.ports.storage import Storage
from collector.kr.service.collection_targets import resolve_dart_targets
from collector.kr.util.pipeline import (
    OpenDartKeyExhaustedError,
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    is_opendart_daily_limit_exhausted,
    should_retry_opendart_result,
    sleep_with_jitter,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_filings(
    filing_receipt_provider: FilingReceiptProvider,
    storage: Storage,
    years: list[int],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    today: date | None = None,
    scope: UniverseScope = UniverseScope.CURRENT,
    lookback_days: int | None = None,
) -> DartFilingReceiptSyncResult:
    """Synchronise OpenDART disclosure-receipt (list.json) history.

    One request window per (corp, calendar year). Skip-if-present is keyed
    on (corp_code, year): a past year is only skipped once at least one
    receipt already exists for it, because
    ``FilingReceiptProvider.fetch_filing_receipts`` never upserts a
    partially-fetched window on failure — any stored row for a year proves
    that year's window fully succeeded on some earlier run. The current
    calendar year is always re-fetched since it keeps accumulating new
    receipts (and corrections to earlier filings can still land within it).
    ``lookback_days`` narrows only that current-year window for the scheduled
    incremental path. Past years always keep their full calendar-year window.
    """
    if lookback_days is not None and lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")

    run = IngestionRun(
        run_type=RunType.DART_FILING_RECEIPT_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "years": years,
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "universe_scope": scope.value,
            "lookback_days": lookback_days,
        },
    )
    executor = _get_executor(filing_receipt_provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartFilingReceiptSyncResult()
    today = today or now_kst().date()
    try:
        targets = resolve_dart_targets(storage, scope, tickers)
        if not targets:
            raise RuntimeError(
                "No active OpenDART corp mappings found. Run `dart sync-corp` first."
            )

        existing_years: set[tuple[str, int]]
        if force:
            existing_years = set()
        else:
            corp_codes = [corp.corp_code for corp in targets]
            existing_years = storage.get_existing_dart_filing_receipt_years(
                years=years,
                corp_codes=corp_codes,
            )

        for corp in targets:
            result.targets_processed += 1
            for year in years:
                request_label = f"{corp.ticker}:{year}:list"
                if year != today.year and (corp.corp_code, year) in existing_years:
                    logger.debug("Skipping existing filing_receipt request %s", request_label)
                    result.requests_skipped += 1
                    continue

                bgn_de = date(year, 1, 1)
                if year == today.year and lookback_days is not None:
                    bgn_de = max(bgn_de, today - timedelta(days=lookback_days))
                end_de = date(year, 12, 31) if year != today.year else today
                result.requests_attempted += 1
                fetch_result = call_with_retry(
                    lambda: filing_receipt_provider.fetch_filing_receipts(
                        corp=corp,
                        bgn_de=bgn_de,
                        end_de=end_de,
                    ),
                    request_label=request_label,
                    logger_instance=logger,
                    should_retry_result=should_retry_opendart_result,
                )
                if is_opendart_daily_limit_exhausted(fetch_result):
                    raise OpenDartKeyExhaustedError(
                        fetch_result.error or "All OpenDART API keys are temporarily rate limited."
                    )
                if fetch_result.error:
                    result.errors[request_label] = fetch_result.error
                elif fetch_result.no_data:
                    result.no_data_requests += 1
                elif fetch_result.records:
                    upsert = storage.upsert_dart_filing_receipt_raw(fetch_result.records)
                    result.upsert.updated += upsert.updated
                    result.upsert.errors += upsert.errors
                    result.rows_upserted += upsert.updated

                sleep_with_jitter(rate_limit_seconds)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="filing receipt requests",
        )
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART filing receipt sync stopped: %s", exc)
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART filing receipt sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

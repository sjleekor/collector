"""Use-case: Sync the OpenDART corporation-code master."""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, Source
from collector.kr.domain.models import DartCorp, DartCorpSyncResult, IngestionRun
from collector.kr.ports.corp_codes import CorpCodeProvider
from collector.kr.ports.storage import Storage
from collector.kr.util.pipeline import call_with_retry, should_retry_opendart_result
from collector.kr.util.time import now_kst, to_kst

logger = logging.getLogger(__name__)

# Re-download the corp master once the last real download is this many days old
# (KST calendar days).  A new listing then reaches DART collection within a
# week, and corpCode.xml costs one request per day, so refreshing is cheap.
CORP_MASTER_MAX_AGE_DAYS = 7

# If a refresh fails while the last real download is younger than this, keep the
# existing master and finish as SUCCESS so the downstream chain keeps running.
# Longer than an OpenDART maintenance window (about 3 days); beyond it the
# failure is raised loudly instead of hiding behind a stale master.
CORP_MASTER_STALE_FAIL_DAYS = 14


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def _format_corp_fetch_error(fetch_result: object) -> str:
    """Compose an error string that also captures the exhaustion reason."""
    base = getattr(fetch_result, "error", None) or "OpenDART corp fetch failed."
    reason = getattr(fetch_result, "exhaustion_reason", None)
    if reason:
        return f"{base} (exhaustion_reason={reason})"
    return base


def _zero_counts() -> dict[str, int]:
    return {
        "fetched_records": 0,
        "matched_active_tickers": 0,
        "unmatched_active_tickers": 0,
        "unmatched_dart_tickers": 0,
        "upsert_updated": 0,
        "upsert_errors": 0,
    }


def sync_dart_corp_master(
    provider: CorpCodeProvider,
    storage: Storage,
    force: bool = False,
    max_age_days: int | None = None,
    stale_fail_days: int | None = None,
) -> DartCorpSyncResult:
    """Synchronise OpenDART corp codes into local storage.

    The master is re-downloaded when the last *real* download (a SUCCESS run with
    ``fetched_records > 0``) is ``max_age_days`` or more KST calendar days old.
    Skipped runs and refresh-failed fallback runs do not count as downloads.
    """
    if max_age_days is None:
        max_age_days = CORP_MASTER_MAX_AGE_DAYS
    if stale_fail_days is None:
        stale_fail_days = CORP_MASTER_STALE_FAIL_DAYS

    run = IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "force": force,
            "max_age_days": max_age_days,
            "stale_fail_days": stale_fail_days,
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    last_real_at = None
    age_days: int | None = None
    try:
        if not force:
            last_real = storage.get_last_successful_run(
                RunType.DART_CORP_SYNC, positive_count="fetched_records"
            )
            if last_real is not None:
                last_real_at = last_real.ended_at or last_real.started_at
            if last_real_at is not None:
                age_days = (run.started_at.date() - to_kst(last_real_at).date()).days
                if age_days < max_age_days:
                    logger.info(
                        "Skipping OpenDART corp master sync; last real fetch at %s "
                        "(run_id=%s, age=%d days < max_age_days=%d). Pass --force to re-fetch.",
                        last_real_at,
                        last_real.run_id if last_real else None,
                        age_days,
                        max_age_days,
                    )
                    run.ended_at = now_kst()
                    run.status = RunStatus.SUCCESS
                    run.counts = {
                        "skipped_existing": 1,
                        **_zero_counts(),
                        "last_real_fetch_age_days": age_days,
                    }
                    if executor is not None:
                        run.counts.update(executor.snapshot_metrics())
                    storage.record_run(run)
                    return DartCorpSyncResult(
                        skipped=True,
                        last_real_fetch_at=last_real_at,
                        last_real_fetch_age_days=age_days,
                    )

        can_fall_back = (
            not force
            and last_real_at is not None
            and age_days is not None
            and age_days < stale_fail_days
        )

        try:
            fetch_result = call_with_retry(
                provider.fetch_corp_codes,
                request_label="corp_code_master",
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )
            fetch_error = _format_corp_fetch_error(fetch_result) if fetch_result.error else None
        except Exception as fetch_exc:
            if not can_fall_back:
                raise
            fetch_result = None
            fetch_error = f"{type(fetch_exc).__name__}: {fetch_exc}"
            logger.warning("OpenDART corp fetch raised: %s", fetch_error)

        if fetch_error:
            if can_fall_back:
                assert last_real_at is not None and age_days is not None
                logger.warning(
                    "OpenDART corp refresh failed; keeping master from %s "
                    "(age=%d days < stale_fail_days=%d): %s",
                    last_real_at,
                    age_days,
                    stale_fail_days,
                    fetch_error,
                )
                run.ended_at = now_kst()
                run.status = RunStatus.SUCCESS
                run.counts = {
                    **_zero_counts(),
                    "refresh_failed": 1,
                    "last_real_fetch_age_days": age_days,
                }
                if executor is not None:
                    run.counts.update(executor.snapshot_metrics())
                run.error_summary = fetch_error
                storage.record_run(run)
                return DartCorpSyncResult(
                    refresh_warning=fetch_error,
                    last_real_fetch_at=last_real_at,
                    last_real_fetch_age_days=age_days,
                )
            raise RuntimeError(fetch_error)
        assert fetch_result is not None

        active_stocks = storage.get_active_stocks()
        active_stocks_by_ticker = {stock.ticker: stock for stock in active_stocks}

        matched_active_tickers: set[str] = set()
        unmatched_dart_tickers: set[str] = set()
        enriched_records: list[DartCorp] = []

        for record in fetch_result.records:
            matched_stock = active_stocks_by_ticker.get(record.ticker or "")
            if matched_stock is None:
                if record.ticker:
                    unmatched_dart_tickers.add(record.ticker)
                enriched_records.append(record)
                continue

            matched_active_tickers.add(matched_stock.ticker)
            enriched_records.append(
                DartCorp(
                    corp_code=record.corp_code,
                    corp_name=record.corp_name,
                    ticker=matched_stock.ticker,
                    market=matched_stock.market,
                    stock_name=matched_stock.name,
                    modify_date=record.modify_date,
                    is_active=True,
                    source=Source.OPENDART,
                    fetched_at=record.fetched_at,
                )
            )

        unmatched_active_tickers = sorted(set(active_stocks_by_ticker) - matched_active_tickers)
        upsert_result = storage.upsert_dart_corp_master(enriched_records)

        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "fetched_records": len(fetch_result.records),
            "matched_active_tickers": len(matched_active_tickers),
            "unmatched_active_tickers": len(unmatched_active_tickers),
            "unmatched_dart_tickers": len(unmatched_dart_tickers),
            "upsert_updated": upsert_result.updated,
            "upsert_errors": upsert_result.errors,
        }
        if executor is not None:
            run.counts.update(executor.snapshot_metrics())
        storage.record_run(run)

        return DartCorpSyncResult(
            upsert=upsert_result,
            total_records=len(fetch_result.records),
            matched_active_tickers=len(matched_active_tickers),
            unmatched_active_tickers=unmatched_active_tickers,
            unmatched_dart_tickers=sorted(unmatched_dart_tickers),
        )
    except Exception as exc:
        logger.exception("OpenDART corp code sync failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        if executor is not None:
            run.counts = executor.snapshot_metrics()
        run.error_summary = str(exc)
        storage.record_run(run)
        return DartCorpSyncResult(error=str(exc))

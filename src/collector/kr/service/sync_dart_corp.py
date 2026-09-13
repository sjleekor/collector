"""Use-case: Sync the OpenDART corporation-code master."""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, Source
from collector.kr.domain.models import DartCorp, DartCorpSyncResult, IngestionRun
from collector.kr.ports.corp_codes import CorpCodeProvider
from collector.kr.ports.storage import Storage
from collector.kr.util.pipeline import call_with_retry, should_retry_opendart_result
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


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


def sync_dart_corp_master(
    provider: CorpCodeProvider,
    storage: Storage,
    force: bool = False,
) -> DartCorpSyncResult:
    """Synchronise OpenDART corp codes into local storage."""
    run = IngestionRun(
        run_type=RunType.DART_CORP_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={"force": force},
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    try:
        if not force:
            last_success = storage.get_last_successful_run(RunType.DART_CORP_SYNC)
            if last_success is not None:
                logger.info(
                    "Skipping OpenDART corp master sync; last success at %s (run_id=%s). "
                    "Pass --force to re-fetch.",
                    last_success.ended_at,
                    last_success.run_id,
                )
                run.ended_at = now_kst()
                run.status = RunStatus.SUCCESS
                run.counts = {
                    "skipped_existing": 1,
                    "fetched_records": 0,
                    "matched_active_tickers": 0,
                    "unmatched_active_tickers": 0,
                    "unmatched_dart_tickers": 0,
                    "upsert_updated": 0,
                    "upsert_errors": 0,
                }
                if executor is not None:
                    run.counts.update(executor.snapshot_metrics())
                storage.record_run(run)
                return DartCorpSyncResult()

        fetch_result = call_with_retry(
            provider.fetch_corp_codes,
            request_label="corp_code_master",
            logger_instance=logger,
            should_retry_result=should_retry_opendart_result,
        )
        if fetch_result.error:
            raise RuntimeError(_format_corp_fetch_error(fetch_result))

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

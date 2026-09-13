"""Use-case: Sync OpenDART financial-statement raw rows."""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import DartFinancialSyncResult, IngestionRun
from collector.kr.ports.financials import FinancialStatementProvider
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
from collector.kr.util.slice_ledger import DEFAULT_NO_DATA_TTL_DAYS, SliceLedger
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

LEDGER_ENDPOINT = "fnlttSinglAcntAll"
LEDGER_FLUSH_EVERY = 200


def _slice_key(corp_code: str, bsns_year: int, reprt_code: str, fs_div: str) -> str:
    """Return the stable ledger key for one financial-statement request."""
    return f"{corp_code}:{bsns_year}:{reprt_code}:{fs_div}"


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_financial_statements(
    provider: FinancialStatementProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    fs_divs: list[str],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    allowed_year_report_pairs: set[tuple[int, str]] | None = None,
    skip_request_keys: set[str] | None = None,
    run_params_extra: dict[str, object] | None = None,
    scope: UniverseScope = UniverseScope.CURRENT,
) -> DartFinancialSyncResult:
    """Synchronise OpenDART financial raw rows into local storage."""
    no_data_ttl_days = (
        None if scope is UniverseScope.HISTORICAL else DEFAULT_NO_DATA_TTL_DAYS
    )
    run = IngestionRun(
        run_type=RunType.DART_FINANCIAL_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
            "fs_divs": fs_divs,
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "universe_scope": scope.value,
            "slice_ledger_endpoint": LEDGER_ENDPOINT,
            "no_data_ttl_days": no_data_ttl_days,
            "allowed_year_report_pairs": (
                [f"{year}:{code}" for year, code in sorted(allowed_year_report_pairs)]
                if allowed_year_report_pairs is not None
                else None
            ),
            **(run_params_extra or {}),
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartFinancialSyncResult()
    no_data_request_keys: list[str] = []
    skip_request_keys = set() if force else (skip_request_keys or set())
    ledger = SliceLedger(
        storage,
        source=Source.OPENDART,
        endpoint=LEDGER_ENDPOINT,
        no_data_ttl_days=no_data_ttl_days,
    )
    ledger_pending: set[str] = set()
    slices_skipped_no_data = 0

    try:
        targets = resolve_dart_targets(storage, scope, tickers)
        if not targets:
            raise RuntimeError(
                "No active OpenDART corp mappings found. Run `dart sync-corp` first."
            )

        existing_keys: set[tuple[str, int, str, str]]
        if force:
            existing_keys = set()
        else:
            existing_keys = storage.get_existing_dart_financial_statement_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                fs_divs=fs_divs,
                corp_codes=[corp.corp_code for corp in targets],
            )

        ledger_keys = [
            _slice_key(corp.corp_code, bsns_year, reprt_code, fs_div)
            for corp in targets
            for bsns_year in bsns_years
            for reprt_code in reprt_codes
            if allowed_year_report_pairs is None
            or (bsns_year, reprt_code) in allowed_year_report_pairs
            for fs_div in fs_divs
        ]
        ledger_plan = ledger.plan(ledger_keys, force=force)
        ledger_pending = set(ledger_plan.pending)
        slices_skipped_no_data = len(ledger_plan.skipped_no_data)

        for corp in targets:
            result.targets_processed += 1

            for bsns_year in bsns_years:
                for reprt_code in reprt_codes:
                    if (
                        allowed_year_report_pairs is not None
                        and (bsns_year, reprt_code) not in allowed_year_report_pairs
                    ):
                        result.requests_skipped += len(fs_divs)
                        continue
                    for fs_div in fs_divs:
                        request_key = f"{corp.ticker}:{bsns_year}:{reprt_code}:{fs_div}"
                        ledger_key = _slice_key(
                            corp.corp_code, bsns_year, reprt_code, fs_div
                        )
                        if ledger_key not in ledger_pending:
                            logger.debug(
                                "Skipping ledger-complete financial request %s", request_key
                            )
                            result.requests_skipped += 1
                            continue
                        if request_key in skip_request_keys:
                            logger.debug(
                                "Skipping negative-cached financial request %s", request_key
                            )
                            result.requests_skipped += 1
                            continue
                        if (corp.corp_code, bsns_year, reprt_code, fs_div) in existing_keys:
                            logger.debug("Skipping existing financial request %s", request_key)
                            result.requests_skipped += 1
                            continue

                        result.requests_attempted += 1
                        fetch_result = call_with_retry(
                            lambda: provider.fetch_financial_statement(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                                fs_div=fs_div,
                            ),
                            request_label=request_key,
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )

                        if is_opendart_daily_limit_exhausted(fetch_result):
                            raise OpenDartKeyExhaustedError(
                                fetch_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if fetch_result.error:
                            logger.warning(
                                "Financial sync failed for %s: %s", request_key, fetch_result.error
                            )
                            result.errors[request_key] = fetch_result.error
                        elif fetch_result.no_data:
                            result.no_data_requests += 1
                            no_data_request_keys.append(request_key)
                            ledger.record_no_data(ledger_key)
                        elif fetch_result.records:
                            upsert_result = storage.upsert_dart_financial_statement_raw(
                                fetch_result.records
                            )
                            result.upsert.updated += upsert_result.updated
                            result.upsert.errors += upsert_result.errors
                            result.rows_upserted += upsert_result.updated
                        else:
                            # A successful response with no rows is the same
                            # durable no-data verdict as OpenDART status 013.
                            result.no_data_requests += 1
                            no_data_request_keys.append(request_key)
                            ledger.record_no_data(ledger_key)

                        if ledger.pending_write_count >= LEDGER_FLUSH_EVERY:
                            ledger.flush()

                        sleep_with_jitter(rate_limit_seconds)

        ledger.flush()
        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
                slices_skipped_no_data=slices_skipped_no_data,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="financial sync requests",
        )
        if no_data_request_keys:
            run.params["no_data_request_keys"] = no_data_request_keys[:1000]
            storage.record_run(run)
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART financial sync stopped: %s", exc)
        ledger.flush()
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART financial sync failed")
        ledger.flush()
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

"""Use-case: Sync OpenDART share-count and shareholder-return raw rows."""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import DartShareInfoSyncResult, IngestionRun
from collector.kr.ports.share_info import (
    CapitalChangeProvider,
    ShareCountProvider,
    ShareholderReturnProvider,
)
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

LEDGER_ENDPOINTS = {
    "share_count": "stockTotqySttus",
    "dividend": "alotMatter",
    "treasury_stock": "tesstkAcqsDspsSttus",
    "capital_change": "irdsSttus",
}
LEDGER_FLUSH_EVERY = 200


def _slice_key(corp_code: str, bsns_year: int, reprt_code: str) -> str:
    """Return the stable ledger key shared by the four DS002 endpoints."""
    return f"{corp_code}:{bsns_year}:{reprt_code}"


def _flush_ledgers(ledgers: dict[str, SliceLedger]) -> None:
    for ledger in ledgers.values():
        ledger.flush()


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_share_info(
    share_count_provider: ShareCountProvider,
    shareholder_return_provider: ShareholderReturnProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    allowed_year_report_pairs: set[tuple[int, str]] | None = None,
    skip_request_keys: set[str] | None = None,
    run_params_extra: dict[str, object] | None = None,
    capital_change_provider: CapitalChangeProvider | None = None,
    scope: UniverseScope = UniverseScope.CURRENT,
) -> DartShareInfoSyncResult:
    """Synchronise OpenDART share-count/dividend/treasury-stock raw rows.

    ``capital_change_provider`` is optional: when omitted, the irdsSttus
    (증자·감자 현황) request is skipped entirely so existing callers that
    only need share-count/shareholder-return keep working unchanged.
    """
    no_data_ttl_days = (
        None if scope is UniverseScope.HISTORICAL else DEFAULT_NO_DATA_TTL_DAYS
    )
    ledger_kinds = ["share_count", "dividend", "treasury_stock"]
    if capital_change_provider is not None:
        ledger_kinds.append("capital_change")
    run = IngestionRun(
        run_type=RunType.DART_SHARE_INFO_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "universe_scope": scope.value,
            "slice_ledger_endpoints": [LEDGER_ENDPOINTS[kind] for kind in ledger_kinds],
            "no_data_ttl_days": no_data_ttl_days,
            "allowed_year_report_pairs": (
                [f"{year}:{code}" for year, code in sorted(allowed_year_report_pairs)]
                if allowed_year_report_pairs is not None
                else None
            ),
            **(run_params_extra or {}),
        },
    )
    executor = (
        _get_executor(share_count_provider)
        or _get_executor(shareholder_return_provider)
        or _get_executor(capital_change_provider)
    )
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartShareInfoSyncResult()
    no_data_request_keys: list[str] = []
    skip_request_keys = set() if force else (skip_request_keys or set())
    ledgers = {
        kind: SliceLedger(
            storage,
            source=Source.OPENDART,
            endpoint=LEDGER_ENDPOINTS[kind],
            no_data_ttl_days=no_data_ttl_days,
        )
        for kind in ledger_kinds
    }
    ledger_pending: dict[str, set[str]] = {kind: set() for kind in ledger_kinds}
    slices_skipped_no_data = 0
    try:
        targets = resolve_dart_targets(storage, scope, tickers)
        if not targets:
            raise RuntimeError(
                "No active OpenDART corp mappings found. Run `dart sync-corp` first."
            )

        existing_share_count_keys: set[tuple[str, int, str]]
        existing_return_keys: set[tuple[str, int, str, str]]
        existing_capital_change_keys: set[tuple[str, int, str]]
        if force:
            existing_share_count_keys = set()
            existing_return_keys = set()
            existing_capital_change_keys = set()
        else:
            corp_codes = [corp.corp_code for corp in targets]
            existing_share_count_keys = storage.get_existing_dart_share_count_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=corp_codes,
            )
            existing_return_keys = storage.get_existing_dart_shareholder_return_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=corp_codes,
            )
            existing_capital_change_keys = (
                storage.get_existing_dart_capital_change_keys(
                    bsns_years=bsns_years,
                    reprt_codes=reprt_codes,
                    corp_codes=corp_codes,
                )
                if capital_change_provider is not None
                else set()
            )

        ledger_keys = [
            _slice_key(corp.corp_code, bsns_year, reprt_code)
            for corp in targets
            for bsns_year in bsns_years
            for reprt_code in reprt_codes
            if allowed_year_report_pairs is None
            or (bsns_year, reprt_code) in allowed_year_report_pairs
        ]
        for kind, ledger in ledgers.items():
            plan = ledger.plan(ledger_keys, force=force)
            ledger_pending[kind] = set(plan.pending)
            slices_skipped_no_data += len(plan.skipped_no_data)

        for corp in targets:
            result.targets_processed += 1
            for bsns_year in bsns_years:
                for reprt_code in reprt_codes:
                    if (
                        allowed_year_report_pairs is not None
                        and (bsns_year, reprt_code) not in allowed_year_report_pairs
                    ):
                        result.requests_skipped += len(ledger_kinds)
                        continue
                    request_prefix = f"{corp.ticker}:{bsns_year}:{reprt_code}"
                    ledger_key = _slice_key(corp.corp_code, bsns_year, reprt_code)
                    attempted_any = False

                    share_count_key = f"{request_prefix}:share_count"
                    if ledger_key not in ledger_pending["share_count"]:
                        logger.debug(
                            "Skipping ledger-complete share_count request %s", request_prefix
                        )
                        result.requests_skipped += 1
                    elif (corp.corp_code, bsns_year, reprt_code) in existing_share_count_keys:
                        logger.debug("Skipping existing share_count request %s", request_prefix)
                        result.requests_skipped += 1
                    elif share_count_key in skip_request_keys:
                        logger.debug(
                            "Skipping negative-cached share_count request %s", request_prefix
                        )
                        result.requests_skipped += 1
                    else:
                        result.requests_attempted += 1
                        attempted_any = True
                        share_count_result = call_with_retry(
                            lambda: share_count_provider.fetch_share_count(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                            ),
                            request_label=f"{request_prefix}:share_count",
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )
                        if is_opendart_daily_limit_exhausted(share_count_result):
                            raise OpenDartKeyExhaustedError(
                                share_count_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if share_count_result.error:
                            result.errors[f"{request_prefix}:share_count"] = (
                                share_count_result.error
                            )
                        elif share_count_result.no_data:
                            result.no_data_requests += 1
                            no_data_request_keys.append(share_count_key)
                            ledgers["share_count"].record_no_data(ledger_key)
                        elif share_count_result.records:
                            upsert = storage.upsert_dart_share_count_raw(share_count_result.records)
                            result.share_count_upsert.updated += upsert.updated
                            result.share_count_upsert.errors += upsert.errors
                            result.share_count_rows_upserted += upsert.updated
                        else:
                            result.no_data_requests += 1
                            no_data_request_keys.append(share_count_key)
                            ledgers["share_count"].record_no_data(ledger_key)

                    dividend_key = f"{request_prefix}:dividend"
                    if ledger_key not in ledger_pending["dividend"]:
                        logger.debug(
                            "Skipping ledger-complete dividend request %s", request_prefix
                        )
                        result.requests_skipped += 1
                    elif (
                        corp.corp_code,
                        bsns_year,
                        reprt_code,
                        "dividend",
                    ) in existing_return_keys:
                        logger.debug("Skipping existing dividend request %s", request_prefix)
                        result.requests_skipped += 1
                    elif dividend_key in skip_request_keys:
                        logger.debug("Skipping negative-cached dividend request %s", request_prefix)
                        result.requests_skipped += 1
                    else:
                        result.requests_attempted += 1
                        attempted_any = True
                        dividend_result = call_with_retry(
                            lambda: shareholder_return_provider.fetch_dividend(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                            ),
                            request_label=f"{request_prefix}:dividend",
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )
                        if is_opendart_daily_limit_exhausted(dividend_result):
                            raise OpenDartKeyExhaustedError(
                                dividend_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if dividend_result.error:
                            result.errors[f"{request_prefix}:dividend"] = dividend_result.error
                        elif dividend_result.no_data:
                            result.no_data_requests += 1
                            no_data_request_keys.append(dividend_key)
                            ledgers["dividend"].record_no_data(ledger_key)
                        elif dividend_result.records:
                            upsert = storage.upsert_dart_shareholder_return_raw(
                                dividend_result.records
                            )
                            result.shareholder_return_upsert.updated += upsert.updated
                            result.shareholder_return_upsert.errors += upsert.errors
                            result.shareholder_return_rows_upserted += upsert.updated
                        else:
                            result.no_data_requests += 1
                            no_data_request_keys.append(dividend_key)
                            ledgers["dividend"].record_no_data(ledger_key)

                    treasury_key = f"{request_prefix}:treasury_stock"
                    if ledger_key not in ledger_pending["treasury_stock"]:
                        logger.debug(
                            "Skipping ledger-complete treasury_stock request %s",
                            request_prefix,
                        )
                        result.requests_skipped += 1
                    elif (
                        corp.corp_code,
                        bsns_year,
                        reprt_code,
                        "treasury_stock",
                    ) in existing_return_keys:
                        logger.debug("Skipping existing treasury_stock request %s", request_prefix)
                        result.requests_skipped += 1
                    elif treasury_key in skip_request_keys:
                        logger.debug(
                            "Skipping negative-cached treasury_stock request %s", request_prefix
                        )
                        result.requests_skipped += 1
                    else:
                        result.requests_attempted += 1
                        attempted_any = True
                        treasury_result = call_with_retry(
                            lambda: shareholder_return_provider.fetch_treasury_stock(
                                corp=corp,
                                bsns_year=bsns_year,
                                reprt_code=reprt_code,
                            ),
                            request_label=f"{request_prefix}:treasury_stock",
                            logger_instance=logger,
                            should_retry_result=should_retry_opendart_result,
                        )
                        if is_opendart_daily_limit_exhausted(treasury_result):
                            raise OpenDartKeyExhaustedError(
                                treasury_result.error
                                or "All OpenDART API keys are temporarily rate limited."
                            )
                        if treasury_result.error:
                            result.errors[f"{request_prefix}:treasury_stock"] = (
                                treasury_result.error
                            )
                        elif treasury_result.no_data:
                            result.no_data_requests += 1
                            no_data_request_keys.append(treasury_key)
                            ledgers["treasury_stock"].record_no_data(ledger_key)
                        elif treasury_result.records:
                            upsert = storage.upsert_dart_shareholder_return_raw(
                                treasury_result.records
                            )
                            result.shareholder_return_upsert.updated += upsert.updated
                            result.shareholder_return_upsert.errors += upsert.errors
                            result.shareholder_return_rows_upserted += upsert.updated
                        else:
                            result.no_data_requests += 1
                            no_data_request_keys.append(treasury_key)
                            ledgers["treasury_stock"].record_no_data(ledger_key)

                    if capital_change_provider is not None:
                        capital_change_key = f"{request_prefix}:capital_change"
                        if ledger_key not in ledger_pending["capital_change"]:
                            logger.debug(
                                "Skipping ledger-complete capital_change request %s",
                                request_prefix,
                            )
                            result.requests_skipped += 1
                        elif (
                            corp.corp_code,
                            bsns_year,
                            reprt_code,
                        ) in existing_capital_change_keys:
                            logger.debug(
                                "Skipping existing capital_change request %s", request_prefix
                            )
                            result.requests_skipped += 1
                        elif capital_change_key in skip_request_keys:
                            logger.debug(
                                "Skipping negative-cached capital_change request %s",
                                request_prefix,
                            )
                            result.requests_skipped += 1
                        else:
                            result.requests_attempted += 1
                            attempted_any = True
                            capital_change_result = call_with_retry(
                                lambda: capital_change_provider.fetch_capital_change(
                                    corp=corp,
                                    bsns_year=bsns_year,
                                    reprt_code=reprt_code,
                                ),
                                request_label=f"{request_prefix}:capital_change",
                                logger_instance=logger,
                                should_retry_result=should_retry_opendart_result,
                            )
                            if is_opendart_daily_limit_exhausted(capital_change_result):
                                raise OpenDartKeyExhaustedError(
                                    capital_change_result.error
                                    or "All OpenDART API keys are temporarily rate limited."
                                )
                            if capital_change_result.error:
                                result.errors[f"{request_prefix}:capital_change"] = (
                                    capital_change_result.error
                                )
                            elif capital_change_result.no_data:
                                result.no_data_requests += 1
                                no_data_request_keys.append(capital_change_key)
                                ledgers["capital_change"].record_no_data(ledger_key)
                            elif capital_change_result.records:
                                upsert = storage.upsert_dart_capital_change_raw(
                                    capital_change_result.records
                                )
                                result.capital_change_upsert.updated += upsert.updated
                                result.capital_change_upsert.errors += upsert.errors
                                result.capital_change_rows_upserted += upsert.updated
                            else:
                                result.no_data_requests += 1
                                no_data_request_keys.append(capital_change_key)
                                ledgers["capital_change"].record_no_data(ledger_key)

                    if (
                        sum(ledger.pending_write_count for ledger in ledgers.values())
                        >= LEDGER_FLUSH_EVERY
                    ):
                        _flush_ledgers(ledgers)

                    if attempted_any:
                        sleep_with_jitter(rate_limit_seconds)

        _flush_ledgers(ledgers)
        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                share_count_rows_upserted=result.share_count_rows_upserted,
                shareholder_return_rows_upserted=result.shareholder_return_rows_upserted,
                capital_change_rows_upserted=result.capital_change_rows_upserted,
                no_data_requests=result.no_data_requests,
                slices_skipped_no_data=slices_skipped_no_data,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="share info requests",
        )
        if no_data_request_keys:
            run.params["no_data_request_keys"] = no_data_request_keys[:1000]
            storage.record_run(run)
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART share info sync stopped: %s", exc)
        _flush_ledgers(ledgers)
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART share info sync failed")
        _flush_ledgers(ledgers)
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

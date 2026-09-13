"""Use-case: Sync parsed OpenDART XBRL documents and fact rows."""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import DartXbrlSyncResult, IngestionRun, XbrlBackfillTarget
from collector.kr.ports.storage import Storage
from collector.kr.ports.xbrl import XbrlProvider
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

LEDGER_ENDPOINT = "fnlttXbrl"
LEDGER_FLUSH_EVERY = 200


def _slice_key(
    corp_code: str, bsns_year: int, reprt_code: str, rcept_no: str
) -> str:
    """Return the stable ledger key for one XBRL receipt request."""
    return f"{corp_code}:{bsns_year}:{reprt_code}:{rcept_no}"


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_xbrl(
    provider: XbrlProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    allowed_year_report_pairs: set[tuple[int, str]] | None = None,
    skip_request_keys: set[str] | None = None,
    run_params_extra: dict[str, object] | None = None,
    scope: UniverseScope = UniverseScope.CURRENT,
) -> DartXbrlSyncResult:
    """Synchronise parsed XBRL ZIP data for filings already present in financial raw."""
    no_data_ttl_days = (
        None if scope is UniverseScope.HISTORICAL else DEFAULT_NO_DATA_TTL_DAYS
    )
    run = IngestionRun(
        run_type=RunType.XBRL_PARSE,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
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

    result = DartXbrlSyncResult()
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
        corp_rows = resolve_dart_targets(storage, scope, tickers)
        corp_by_ticker = {corp.ticker: corp for corp in corp_rows if corp.ticker}
        if not corp_by_ticker:
            raise RuntimeError(
                "No active OpenDART corp mappings found. Run `dart sync-corp` first."
            )

        financial_rows = storage.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
        request_targets: dict[tuple[str, int, str, str], tuple[str, int, str, str]] = {}
        for row in financial_rows:
            if not row.ticker or not row.rcept_no:
                continue
            if (
                allowed_year_report_pairs is not None
                and (row.bsns_year, row.reprt_code) not in allowed_year_report_pairs
            ):
                continue
            key = (row.ticker, row.bsns_year, row.reprt_code, row.rcept_no)
            request_targets.setdefault(key, key)

        if not request_targets:
            raise RuntimeError(
                "No financial raw rows with rcept_no found. Run `dart sync-financials` first."
            )

        existing_doc_keys: set[tuple[str, int, str, str]]
        if force:
            existing_doc_keys = set()
        else:
            existing_doc_keys = storage.get_existing_dart_xbrl_document_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=[corp.corp_code for corp in corp_by_ticker.values()],
            )

        ledger_keys = [
            _slice_key(corp.corp_code, bsns_year, reprt_code, rcept_no)
            for ticker, bsns_year, reprt_code, rcept_no in request_targets.values()
            if (corp := corp_by_ticker.get(ticker)) is not None
        ]
        ledger_plan = ledger.plan(ledger_keys, force=force)
        ledger_pending = set(ledger_plan.pending)
        slices_skipped_no_data = len(ledger_plan.skipped_no_data)

        result.targets_processed = len(request_targets)
        for ticker, bsns_year, reprt_code, rcept_no in request_targets.values():
            corp = corp_by_ticker.get(ticker)
            if corp is None:
                continue

            request_key = f"{ticker}:{bsns_year}:{reprt_code}:{rcept_no}"
            ledger_key = _slice_key(corp.corp_code, bsns_year, reprt_code, rcept_no)
            if ledger_key not in ledger_pending:
                logger.debug("Skipping ledger-complete XBRL document %s", request_key)
                result.requests_skipped += 1
                continue
            if (corp.corp_code, bsns_year, reprt_code, rcept_no) in existing_doc_keys:
                logger.debug("Skipping existing XBRL document %s", request_key)
                result.requests_skipped += 1
                continue
            if request_key in skip_request_keys:
                logger.debug("Skipping negative-cached XBRL document %s", request_key)
                result.requests_skipped += 1
                continue

            result.requests_attempted += 1
            fetch_result = call_with_retry(
                lambda: provider.fetch_xbrl(
                    corp=corp,
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no=rcept_no,
                ),
                request_label=request_key,
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )

            if is_opendart_daily_limit_exhausted(fetch_result):
                raise OpenDartKeyExhaustedError(
                    fetch_result.error or "All OpenDART API keys are temporarily rate limited."
                )
            if fetch_result.error:
                logger.warning("XBRL sync failed for %s: %s", request_key, fetch_result.error)
                result.errors[request_key] = fetch_result.error
            elif fetch_result.no_data:
                result.no_data_requests += 1
                no_data_request_keys.append(request_key)
                ledger.record_no_data(ledger_key)
            else:
                if fetch_result.document is not None:
                    upsert_document = storage.upsert_dart_xbrl_documents([fetch_result.document])
                    result.document_upsert.updated += upsert_document.updated
                    result.document_upsert.errors += upsert_document.errors
                    result.documents_upserted += upsert_document.updated

                if fetch_result.facts:
                    upsert_facts = storage.upsert_dart_xbrl_fact_raw(fetch_result.facts)
                    result.fact_upsert.updated += upsert_facts.updated
                    result.fact_upsert.errors += upsert_facts.errors
                    result.facts_upserted += upsert_facts.updated

                if fetch_result.document is None and not fetch_result.facts:
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
                documents_upserted=result.documents_upserted,
                facts_upserted=result.facts_upserted,
                no_data_requests=result.no_data_requests,
                slices_skipped_no_data=slices_skipped_no_data,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="XBRL sync requests",
        )
        if no_data_request_keys:
            run.params["no_data_request_keys"] = no_data_request_keys[:1000]
            storage.record_run(run)
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART XBRL sync stopped: %s", exc)
        ledger.flush()
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART XBRL sync failed")
        ledger.flush()
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def sync_dart_xbrl_receipt_targeted(
    provider: XbrlProvider,
    storage: Storage,
    targets: list[XbrlBackfillTarget],
    rate_limit_seconds: float = 0.2,
    force: bool = False,
) -> DartXbrlSyncResult:
    """Backfill XBRL for an explicit list of (corp, filing, receipt) targets.

    Unlike ``sync_dart_xbrl`` (which derives its targets from whichever
    ``rcept_no`` is already captured in ``dart_financial_statement_raw``),
    this fetches exactly the ``rcept_no`` values the caller supplies — e.g.
    an original filing's receipt discovered via ``dart_filing_receipt_raw``
    that differs from the currently captured vintage (Phase B §3.5). Target
    discovery itself (which receipts are missing/original) is not performed
    here; it depends on real receipt-history data and is left to the caller.
    """
    run = IngestionRun(
        run_type=RunType.XBRL_RECEIPT_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "target_count": len(targets),
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "slice_ledger_endpoint": LEDGER_ENDPOINT,
            "no_data_ttl_days": None,
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartXbrlSyncResult()
    ledger = SliceLedger(
        storage,
        source=Source.OPENDART,
        endpoint=LEDGER_ENDPOINT,
        no_data_ttl_days=None,
    )
    ledger_pending: set[str] = set()
    slices_skipped_no_data = 0
    try:
        if not targets:
            raise RuntimeError("No receipt-targeted XBRL backfill targets were supplied.")

        # HISTORICAL: the caller supplies explicit receipts, and a receipt can
        # belong to a corp that has since delisted. Resolving the lookup against
        # the current universe would silently drop those targets.
        corp_rows = resolve_dart_targets(storage, UniverseScope.HISTORICAL)
        corp_by_ticker = {corp.ticker: corp for corp in corp_rows if corp.ticker}

        bsns_years = sorted({target.bsns_year for target in targets})
        reprt_codes = sorted({target.reprt_code for target in targets})
        existing_doc_keys: set[tuple[str, int, str, str]]
        if force:
            existing_doc_keys = set()
        else:
            existing_doc_keys = storage.get_existing_dart_xbrl_document_keys(
                bsns_years=bsns_years,
                reprt_codes=reprt_codes,
                corp_codes=[target.corp_code for target in targets],
            )

        ledger_keys = [
            _slice_key(
                target.corp_code,
                target.bsns_year,
                target.reprt_code,
                target.rcept_no,
            )
            for target in targets
        ]
        ledger_plan = ledger.plan(ledger_keys, force=force)
        ledger_pending = set(ledger_plan.pending)
        slices_skipped_no_data = len(ledger_plan.skipped_no_data)

        result.targets_processed = len(targets)
        for target in targets:
            corp = corp_by_ticker.get(target.ticker)
            if corp is None:
                result.errors[
                    f"{target.ticker}:{target.bsns_year}:{target.reprt_code}:{target.rcept_no}"
                ] = "No active OpenDART corp mapping for this ticker."
                continue

            request_key = (
                f"{target.ticker}:{target.bsns_year}:{target.reprt_code}:{target.rcept_no}"
            )
            ledger_key = _slice_key(
                target.corp_code,
                target.bsns_year,
                target.reprt_code,
                target.rcept_no,
            )
            if ledger_key not in ledger_pending:
                logger.debug("Skipping ledger-complete XBRL receipt %s", request_key)
                result.requests_skipped += 1
                continue
            if (
                corp.corp_code,
                target.bsns_year,
                target.reprt_code,
                target.rcept_no,
            ) in existing_doc_keys:
                logger.debug("Skipping already-backfilled XBRL receipt %s", request_key)
                result.requests_skipped += 1
                continue

            result.requests_attempted += 1
            fetch_result = call_with_retry(
                lambda: provider.fetch_xbrl(
                    corp=corp,
                    bsns_year=target.bsns_year,
                    reprt_code=target.reprt_code,
                    rcept_no=target.rcept_no,
                ),
                request_label=request_key,
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )

            if is_opendart_daily_limit_exhausted(fetch_result):
                raise OpenDartKeyExhaustedError(
                    fetch_result.error or "All OpenDART API keys are temporarily rate limited."
                )
            if fetch_result.error:
                logger.warning(
                    "XBRL receipt backfill failed for %s: %s", request_key, fetch_result.error
                )
                result.errors[request_key] = fetch_result.error
            elif fetch_result.no_data:
                result.no_data_requests += 1
                ledger.record_no_data(ledger_key)
            else:
                if fetch_result.document is not None:
                    upsert_document = storage.upsert_dart_xbrl_documents([fetch_result.document])
                    result.document_upsert.updated += upsert_document.updated
                    result.document_upsert.errors += upsert_document.errors
                    result.documents_upserted += upsert_document.updated

                if fetch_result.facts:
                    upsert_facts = storage.upsert_dart_xbrl_fact_raw(fetch_result.facts)
                    result.fact_upsert.updated += upsert_facts.updated
                    result.fact_upsert.errors += upsert_facts.errors
                    result.facts_upserted += upsert_facts.updated

                if fetch_result.document is None and not fetch_result.facts:
                    result.no_data_requests += 1
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
                documents_upserted=result.documents_upserted,
                facts_upserted=result.facts_upserted,
                no_data_requests=result.no_data_requests,
                slices_skipped_no_data=slices_skipped_no_data,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="XBRL receipt-targeted backfill requests",
        )
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART XBRL receipt-targeted backfill stopped: %s", exc)
        ledger.flush()
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART XBRL receipt-targeted backfill failed")
        ledger.flush()
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

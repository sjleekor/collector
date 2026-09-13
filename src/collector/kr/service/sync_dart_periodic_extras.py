"""Use-case: Sync OpenDART DS002 periodic-report extras (N6).

This is the first collector to resume from ``collection_slice_state`` (L-1)
rather than from ``ingestion_runs.params``, and it is the reason that ledger
exists. The backfill is ~83,700 calls over about a day and a half, spanning
several OpenDART daily-limit stops. The old mechanism cannot survive that: the
no-data tombstone list is capped per run and only recent runs are read back, so
a job spread over three days cannot reconstruct its own progress.

Two response shapes cut the call count from 162,000 to 83,700, and both are
expressed here as year arithmetic rather than as extra requests:

* ``hyslrChgSttus`` returns the *accumulated* change history — a 2023 request
  came back with changes going to 2019 — so 12 years cost 3 requests, not 12.
  ``change_on`` in the payload keeps each event's date exact, which is why
  annual collection is enough for a transition feature.
* the audit-opinion endpoint returns 당기/전기/전전기 in one response, so 12
  years cost 6 requests.

Each step is the measured span minus one, so consecutive requests overlap by a
year and no boundary year can fall between two of them.
"""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import (
    PeriodicExtraStatement,
    RunStatus,
    RunType,
    Source,
    UniverseScope,
)
from collector.kr.domain.models import DartPeriodicExtrasSyncResult, IngestionRun
from collector.kr.ports.periodic_extras import PeriodicExtrasProvider
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
from collector.kr.util.slice_ledger import SliceLedger
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

#: Endpoint name of the ledger, shared by every statement type because the
#: slice key already carries the statement.
LEDGER_ENDPOINT = "dart_periodic_extras"

#: How many business years one response covers, measured in the N6 PoC.
#: The value is the *step* between requested years, one less than the measured
#: span, so a boundary year can never fall between two requests.
YEAR_STEP: dict[PeriodicExtraStatement, int] = {
    # One year per response.
    PeriodicExtraStatement.EMPLOYEE: 1,
    PeriodicExtraStatement.EXECUTIVE: 1,
    PeriodicExtraStatement.MAJOR_SHAREHOLDER: 1,
    # Accumulated history: a 2023 request returned changes back to 2019.
    PeriodicExtraStatement.MAJOR_CHANGE: 4,
    # 당기 / 전기 / 전전기 in one response.
    PeriodicExtraStatement.AUDIT_OPINION: 2,
}

#: What ``dart sync-periodic-extras`` collects unless told otherwise.
#: ``EXECUTIVE`` is deliberately absent: 32,400 of the 83,700 calls for one
#: thin candidate feature that ``MAJOR_CHANGE`` covers better (PoC §3③).
DEFAULT_STATEMENTS: tuple[PeriodicExtraStatement, ...] = (
    PeriodicExtraStatement.EMPLOYEE,
    PeriodicExtraStatement.MAJOR_SHAREHOLDER,
    PeriodicExtraStatement.MAJOR_CHANGE,
    PeriodicExtraStatement.AUDIT_OPINION,
)


def resolve_target_years(
    statement_type: PeriodicExtraStatement,
    bsns_years: list[int],
) -> list[int]:
    """Thin *bsns_years* down to the requests that actually add coverage.

    Args:
        statement_type: Which disclosure is being collected.
        bsns_years: Every year the caller wants covered.

    Returns:
        The subset to request, newest first. Newest first matters: the most
        recent request is the one that covers the years just before it, so a
        run cut short by the daily key limit has still covered the recent end
        rather than a scatter of old years.
    """
    if not bsns_years:
        return []
    step = max(1, YEAR_STEP.get(statement_type, 1))
    ordered = sorted(set(bsns_years), reverse=True)
    if step == 1:
        return ordered

    # Greedy from the newest year. A request for Y answers for Y-step..Y, so
    # the next request is the first year that has fallen out of that window.
    # The oldest wanted year needs no special case: if it were uncovered, the
    # loop would already have selected it on the way past.
    selected: list[int] = []
    for year in ordered:
        if not selected or selected[-1] - year >= step:
            selected.append(year)
    return selected


def slice_key(corp_code: str, bsns_year: int, reprt_code: str, statement: str) -> str:
    """Build the ledger key for one request."""
    return f"{corp_code}:{bsns_year}:{reprt_code}:{statement}"


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_dart_periodic_extras(
    provider: PeriodicExtrasProvider,
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    statements: list[PeriodicExtraStatement] | None = None,
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    scope: UniverseScope = UniverseScope.HISTORICAL,
    flush_every: int = 200,
    no_data_ttl_days: int | None = None,
) -> DartPeriodicExtrasSyncResult:
    """Synchronise DS002 periodic-report extras into the two raw tables.

    Args:
        provider: DS002 provider.
        storage: Raw storage, also holding the slice ledger.
        bsns_years: Business years to cover. Thinned per statement type.
        reprt_codes: Report codes; normally just ``11011``.
        statements: Which disclosures to collect. Defaults to
            :data:`DEFAULT_STATEMENTS`.
        tickers: Optional ticker allowlist.
        rate_limit_seconds: Pause between requests.
        force: Re-collect everything, ignoring the ledger. The ledger is still
            written, so a forced run repairs stale rows.
        scope: Which universe to target. Defaults to ``HISTORICAL`` — the
            delisted companies are the ones an adverse audit opinion is about,
            so collecting only current listings would bias the very feature
            this package exists for.
        flush_every: How many ledger writes to batch before flushing. Flushing
            periodically is the point: the ledger has to survive a run that
            stops on the daily key limit.
        no_data_ttl_days: Override the ledger's no-data expiry.

    Returns:
        ``DartPeriodicExtrasSyncResult``. Never raises for an upstream failure.
    """
    statements = list(statements or DEFAULT_STATEMENTS)
    run = IngestionRun(
        run_type=RunType.DART_PERIODIC_EXTRAS_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "bsns_years": bsns_years,
            "reprt_codes": reprt_codes,
            "statements": [statement.value for statement in statements],
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "universe_scope": scope.value,
            "target_years": {
                statement.value: resolve_target_years(statement, bsns_years)
                for statement in statements
            },
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = DartPeriodicExtrasSyncResult()
    ledger_kwargs = {} if no_data_ttl_days is None else {"no_data_ttl_days": no_data_ttl_days}
    ledger = SliceLedger(
        storage,
        source=Source.OPENDART,
        endpoint=LEDGER_ENDPOINT,
        **ledger_kwargs,
    )

    try:
        targets = resolve_dart_targets(storage, scope, tickers)
        if not targets:
            raise RuntimeError("No OpenDART corp mappings found. Run `dart sync-corp` first.")

        # Plan the whole run up front, in one ledger read, so the counters in
        # `ingestion_runs` describe the work rather than the first slice.
        requests: list[tuple[object, int, str, PeriodicExtraStatement]] = []
        keys: list[str] = []
        for corp in targets:
            for statement in statements:
                for bsns_year in resolve_target_years(statement, bsns_years):
                    for reprt_code in reprt_codes:
                        requests.append((corp, bsns_year, reprt_code, statement))
                        keys.append(
                            slice_key(corp.corp_code, bsns_year, reprt_code, statement.value)
                        )

        plan = ledger.plan(keys, force=force)
        pending = set(plan.pending)
        result.slices_pending = len(plan.pending)
        result.slices_skipped_complete = len(plan.skipped_complete)
        result.slices_skipped_no_data = len(plan.skipped_no_data)
        result.slices_retrying = len(plan.retrying)
        result.requests_skipped = plan.skipped
        run.params.update(plan.as_counts())

        seen_corps: set[str] = set()
        for (corp, bsns_year, reprt_code, statement), key in zip(requests, keys, strict=True):
            if key not in pending:
                continue
            if corp.corp_code not in seen_corps:
                seen_corps.add(corp.corp_code)
                result.targets_processed += 1

            result.requests_attempted += 1
            fetch_result = call_with_retry(
                lambda corp=corp, bsns_year=bsns_year, reprt_code=reprt_code, statement=statement: (
                    provider.fetch_periodic_extra(
                        corp=corp,
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        statement_type=statement,
                    )
                ),
                request_label=key,
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )

            if is_opendart_daily_limit_exhausted(fetch_result):
                # Stop cleanly and keep what the ledger already knows. Every
                # slice not yet recorded stays pending, which is the whole
                # point of flushing as we go.
                ledger.flush()
                raise OpenDartKeyExhaustedError(
                    fetch_result.error or "All OpenDART API keys are temporarily rate limited."
                )

            if fetch_result.error:
                result.errors[key] = fetch_result.error
                ledger.record_failure(key, fetch_result.error)
            elif fetch_result.no_data:
                result.no_data_requests += 1
                ledger.record_no_data(key)
            elif fetch_result.records:
                upsert = storage.upsert_dart_periodic_extras_raw(fetch_result.records)
                result.upsert.updated += upsert.updated
                result.upsert.errors += upsert.errors
                result.rows_upserted += upsert.updated
                # What the response carried against what storage wrote. "The
                # slice has rows" is not completion: a half-written slice would
                # otherwise be skipped by every later run and repaired by none.
                ledger.record_success(
                    key,
                    expected_rows=fetch_result.response_rows or len(fetch_result.records),
                    actual_rows=upsert.updated,
                )
            else:
                # A 000 response with an empty list. Upstream answered and had
                # nothing, which is a no-data verdict rather than a failure.
                result.no_data_requests += 1
                ledger.record_no_data(key)

            if ledger.pending_write_count >= flush_every:
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
                **plan.as_counts(),
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="periodic extras requests",
        )
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART periodic extras sync stopped: %s", exc)
        ledger.flush()
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART periodic extras sync failed")
        ledger.flush()
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

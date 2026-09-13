"""Use-case: Sync OpenDART company profiles (industry / incorporation / FY end).

``corpCode.xml``, which fills ``dart_corp_master`` today, carries only
corp_code / corp_name / stock_code / modify_date.  Industry code comes from a
separate per-corporation endpoint, and its absence is why every cross-sectional
z-score in ``fin_scan.py`` partitions on ``(trade_date, market)`` alone — banks,
biotech, shipbuilders and game studios all normalise against one KOSPI pool.

Targets are the ~3,959 corporations with a ticker mapping.  Including unlisted
entities would multiply the request count by about thirty for rows no feature
ever reads.

Skip-if-present is ``profile_fetched_at IS NOT NULL``.  One call per
corporation with no sub-parts, so there is no partial-response hazard here —
unlike the slice-based collectors, a row either has its profile or it does not.

Every fetch is also appended to ``dart_corp_profile_history`` (F-1).  The
master row is an upsert and forgets the previous ``induty_code``, which is why
industry is a *current* value in this repository and why every
industry-neutral variant in the marts is a diagnostic rather than a feature.
KRX index membership is not published and the KRX-to-KSIC crosswalk resolved
only 36% of tickers 1:1, so versioning this endpoint monthly from now on is the
only remaining route to a point-in-time industry.  It buys nothing for the
past: the history starts the month it starts.  See
``../modeler/docs/dev/20260907_additional_feature/01_industry_pit.md`` §2.
"""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType, UniverseScope
from collector.kr.domain.models import (
    CompanyProfile,
    CompanyProfileHistorySeedResult,
    CompanyProfileSyncResult,
    IngestionRun,
)
from collector.kr.ports.corp_codes import CorpProfileProvider
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

UPSERT_BATCH_SIZE = 200


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def observation_month(today: date | None = None) -> date:
    """The month an observation made now belongs to (first of the month).

    One row per corporation per calendar month is the history's grain, so a run
    on the 1st and a re-run on the 20th are the same observation and the second
    is a no-op.  That is deliberate: the point is a monthly series, not a log of
    every time the endpoint was called.
    """
    return (today or now_kst().date()).replace(day=1)


def sync_dart_corp_profile(
    profile_provider: CorpProfileProvider,
    storage: Storage,
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    force: bool = False,
    scope: UniverseScope = UniverseScope.CURRENT,
    observed_month: date | None = None,
) -> CompanyProfileSyncResult:
    """Fetch ``company.json`` for every ticker-mapped corporation.

    Args:
        profile_provider: OpenDART company-profile provider.
        storage: Target storage.
        tickers: Optional ticker allowlist.
        rate_limit_seconds: Base delay between requests.
        force: Re-fetch corporations that already have a profile.
        scope: Which universe to target.  Monthly history runs want
            ``HISTORICAL``: a delisted corporation's ``corp_cls`` flipping to
            ``E`` is one of the things the history is for.
        observed_month: Override the month the appended history rows are
            stamped with.  Defaults to the current month.

    Returns:
        ``CompanyProfileSyncResult`` with per-request counters and errors.
    """
    month = observed_month or observation_month()
    run = IngestionRun(
        run_type=RunType.DART_CORP_PROFILE_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "tickers": tickers,
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "universe_scope": scope.value,
            "observed_month": month.isoformat(),
        },
    )
    executor = _get_executor(profile_provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    storage.record_run(run)

    result = CompanyProfileSyncResult()
    pending: list[CompanyProfile] = []

    def flush() -> None:
        if not pending:
            return
        upsert = storage.upsert_company_profiles(pending)
        result.rows_upserted += upsert.updated
        # The master keeps the latest value; the history keeps every month's.
        # Appended in the same flush so an exit-75 stop leaves the two in step
        # rather than a master row whose observation was never recorded.
        history = storage.append_company_profile_history(pending, month, run.run_id)
        result.history_rows_appended += history.updated
        pending.clear()

    try:
        targets = resolve_dart_targets(storage, scope, tickers)
        if not targets:
            raise RuntimeError(
                "No ticker-mapped OpenDART corp rows found. Run `dart sync-corp` first."
            )

        already = set() if force else storage.get_profiled_corp_codes()
        logger.info(
            "Company profile sync: %d targets (%d already profiled)",
            len(targets),
            len(already & {c.corp_code for c in targets}),
        )

        for corp in targets:
            label = f"{corp.ticker}:company"

            if corp.corp_code in already:
                result.requests_skipped += 1
                continue

            result.requests_attempted += 1
            fetch = call_with_retry(
                lambda corp=corp: profile_provider.fetch_company_profile(corp),
                request_label=label,
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )

            if is_opendart_daily_limit_exhausted(fetch):
                # Flush what is already fetched before exiting: the run resumes
                # from profile_fetched_at, so unsaved work is re-fetched.
                flush()
                raise OpenDartKeyExhaustedError(
                    fetch.error or "All OpenDART API keys are temporarily rate limited."
                )

            if fetch.error:
                result.errors[label] = fetch.error
            elif fetch.no_data or fetch.profile is None:
                result.no_data += 1
            else:
                pending.append(fetch.profile)
                if len(pending) >= UPSERT_BATCH_SIZE:
                    flush()

            sleep_with_jitter(rate_limit_seconds)

        flush()

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=len(targets),
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data,
                history_rows_appended=result.history_rows_appended,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="company profile requests",
        )
        return result

    except OpenDartKeyExhaustedError as exc:
        logger.warning("Company profile sync stopped: %s", exc)
        fail_run(storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("Company profile sync failed")
        flush()
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def seed_dart_corp_profile_history(
    storage: Storage,
    observed_month: date | None = None,
) -> CompanyProfileHistorySeedResult:
    """Seed the industry history from the corp master's current state (F-1.3).

    No API calls: this copies what ``dart sync-corp-profile`` already stored.
    The seed rows are the history's floor, not an observation — they carry the
    master's own ``profile_fetched_at`` and ``is_seed = true``, so a change
    first *seen* between the seed and the next monthly snapshot cannot be dated
    more precisely than "before the snapshot".  Re-running inserts nothing.

    Args:
        storage: Target storage.
        observed_month: Month to seed as.  Defaults to the current month.

    Returns:
        ``CompanyProfileHistorySeedResult`` with the row count.
    """
    month = observed_month or observation_month()
    run = IngestionRun(
        run_type=RunType.DART_CORP_PROFILE_HISTORY_SEED,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={"observed_month": month.isoformat()},
    )
    storage.record_run(run)

    result = CompanyProfileHistorySeedResult(observed_month=month)
    try:
        seeded = storage.seed_company_profile_history(month, run.run_id)
        result.rows_inserted = seeded.updated
        logger.info(
            "Corp profile history seed: %d rows at observed_month=%s",
            result.rows_inserted,
            month,
        )
        complete_run(
            storage,
            run,
            counts=build_run_counts(rows_inserted=result.rows_inserted),
            errors=result.errors,
            partial_subject="corp profile history seed",
        )
        return result
    except Exception as exc:
        logger.exception("Corp profile history seed failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

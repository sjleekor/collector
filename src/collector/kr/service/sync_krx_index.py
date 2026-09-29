"""Use-case: sync KRX index daily levels into ``krx_index_daily``.

The unit of work is one ``(index_group, date)`` call.  A full history is about
4,100 weekdays x 3 groups, more than one day's quota (~10,000 calls per key), so
the run is resumable and chunkable:

* dates that already have rows for a group are skipped (unless ``force``);
* ``max_calls`` stops cleanly after that many HTTP calls;
* groups are the outer loop, dates ascending the inner loop, so a stopped run
  resumes by rerunning the same command.

An empty response is not an error and is not stored, so non-trading days are
polled again on a rerun.  Same-day data is not published, hence ``end`` is never
later than yesterday (KST).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Protocol

from collector.kr.adapters.index_krx_openapi.provider import (
    KRX_INDEX_HISTORY_START,
    KrxIndexDailyRow,
)
from collector.kr.adapters.market_data_krx_openapi.client import (
    KrxOpenApiEndpointNotApprovedError,
)
from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.models import IngestionRun, UpsertResult
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.util.pipeline import (
    ConsecutiveFailureGuard,
    SourceBlockedError,
    SourceQuotaExhaustedError,
    build_run_counts,
    complete_run,
    fail_run,
)
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 7


class IndexProvider(Protocol):
    def fetch_by_date(self, index_group: str, day: date) -> list[KrxIndexDailyRow]: ...


class IndexStorage(Protocol):
    def record_run(self, run: IngestionRun) -> None: ...

    def get_krx_index_dates(self, index_group: str, start: date, end: date) -> set[date]: ...

    def upsert_krx_index_daily(self, rows: list[KrxIndexDailyRow]) -> UpsertResult: ...


@dataclass(slots=True)
class IndexSyncResult:
    """Counters and errors for one ``index sync`` run."""

    calls: int = 0
    dates_fetched: int = 0
    dates_skipped: int = 0
    empty_dates: int = 0
    rows_upserted: int = 0
    failures: int = 0
    stopped_by_max_calls: bool = False
    errors: dict[str, str] = field(default_factory=dict)


def last_available_date(today: date | None = None) -> date:
    """Latest date that can have data: yesterday (KST); today is never published."""
    return (today or today_kst()) - timedelta(days=1)


def resolve_range(
    *,
    start: date | None,
    end: date | None,
    incremental: bool,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    today: date | None = None,
) -> tuple[date, date]:
    """Resolve ``[start, end]``: ``end`` defaults to yesterday and is capped there."""
    latest = last_available_date(today)
    resolved_end = min(end, latest) if end is not None else latest
    if incremental or start is None:
        resolved_start = resolved_end - timedelta(days=lookback_days)
    else:
        resolved_start = start
    return max(resolved_start, KRX_INDEX_HISTORY_START), resolved_end


def sync_krx_index(
    provider: IndexProvider,
    storage: IndexStorage,
    *,
    groups: list[str],
    start: date,
    end: date,
    force: bool = False,
    max_calls: int | None = None,
    max_consecutive_failures: int = 5,
) -> IndexSyncResult:
    """Fetch and upsert index levels for *groups* over ``[start, end]``."""
    result = IndexSyncResult()
    run = IngestionRun(
        run_type=RunType.INDEX_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "groups": groups,
            "start": str(start),
            "end": str(end),
            "force": force,
            "max_calls": max_calls,
            "max_consecutive_failures": max_consecutive_failures,
        },
    )
    storage.record_run(run)
    guard = ConsecutiveFailureGuard(
        max_consecutive_failures,
        label="krx index sync",
        backoff_seconds=0.0,
        logger_instance=logger,
    )

    try:
        if start > end:
            raise ValueError(f"start ({start}) must be <= end ({end})")
        days = get_trading_days(start, end)

        for group in groups:
            if result.stopped_by_max_calls:
                break
            have = set() if force else storage.get_krx_index_dates(group, start, end)
            for day in days:
                if day in have:
                    result.dates_skipped += 1
                    continue
                if max_calls is not None and result.calls >= max_calls:
                    result.stopped_by_max_calls = True
                    logger.info("max_calls=%d reached; stopping cleanly", max_calls)
                    break

                label = f"{group}/{day.isoformat()}"
                result.calls += 1
                try:
                    rows = provider.fetch_by_date(group, day)
                except KrxOpenApiEndpointNotApprovedError as exc:
                    result.errors[f"not_approved/{group}"] = str(exc)
                    logger.error("%s", exc)
                    break  # next group
                except SourceQuotaExhaustedError as exc:
                    result.errors["quota_exhausted"] = str(exc)
                    logger.warning("Quota exhausted at %s: %s", label, exc)
                    raise
                except SourceBlockedError:
                    raise
                except Exception as exc:  # noqa: BLE001 - collected, not raised
                    result.failures += 1
                    result.errors[label] = str(exc)
                    logger.warning("%s failed: %s", label, exc)
                    guard.record_failure(f"{label}: {exc}")
                    continue

                guard.record_success()
                if not rows:
                    result.empty_dates += 1
                    logger.info("%s: no rows (non-trading day or not yet published)", label)
                    continue

                upsert = storage.upsert_krx_index_daily(rows)
                result.dates_fetched += 1
                result.rows_upserted += upsert.updated + upsert.inserted

        if "quota_exhausted" not in result.errors:
            complete_run(
                storage,
                run,
                counts=build_run_counts(
                    calls=result.calls,
                    dates_fetched=result.dates_fetched,
                    dates_skipped=result.dates_skipped,
                    empty_dates=result.empty_dates,
                    rows_upserted=result.rows_upserted,
                    failures=result.failures,
                ),
                errors=result.errors,
                partial_subject="index dates",
            )
        return result

    except (SourceBlockedError, SourceQuotaExhaustedError) as exc:
        key = "quota_exhausted" if isinstance(exc, SourceQuotaExhaustedError) else "source_blocked"
        result.errors[key] = str(exc)
        fail_run(storage, run, exc)
        return result
    except Exception as exc:
        logger.exception("Index sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result

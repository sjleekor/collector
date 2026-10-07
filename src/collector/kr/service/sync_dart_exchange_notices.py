"""Use-case: collect OpenDART 거래소공시 (``pblntf_ty=I``) market-wide and verify coverage.

백필은 달력 분기 창으로, 증분은 최근 N일 창으로 같은 함수가 받는다. 창마다 페이지를
하나씩 적재하고 ``dart_exchange_notice_window``에 진행을 남기므로, 일일 한도에 걸리거나
중단돼도 다음 실행이 끝난 페이지 다음부터 이어 받는다. 창이 ``done``이 되는 조건은
total_page까지 받았고 저장 건수가 total_count에 닿는 것이다.

분류(지정·해제·정지 등)는 하지 않는다 — 반영 규칙은 4.2-a2가 따로 동결한다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.exchange_notice import (
    PBLNTF_TY_EXCHANGE,
    WINDOW_STATUS_DONE,
    WINDOW_STATUS_FAILED,
    WINDOW_STATUS_RUNNING,
    ExchangeNoticeCheck,
    ExchangeNoticePage,
    ExchangeNoticeSyncResult,
    ExchangeNoticeWindow,
    QuarterCheck,
)
from collector.kr.domain.models import IngestionRun
from collector.kr.ports.exchange_notice import ExchangeNoticePageProvider, ExchangeNoticeStore
from collector.kr.ports.storage import Storage
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

# 2008Q1 is the earliest quarter probed (14,868 rows). 2008 이전은 확인한 적이 없다.
DEFAULT_START = date(2008, 1, 1)

# Hard backstop: 2020Q1 (the busiest probed) is 213 pages. A window that claims more
# than this is a malformed response, not volume.
MAX_PAGES_PER_WINDOW = 1000

# A quarter that returns no 거래소공시 at all is a gap, not a quiet quarter.
EMPTY_QUARTER_MIN_DAYS = 60


class _BudgetReached(Exception):
    """Raised inside a window when ``max_requests`` is spent."""


# ---------------------------------------------------------------- windows


def quarter_bounds(day: date) -> tuple[date, date]:
    """First and last day of the calendar quarter containing ``day``."""
    first_month = 3 * ((day.month - 1) // 3) + 1
    bgn = date(day.year, first_month, 1)
    next_first = date(day.year + (first_month + 3 > 12), (first_month + 3 - 1) % 12 + 1, 1)
    return bgn, next_first - timedelta(days=1)


def quarter_label(day: date) -> str:
    return f"{day.year}Q{(day.month - 1) // 3 + 1}"


def quarter_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Calendar-quarter windows covering ``[start, end]``, clipped at both ends.

    Newest first is not used: the backfill walks oldest to newest so a partial run
    leaves one contiguous covered prefix.
    """
    if end < start:
        raise ValueError("end must be >= start")
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        _, q_end = quarter_bounds(cursor)
        windows.append((cursor, min(q_end, end)))
        cursor = q_end + timedelta(days=1)
    return windows


def incremental_window(today: date, lookback_days: int) -> tuple[date, date]:
    """Recent-days window for the daily run (overlaps earlier runs on purpose)."""
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    return today - timedelta(days=lookback_days), today


# ------------------------------------------------------------------- sync


def _get_executor(provider: object) -> OpenDartRequestExecutor | None:
    executor = getattr(provider, "request_executor", None)
    return executor if isinstance(executor, OpenDartRequestExecutor) else None


def sync_exchange_notices(
    provider: ExchangeNoticePageProvider,
    store: ExchangeNoticeStore,
    run_storage: Storage,
    windows: list[tuple[date, date]],
    *,
    rate_limit_seconds: float = 0.3,
    force: bool = False,
    max_requests: int | None = None,
    mode: str = "backfill",
    sleep_fn: Callable[[float], None] = sleep_with_jitter,
) -> ExchangeNoticeSyncResult:
    """Fetch every window page by page, resuming unfinished windows.

    ``max_requests`` bounds the HTTP calls of this run (retries count). Reaching it
    is a clean stop (``stopped_reason="max_requests"``), not an error: the window in
    progress keeps its ``pages_done`` and the next run continues there.
    """
    run = IngestionRun(
        run_type=RunType.DART_EXCHANGE_NOTICE_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "mode": mode,
            "pblntf_ty": PBLNTF_TY_EXCHANGE,
            "windows": [[b.isoformat(), e.isoformat()] for b, e in windows[:5]],
            "windows_total": len(windows),
            "rate_limit_seconds": rate_limit_seconds,
            "force": force,
            "max_requests": max_requests,
        },
    )
    executor = _get_executor(provider)
    if executor is not None:
        run.params["opendart_key_count"] = executor.configured_key_count
    run_storage.record_run(run)

    result = ExchangeNoticeSyncResult(windows_total=len(windows))
    try:
        for bgn_de, end_de in windows:
            outcome = _sync_window(
                provider,
                store,
                bgn_de,
                end_de,
                result=result,
                rate_limit_seconds=rate_limit_seconds,
                force=force,
                max_requests=max_requests,
                sleep_fn=sleep_fn,
            )
            if outcome == "budget":
                result.stopped_reason = "max_requests"
                break

        complete_run(
            run_storage,
            run,
            counts=build_run_counts(
                windows_total=result.windows_total,
                windows_completed=result.windows_completed,
                windows_skipped=result.windows_skipped,
                windows_failed=result.windows_failed,
                requests_attempted=result.requests_attempted,
                rows_upserted=result.rows_upserted,
                **(executor.snapshot_metrics() if executor is not None else {}),
            ),
            errors=result.errors,
            partial_subject="exchange notice windows",
        )
        return result
    except OpenDartKeyExhaustedError as exc:
        logger.warning("OpenDART exchange notice sync stopped: %s", exc)
        fail_run(run_storage, run, exc)
        result.opendart_exhaustion_reason = "all_rate_limited"
        result.errors["pipeline"] = str(exc)
        return result
    except Exception as exc:
        logger.exception("OpenDART exchange notice sync failed")
        fail_run(run_storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def _sync_window(
    provider: ExchangeNoticePageProvider,
    store: ExchangeNoticeStore,
    bgn_de: date,
    end_de: date,
    *,
    result: ExchangeNoticeSyncResult,
    rate_limit_seconds: float,
    force: bool,
    max_requests: int | None,
    sleep_fn: Callable[[float], None],
) -> str:
    """Returns ``done`` | ``skipped`` | ``failed`` | ``budget``."""
    label = f"{bgn_de.isoformat()}~{end_de.isoformat()}"
    now = now_kst()
    window = store.get_window(bgn_de, end_de)
    if window is not None and window.status == WINDOW_STATUS_DONE and not force:
        result.windows_skipped += 1
        return "skipped"
    if window is None or force:
        window = ExchangeNoticeWindow(bgn_de=bgn_de, end_de=end_de, started_at=now)
    window.status = WINDOW_STATUS_RUNNING
    window.last_error = None

    page_no = window.pages_done + 1 if window.pages_done > 0 and window.total_page > 0 else 1
    verifying_resume = page_no > 1

    def fail(message: str) -> str:
        window.status = WINDOW_STATUS_FAILED
        window.last_error = message
        window.updated_at = now_kst()
        store.save_window(window)
        result.windows_failed += 1
        result.errors[label] = message
        return "failed"

    while True:
        if max_requests is not None and result.requests_attempted >= max_requests:
            window.updated_at = now_kst()
            store.save_window(window)
            return "budget"

        def attempt(page_no: int = page_no) -> ExchangeNoticePage:
            if max_requests is not None and result.requests_attempted >= max_requests:
                raise _BudgetReached
            result.requests_attempted += 1
            window.requests += 1
            return provider.fetch_page(bgn_de, end_de, page_no)

        try:
            page = call_with_retry(
                attempt,
                request_label=f"{label}:p{page_no}",
                logger_instance=logger,
                should_retry_result=should_retry_opendart_result,
            )
        except _BudgetReached:
            window.updated_at = now_kst()
            store.save_window(window)
            return "budget"

        if is_opendart_daily_limit_exhausted(page):
            window.updated_at = now_kst()
            store.save_window(window)
            raise OpenDartKeyExhaustedError(
                page.error or "All OpenDART API keys are temporarily rate limited."
            )
        if page.error:
            return fail(f"page {page_no}: {page.error}")

        if page.no_data:
            if page_no > 1:
                return fail(f"page {page_no}: unexpected no-data after {page_no - 1} pages")
            # 013: the window really has no notices (e.g. a weekend-only increment).
            window.total_count = window.total_page = window.pages_done = 0
            window.rows_fetched = 0
            break

        if verifying_resume and page.total_count != window.total_count:
            # The upstream list changed since the partial run; page boundaries moved.
            logger.warning(
                "Window %s total_count changed %d -> %d; restarting from page 1",
                label,
                window.total_count,
                page.total_count,
            )
            window.pages_done = window.total_page = window.total_count = 0
            window.rows_fetched = 0
            page_no = 1
            verifying_resume = False
            continue
        verifying_resume = False

        if page.total_page > MAX_PAGES_PER_WINDOW:
            return fail(f"page {page_no}: total_page {page.total_page} > {MAX_PAGES_PER_WINDOW}")
        if page_no <= page.total_page and page.total_count > 0 and not page.records:
            return fail(f"page {page_no}: empty page but total_page={page.total_page}")

        result.rows_upserted += store.upsert_notices(page.records)
        window.total_count = page.total_count
        window.total_page = page.total_page
        window.pages_done = page_no
        window.rows_fetched += len(page.records)
        window.updated_at = now_kst()
        store.save_window(window)

        if page_no >= page.total_page:
            break
        page_no += 1
        sleep_fn(rate_limit_seconds)

    stored = store.count_notices(bgn_de, end_de)
    if stored < window.total_count:
        return fail(f"stored {stored} rows < total_count {window.total_count}")

    window.status = WINDOW_STATUS_DONE
    window.completed_at = window.updated_at = now_kst()
    store.save_window(window)
    result.windows_completed += 1
    sleep_fn(rate_limit_seconds)
    return "done"


# ------------------------------------------------------------------ check


def _merge(intervals: list[tuple[date, date]]) -> list[tuple[date, date]]:
    merged: list[tuple[date, date]] = []
    for bgn, end in sorted(intervals):
        if merged and bgn <= merged[-1][1] + timedelta(days=1):
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((bgn, end))
    return merged


def _gaps(intervals: list[tuple[date, date]], lo: date, hi: date) -> list[tuple[date, date]]:
    """Sub-ranges of ``[lo, hi]`` not covered by ``intervals``."""
    gaps: list[tuple[date, date]] = []
    cursor = lo
    for bgn, end in _merge([(max(b, lo), min(e, hi)) for b, e in intervals if e >= lo and b <= hi]):
        if bgn > cursor:
            gaps.append((cursor, bgn - timedelta(days=1)))
        cursor = max(cursor, end + timedelta(days=1))
    if cursor <= hi:
        gaps.append((cursor, hi))
    return gaps


def check_exchange_notices(
    store: ExchangeNoticeStore,
    start: date = DEFAULT_START,
    through: date | None = None,
) -> ExchangeNoticeCheck:
    """전체 구간 페이지·건수 확인과 빠진 분기 검사.

    분기마다 ``done`` 창의 합집합이 ``[start, through]``를 빈틈없이 덮는지(빠진 날),
    창이 기록한 total_count·total_page가 저장 상태와 맞는지, 분기 전체가 0건이 아닌지
    본다. ``ok``가 참이면 빠진 분기 0이다.
    """
    through = through or (now_kst().date() - timedelta(days=1))
    check = ExchangeNoticeCheck(start=start, through=through)
    if through < start:
        return check

    windows = store.list_windows(start, through)
    done = [w for w in windows if w.status == WINDOW_STATUS_DONE]
    done_ranges = [(w.bgn_de, w.end_de) for w in done]

    for bgn_de, end_de in quarter_windows(start, through):
        quarter = QuarterCheck(
            quarter=quarter_label(bgn_de),
            bgn_de=bgn_de,
            end_de=end_de,
            uncovered=_gaps(done_ranges, bgn_de, end_de),
            notice_rows=store.count_notices(bgn_de, end_de),
        )
        if quarter.uncovered:
            quarter.problems.append(
                "uncovered "
                + ", ".join(f"{b.isoformat()}~{e.isoformat()}" for b, e in quarter.uncovered)
            )
        exact = next((w for w in done if w.bgn_de == bgn_de and w.end_de == end_de), None)
        if exact is not None:
            quarter.window_total_count = exact.total_count
            quarter.window_total_page = exact.total_page
            quarter.window_pages_done = exact.pages_done
        for w in done:
            if not (bgn_de <= w.bgn_de <= end_de):
                continue
            if w.pages_done != w.total_page:
                quarter.problems.append(
                    f"window {w.bgn_de}~{w.end_de}: "
                    f"pages_done {w.pages_done} != total_page {w.total_page}"
                )
            stored = store.count_notices(w.bgn_de, w.end_de)
            if stored < w.total_count:
                quarter.problems.append(
                    f"window {w.bgn_de}~{w.end_de}: stored {stored} < total_count {w.total_count}"
                )
        covered_days = (end_de - bgn_de).days + 1
        if (
            not quarter.uncovered
            and quarter.notice_rows == 0
            and covered_days >= EMPTY_QUARTER_MIN_DAYS
        ):
            quarter.problems.append("covered but 0 rows")
        check.quarters.append(quarter)

    check.unfinished_windows = [
        w
        for w in windows
        if w.status != WINDOW_STATUS_DONE and _gaps(done_ranges, w.bgn_de, w.end_de)
    ]
    return check

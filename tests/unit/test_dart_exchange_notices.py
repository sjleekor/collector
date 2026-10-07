"""DART 거래소공시 (pblntf_ty=I) 수집·검사 시험. 실제 API·DB를 부르지 않는다."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date

import pytest

from collector.kr.adapters.opendart_common.client import OpenDartCallResult
from collector.kr.adapters.opendart_exchange_notice.provider import (
    OpenDartExchangeNoticeProvider,
    parse_exchange_notice_page,
)
from collector.kr.domain.enums import RunStatus, Source
from collector.kr.domain.exchange_notice import (
    WINDOW_STATUS_DONE,
    WINDOW_STATUS_FAILED,
    WINDOW_STATUS_RUNNING,
    ExchangeNoticePage,
    ExchangeNoticeRecord,
    ExchangeNoticeWindow,
)
from collector.kr.domain.models import IngestionRun
from collector.kr.service.sync_dart_exchange_notices import (
    check_exchange_notices,
    incremental_window,
    quarter_bounds,
    quarter_windows,
    sync_exchange_notices,
)
from collector.kr.util.time import now_kst
from tests.helpers.fake_opendart_executor import FakeOpenDartExecutor

# ------------------------------------------------------------------ fakes


def _record(
    rcept_no: str, rcept_dt: date, report_nm: str = "주권매매거래정지"
) -> ExchangeNoticeRecord:
    return ExchangeNoticeRecord(
        rcept_no=rcept_no,
        corp_code="00000001",
        corp_name="가나다",
        stock_code="000001",
        corp_cls="K",
        report_nm=report_nm,
        flr_nm="코스닥시장본부",
        rcept_dt=rcept_dt,
        rm="",
        pblntf_ty="I",
        source=Source.OPENDART,
        fetched_at=now_kst(),
        raw_payload={},
    )


class MemoryStore:
    def __init__(self) -> None:
        self.windows: dict[tuple[date, date], ExchangeNoticeWindow] = {}
        self.notices: dict[str, ExchangeNoticeRecord] = {}

    def get_window(self, bgn_de, end_de):
        w = self.windows.get((bgn_de, end_de))
        return None if w is None else replace(w)

    def save_window(self, window):
        self.windows[(window.bgn_de, window.end_de)] = replace(window)

    def list_windows(self, start, end):
        return sorted(
            (w for w in self.windows.values() if w.end_de >= start and w.bgn_de <= end),
            key=lambda w: (w.bgn_de, w.end_de),
        )

    def upsert_notices(self, records):
        for r in records:
            self.notices[r.rcept_no] = r
        return len(records)

    def count_notices(self, bgn_de, end_de):
        return sum(
            1 for r in self.notices.values() if r.rcept_dt and bgn_de <= r.rcept_dt <= end_de
        )


class MemoryRuns:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []

    def record_run(self, run):
        self.runs.append(run)


class PageProvider:
    """Serves ``pages[(bgn, end)]`` = list of record lists; one entry per page."""

    def __init__(self, pages: dict[tuple[date, date], list[list[ExchangeNoticeRecord]]]) -> None:
        self.pages = pages
        self.calls: list[tuple[date, date, int]] = []
        self.fail_on: dict[tuple[date, date, int], ExchangeNoticePage] = {}

    def fetch_page(self, bgn_de, end_de, page_no):
        self.calls.append((bgn_de, end_de, page_no))
        if (bgn_de, end_de, page_no) in self.fail_on:
            return self.fail_on[(bgn_de, end_de, page_no)]
        pages = self.pages.get((bgn_de, end_de), [])
        if not pages:
            return ExchangeNoticePage(bgn_de=bgn_de, end_de=end_de, page_no=page_no, no_data=True)
        total = sum(len(p) for p in pages)
        return ExchangeNoticePage(
            bgn_de=bgn_de,
            end_de=end_de,
            page_no=page_no,
            records=pages[page_no - 1],
            total_page=len(pages),
            total_count=total,
        )


def _run(provider, store, windows, **kw):
    kw.setdefault("sleep_fn", lambda _s: None)
    kw.setdefault("rate_limit_seconds", 0.0)
    runs = MemoryRuns()
    result = sync_exchange_notices(provider, store, runs, windows, **kw)
    return result, runs


Q1 = (date(2020, 1, 1), date(2020, 3, 31))
Q2 = (date(2020, 4, 1), date(2020, 6, 30))


def _three_pages(prefix: str = "A", dt: date = date(2020, 2, 3)):
    return [
        [_record(f"{prefix}{i}{j}", dt) for j in range(2)] for i in range(3)
    ]  # 3 pages x 2 rows


# --------------------------------------------------------- parse / provider


def _payload(page_no=1, total_page=1, total_count=1, rows=None) -> bytes:
    rows = (
        rows
        if rows is not None
        else [
            {
                "corp_code": "00919966",
                "corp_name": "신라젠",
                "stock_code": "215600",
                "corp_cls": "K",
                "report_nm": "주권매매거래정지(상장폐지 사유 발생)",
                "rcept_no": "20200514000001",
                "flr_nm": "코스닥시장본부",
                "rcept_dt": "20200514",
                "rm": "",
            }
        ]
    )
    return json.dumps(
        {
            "status": "000",
            "message": "정상",
            "page_no": page_no,
            "total_page": total_page,
            "total_count": total_count,
            "list": rows,
        }
    ).encode()


def test_parse_page_maps_fields_and_drops_rows_without_rcept_no() -> None:
    payload = json.loads(
        _payload(
            rows=[
                {"rcept_no": "1", "corp_code": "9", "rcept_dt": "20200102", "report_nm": " X "},
                {"corp_code": "9", "rcept_dt": "20200102"},
                "junk",
            ]
        )
    )
    records, page_no, total_page, total_count = parse_exchange_notice_page(payload)
    assert [r.rcept_no for r in records] == ["1"]
    assert records[0].report_nm == "X"
    assert records[0].rcept_dt == date(2020, 1, 2)
    assert records[0].pblntf_ty == "I"
    assert (page_no, total_page, total_count) == (1, 1, 1)


def test_provider_requests_market_wide_type_i_without_corp_code() -> None:
    executor = FakeOpenDartExecutor([_payload(page_no=2, total_page=3, total_count=250)])
    provider = OpenDartExchangeNoticeProvider(request_executor=executor)

    page = provider.fetch_page(date(2020, 1, 1), date(2020, 3, 31), 2)

    assert page.error is None
    assert (page.page_no, page.total_page, page.total_count) == (2, 3, 250)
    assert len(page.records) == 1
    (call,) = executor.calls
    assert call.params == {
        "bgn_de": "20200101",
        "end_de": "20200331",
        "pblntf_ty": "I",
        "page_no": "2",
        "page_count": "100",
    }
    assert "corp_code" not in call.params
    assert "pblntf_detail_ty" not in call.params


def test_provider_maps_no_data_status() -> None:
    body = json.dumps({"status": "013", "message": "조회된 데이터가 없습니다."}).encode()
    page = OpenDartExchangeNoticeProvider(FakeOpenDartExecutor([body])).fetch_page(*Q1, 1)
    assert page.no_data is True
    assert page.error is None


def test_provider_reports_wrong_page_as_error() -> None:
    executor = FakeOpenDartExecutor([_payload(page_no=1, total_page=3, total_count=250)])
    page = OpenDartExchangeNoticeProvider(executor).fetch_page(*Q1, 2)
    assert page.error is not None
    assert page.records == []


def test_provider_passes_rate_limit_through() -> None:
    limited = OpenDartCallResult(
        error="All OpenDART API keys are temporarily rate limited.",
        retryable=True,
        exhaustion_reason="all_rate_limited",
    )
    page = OpenDartExchangeNoticeProvider(FakeOpenDartExecutor([limited])).fetch_page(*Q1, 1)
    assert page.exhaustion_reason == "all_rate_limited"


# ----------------------------------------------------------------- windows


def test_quarter_bounds_year_end_and_leap_year() -> None:
    assert quarter_bounds(date(2020, 11, 15)) == (date(2020, 10, 1), date(2020, 12, 31))
    assert quarter_bounds(date(2020, 2, 29)) == (date(2020, 1, 1), date(2020, 3, 31))
    assert quarter_bounds(date(2008, 5, 1)) == (date(2008, 4, 1), date(2008, 6, 30))


def test_quarter_windows_clip_both_ends() -> None:
    windows = quarter_windows(date(2019, 11, 20), date(2020, 5, 10))
    assert windows == [
        (date(2019, 11, 20), date(2019, 12, 31)),
        (date(2020, 1, 1), date(2020, 3, 31)),
        (date(2020, 4, 1), date(2020, 5, 10)),
    ]


def test_quarter_windows_full_range() -> None:
    windows = quarter_windows(date(2008, 1, 1), date(2026, 10, 6))
    assert len(windows) == 19 * 4  # 2008Q1 .. 2026Q4 (the last one clipped at the end date)
    assert windows[0][0] == date(2008, 1, 1)
    assert all((e - b).days <= 91 for b, e in windows)  # inside the 3-month cap


def test_incremental_window() -> None:
    assert incremental_window(date(2026, 10, 7), 7) == (date(2026, 9, 30), date(2026, 10, 7))
    with pytest.raises(ValueError):
        incremental_window(date(2026, 10, 7), -1)


# -------------------------------------------------------------------- sync


def test_sync_window_fetches_all_pages_and_marks_done() -> None:
    store = MemoryStore()
    provider = PageProvider({Q1: _three_pages()})
    sleeps: list[float] = []

    result, runs = _run(provider, store, [Q1], rate_limit_seconds=0.5, sleep_fn=sleeps.append)

    assert [c[2] for c in provider.calls] == [1, 2, 3]
    assert result.windows_completed == 1
    assert result.requests_attempted == 3
    assert result.rows_upserted == 6
    window = store.windows[Q1]
    assert (window.status, window.pages_done, window.total_page, window.total_count) == (
        WINDOW_STATUS_DONE,
        3,
        3,
        6,
    )
    assert window.completed_at is not None
    assert len(sleeps) == 3  # between pages (2) + after the window (1)
    assert runs.runs[-1].status == RunStatus.SUCCESS
    assert runs.runs[-1].counts["requests_attempted"] == 3


def test_empty_window_is_done_with_zero() -> None:
    store = MemoryStore()
    result, _ = _run(PageProvider({}), store, [Q1])
    assert result.windows_completed == 1
    assert store.windows[Q1].status == WINDOW_STATUS_DONE
    assert store.windows[Q1].total_count == 0


def test_done_window_is_skipped_unless_forced() -> None:
    store = MemoryStore()
    provider = PageProvider({Q1: _three_pages()})
    _run(provider, store, [Q1])
    provider.calls.clear()

    result, _ = _run(provider, store, [Q1])
    assert result.windows_skipped == 1
    assert provider.calls == []

    result, _ = _run(provider, store, [Q1], force=True)
    assert result.windows_completed == 1
    assert len(provider.calls) == 3


def test_resume_continues_after_last_finished_page() -> None:
    store = MemoryStore()
    pages = _three_pages()
    provider = PageProvider({Q1: pages})
    store.upsert_notices(pages[0])
    store.save_window(
        ExchangeNoticeWindow(
            *Q1,
            status=WINDOW_STATUS_RUNNING,
            total_count=6,
            total_page=3,
            pages_done=1,
            rows_fetched=2,
            started_at=now_kst(),
        )
    )

    result, _ = _run(provider, store, [Q1])

    assert [c[2] for c in provider.calls] == [2, 3]
    assert store.windows[Q1].status == WINDOW_STATUS_DONE
    assert store.count_notices(*Q1) == 6
    assert result.requests_attempted == 2


def test_resume_restarts_when_total_count_changed() -> None:
    store = MemoryStore()
    provider = PageProvider({Q1: _three_pages()})  # total_count 6
    store.save_window(
        ExchangeNoticeWindow(
            *Q1,
            status=WINDOW_STATUS_RUNNING,
            total_count=5,
            total_page=3,
            pages_done=2,
            rows_fetched=4,
            started_at=now_kst(),
        )
    )

    _run(provider, store, [Q1])

    assert [c[2] for c in provider.calls] == [3, 1, 2, 3]
    assert store.windows[Q1].status == WINDOW_STATUS_DONE
    assert store.windows[Q1].rows_fetched == 6


def test_max_requests_stops_cleanly_and_next_run_resumes() -> None:
    store = MemoryStore()
    provider = PageProvider({Q1: _three_pages(), Q2: _three_pages("B", date(2020, 5, 4))})

    result, runs = _run(provider, store, [Q1, Q2], max_requests=2)

    assert result.stopped_reason == "max_requests"
    assert result.requests_attempted == 2
    assert result.errors == {}
    assert store.windows[Q1].status == WINDOW_STATUS_RUNNING
    assert store.windows[Q1].pages_done == 2
    assert Q2 not in store.windows
    assert runs.runs[-1].status == RunStatus.SUCCESS

    provider.calls.clear()
    result, _ = _run(provider, store, [Q1, Q2])
    assert [c[:3][2] for c in provider.calls] == [3, 1, 2, 3]
    assert result.windows_completed == 2
    assert store.windows[Q2].status == WINDOW_STATUS_DONE


def test_page_error_fails_window_keeps_progress_and_continues() -> None:
    store = MemoryStore()
    provider = PageProvider({Q1: _three_pages(), Q2: _three_pages("B", date(2020, 5, 4))})
    provider.fail_on[(*Q1, 2)] = ExchangeNoticePage(
        bgn_de=Q1[0], end_de=Q1[1], page_no=2, error="OpenDART error 800: down"
    )

    result, runs = _run(provider, store, [Q1, Q2])

    assert result.windows_failed == 1
    assert result.windows_completed == 1  # Q2 still ran
    assert "2020-01-01~2020-03-31" in result.errors
    assert store.windows[Q1].status == WINDOW_STATUS_FAILED
    assert store.windows[Q1].pages_done == 1
    assert runs.runs[-1].status == RunStatus.PARTIAL

    # the failure clears and the rerun resumes at page 2
    del provider.fail_on[(*Q1, 2)]
    provider.calls.clear()
    result, _ = _run(provider, store, [Q1, Q2])
    assert [c[2] for c in provider.calls[:2]] == [2, 3]
    assert store.windows[Q1].status == WINDOW_STATUS_DONE


def test_retryable_page_error_is_retried(monkeypatch) -> None:
    monkeypatch.setattr("collector.kr.util.pipeline.time.sleep", lambda _s: None)
    store = MemoryStore()
    provider = PageProvider({Q1: [[_record("A1", date(2020, 2, 3))]]})
    flaky = ExchangeNoticePage(*Q1, 1, error="timeout", retryable=True)
    real = provider.fetch_page
    state = {"n": 0}

    def fetch(b, e, p):
        state["n"] += 1
        return flaky if state["n"] == 1 else real(b, e, p)

    provider.fetch_page = fetch  # type: ignore[method-assign]
    result, _ = _run(provider, store, [Q1])
    assert result.windows_completed == 1
    assert state["n"] == 2


def test_rate_limit_exhaustion_stops_run_and_keeps_window_resumable() -> None:
    store = MemoryStore()
    provider = PageProvider({Q1: _three_pages()})
    provider.fail_on[(*Q1, 2)] = ExchangeNoticePage(
        *Q1,
        2,
        error="All OpenDART API keys are temporarily rate limited.",
        retryable=True,
        exhaustion_reason="all_rate_limited",
    )

    result, runs = _run(provider, store, [Q1, Q2])

    assert result.opendart_exhaustion_reason == "all_rate_limited"
    assert store.windows[Q1].status == WINDOW_STATUS_RUNNING
    assert store.windows[Q1].pages_done == 1
    assert Q2 not in store.windows
    assert runs.runs[-1].status == RunStatus.FAILED


def test_stored_shortfall_fails_window() -> None:
    """Paging that repeats a receipt leaves fewer stored rows than total_count."""
    store = MemoryStore()
    dup = _record("DUP", date(2020, 2, 3))
    provider = PageProvider(
        {Q1: [[dup, _record("A", date(2020, 2, 3))], [dup]]}
    )  # 3 claimed, 2 stored

    result, _ = _run(provider, store, [Q1])

    assert result.windows_failed == 1
    assert store.windows[Q1].status == WINDOW_STATUS_FAILED
    assert "stored 2 rows < total_count 3" in store.windows[Q1].last_error


def test_empty_page_inside_total_page_fails_window() -> None:
    store = MemoryStore()
    provider = PageProvider(
        {Q1: [[_record("A", date(2020, 2, 3))], [_record("B", date(2020, 2, 3))]]}
    )
    provider.fail_on[(*Q1, 2)] = ExchangeNoticePage(*Q1, 2, records=[], total_page=2, total_count=2)
    result, _ = _run(provider, store, [Q1])
    assert result.windows_failed == 1
    assert "empty page" in store.windows[Q1].last_error


# ------------------------------------------------------------------- check


def _done(bgn: date, end: date, *, count: int, pages: int) -> ExchangeNoticeWindow:
    return ExchangeNoticeWindow(
        bgn,
        end,
        status=WINDOW_STATUS_DONE,
        total_count=count,
        total_page=pages,
        pages_done=pages,
        rows_fetched=count,
        started_at=now_kst(),
        completed_at=now_kst(),
    )


def _seed(store: MemoryStore, window: ExchangeNoticeWindow, rows: int, prefix: str) -> None:
    store.save_window(window)
    for i in range(rows):
        store.notices[f"{prefix}{i}"] = _record(f"{prefix}{i}", window.bgn_de)


def test_check_ok_when_every_quarter_is_covered_and_counts_match() -> None:
    store = MemoryStore()
    _seed(store, _done(*Q1, count=3, pages=1), 3, "a")
    _seed(store, _done(*Q2, count=2, pages=1), 2, "b")

    check = check_exchange_notices(store, date(2020, 1, 1), date(2020, 6, 30))

    assert check.ok
    assert check.missing_quarters == []
    assert [q.quarter for q in check.quarters] == ["2020Q1", "2020Q2"]
    assert check.total_rows == 5
    assert check.quarters[0].window_total_count == 3
    assert check.quarters[0].window_pages_done == 1


def test_check_flags_missing_quarter() -> None:
    store = MemoryStore()
    _seed(store, _done(*Q1, count=3, pages=1), 3, "a")

    check = check_exchange_notices(store, date(2020, 1, 1), date(2020, 6, 30))

    assert not check.ok
    assert check.missing_quarters == ["2020Q2"]
    assert check.quarters[1].uncovered == [Q2]


def test_check_flags_failed_window_as_missing_and_unfinished() -> None:
    store = MemoryStore()
    _seed(store, _done(*Q1, count=3, pages=1), 3, "a")
    failed = ExchangeNoticeWindow(
        *Q2,
        status=WINDOW_STATUS_FAILED,
        total_count=9,
        total_page=3,
        pages_done=1,
        started_at=now_kst(),
    )
    store.save_window(failed)

    check = check_exchange_notices(store, date(2020, 1, 1), date(2020, 6, 30))

    assert check.missing_quarters == ["2020Q2"]
    assert [(w.bgn_de, w.end_de) for w in check.unfinished_windows] == [Q2]


def test_check_flags_count_shortfall_and_page_mismatch() -> None:
    store = MemoryStore()
    window = _done(*Q1, count=5, pages=2)
    window.pages_done = 1
    _seed(store, window, 3, "a")

    check = check_exchange_notices(store, date(2020, 1, 1), date(2020, 3, 31))

    assert not check.ok
    problems = " ".join(check.quarters[0].problems)
    assert "stored 3 < total_count 5" in problems
    assert "pages_done 1 != total_page 2" in problems


def test_check_flags_covered_quarter_with_zero_rows() -> None:
    store = MemoryStore()
    store.save_window(_done(*Q1, count=0, pages=0))

    check = check_exchange_notices(store, date(2020, 1, 1), date(2020, 3, 31))

    assert check.quarters[0].problems == ["covered but 0 rows"]


def test_check_accepts_incremental_windows_covering_open_quarter() -> None:
    store = MemoryStore()
    q = (date(2026, 7, 1), date(2026, 9, 30))
    _seed(store, _done(*q, count=2, pages=1), 2, "a")
    _seed(store, _done(date(2026, 9, 25), date(2026, 10, 6), count=1, pages=1), 0, "x")
    store.notices["n"] = _record("n", date(2026, 10, 1))

    check = check_exchange_notices(store, date(2026, 7, 1), date(2026, 10, 6))

    assert check.ok, [q.problems for q in check.quarters]
    assert [x.quarter for x in check.quarters] == ["2026Q3", "2026Q4"]
    assert check.quarters[1].end_de == date(2026, 10, 6)  # clipped to ``through``


def test_check_flags_gap_between_backfill_and_incremental() -> None:
    store = MemoryStore()
    q = (date(2026, 7, 1), date(2026, 9, 30))
    _seed(store, _done(*q, count=2, pages=1), 2, "a")
    _seed(store, _done(date(2026, 10, 3), date(2026, 10, 6), count=1, pages=1), 1, "x")

    check = check_exchange_notices(store, date(2026, 7, 1), date(2026, 10, 6))

    assert check.missing_quarters == ["2026Q4"]
    assert check.quarters[1].uncovered == [(date(2026, 10, 1), date(2026, 10, 2))]


# --------------------------------------------------------------------- CLI


def test_cli_registers_exchange_notice_commands() -> None:
    from collector.kr.cli.app import build_parser

    parser = build_parser()
    args = parser.parse_args(
        ["dart", "backfill-exchange-notices", "--from", "2008-01-01", "--max-requests", "100"]
    )
    assert args.from_date == date(2008, 1, 1)
    assert args.to_date is None
    assert args.max_requests == 100
    assert args.rate_limit_seconds == 0.3

    args = parser.parse_args(["dart", "sync-exchange-notices"])
    assert args.lookback_days == 7

    args = parser.parse_args(["dart", "check-exchange-notices", "--to", "2026-10-06"])
    assert args.to_date == date(2026, 10, 6)

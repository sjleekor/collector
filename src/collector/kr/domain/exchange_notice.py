"""Domain types for OpenDART 거래소공시 (``list.json`` with ``pblntf_ty=I``).

관리종목 지정·매매거래정지·상장폐지 같은 시장조치는 KRX 시장본부가 내는 공시이고,
DART가 그것을 거래소공시(I)로 받아 접수일과 함께 준다. 종목이 아니라 **날짜 구간**으로
시장 전체를 받는다 (``corp_code`` 없이 요청하면 구간이 3개월을 넘을 수 없다).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from collector.kr.domain.enums import Source

PBLNTF_TY_EXCHANGE = "I"

# OpenDART rejects a window longer than 3 months when ``corp_code`` is absent.
# Calendar quarters (<= 92 days) are the largest window verified to work
# (2020Q1 = 213 pages / 21,219 rows, 2008Q1 = 14,868 rows).
WINDOW_STATUS_RUNNING = "running"
WINDOW_STATUS_DONE = "done"
WINDOW_STATUS_FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ExchangeNoticeRecord:
    """One raw 거래소공시 row. ``rcept_no`` alone identifies a receipt."""

    rcept_no: str
    corp_code: str
    corp_name: str
    stock_code: str
    corp_cls: str
    report_nm: str
    flr_nm: str
    rcept_dt: date | None
    rm: str
    pblntf_ty: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(slots=True)
class ExchangeNoticePage:
    """Outcome of one ``list.json`` page request (not aggregated)."""

    bgn_de: date
    end_de: date
    page_no: int
    records: list[ExchangeNoticeRecord] = field(default_factory=list)
    total_page: int = 0
    total_count: int = 0
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class ExchangeNoticeWindow:
    """Progress ledger for one ``[bgn_de, end_de]`` request window.

    ``pages_done`` pages are already upserted, so an interrupted window resumes
    at ``pages_done + 1``. ``status == done`` means every page up to
    ``total_page`` was fetched and the stored row count reached ``total_count``.
    """

    bgn_de: date
    end_de: date
    pblntf_ty: str = PBLNTF_TY_EXCHANGE
    status: str = WINDOW_STATUS_RUNNING
    total_count: int = 0
    total_page: int = 0
    pages_done: int = 0
    rows_fetched: int = 0
    requests: int = 0
    last_error: str | None = None
    started_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(slots=True)
class ExchangeNoticeSyncResult:
    """Outcome of one backfill / incremental run."""

    windows_total: int = 0
    windows_completed: int = 0
    windows_skipped: int = 0
    windows_failed: int = 0
    requests_attempted: int = 0
    rows_upserted: int = 0
    stopped_reason: str | None = None  # None | "max_requests"
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class QuarterCheck:
    """Coverage of one calendar quarter, as seen by ``check``."""

    quarter: str  # e.g. "2020Q1"
    bgn_de: date
    end_de: date  # clipped to the check's ``through`` date
    uncovered: list[tuple[date, date]] = field(default_factory=list)
    notice_rows: int = 0
    window_total_count: int | None = None  # only for a window exactly equal to the quarter
    window_total_page: int | None = None
    window_pages_done: int | None = None
    problems: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ExchangeNoticeCheck:
    """Result of the 전체 구간 페이지·건수 확인 / 빠진 분기 검사."""

    start: date
    through: date
    quarters: list[QuarterCheck] = field(default_factory=list)
    unfinished_windows: list[ExchangeNoticeWindow] = field(default_factory=list)

    @property
    def missing_quarters(self) -> list[str]:
        return [q.quarter for q in self.quarters if q.uncovered]

    @property
    def problem_quarters(self) -> list[str]:
        return [q.quarter for q in self.quarters if q.problems]

    @property
    def total_rows(self) -> int:
        return sum(q.notice_rows for q in self.quarters)

    @property
    def ok(self) -> bool:
        return not self.problem_quarters and not self.unfinished_windows

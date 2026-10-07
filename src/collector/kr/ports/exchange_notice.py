"""Ports for 거래소공시 (``pblntf_ty=I``) collection."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from collector.kr.domain.exchange_notice import (
    ExchangeNoticePage,
    ExchangeNoticeRecord,
    ExchangeNoticeWindow,
)


class ExchangeNoticePageProvider(Protocol):
    """Fetches one page of the market-wide 거래소공시 list."""

    def fetch_page(self, bgn_de: date, end_de: date, page_no: int) -> ExchangeNoticePage: ...


class ExchangeNoticeStore(Protocol):
    """What the sync/check services need from storage."""

    def get_window(self, bgn_de: date, end_de: date) -> ExchangeNoticeWindow | None: ...

    def save_window(self, window: ExchangeNoticeWindow) -> None: ...

    def list_windows(self, start: date, end: date) -> list[ExchangeNoticeWindow]:
        """Windows overlapping ``[start, end]``, ordered by ``bgn_de, end_de``."""
        ...

    def upsert_notices(self, records: list[ExchangeNoticeRecord]) -> int: ...

    def count_notices(self, bgn_de: date, end_de: date) -> int:
        """Stored rows with ``rcept_dt`` inside ``[bgn_de, end_de]``."""
        ...

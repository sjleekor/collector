"""OpenDART 거래소공시 market-wide page fetcher (``list.json``, ``pblntf_ty=I``).

종목별이 아니라 날짜 구간으로 시장 전체를 받는다. 한 번에 한 페이지만 가져오고,
페이지 순회·이어 받기는 서비스가 맡는다 (페이지마다 적재해야 중단 지점을 남길 수 있다).
``pblntf_detail_ty``(I001-I006)는 total_count가 안 변해 필터가 안 먹는다 (2026-09-24 실측)
— 쓰지 않는다.
"""

from __future__ import annotations

from datetime import date

from collector.kr.adapters.opendart_common import (
    FILING_RECEIPT_POLICY,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from collector.kr.domain.enums import Source
from collector.kr.domain.exchange_notice import (
    PBLNTF_TY_EXCHANGE,
    ExchangeNoticePage,
    ExchangeNoticeRecord,
)
from collector.kr.util.time import now_kst

OPENDART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"

# OpenDART caps page_count at 100.
PAGE_SIZE = 100


def _parse_yyyymmdd(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def parse_exchange_notice_page(
    payload: dict[str, object],
    *,
    pblntf_ty: str = PBLNTF_TY_EXCHANGE,
) -> tuple[list[ExchangeNoticeRecord], int, int, int]:
    """Parse one ``list.json`` page into ``(records, page_no, total_page, total_count)``.

    Rows without an ``rcept_no`` are dropped — nothing could key them.
    """
    fetched_at = now_kst()
    records: list[ExchangeNoticeRecord] = []
    for row in payload.get("list", []) or []:
        if not isinstance(row, dict):
            continue
        rcept_no = str(row.get("rcept_no", "")).strip()
        if not rcept_no:
            continue
        records.append(
            ExchangeNoticeRecord(
                rcept_no=rcept_no,
                corp_code=str(row.get("corp_code", "")).strip(),
                corp_name=str(row.get("corp_name", "")).strip(),
                stock_code=str(row.get("stock_code", "")).strip(),
                corp_cls=str(row.get("corp_cls", "")).strip(),
                report_nm=str(row.get("report_nm", "")).strip(),
                flr_nm=str(row.get("flr_nm", "")).strip(),
                rcept_dt=_parse_yyyymmdd(row.get("rcept_dt")),
                rm=str(row.get("rm", "")).strip(),
                pblntf_ty=pblntf_ty,
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=dict(row),
            )
        )
    page_no = int(payload.get("page_no") or 1)
    total_page = int(payload.get("total_page") or page_no)
    total_count = int(payload.get("total_count") or 0)
    return records, page_no, total_page, total_count


class OpenDartExchangeNoticeProvider:
    """Fetch single pages of the market-wide 거래소공시 list."""

    def __init__(
        self,
        request_executor: OpenDartRequestExecutor,
        timeout_seconds: float = 30.0,
        pblntf_ty: str = PBLNTF_TY_EXCHANGE,
    ) -> None:
        self._request_executor = request_executor
        self._timeout_seconds = timeout_seconds
        self._pblntf_ty = pblntf_ty

    @property
    def request_executor(self) -> OpenDartRequestExecutor:
        """Expose the shared executor for run-level metrics."""
        return self._request_executor

    def fetch_page(self, bgn_de: date, end_de: date, page_no: int) -> ExchangeNoticePage:
        """Fetch one page for ``[bgn_de, end_de]``. Never raises on source errors."""
        page = ExchangeNoticePage(bgn_de=bgn_de, end_de=end_de, page_no=page_no)
        try:
            call_result = self._request_executor.fetch_bytes(
                endpoint_url=OPENDART_LIST_URL,
                params={
                    "bgn_de": bgn_de.strftime("%Y%m%d"),
                    "end_de": end_de.strftime("%Y%m%d"),
                    "pblntf_ty": self._pblntf_ty,
                    "page_no": str(page_no),
                    "page_count": str(PAGE_SIZE),
                },
                request_label=(
                    f"{self._pblntf_ty}:{bgn_de.isoformat()}:{end_de.isoformat()}:p{page_no}"
                ),
                parser=FILING_RECEIPT_POLICY.classify_json_payload,
                timeout_seconds=self._timeout_seconds,
            )
            apply_call_result_meta(page, call_result)
            if call_result.error or call_result.no_data:
                return page
            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                page.error = "OpenDART returned an unexpected JSON payload."
                return page
            records, returned_page_no, page.total_page, page.total_count = (
                parse_exchange_notice_page(payload, pblntf_ty=self._pblntf_ty)
            )
            page.records = records
            if returned_page_no != page_no:
                page.error = (
                    f"requested page {page_no} but OpenDART returned page {returned_page_no}"
                )
                page.records = []
            return page
        except Exception as exc:
            page.error = str(exc)
            return page

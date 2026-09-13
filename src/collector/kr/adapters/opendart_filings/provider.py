"""OpenDART disclosure-receipt-history (공시검색, list.json) adapter."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date

from collector.kr.adapters.opendart_common import (
    FILING_RECEIPT_POLICY,
    OpenDartCallResult,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from collector.kr.domain.enums import Source
from collector.kr.domain.models import DartCorp, DartFilingReceiptLine, DartFilingReceiptResult
from collector.kr.util.pipeline import sleep_with_jitter
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_FILING_RECEIPT_URL = "https://opendart.fss.or.kr/api/list.json"

# OpenDART caps page_count at 100. MAX_PAGES is a hard safety backstop (100 *
# 500 = 50,000 receipts for one corp/window), not an expected volume.
PAGE_SIZE = 100
MAX_PAGES = 500


def _parse_yyyymmdd(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def parse_filing_receipt_page(
    payload: dict[str, object],
    corp: DartCorp,
) -> tuple[list[DartFilingReceiptLine], int, int, int]:
    """Parse one page of a list.json response.

    Returns:
        (records, page_no, total_page, total_count) from this page.
    """
    fetched_at = now_kst()
    records: list[DartFilingReceiptLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        records.append(
            DartFilingReceiptLine(
                corp_code=str(row.get("corp_code", "")).strip() or corp.corp_code,
                ticker=corp.ticker or "",
                corp_name=str(row.get("corp_name", "")).strip(),
                stock_code=str(row.get("stock_code", "")).strip(),
                corp_cls=str(row.get("corp_cls", "")).strip(),
                report_nm=str(row.get("report_nm", "")).strip(),
                rcept_no=str(row.get("rcept_no", "")).strip(),
                flr_nm=str(row.get("flr_nm", "")).strip(),
                rcept_dt=_parse_yyyymmdd(row.get("rcept_dt")),
                rm=str(row.get("rm", "")).strip(),
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=dict(row),
            )
        )
    page_no = int(payload.get("page_no") or 1)
    total_page = int(payload.get("total_page") or page_no)
    total_count = int(payload.get("total_count") or 0)
    return records, page_no, total_page, total_count


class OpenDartFilingReceiptProvider:
    """Fetch OpenDART disclosure-receipt history, paginating internally."""

    def __init__(
        self,
        request_executor: OpenDartRequestExecutor,
        timeout_seconds: float = 30.0,
        page_delay_seconds: float = 0.2,
        sleep_fn: Callable[[float], None] = sleep_with_jitter,
    ) -> None:
        self._request_executor = request_executor
        self._timeout_seconds = timeout_seconds
        self._page_delay_seconds = page_delay_seconds
        self._sleep_fn = sleep_fn

    @property
    def request_executor(self) -> OpenDartRequestExecutor:
        """Expose the shared executor for run-level metrics."""
        return self._request_executor

    def _fetch_page(
        self,
        corp: DartCorp,
        bgn_de: date,
        end_de: date,
        page_no: int,
    ) -> OpenDartCallResult:
        return self._request_executor.fetch_bytes(
            endpoint_url=OPENDART_FILING_RECEIPT_URL,
            params={
                "corp_code": corp.corp_code,
                "bgn_de": bgn_de.strftime("%Y%m%d"),
                "end_de": end_de.strftime("%Y%m%d"),
                "page_no": str(page_no),
                "page_count": str(PAGE_SIZE),
            },
            request_label=f"{corp.ticker}:{bgn_de.isoformat()}:{end_de.isoformat()}:list:p{page_no}",
            parser=FILING_RECEIPT_POLICY.classify_json_payload,
            timeout_seconds=self._timeout_seconds,
        )

    def fetch_filing_receipts(
        self,
        corp: DartCorp,
        bgn_de: date,
        end_de: date,
    ) -> DartFilingReceiptResult:
        """Fetch every page of receipts for ``corp`` within ``[bgn_de, end_de]``.

        Returns a single aggregated result. If any page fails, the whole
        window is reported as failed (no partial upsert) so a later
        skip-if-present check never mistakes a partial fetch for complete.

        Sleeps ``page_delay_seconds`` *between* pages (never after the last
        one). Without it a heavy filer's window would burst every page
        back-to-back, making this the only OpenDART caller whose request rate
        is not paced 1:1 with its sleep — every other one sleeps once per HTTP
        request in its service loop.
        """
        all_records: list[DartFilingReceiptLine] = []
        total_count = 0
        page_no = 1
        try:
            while True:
                call_result = self._fetch_page(corp, bgn_de, end_de, page_no)
                if call_result.error or call_result.no_data:
                    result = DartFilingReceiptResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bgn_de=bgn_de,
                        end_de=end_de,
                        records=[] if call_result.error else all_records,
                        total_count=total_count,
                    )
                    return apply_call_result_meta(result, call_result)

                payload = call_result.parsed_payload
                if not isinstance(payload, dict):
                    raise RuntimeError("OpenDART returned an unexpected JSON payload.")
                records, returned_page_no, total_page, total_count = parse_filing_receipt_page(
                    payload, corp
                )
                all_records.extend(records)
                if returned_page_no >= total_page or page_no >= MAX_PAGES:
                    return DartFilingReceiptResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bgn_de=bgn_de,
                        end_de=end_de,
                        records=all_records,
                        total_count=total_count,
                    )
                if self._page_delay_seconds > 0:
                    self._sleep_fn(self._page_delay_seconds)
                page_no += 1
        except Exception as exc:
            return DartFilingReceiptResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bgn_de=bgn_de,
                end_de=end_de,
                error=str(exc),
            )

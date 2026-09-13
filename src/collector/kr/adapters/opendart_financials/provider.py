"""OpenDART single-company full financial-statement adapter."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

from collector.kr.adapters.opendart_common import (
    FINANCIAL_STATEMENT_POLICY,
    OpenDartCallResult,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartFinancialStatementResult,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_FINANCIAL_STATEMENT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    normalized = text.replace(",", "")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _parse_int(value: object) -> int | None:
    decimal_value = _parse_decimal(value)
    if decimal_value is None:
        return None
    return int(decimal_value)


def parse_fnltt_singl_acnt_all_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
    fs_div: str,
) -> DartFinancialStatementResult:
    """Parse an OpenDART financial-statement success (``status=000``) JSON payload.

    Non-success status codes are classified upstream by the endpoint policy,
    so this parser assumes the payload is a ``000`` response and only turns
    the ``list`` into raw rows.
    """
    fetched_at = now_kst()
    records: list[DartFinancialStatementLine] = []

    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        raw_payload = dict(row)
        records.append(
            DartFinancialStatementLine(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                sj_div=str(row.get("sj_div", "")).strip(),
                sj_nm=str(row.get("sj_nm", "")).strip(),
                account_id=str(row.get("account_id", "")).strip(),
                account_nm=str(row.get("account_nm", "")).strip(),
                account_detail=str(row.get("account_detail", "")).strip(),
                thstrm_nm=str(row.get("thstrm_nm", "")).strip(),
                thstrm_amount=_parse_decimal(row.get("thstrm_amount")),
                thstrm_add_amount=_parse_decimal(row.get("thstrm_add_amount")),
                frmtrm_nm=str(row.get("frmtrm_nm", "")).strip(),
                frmtrm_amount=_parse_decimal(row.get("frmtrm_amount")),
                frmtrm_q_nm=str(row.get("frmtrm_q_nm", "")).strip(),
                frmtrm_q_amount=_parse_decimal(row.get("frmtrm_q_amount")),
                frmtrm_add_amount=_parse_decimal(row.get("frmtrm_add_amount")),
                bfefrmtrm_nm=str(row.get("bfefrmtrm_nm", "")).strip(),
                bfefrmtrm_amount=_parse_decimal(row.get("bfefrmtrm_amount")),
                ord=_parse_int(row.get("ord")) or 0,
                currency=str(row.get("currency", "")).strip(),
                rcept_no=str(row.get("rcept_no", "")).strip(),
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=raw_payload,
            )
        )

    return DartFinancialStatementResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        fs_div=fs_div,
        records=records,
    )


class OpenDartFinancialStatementProvider:
    """Fetch single-company full financial statements from OpenDART."""

    def __init__(
        self,
        request_executor: OpenDartRequestExecutor,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._request_executor = request_executor
        self._timeout_seconds = timeout_seconds

    @property
    def request_executor(self) -> OpenDartRequestExecutor:
        """Expose the shared executor for run-level metrics."""
        return self._request_executor

    def _parse_financial_payload(self, payload_bytes: bytes) -> OpenDartCallResult:
        return FINANCIAL_STATEMENT_POLICY.classify_json_payload(payload_bytes)

    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        try:
            call_result = self._request_executor.fetch_bytes(
                endpoint_url=OPENDART_FINANCIAL_STATEMENT_URL,
                params={
                    "corp_code": corp.corp_code,
                    "bsns_year": str(bsns_year),
                    "reprt_code": reprt_code,
                    "fs_div": fs_div,
                },
                request_label=f"{corp.ticker}:{bsns_year}:{reprt_code}:{fs_div}",
                parser=self._parse_financial_payload,
                timeout_seconds=self._timeout_seconds,
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartFinancialStatementResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        fs_div=fs_div,
                    ),
                    call_result,
                )

            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            result = parse_fnltt_singl_acnt_all_response(
                payload, corp, bsns_year, reprt_code, fs_div
            )
            return apply_call_result_meta(result, call_result)
        except Exception as exc:
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error=str(exc),
            )

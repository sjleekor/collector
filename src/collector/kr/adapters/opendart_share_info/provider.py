"""OpenDART share-count / dividend / treasury-stock adapter."""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from collector.kr.adapters.opendart_common import (
    CAPITAL_CHANGE_POLICY,
    DIVIDEND_POLICY,
    SHARE_COUNT_POLICY,
    TREASURY_STOCK_POLICY,
    OpenDartCallResult,
    OpenDartEndpointPolicy,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    DartCapitalChangeLine,
    DartCapitalChangeResult,
    DartCorp,
    DartShareCountLine,
    DartShareCountResult,
    DartShareholderReturnLine,
    DartShareholderReturnResult,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_STOCK_COUNT_URL = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
OPENDART_DIVIDEND_URL = "https://opendart.fss.or.kr/api/alotMatter.json"
OPENDART_TREASURY_STOCK_URL = "https://opendart.fss.or.kr/api/tesstkAcqsDspsSttus.json"
OPENDART_CAPITAL_CHANGE_URL = "https://opendart.fss.or.kr/api/irdsSttus.json"

DIVIDEND_METRICS = {
    "thstrm": "당기",
    "frmtrm": "전기",
    "lwfr": "전전기",
}

TREASURY_STOCK_METRICS = {
    "bsis_qy": "기초수량",
    "change_qy_acqs": "취득변동수량",
    "change_qy_dsps": "처분변동수량",
    "change_qy_incnr": "소각변동수량",
    "trmend_qy": "기말수량",
}


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


_DATE_FORMATS = ("%Y-%m-%d", "%Y%m%d")


def _parse_date(value: object) -> date | None:
    """Parse an OpenDART date field.

    ``stlm_dt`` arrives ISO (``2025-03-31``), but irdsSttus returns
    ``isu_dcrs_de`` dot-separated (``2010.06.15``), so normalise the separator
    before parsing. Unparseable values log and yield NULL rather than raising:
    a response is parsed as a whole, so one bad field would otherwise discard
    every row for that (corp, year, report) — and the original string is kept
    verbatim in ``raw_payload`` either way.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    normalized = text.replace(".", "-").replace("/", "-")
    for date_format in _DATE_FORMATS:
        try:
            return datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue
    logger.warning("Unparseable OpenDART date %r; storing NULL", text)
    return None


def parse_stock_count_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartShareCountResult:
    """Parse an OpenDART stock-count success (``status=000``) payload."""
    fetched_at = now_kst()
    records: list[DartShareCountLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        records.append(
            DartShareCountLine(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=str(row.get("rcept_no", "")).strip(),
                corp_cls=str(row.get("corp_cls", "")).strip(),
                se=str(row.get("se", "")).strip(),
                isu_stock_totqy=_parse_int(row.get("isu_stock_totqy")),
                now_to_isu_stock_totqy=_parse_int(row.get("now_to_isu_stock_totqy")),
                now_to_dcrs_stock_totqy=_parse_int(row.get("now_to_dcrs_stock_totqy")),
                redc=str(row.get("redc", "")).strip(),
                profit_incnr=str(row.get("profit_incnr", "")).strip(),
                rdmstk_repy=str(row.get("rdmstk_repy", "")).strip(),
                etc=str(row.get("etc", "")).strip(),
                istc_totqy=_parse_int(row.get("istc_totqy")),
                tesstk_co=_parse_int(row.get("tesstk_co")),
                distb_stock_co=_parse_int(row.get("distb_stock_co")),
                stlm_dt=_parse_date(row.get("stlm_dt")),
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=dict(row),
            )
        )

    return DartShareCountResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        records=records,
    )


def parse_dividend_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartShareholderReturnResult:
    """Parse an OpenDART dividend success (``status=000``) payload."""
    fetched_at = now_kst()
    records: list[DartShareholderReturnLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        row_payload = dict(row)
        row_name = str(row.get("se", "")).strip()
        stock_knd = str(row.get("stock_knd", "")).strip()
        rcept_no = str(row.get("rcept_no", "")).strip()
        stlm_dt = _parse_date(row.get("stlm_dt"))

        for metric_code, metric_name in DIVIDEND_METRICS.items():
            raw_value = row.get(metric_code)
            text_value = "" if raw_value is None else str(raw_value).strip()
            numeric_value = _parse_decimal(raw_value)
            records.append(
                DartShareholderReturnLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    statement_type="dividend",
                    row_name=row_name,
                    stock_knd=stock_knd,
                    dim1="",
                    dim2="",
                    dim3="",
                    metric_code=metric_code,
                    metric_name=metric_name,
                    value_numeric=numeric_value,
                    value_text=text_value,
                    unit="",
                    rcept_no=rcept_no,
                    stlm_dt=stlm_dt,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                    raw_payload=row_payload,
                )
            )

    return DartShareholderReturnResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        statement_type="dividend",
        records=records,
    )


def parse_treasury_stock_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartShareholderReturnResult:
    """Parse an OpenDART treasury-stock success (``status=000``) payload."""
    fetched_at = now_kst()
    records: list[DartShareholderReturnLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        row_payload = dict(row)
        stock_knd = str(row.get("stock_knd", "")).strip()
        dim1 = str(row.get("acqs_mth1", "")).strip()
        dim2 = str(row.get("acqs_mth2", "")).strip()
        dim3 = str(row.get("acqs_mth3", "")).strip()
        row_name = "자기주식 취득 및 처분 현황"
        rcept_no = str(row.get("rcept_no", "")).strip()
        stlm_dt = _parse_date(row.get("stlm_dt"))

        for metric_code, metric_name in TREASURY_STOCK_METRICS.items():
            raw_value = row.get(metric_code)
            text_value = "" if raw_value is None else str(raw_value).strip()
            numeric_value = _parse_decimal(raw_value)
            records.append(
                DartShareholderReturnLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    statement_type="treasury_stock",
                    row_name=row_name,
                    stock_knd=stock_knd,
                    dim1=dim1,
                    dim2=dim2,
                    dim3=dim3,
                    metric_code=metric_code,
                    metric_name=metric_name,
                    value_numeric=numeric_value,
                    value_text=text_value,
                    unit="shares",
                    rcept_no=rcept_no,
                    stlm_dt=stlm_dt,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                    raw_payload=row_payload,
                )
            )

    return DartShareholderReturnResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        statement_type="treasury_stock",
        records=records,
    )


def parse_capital_change_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartCapitalChangeResult:
    """Parse an OpenDART capital-change success (``status=000``) payload."""
    fetched_at = now_kst()
    records: list[DartCapitalChangeLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        records.append(
            DartCapitalChangeLine(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=str(row.get("rcept_no", "")).strip(),
                corp_cls=str(row.get("corp_cls", "")).strip(),
                isu_dcrs_de=_parse_date(row.get("isu_dcrs_de")),
                isu_dcrs_stle=str(row.get("isu_dcrs_stle", "")).strip(),
                isu_dcrs_stock_knd=str(row.get("isu_dcrs_stock_knd", "")).strip(),
                isu_dcrs_qy=_parse_int(row.get("isu_dcrs_qy")),
                isu_dcrs_mstvdv_fval_amount=_parse_decimal(row.get("isu_dcrs_mstvdv_fval_amount")),
                isu_dcrs_mstvdv_fval_amount2=_parse_decimal(
                    row.get("isu_dcrs_mstvdv_fval_amount2")
                ),
                stlm_dt=_parse_date(row.get("stlm_dt")),
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=dict(row),
            )
        )

    return DartCapitalChangeResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        records=records,
    )


class OpenDartShareInfoProvider:
    """Fetch share-count and shareholder-return disclosures from OpenDART."""

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

    def _fetch_json(
        self,
        endpoint_url: str,
        policy: OpenDartEndpointPolicy,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> OpenDartCallResult:
        return self._request_executor.fetch_bytes(
            endpoint_url=endpoint_url,
            params={
                "corp_code": corp.corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
            request_label=(
                f"{corp.ticker}:{bsns_year}:{reprt_code}:{endpoint_url.rsplit('/', 1)[-1]}"
            ),
            parser=policy.classify_json_payload,
            timeout_seconds=self._timeout_seconds,
        )

    def fetch_share_count(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareCountResult:
        try:
            call_result = self._fetch_json(
                OPENDART_STOCK_COUNT_URL, SHARE_COUNT_POLICY, corp, bsns_year, reprt_code
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartShareCountResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                    ),
                    call_result,
                )

            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            result = parse_stock_count_response(payload, corp, bsns_year, reprt_code)
            return apply_call_result_meta(result, call_result)
        except Exception as exc:
            return DartShareCountResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                error=str(exc),
            )

    def fetch_dividend(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        try:
            call_result = self._fetch_json(
                OPENDART_DIVIDEND_URL, DIVIDEND_POLICY, corp, bsns_year, reprt_code
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartShareholderReturnResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        statement_type="dividend",
                    ),
                    call_result,
                )

            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            result = parse_dividend_response(payload, corp, bsns_year, reprt_code)
            return apply_call_result_meta(result, call_result)
        except Exception as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="dividend",
                error=str(exc),
            )

    def fetch_treasury_stock(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        try:
            call_result = self._fetch_json(
                OPENDART_TREASURY_STOCK_URL,
                TREASURY_STOCK_POLICY,
                corp,
                bsns_year,
                reprt_code,
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartShareholderReturnResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        statement_type="treasury_stock",
                    ),
                    call_result,
                )

            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            result = parse_treasury_stock_response(payload, corp, bsns_year, reprt_code)
            return apply_call_result_meta(result, call_result)
        except Exception as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="treasury_stock",
                error=str(exc),
            )

    def fetch_capital_change(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartCapitalChangeResult:
        try:
            call_result = self._fetch_json(
                OPENDART_CAPITAL_CHANGE_URL, CAPITAL_CHANGE_POLICY, corp, bsns_year, reprt_code
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartCapitalChangeResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                    ),
                    call_result,
                )

            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            result = parse_capital_change_response(payload, corp, bsns_year, reprt_code)
            return apply_call_result_meta(result, call_result)
        except Exception as exc:
            return DartCapitalChangeResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                error=str(exc),
            )

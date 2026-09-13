"""OpenDART DS002 periodic-report extras — employees, control, audit opinion.

Five endpoints, one shape.  Each takes ``corp_code`` + ``bsns_year`` +
``reprt_code`` and returns a ``list`` of flat rows, so this adapter is a table
of endpoints rather than five near-identical methods.

Two things about DS002 that the schema depends on, both measured rather than
read from the guide (``poc/n6_periodic_extras.md``):

**There is no way to ask for a particular vintage.**  The request takes no
``rcept_no``; the response merely reports one.  So asking today for FY2016
returns the *latest corrected* version, not what was on file in 2017.  Storing
``rcept_no`` is what makes the corrections observable going forward, and it is
why it belongs in the unique key — otherwise a correction overwrites the row it
corrects, and for audit opinions and changes of control the correction is the
signal.

**One request can answer for several years.**  ``hyslrChgSttus`` returns the
accumulated change history, not just the requested year, and the audit-opinion
endpoint returns three years at a time.  That is what takes the backfill from
162,000 calls to 83,700; the year arithmetic lives in the service, but it is
this response shape that permits it.
"""

from __future__ import annotations

import logging

from collector.kr.adapters.opendart_common import (
    OpenDartCallResult,
    OpenDartEndpointPolicy,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from collector.kr.adapters.opendart_common.policy import OPENDART_REQUEST_INVALID_STATUSES
from collector.kr.domain.enums import PeriodicExtraStatement, Source
from collector.kr.domain.models import (
    DartCorp,
    DartPeriodicExtraLine,
    DartPeriodicExtraResult,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_API_BASE = "https://opendart.fss.or.kr/api"

#: Endpoint name per statement type.  ``013`` is "no data" for all five and
#: ``014`` means the argument combination is not offered, which is a request
#: problem rather than a transient one.
ENDPOINTS: dict[PeriodicExtraStatement, str] = {
    PeriodicExtraStatement.EMPLOYEE: "empSttus",
    PeriodicExtraStatement.EXECUTIVE: "exctvSttus",
    PeriodicExtraStatement.MAJOR_SHAREHOLDER: "hyslrSttus",
    PeriodicExtraStatement.MAJOR_CHANGE: "hyslrChgSttus",
    PeriodicExtraStatement.AUDIT_OPINION: "accnutAdtorNmNdAdtOpinion",
}

POLICIES: dict[PeriodicExtraStatement, OpenDartEndpointPolicy] = {
    statement: OpenDartEndpointPolicy(
        endpoint=endpoint,
        payload_kind="json",
        no_data_statuses=frozenset({"013"}),
        request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
    )
    for statement, endpoint in ENDPOINTS.items()
}


def parse_periodic_extra_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
    statement_type: PeriodicExtraStatement,
) -> DartPeriodicExtraResult:
    """Parse a DS002 success (``status=000``) payload into raw rows.

    Row identity is the response's own ordering (``row_ordinal``).  One response
    carries several rows — by division and gender for employees, by related
    party for shareholders — and those labels are rewritten between years, so a
    natural key built from them breaks joins silently.  Repeated requests were
    checked and the order is stable.

    A row without ``rcept_no`` fails the whole response rather than being stored
    with a blank one: a blank would collapse every vintage of that report onto
    one row in the unique key, which is the exact loss this table exists to
    prevent.
    """
    fetched_at = now_kst()
    rows = payload.get("list", [])
    if not isinstance(rows, list):
        return DartPeriodicExtraResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type=statement_type,
            error="OpenDART returned a non-list `list` field",
        )

    records: list[DartPeriodicExtraLine] = []
    for ordinal, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        rcept_no = str(row.get("rcept_no", "")).strip()
        if not rcept_no:
            return DartPeriodicExtraResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type=statement_type,
                response_rows=len(rows),
                error=(
                    f"row {ordinal} of {ENDPOINTS[statement_type]} has no rcept_no; "
                    "storing it would merge this report's vintages into one row"
                ),
            )
        records.append(
            DartPeriodicExtraLine(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=rcept_no,
                statement_type=statement_type,
                row_ordinal=ordinal,
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=dict(row),
            )
        )

    return DartPeriodicExtraResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        statement_type=statement_type,
        records=records,
        response_rows=len(rows),
    )


class OpenDartPeriodicExtrasProvider:
    """Fetch DS002 periodic-report extras from OpenDART.

    Conforms to
    :class:`~collector.kr.ports.periodic_extras.PeriodicExtrasProvider`.
    """

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

    def fetch_periodic_extra(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        statement_type: PeriodicExtraStatement,
    ) -> DartPeriodicExtraResult:
        """Retrieve one DS002 disclosure.

        Args:
            corp: Target company, carrying ``corp_code`` and ``ticker``.
            bsns_year: Business year.
            reprt_code: Report code, normally ``11011`` (사업보고서).
            statement_type: Which disclosure to fetch.

        Returns:
            ``DartPeriodicExtraResult`` with rows, ``no_data``, or ``error``.
            Never raises for an upstream failure.
        """
        try:
            call_result = self._fetch_json(corp, bsns_year, reprt_code, statement_type)
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartPeriodicExtraResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        statement_type=statement_type,
                    ),
                    call_result,
                )

            payload = call_result.parsed_payload
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            result = parse_periodic_extra_response(
                payload, corp, bsns_year, reprt_code, statement_type
            )
            return apply_call_result_meta(result, call_result)
        except Exception as exc:
            return DartPeriodicExtraResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type=statement_type,
                error=str(exc),
            )

    def _fetch_json(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        statement_type: PeriodicExtraStatement,
    ) -> OpenDartCallResult:
        endpoint = ENDPOINTS[statement_type]
        return self._request_executor.fetch_bytes(
            endpoint_url=f"{OPENDART_API_BASE}/{endpoint}.json",
            params={
                "corp_code": corp.corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
            request_label=f"{corp.ticker}:{bsns_year}:{reprt_code}:{endpoint}",
            parser=POLICIES[statement_type].classify_json_payload,
            timeout_seconds=self._timeout_seconds,
        )

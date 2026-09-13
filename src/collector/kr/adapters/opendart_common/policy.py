"""OpenDART endpoint-level error-handling policies.

Each ``OpenDartEndpointPolicy`` captures the per-endpoint rules that drive
``classify_status``: which status codes map to ``no_data`` vs. ``request_invalid``
for that endpoint, and what kind of payload (JSON or XML/ZIP) the endpoint
returns. Centralising these rules here lets providers stay small: they pick
a policy, apply it to a decoded payload, and parse only the success shape.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from collector.kr.adapters.opendart_common.client import (
    OPENDART_REQUEST_INVALID_STATUSES,
    OpenDartCallResult,
    classify_status,
    decode_json_payload,
    extract_xml_status,
)


def apply_call_result_meta[T](result: T, call_result: OpenDartCallResult) -> T:
    """Copy transport-level metadata from ``call_result`` onto a domain result.

    Sets any of ``status_code``, ``error``, ``no_data``, ``retryable``,
    ``retry_after_seconds``, ``exhaustion_reason`` that exist on the target.
    Missing fields are skipped so this is safe for both legacy results
    (`DartCorpCodeResult` with ``records`` / ``error`` only) and the fuller
    transport-meta results.
    """
    for name in (
        "status_code",
        "error",
        "no_data",
        "retryable",
        "retry_after_seconds",
        "exhaustion_reason",
    ):
        if hasattr(result, name) and hasattr(call_result, name):
            setattr(result, name, getattr(call_result, name))
    return result


@dataclass(slots=True, frozen=True)
class OpenDartEndpointPolicy:
    """Per-endpoint status-code handling rules."""

    endpoint: str
    payload_kind: str  # "json" | "xml_zip"
    no_data_statuses: frozenset[str] = field(default_factory=frozenset)
    request_invalid_statuses: frozenset[str] = field(
        default_factory=lambda: frozenset(OPENDART_REQUEST_INVALID_STATUSES)
    )

    def classify_json_payload(self, payload_bytes: bytes) -> OpenDartCallResult:
        """Decode a JSON OpenDART response and classify its ``status`` field."""
        try:
            payload = decode_json_payload(payload_bytes)
        except json.JSONDecodeError:
            return OpenDartCallResult(
                error=f"OpenDART returned invalid JSON: {payload_bytes[:200]!r}",
            )
        except Exception as exc:
            return OpenDartCallResult(error=str(exc))

        status = str(payload.get("status", "")).strip()
        message = str(payload.get("message", "")).strip()
        return classify_status(
            status_code=status,
            message=message,
            no_data_statuses=self.no_data_statuses,
            request_invalid_statuses=self.request_invalid_statuses,
            payload=payload_bytes,
            parsed_payload=payload,
        )

    def classify_xml_zip_payload(self, payload_bytes: bytes) -> OpenDartCallResult:
        """Handle a payload that is either a ZIP success or an XML error body."""
        if payload_bytes.startswith(b"PK"):
            return OpenDartCallResult(payload=payload_bytes, status_code="000")

        status_message = extract_xml_status(payload_bytes)
        if status_message is None:
            return OpenDartCallResult(
                error=f"OpenDART returned a non-ZIP payload: {payload_bytes[:120]!r}",
            )

        status, message = status_message
        return classify_status(
            status_code=status,
            message=message,
            no_data_statuses=self.no_data_statuses,
            request_invalid_statuses=self.request_invalid_statuses,
            payload=payload_bytes,
        )


CORP_CODE_POLICY = OpenDartEndpointPolicy(
    endpoint="corpCode",
    payload_kind="xml_zip",
    no_data_statuses=frozenset(),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

COMPANY_PROFILE_POLICY = OpenDartEndpointPolicy(
    endpoint="company",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

FINANCIAL_STATEMENT_POLICY = OpenDartEndpointPolicy(
    endpoint="fnlttSinglAcntAll",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

SHARE_COUNT_POLICY = OpenDartEndpointPolicy(
    endpoint="stockTotqySttus",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

DIVIDEND_POLICY = OpenDartEndpointPolicy(
    endpoint="alotMatter",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

TREASURY_STOCK_POLICY = OpenDartEndpointPolicy(
    endpoint="tesstkAcqsDspsSttus",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

XBRL_POLICY = OpenDartEndpointPolicy(
    endpoint="fnlttXbrl",
    payload_kind="xml_zip",
    no_data_statuses=frozenset({"013", "014"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES),
)

CAPITAL_CHANGE_POLICY = OpenDartEndpointPolicy(
    endpoint="irdsSttus",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

FILING_RECEIPT_POLICY = OpenDartEndpointPolicy(
    endpoint="list",
    payload_kind="json",
    no_data_statuses=frozenset({"013"}),
    request_invalid_statuses=frozenset(OPENDART_REQUEST_INVALID_STATUSES | {"014"}),
)

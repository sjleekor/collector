"""OpenDART XBRL ZIP adapter."""

from __future__ import annotations

import io
import logging
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation

from collector.kr.adapters.opendart_common import (
    XBRL_POLICY,
    OpenDartCallResult,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from collector.kr.domain.enums import Source
from collector.kr.domain.models import DartCorp, DartXbrlDocument, DartXbrlFactLine, DartXbrlResult
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_XBRL_URL = "https://opendart.fss.or.kr/api/fnlttXbrl.xml"
XBRLI_NS = "http://www.xbrl.org/2003/instance"
XLINK_NS = "http://www.w3.org/1999/xlink"


def _parse_decimal(value: str) -> Decimal | None:
    text = value.strip()
    if not text or text == "-":
        return None
    normalized = text.replace(",", "")
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _local_name(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag.split("}", 1)[1]
    if ":" in tag:
        return tag.split(":", 1)[1]
    return tag


def _namespace_uri(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}", 1)[0]
    return ""


def _parse_xml_with_namespaces(xml_bytes: bytes) -> tuple[ET.Element, dict[str, str]]:
    ns_map: dict[str, str] = {}
    root: ET.Element | None = None
    for event, payload in ET.iterparse(io.BytesIO(xml_bytes), events=("start", "start-ns")):
        if event == "start-ns":
            prefix, uri = payload
            ns_map[prefix or ""] = uri
        elif root is None:
            root = payload
    if root is None:
        raise RuntimeError("XBRL XML root not found.")
    return root, ns_map


def _build_concept_id(tag: str, prefix_by_uri: dict[str, str]) -> tuple[str, str, str]:
    local_name = _local_name(tag)
    namespace_uri = _namespace_uri(tag)
    prefix = prefix_by_uri.get(namespace_uri, "")
    concept_id = f"{prefix}_{local_name}" if prefix else local_name
    return concept_id, local_name, namespace_uri


def _parse_contexts(root: ET.Element) -> dict[str, dict[str, object]]:
    contexts: dict[str, dict[str, object]] = {}
    for context in root.findall(f".//{{{XBRLI_NS}}}context"):
        context_id = context.attrib.get("id", "").strip()
        if not context_id:
            continue

        period = context.find(f"./{{{XBRLI_NS}}}period")
        start_date = None
        end_date = None
        instant_date = None
        context_type = ""
        if period is not None:
            start_date = _parse_date(period.findtext(f"./{{{XBRLI_NS}}}startDate"))
            end_date = _parse_date(period.findtext(f"./{{{XBRLI_NS}}}endDate"))
            instant_date = _parse_date(period.findtext(f"./{{{XBRLI_NS}}}instant"))
            if instant_date is not None:
                context_type = "instant"
            elif start_date is not None or end_date is not None:
                context_type = "duration"

        dimensions: list[str] = []
        for member in context.iter():
            local_name = _local_name(member.tag)
            if local_name not in {"explicitMember", "typedMember"}:
                continue
            dimension = member.attrib.get("dimension", "").strip()
            member_text = "".join(member.itertext()).strip()
            dimensions.append(f"{dimension}={member_text}" if dimension else member_text)
        contexts[context_id] = {
            "context_type": context_type,
            "period_start": start_date,
            "period_end": end_date,
            "instant_date": instant_date,
            "dimensions": dimensions,
        }
    return contexts


def _parse_units(root: ET.Element) -> dict[str, dict[str, str]]:
    units: dict[str, dict[str, str]] = {}
    for unit in root.findall(f".//{{{XBRLI_NS}}}unit"):
        unit_id = unit.attrib.get("id", "").strip()
        if not unit_id:
            continue
        measures = [text.strip() for text in unit.itertext() if text and text.strip()]
        units[unit_id] = {
            "unit_measure": " / ".join(measures),
        }
    return units


def _parse_label_ko_map(xml_bytes: bytes, prefix_by_uri: dict[str, str]) -> dict[str, str]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return {}

    loc_map: dict[str, str] = {}
    label_text_map: dict[str, str] = {}
    arcs: list[tuple[str, str]] = []

    for element in root.iter():
        local_name = _local_name(element.tag)
        if local_name == "loc":
            label = element.attrib.get(f"{{{XLINK_NS}}}label", "").strip()
            href = element.attrib.get(f"{{{XLINK_NS}}}href", "").strip()
            if not label or "#" not in href:
                continue
            fragment = href.split("#", 1)[1].strip()
            if "_" in fragment:
                concept_id = fragment
            else:
                concept_id = fragment
            loc_map[label] = concept_id
        elif local_name == "label":
            label_key = element.attrib.get(f"{{{XLINK_NS}}}label", "").strip()
            text = "".join(element.itertext()).strip()
            if label_key and text:
                label_text_map[label_key] = text
        elif local_name == "labelArc":
            from_key = element.attrib.get(f"{{{XLINK_NS}}}from", "").strip()
            to_key = element.attrib.get(f"{{{XLINK_NS}}}to", "").strip()
            if from_key and to_key:
                arcs.append((from_key, to_key))

    concept_labels: dict[str, list[str]] = defaultdict(list)
    for from_key, to_key in arcs:
        concept_fragment = loc_map.get(from_key, "")
        label_text = label_text_map.get(to_key, "")
        if not concept_fragment or not label_text:
            continue
        concept_labels[concept_fragment].append(label_text)

    normalized: dict[str, str] = {}
    for concept_id, labels in concept_labels.items():
        if labels:
            normalized[concept_id] = labels[0]

    if normalized:
        return normalized

    fallback: dict[str, str] = {}
    for concept_id, labels in concept_labels.items():
        if not labels or "_" in concept_id:
            continue
        for prefix in prefix_by_uri.values():
            if prefix:
                fallback[f"{prefix}_{concept_id}"] = labels[0]
    return fallback


def parse_xbrl_zip_response(
    payload_bytes: bytes,
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
    rcept_no: str,
) -> DartXbrlResult:
    """Parse an OpenDART XBRL ZIP success payload into document metadata and fact rows.

    Non-ZIP error bodies are classified upstream by ``XBRL_POLICY`` before this
    parser runs, so this function assumes ``payload_bytes`` is a valid ZIP.
    """
    with zipfile.ZipFile(io.BytesIO(payload_bytes)) as archive:
        entry_names = sorted(archive.namelist())
        instance_name = next((name for name in entry_names if name.endswith(".xbrl")), "")
        label_ko_name = next((name for name in entry_names if name.endswith("_lab-ko.xml")), "")
        if not instance_name:
            return DartXbrlResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=rcept_no,
                error="XBRL ZIP did not contain an instance .xbrl file.",
            )

        instance_bytes = archive.read(instance_name)
        root, ns_map = _parse_xml_with_namespaces(instance_bytes)
        prefix_by_uri = {uri: prefix for prefix, uri in ns_map.items()}
        contexts = _parse_contexts(root)
        units = _parse_units(root)
        labels = (
            _parse_label_ko_map(archive.read(label_ko_name), prefix_by_uri) if label_ko_name else {}
        )

        fetched_at = now_kst()
        document = DartXbrlDocument(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            rcept_no=rcept_no,
            zip_entry_count=len(entry_names),
            instance_document_name=instance_name,
            label_ko_document_name=label_ko_name,
            source=Source.OPENDART,
            fetched_at=fetched_at,
            raw_payload={
                "entry_names": entry_names,
            },
        )

        facts: list[DartXbrlFactLine] = []
        for element in root.iter():
            if not isinstance(element.tag, str):
                continue
            context_id = element.attrib.get("contextRef", "").strip()
            if not context_id:
                continue

            concept_id, concept_name, namespace_uri = _build_concept_id(element.tag, prefix_by_uri)
            context = contexts.get(context_id, {})
            unit_id = element.attrib.get("unitRef", "").strip()
            unit = units.get(unit_id, {})
            value_text = "".join(element.itertext()).strip()
            is_nil = (
                element.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}nil", "").lower()
                == "true"
            )

            facts.append(
                DartXbrlFactLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no=rcept_no,
                    concept_id=concept_id,
                    concept_name=concept_name,
                    namespace_uri=namespace_uri,
                    context_id=context_id,
                    context_type=str(context.get("context_type", "")),
                    period_start=context.get("period_start"),
                    period_end=context.get("period_end"),
                    instant_date=context.get("instant_date"),
                    dimensions=list(context.get("dimensions", [])),
                    unit_id=unit_id,
                    unit_measure=str(unit.get("unit_measure", "")),
                    decimals=element.attrib.get("decimals", "").strip(),
                    value_numeric=None if is_nil else _parse_decimal(value_text),
                    value_text=value_text,
                    is_nil=is_nil,
                    label_ko=labels.get(concept_id, ""),
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                    raw_payload={
                        "attributes": dict(element.attrib),
                        "tag": element.tag,
                    },
                )
            )

    return DartXbrlResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        rcept_no=rcept_no,
        document=document,
        facts=facts,
    )


class OpenDartXbrlProvider:
    """Fetch and parse OpenDART XBRL ZIP documents."""

    def __init__(
        self,
        request_executor: OpenDartRequestExecutor,
        timeout_seconds: float = 60.0,
    ) -> None:
        self._request_executor = request_executor
        self._timeout_seconds = timeout_seconds

    @property
    def request_executor(self) -> OpenDartRequestExecutor:
        """Expose the shared executor for run-level metrics."""
        return self._request_executor

    def _parse_xbrl_payload(self, payload_bytes: bytes) -> OpenDartCallResult:
        return XBRL_POLICY.classify_xml_zip_payload(payload_bytes)

    def fetch_xbrl(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        rcept_no: str,
    ) -> DartXbrlResult:
        try:
            call_result = self._request_executor.fetch_bytes(
                endpoint_url=OPENDART_XBRL_URL,
                params={"rcept_no": rcept_no},
                request_label=f"{corp.ticker}:{bsns_year}:{reprt_code}:{rcept_no}",
                parser=self._parse_xbrl_payload,
                timeout_seconds=self._timeout_seconds,
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(
                    DartXbrlResult(
                        corp_code=corp.corp_code,
                        ticker=corp.ticker or "",
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        rcept_no=rcept_no,
                    ),
                    call_result,
                )

            result = parse_xbrl_zip_response(
                call_result.payload or b"",
                corp,
                bsns_year,
                reprt_code,
                rcept_no,
            )
            return apply_call_result_meta(result, call_result)
        except zipfile.BadZipFile:
            preview = (call_result.payload or b"")[:120]
            return DartXbrlResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=rcept_no,
                error=f"OpenDART returned an invalid ZIP payload: {preview!r}",
            )
        except Exception as exc:
            return DartXbrlResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=rcept_no,
                error=str(exc),
            )

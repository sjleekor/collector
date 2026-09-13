"""OpenDART corporation-code master adapter."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date
from xml.etree import ElementTree as ET

from collector.kr.adapters.opendart_common import (
    COMPANY_PROFILE_POLICY,
    CORP_CODE_POLICY,
    OpenDartCallResult,
    OpenDartRequestExecutor,
    apply_call_result_meta,
    decode_json_payload,
)
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CompanyProfile,
    CompanyProfileResult,
    DartCorp,
    DartCorpCodeResult,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
OPENDART_COMPANY_PROFILE_URL = "https://opendart.fss.or.kr/api/company.json"


def _parse_yyyymmdd(value: str | None) -> date | None:
    """Parse an OpenDART ``YYYYMMDD`` string, tolerating junk."""
    text = (value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:8]}")
    except ValueError:
        return None


def parse_corp_code_zip_bytes(payload: bytes) -> list[DartCorp]:
    """Parse OpenDART corpCode zip bytes into ``DartCorp`` rows."""
    fetched_at = now_kst()

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        xml_names = [name for name in archive.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError("OpenDART corpCode payload contained no XML file.")

        xml_bytes = archive.read(xml_names[0])

    root = ET.fromstring(xml_bytes)
    records: list[DartCorp] = []

    for item in root.findall(".//list"):
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        ticker = (item.findtext("stock_code") or "").strip() or None
        modify_date_raw = (item.findtext("modify_date") or "").strip()

        if not corp_code or not corp_name:
            continue

        modify_date = None
        if modify_date_raw:
            modify_date = date.fromisoformat(
                f"{modify_date_raw[:4]}-{modify_date_raw[4:6]}-{modify_date_raw[6:8]}"
            )

        records.append(
            DartCorp(
                corp_code=corp_code,
                corp_name=corp_name,
                ticker=ticker,
                market=None,
                stock_name=corp_name,
                modify_date=modify_date,
                is_active=False,
                source=Source.OPENDART,
                fetched_at=fetched_at,
            )
        )

    return records


class OpenDartCorpCodeProvider:
    """Fetch the OpenDART corporation-code master zip file."""

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

    def _parse_corp_code_payload(self, payload: bytes) -> OpenDartCallResult:
        return CORP_CODE_POLICY.classify_xml_zip_payload(payload)

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        """Download and parse the OpenDART corp-code master."""
        try:
            call_result = self._request_executor.fetch_bytes(
                endpoint_url=OPENDART_CORP_CODE_URL,
                params={},
                request_label="corp_code_master",
                parser=self._parse_corp_code_payload,
                timeout_seconds=self._timeout_seconds,
            )
            if call_result.error:
                return apply_call_result_meta(DartCorpCodeResult(), call_result)

            records = parse_corp_code_zip_bytes(call_result.payload or b"")
            return apply_call_result_meta(DartCorpCodeResult(records=records), call_result)
        except zipfile.BadZipFile:
            preview = (call_result.payload or b"")[:120]
            return DartCorpCodeResult(
                error=f"OpenDART returned an invalid ZIP payload: {preview!r}"
            )
        except Exception as exc:
            return DartCorpCodeResult(error=str(exc))

    def fetch_company_profile(self, corp: DartCorp) -> CompanyProfileResult:
        """Fetch ``company.json`` (DS001) for one corporation.

        A separate endpoint from ``corpCode.xml``: the bulk zip carries only
        corp_code / corp_name / stock_code / modify_date, so industry,
        incorporation date and fiscal-year-end need one call per corporation.

        Args:
            corp: Corporation to profile.

        Returns:
            ``CompanyProfileResult`` with the profile or an error.  Never raises.
        """
        try:
            call_result = self._request_executor.fetch_bytes(
                endpoint_url=OPENDART_COMPANY_PROFILE_URL,
                params={"corp_code": corp.corp_code},
                request_label=f"{corp.ticker or corp.corp_code}:company",
                parser=COMPANY_PROFILE_POLICY.classify_json_payload,
                timeout_seconds=self._timeout_seconds,
            )
            if call_result.error or call_result.no_data:
                return apply_call_result_meta(CompanyProfileResult(), call_result)

            payload = decode_json_payload(call_result.payload or b"{}")

            profile = CompanyProfile(
                corp_code=corp.corp_code,
                # Blank string and missing field both mean "no industry"; keep
                # them as NULL so the coverage metric is honest.
                induty_code=(payload.get("induty_code") or "").strip() or None,
                corp_cls=(payload.get("corp_cls") or "").strip() or None,
                est_dt=_parse_yyyymmdd(payload.get("est_dt")),
                acc_mt=(payload.get("acc_mt") or "").strip() or None,
                raw_payload=payload,
                fetched_at=now_kst(),
                # F-1: the monthly history keeps the response's own identity
                # fields, not the corp master's. A delisting shows up here as
                # corp_cls 'E' and an emptied stock_code before the master's
                # own is_active flips.
                ticker=(payload.get("stock_code") or "").strip() or None,
                corp_name=(payload.get("corp_name") or "").strip() or None,
                stock_name=(payload.get("stock_name") or "").strip() or None,
            )
            return apply_call_result_meta(CompanyProfileResult(profile=profile), call_result)
        except Exception as exc:
            return CompanyProfileResult(error=str(exc))

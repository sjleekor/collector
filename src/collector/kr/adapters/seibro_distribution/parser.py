"""SEIBro 응답 XML 파서.

실제 응답(2026-10-09 서버 조사 원본으로 확인)은 ``<vector …><data …><result>
<ISIN value="…"/>…</result></data>…</vector>`` 모양이고, 건수 응답은 ``<result …>
<LIST_CNT value="N"/></result>``입니다. 행은 ``ISIN`` 자식을 가진 ``result``
원소입니다. 빈 값은 ``value=""``입니다(예: 청산분배의 ``BUNBE``).

저장용 열 이름 규칙: 원문 칸은 **원래 이름 그대로 문자열**, 변환 칸은 원문 칸 이름과
대소문자를 무시해도 겹치지 않는 이름입니다(DuckDB는 열 이름 대소문자를 구분하지
않습니다).
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from collector.kr.adapters.seibro_distribution.client import SeibroMalformedResponseError

#: 응답의 원문 칸(응답에 나오는 순서).
SOURCE_COLUMNS: tuple[str, ...] = (
    "ISIN",
    "KOR_SECN_NM",
    "ETF_SORT_CD",
    "ISSUCO_CUSTNO",
    "RGT_STD_DT",
    "TH1_PAY_TERM_BEGIN_DT",
    "ESTM_STDPRC",
    "BUNBE",
    "ETF_SORT_NM",
    "REP_SECN_NM",
    "TAXSTD",
    "RGT_RSN_DTAIL_NM",
)
#: 변환 칸. 원문 칸과 대소문자 무시로도 겹치지 않습니다.
PARSED_COLUMNS: tuple[str, ...] = (
    "stock_code",
    "rgt_std_date",
    "pay_begin_date",
    "per_share_amount",
    "dist_rate_pct",
    "tax_std_price",
    "tax_std_is_zero",
)
#: 업무 키(원문 칸 이름): ISIN, 기준일, 유형(이익분배·청산분배).
KEY_COLUMNS: tuple[str, ...] = ("ISIN", "RGT_STD_DT", "RGT_RSN_DTAIL_NM")
TYPE_FIELD = "RGT_RSN_DTAIL_NM"
#: 빈 값으로 보는 표기. 실제 응답은 ``""``만 확인했습니다. ``"-"``는 방어용입니다.
BLANK_VALUES = frozenset({"", "-"})


@dataclass(frozen=True, slots=True)
class DistributionRow:
    """분배금 한 행. 업무 키는 ``key``입니다."""

    raw: dict[str, str]
    stock_code: str
    rgt_std_date: date | None
    pay_begin_date: date | None
    per_share_amount: float | None  # ESTM_STDPRC, 주당 분배금(원)
    dist_rate_pct: float | None  # BUNBE, 분배율(%)
    tax_std_price: float | None  # TAXSTD, 0이면 None(결측 표시)
    tax_std_is_zero: bool  # TAXSTD 원문이 0이었음(2009 일부 등)

    @property
    def isin(self) -> str:
        return self.raw["ISIN"]

    @property
    def dist_type(self) -> str:
        return self.raw.get(TYPE_FIELD, "")

    @property
    def key(self) -> tuple[str, ...]:
        """(ISIN, 기준일 원문, 유형 원문)."""
        return tuple(self.raw.get(c, "") for c in KEY_COLUMNS)

    def to_record(self) -> dict[str, Any]:
        """저장용 dict: 원문 칸(문자열, 없으면 None) + 변환 칸."""
        record: dict[str, Any] = {c: self.raw.get(c) for c in SOURCE_COLUMNS}
        for c in PARSED_COLUMNS:
            record[c] = getattr(self, c)
        return record


def is_blank(value: str | None) -> bool:
    return value is None or value.strip() in BLANK_VALUES


def parse_date(value: str | None) -> date | None:
    """``YYYYMMDD`` → date. 빈 값은 None."""
    if is_blank(value):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y%m%d").date()
    except ValueError as exc:
        raise SeibroMalformedResponseError(f"bad date value: {value!r}") from exc


def parse_float(value: str | None) -> float | None:
    """숫자(콤마 제거, ``.02059`` 같은 앞자리 없는 소수 허용). 빈 값은 None."""
    if is_blank(value):
        return None
    try:
        return float(str(value).strip().replace(",", ""))
    except ValueError as exc:
        raise SeibroMalformedResponseError(f"bad number value: {value!r}") from exc


def _root(body: bytes) -> ET.Element:
    try:
        return ET.fromstring(body)
    except ET.ParseError as exc:
        raise SeibroMalformedResponseError(f"response is not XML: {exc}") from exc


def parse_count(body: bytes) -> int:
    """건수 응답 → ``LIST_CNT`` 정수."""
    for element in _root(body).iter("LIST_CNT"):
        text = element.attrib.get("value", "").strip().replace(",", "")
        if not text.isdigit():
            raise SeibroMalformedResponseError(f"LIST_CNT is not an integer: {text!r}")
        return int(text)
    raise SeibroMalformedResponseError("LIST_CNT not found in count response")


def parse_rows(body: bytes) -> list[DistributionRow]:
    """목록 응답 → 행 목록. 행이 없는 정상 응답은 빈 목록."""
    rows: list[DistributionRow] = []
    for element in _root(body).iter("result"):
        fields = {c.tag: c.attrib["value"] for c in element if "value" in c.attrib}
        if "ISIN" not in fields:
            continue
        rows.append(_row(fields))
    return rows


def _row(fields: dict[str, str]) -> DistributionRow:
    isin = fields["ISIN"].strip()
    if len(isin) < 9:
        raise SeibroMalformedResponseError(f"bad ISIN: {isin!r}")
    tax = parse_float(fields.get("TAXSTD"))
    return DistributionRow(
        raw=dict(fields),
        stock_code=isin[3:9],
        rgt_std_date=parse_date(fields.get("RGT_STD_DT")),
        pay_begin_date=parse_date(fields.get("TH1_PAY_TERM_BEGIN_DT")),
        per_share_amount=parse_float(fields.get("ESTM_STDPRC")),
        dist_rate_pct=parse_float(fields.get("BUNBE")),
        tax_std_price=None if tax is None or tax == 0 else tax,
        tax_std_is_zero=tax is not None and tax == 0,
    )

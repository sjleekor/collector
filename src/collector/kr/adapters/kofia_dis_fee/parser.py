"""DIS 보수 응답 → 정규화 행.

**열 대응(``tmpV`` 번호 → 의미).** 화면 정의(``DISComOutputMetaSO``)를 직접 받아 대조하지는
않았고, 연구용 사본 84,873행(280개 달)으로 **산식이 맞는지** 확인해 정했다
(2026-10-11 · 합계 = 운용+판매+수탁+사무관리, TER = 합계 + 기타비용이 전 행에서 0.01 안).

| tmpV | 원천 칸 | 뜻 |
|---|---|---|
| 1 | ``COMPANY_NM`` | 운용회사 |
| 2 | ``FUND_NM`` | 펀드명 |
| 3 | ``FUND_TYPE`` | 펀드유형(주식형·채권형 …, ETF 구분 칸이 아니다) |
| 4 | ``SETUP_YMD`` | 설정일 YYYYMMDD |
| 5~8 | ``FEE_MGMT``·``FEE_SELL``·``FEE_TRUST``·``FEE_ADMIN`` | 운용·판매·수탁·사무관리 보수 |
| 9 | ``FEE_TOTAL`` | 총보수 합계 (A) |
| 10 | ``FEE_PEER_AVG`` | 유사유형 평균 총보수 |
| 11 | ``COST_OTHER`` | 기타비용 (B) — 음수가 나오는 옛 행이 있다 |
| 12 | ``TER`` | 총보수·비용비율 (A+B) |
| 13·14 | ``LOAD_FRONT``·``LOAD_BACK`` | 판매수수료 선취·후취 (C) |
| 15 | ``FUND_CODE`` | **펀드 표준코드** — 키. ``K553N9EC3266`` 꼴도 있다. ETF ISIN이 아니다 |
| 16 | ``TRADE_COST`` | 매매·중개수수료율 (D) |

**단위.** 값은 **퍼센트**를 문자열 그대로 받는다(``.15`` = 0.15%). 원천 칸은 원문 문자열이라
앞의 0이 없다. ``*_num`` 칸이 float 변환값이고 단위는 같은 퍼센트다. 빈 칸은 ``None``이다.
``tmpV17`` 이후는 응답에 늘 빈 칸이라 저장하지 않는다. 저장하지 않는 칸에 값이 들어오면
``unexpected_filled``로 알려 준다(화면 정의가 바뀐 신호).
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime

KEY_COLUMNS: tuple[str, ...] = ("STD_YMD", "FUND_CODE")

#: ``(tmpV 번호, 원천 칸)``. 이 순서가 ``row_hash`` 순서라 바꾸면 기존 관측이 전부 달라 보인다.
_SOURCE_MAP: tuple[tuple[int, str], ...] = (
    (1, "COMPANY_NM"),
    (2, "FUND_NM"),
    (3, "FUND_TYPE"),
    (4, "SETUP_YMD"),
    (5, "FEE_MGMT"),
    (6, "FEE_SELL"),
    (7, "FEE_TRUST"),
    (8, "FEE_ADMIN"),
    (9, "FEE_TOTAL"),
    (10, "FEE_PEER_AVG"),
    (11, "COST_OTHER"),
    (12, "TER"),
    (13, "LOAD_FRONT"),
    (14, "LOAD_BACK"),
    (16, "TRADE_COST"),
)
_CODE_INDEX = 15
SOURCE_COLUMNS: tuple[str, ...] = ("FUND_CODE", *(name for _, name in _SOURCE_MAP))

_NUMERIC = (
    "FEE_MGMT",
    "FEE_SELL",
    "FEE_TRUST",
    "FEE_ADMIN",
    "FEE_TOTAL",
    "FEE_PEER_AVG",
    "COST_OTHER",
    "TER",
    "LOAD_FRONT",
    "LOAD_BACK",
    "TRADE_COST",
)
#: 변환 칸 — 대문자 원천 칸과 대소문자만 다르지 않게 ``_num``을 붙인다(DuckDB).
PARSED_TYPES: dict[str, str] = {
    "std_date": "date32",
    "setup_date": "date32",
    **{f"{c.lower()}_num": "float64" for c in _NUMERIC},
}
PARSED_COLUMNS: tuple[str, ...] = tuple(PARSED_TYPES)

_KNOWN = {i for i, _ in _SOURCE_MAP} | {_CODE_INDEX}
_YMD = re.compile(r"^\d{8}$")


class DisParseError(ValueError):
    """응답이 기대한 모양이 아닐 때."""


@dataclass(frozen=True)
class ParsedFees:
    rows: list[dict]
    total_count: int
    unexpected_filled: int  # 저장하지 않는 칸(tmpV17 이후)에 값이 든 행 수


def _root(body: bytes) -> ET.Element:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise DisParseError(f"응답이 XML이 아닙니다: {exc}") from exc
    if root.tag != "root":
        raise DisParseError(f"뿌리가 root가 아닙니다: {root.tag}")
    return root


def _to_float(text: str) -> float | None:
    text = text.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise DisParseError(f"숫자가 아닙니다: {text!r}") from exc


def _to_date(text: str) -> date | None:
    text = text.strip()
    if not text:
        return None
    if not _YMD.match(text):
        raise DisParseError(f"날짜가 YYYYMMDD가 아닙니다: {text!r}")
    return datetime.strptime(text, "%Y%m%d").date()


def parse_fee_rows(body: bytes, std_ymd: str) -> ParsedFees:
    """보수 응답을 행 목록으로. 행 수가 ``dbio_total_count_``와 다르면 ``DisParseError``.

    ``std_ymd``는 **요청한 기준일**이다(응답에 기준일 칸이 없다). 한 응답에 같은
    표준코드가 둘이면 키가 겹치므로 오류로 올린다.
    """
    root = _root(body)
    count_el = root.find(".//dbio_total_count_")
    if count_el is None or not (count_el.text or "").strip().isdigit():
        raise DisParseError("dbio_total_count_가 없습니다")
    total = int(count_el.text.strip())
    rows: list[dict] = []
    seen: set[str] = set()
    unexpected = 0
    for meta in root.iter("selectMeta"):
        cells: dict[int, str] = {}
        for child in meta:
            match = re.fullmatch(r"tmpV(\d+)", child.tag)
            if match:
                cells[int(match[1])] = child.text or ""
        code = cells.get(_CODE_INDEX, "")
        if not code.strip():
            raise DisParseError("표준코드(tmpV15)가 빈 행이 있습니다")
        if code in seen:
            raise DisParseError(f"한 응답에 같은 표준코드가 둘 있습니다: {code}")
        seen.add(code)
        if any(v.strip() for i, v in cells.items() if i not in _KNOWN):
            unexpected += 1
        row: dict = {"STD_YMD": std_ymd, "FUND_CODE": code}
        for index, name in _SOURCE_MAP:
            row[name] = cells.get(index, "")
        row["std_date"] = _to_date(std_ymd)
        row["setup_date"] = _to_date(row["SETUP_YMD"])
        for name in _NUMERIC:
            row[f"{name.lower()}_num"] = _to_float(row[name])
        rows.append(row)
    if len(rows) != total:
        raise DisParseError(f"행 수 {len(rows)}가 dbio_total_count_ {total}와 다릅니다")
    return ParsedFees(rows, total, unexpected)


def count_rows(body: bytes) -> int:
    """``dbio_total_count_``만 읽는다. 월말 탐색에서 값이 나온 날인지 가리는 데 쓴다."""
    match = re.search(rb"<dbio_total_count_>(\d+)<", body)
    if match is None:
        raise DisParseError("dbio_total_count_가 없습니다")
    return int(match[1])


def parse_latest_std_date(body: bytes, today: date | None = None) -> date | None:
    """최근 기준일 응답(``J_RD``)에서 날짜를 읽는다. 못 읽으면 ``None``.

    **응답 모양을 모른다**(요청 본문과 같은 이유, ``request.py`` 경고). 그래서 응답 머리
    (``proframeHeader`` — ``pfmTrDate``에 오늘 날짜가 들어 있다)를 뺀 모든 칸의 글자에서
    ``YYYYMMDD``·``YYYY-MM-DD``를 찾아 가장 늦은 날짜를 쓴다. ``today``보다 늦은 날짜는
    버린다. 후보가 없으면 ``None`` — 호출자가 직접 탐색으로 물러난다.
    """
    try:
        root = _root(body)
    except DisParseError:
        return None
    skip = {id(el) for header in root.iter("proframeHeader") for el in header.iter()}
    found: list[date] = []
    for el in root.iter():
        if id(el) in skip:
            continue
        compact = (el.text or "").strip().replace("-", "")
        if _YMD.match(compact) and compact.startswith("20"):
            try:
                found.append(datetime.strptime(compact, "%Y%m%d").date())
            except ValueError:
                continue
    if today is not None:
        found = [d for d in found if d <= today]
    return max(found) if found else None

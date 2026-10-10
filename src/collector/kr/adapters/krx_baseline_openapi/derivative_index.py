"""KRX 파생상품지수 시세정보(idx/drvprod_dd_trd) 파서와 날짜 상태 판정.

코스피 200 TR(총수익) 지수의 원천이다.
"""

from __future__ import annotations

from datetime import date

from collector.kr.adapters.krx_baseline_openapi._parse import (
    check_columns,
    is_empty,
    to_date,
    to_float,
)
from collector.kr.adapters.krx_baseline_openapi.day_status import DayStatus

GROUP = "idx"
ENDPOINT = "drvprod_dd_trd"

# 응답 순서 그대로(2026-10-10 서버 조사 사본 20101230·20110103·20261007에서 확인).
SOURCE_COLUMNS: tuple[str, ...] = (
    "BAS_DD",
    "IDX_CLSS",
    "IDX_NM",
    "CLSPRC_IDX",
    "CMPPREVDD_IDX",
    "FLUC_RT",
    "OPNPRC_IDX",
    "HGPRC_IDX",
    "LWPRC_IDX",
)
KEY_COLUMNS: tuple[str, ...] = ("BAS_DD", "IDX_NM")

_FLOAT_COLUMNS = SOURCE_COLUMNS[3:]

# 변환 칸 이름. 원문 칸과 대소문자를 무시해도 겹치지 않는다(DuckDB 호환).
PARSED_COLUMNS: tuple[str, ...] = (
    "bas_date",
    *(f"{c.lower()}_num" for c in _FLOAT_COLUMNS),
)

KOSPI200_TR_NAME = "코스피 200 TR"
# 조사 사본에서 코스피 200 TR 행이 처음 나온 날: 2010-12-30에는 없고(38행)
# 2011-01-03에 처음 나온다(CLSPRC_IDX 273.81, 91행).
KOSPI200_TR_FIRST_DATE = date(2011, 1, 3)


def parse_derivative_rows(rows: list[dict]) -> list[dict]:
    """원천 칸은 원문 문자열 그대로 둡니다.

    변환 칸은 ``<소문자 이름>_num``(float, 빈 값 None)과 ``bas_date``(date)입니다.
    기대 필드가 빠진 행은 KeyError입니다.
    """
    out: list[dict] = []
    for row in rows:
        check_columns(row, SOURCE_COLUMNS)
        parsed: dict = {c: row[c] for c in SOURCE_COLUMNS}
        parsed["bas_date"] = to_date(row["BAS_DD"])
        for c in _FLOAT_COLUMNS:
            parsed[f"{c.lower()}_num"] = to_float(row[c])
        out.append(parsed)
    return out


def classify_derivative_day(rows: list[dict]) -> DayStatus:
    """종가지수(CLSPRC_IDX)가 있는 행이 있으면 trading, 행은 있는데 모두 비면 no_price,
    행이 없으면 empty입니다.

    required_missing은 ``BAS_DD >= KOSPI200_TR_FIRST_DATE``인데 코스피 200 TR 행의
    값이 없으면 1, 아니면 0입니다. 행이 없는 날(empty)은 기준 날짜를 알 수 없어 0이고,
    호출 쪽이 요청 날짜로 따로 판단합니다.
    """
    priced = 0
    tr_has_value = False
    tr_applies = False
    for row in rows:
        check_columns(row, ("BAS_DD", "IDX_NM", "CLSPRC_IDX"))
        has = not is_empty(row["CLSPRC_IDX"])
        priced += has
        if to_date(row["BAS_DD"]) >= KOSPI200_TR_FIRST_DATE:
            tr_applies = True
            if row["IDX_NM"] == KOSPI200_TR_NAME and has:
                tr_has_value = True
    if not rows:
        kind = "empty"
    elif priced:
        kind = "trading"
    else:
        kind = "no_price"
    return DayStatus(kind, len(rows), priced, 0, int(tr_applies and not tr_has_value))

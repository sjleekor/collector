"""KRX ETF 일별매매정보(etp/etf_bydd_trd) 파서와 날짜 상태 판정."""

from __future__ import annotations

from collector.kr.adapters.krx_baseline_openapi._parse import (
    check_columns,
    is_empty,
    to_date,
    to_float,
    to_int,
)
from collector.kr.adapters.krx_baseline_openapi.day_status import DayStatus

GROUP = "etp"
ENDPOINT = "etf_bydd_trd"

# 응답 순서 그대로(2026-10-10 서버 조사 사본에서 확인).
SOURCE_COLUMNS: tuple[str, ...] = (
    "BAS_DD",
    "ISU_CD",
    "ISU_NM",
    "TDD_CLSPRC",
    "CMPPREVDD_PRC",
    "FLUC_RT",
    "NAV",
    "TDD_OPNPRC",
    "TDD_HGPRC",
    "TDD_LWPRC",
    "ACC_TRDVOL",
    "ACC_TRDVAL",
    "MKTCAP",
    "INVSTASST_NETASST_TOTAMT",
    "LIST_SHRS",
    "IDX_IND_NM",
    "OBJ_STKPRC_IDX",
    "CMPPREVDD_IDX",
    "FLUC_RT_IDX",
)
KEY_COLUMNS: tuple[str, ...] = ("BAS_DD", "ISU_CD")

_INT_COLUMNS = (
    "TDD_CLSPRC",
    "CMPPREVDD_PRC",
    "TDD_OPNPRC",
    "TDD_HGPRC",
    "TDD_LWPRC",
    "ACC_TRDVOL",
    "ACC_TRDVAL",
    "MKTCAP",
    "INVSTASST_NETASST_TOTAMT",
    "LIST_SHRS",
)
_FLOAT_COLUMNS = ("FLUC_RT", "NAV", "OBJ_STKPRC_IDX", "CMPPREVDD_IDX", "FLUC_RT_IDX")

# 변환 칸 이름. 원문 칸과 대소문자를 무시해도 겹치지 않는다(DuckDB 호환).
PARSED_COLUMNS: tuple[str, ...] = (
    "bas_date",
    *(f"{c.lower()}_num" for c in _INT_COLUMNS + _FLOAT_COLUMNS),
)


def parse_etf_rows(rows: list[dict]) -> list[dict]:
    """응답 행을 파싱합니다.

    원천 칸은 원래 이름 그대로 원문 문자열로 둡니다. 변환 칸은
    ``<소문자 이름>_num``(정수는 int, 소수는 float)과 ``bas_date``(date)입니다.
    빈 값은 None이고, 코드는 원문 ``ISU_CD`` 문자열 그대로입니다.
    기대 필드가 빠진 행은 KeyError입니다.
    """
    out: list[dict] = []
    for row in rows:
        check_columns(row, SOURCE_COLUMNS)
        parsed: dict = {c: row[c] for c in SOURCE_COLUMNS}
        parsed["bas_date"] = to_date(row["BAS_DD"])
        for c in _INT_COLUMNS:
            parsed[f"{c.lower()}_num"] = to_int(row[c])
        for c in _FLOAT_COLUMNS:
            parsed[f"{c.lower()}_num"] = to_float(row[c])
        out.append(parsed)
    return out


def classify_etf_day(rows: list[dict]) -> DayStatus:
    """원문 행(또는 파싱된 행)으로 날짜 상태를 판정합니다.

    종가 있는 행이 있으면 trading, 행은 있는데 종가가 모두 비면 no_price,
    행이 없으면 empty입니다. no_price만으로 확정 휴장이라 단정하지 않습니다
    (새 날짜는 미발표일 수 있어 호출 쪽이 재확인합니다).
    """
    priced = 0
    zero = 0
    for row in rows:
        check_columns(row, ("TDD_CLSPRC", "INVSTASST_NETASST_TOTAMT"))
        if not is_empty(row["TDD_CLSPRC"]):
            priced += 1
        if to_int(row["INVSTASST_NETASST_TOTAMT"]) == 0:
            zero += 1
    if not rows:
        kind = "empty"
    elif priced:
        kind = "trading"
    else:
        kind = "no_price"
    return DayStatus(kind, len(rows), priced, zero, 0)

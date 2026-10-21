"""KRX 채권지수 일별매매정보(idx/bon_dd_trd) 파서와 날짜 상태 판정."""

from __future__ import annotations

from collector.kr.adapters.krx_baseline_openapi._parse import (
    check_columns,
    is_empty,
    to_date,
    to_float,
)
from collector.kr.adapters.krx_baseline_openapi.day_status import DayStatus

GROUP = "idx"
ENDPOINT = "bon_dd_trd"

SOURCE_COLUMNS: tuple[str, ...] = (
    "BAS_DD",
    "BND_IDX_GRP_NM",
    "TOT_EARNG_IDX",
    "TOT_EARNG_IDX_CMPPREVDD",
    "NETPRC_IDX",
    "NETPRC_IDX_CMPPREVDD",
    "ZERO_REINVST_IDX",
    "ZERO_REINVST_IDX_CMPPREVDD",
    "CALL_REINVST_IDX",
    "CALL_REINVST_IDX_CMPPREVDD",
    "MKT_PRC_IDX",
    "MKT_PRC_IDX_CMPPREVDD",
    "AVG_DURATION",
    "AVG_CONVEXITY_PRC",
    "BND_IDX_AVG_YD",
)
KEY_COLUMNS: tuple[str, ...] = ("BAS_DD", "BND_IDX_GRP_NM")

# 2010-01-04\~2026-10-08 응답에서 확인한 그룹 이름.
EXPECTED_GROUPS: frozenset[str] = frozenset({"KRX 채권지수", "KTB 지수", "국고채프라임지수"})

_REQUIRED_VALUE = "TOT_EARNG_IDX"
_FLOAT_COLUMNS = SOURCE_COLUMNS[2:]

# 변환 칸 이름. 원문 칸과 대소문자를 무시해도 겹치지 않는다(DuckDB 호환).
PARSED_COLUMNS: tuple[str, ...] = (
    "bas_date",
    *(f"{c.lower()}_num" for c in _FLOAT_COLUMNS),
)


def parse_bond_rows(rows: list[dict]) -> list[dict]:
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


def classify_bond_day(rows: list[dict]) -> DayStatus:
    """세 그룹 모두 총수익지수가 있으면 trading, 일부만 있으면 partial, 행이 없으면 empty.

    required_missing은 총수익지수가 없는 기대 그룹 수입니다(행이 아예 없는
    그룹도 센다). 행은 있는데 어느 그룹도 값이 없으면 partial입니다.
    """
    have: set[str] = set()
    priced = 0
    for row in rows:
        check_columns(row, ("BND_IDX_GRP_NM", _REQUIRED_VALUE))
        if not is_empty(row[_REQUIRED_VALUE]):
            priced += 1
            if row["BND_IDX_GRP_NM"] in EXPECTED_GROUPS:
                have.add(row["BND_IDX_GRP_NM"])
    missing = len(EXPECTED_GROUPS) - len(have)
    if not rows:
        kind = "empty"
    elif missing == 0:
        kind = "trading"
    else:
        kind = "partial"
    return DayStatus(kind, len(rows), priced, 0, missing if rows else len(EXPECTED_GROUPS))

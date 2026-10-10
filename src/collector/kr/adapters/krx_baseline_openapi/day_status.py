"""날짜별 응답 상태. 원문 행 수와 가격 있는 행 수로 날짜의 성격을 남긴다."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DayStatus:
    """한 서비스·한 요청 날짜의 응답 요약입니다.

    day_kind: ETF는 trading / no_price / empty, 채권지수는 trading / partial / empty.
    row_count: 원문 행 수.
    priced_rows: 값(ETF는 종가, 채권은 총수익지수)이 있는 행 수.
    netasst_zero_rows: 순자산이 숫자 0인 행 수(빈 값은 세지 않음. ETF만 의미 있음).
    required_missing: 필수값이 빠진 항목 수(ETF는 0, 채권은 값이 없는 기대 그룹 수).
    """

    day_kind: str
    row_count: int
    priced_rows: int
    netasst_zero_rows: int
    required_missing: int

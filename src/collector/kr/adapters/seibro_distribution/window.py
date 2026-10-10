"""SEIBro 받을 창 계산 (계획 §3.1, R07)."""

from __future__ import annotations

from datetime import date, timedelta

#: SEIBro 분배금 이력의 시작(첫 행은 2003-04-30). 끝 날짜는 박지 않습니다.
EARLIEST_RGT_STD_DT = date(2003, 1, 1)
DEFAULT_LOOKBACK_DAYS = 90


def weekly_window(
    today: date,
    last_complete_end: date | None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> tuple[date, date]:
    """주간 창 ``(시작, 끝)``.

    시작 = ``min(today − 90일, 마지막 완료 창의 끝 − 90일)``, 끝 = ``today``.
    완료한 창이 없으면 전체(``EARLIEST_RGT_STD_DT`` ~ today)입니다. 120일 꺼 두었다
    켜도 마지막 완료 끝에서 90일 앞까지 덮으므로 사이 구간이 빠지지 않습니다.
    """
    if last_complete_end is None:
        return EARLIEST_RGT_STD_DT, today
    delta = timedelta(days=lookback_days)
    start = min(today - delta, last_complete_end - delta)
    return max(start, EARLIEST_RGT_STD_DT), today


def is_quarterly_full_day(d: date) -> bool:
    """1·4·7·10월의 첫 일요일이면 True (전체 재확인 날)."""
    return d.month in (1, 4, 7, 10) and d.weekday() == 6 and d.day <= 7


def year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """``start``~``end``를 달력 연도 구간으로 자릅니다(양끝 포함)."""
    chunks: list[tuple[date, date]] = []
    year = start.year
    while year <= end.year:
        lo = max(start, date(year, 1, 1))
        hi = min(end, date(year, 12, 31))
        if lo <= hi:
            chunks.append((lo, hi))
        year += 1
    return chunks

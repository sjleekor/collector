"""월말 기준일 계산.

DIS 기준일은 **월말 영업일**이고 정확히 맞아야 값이 나온다. 연구용 사본 280개 달에서 값이
나온 날은 모두 달력 말일에서 0~2일 안이었다. 그래서 그 달의 마지막 평일부터 하루씩
(평일만) 거슬러 올라가며, 최대 ``MAX_STEPS_BACK``(3)평일 앞까지 후보로 둔다 — 후보는
모두 4개다. 공휴일 달력은 쓰지 않는다(평일이면 일단 물어본다).
"""

from __future__ import annotations

from datetime import date, timedelta

MAX_STEPS_BACK = 3
PUBLIC_LAG_NOTE = (
    "공개 지연: 2026-10-10·11 조회 때 최근 기준일이 2026-08-31이었다(9월 말 값은 아직 없음). "
    "월말 값이 몇 일에 올라오는지는 정기 수집이 새 달을 처음 본 시각(fetched_at)으로 잰다."
)

Month = tuple[int, int]


def month_of(day: date) -> Month:
    return (day.year, day.month)


def month_key(month: Month) -> str:
    return f"{month[0]}-{month[1]:02d}"


def parse_month(text: str) -> Month:
    year, _, mon = text.partition("-")
    if not (year.isdigit() and mon.isdigit() and len(year) == 4 and 1 <= int(mon) <= 12):
        raise ValueError(f"YYYY-MM 형식이어야 합니다: {text!r}")
    return (int(year), int(mon))


def next_month(month: Month) -> Month:
    year, mon = month
    return (year + 1, 1) if mon == 12 else (year, mon + 1)


def iter_months(first: Month, last: Month):
    month = first
    while month <= last:
        yield month
        month = next_month(month)


def calendar_month_end(month: Month) -> date:
    year, mon = next_month(month)
    return date(year, mon, 1) - timedelta(days=1)


def month_end_candidates(month: Month, steps_back: int = MAX_STEPS_BACK) -> list[date]:
    """그 달 말일 이하의 마지막 평일부터 거슬러 올라가는 후보. ``1 + steps_back``개."""
    day = calendar_month_end(month)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    out = [day]
    while len(out) < steps_back + 1:
        day -= timedelta(days=1)
        while day.weekday() >= 5:
            day -= timedelta(days=1)
        out.append(day)
    return out


def is_plausible_month_end(day: date) -> bool:
    """``day``가 그 달의 월말 후보 4개 중 하나인가(verify의 경고 기준)."""
    return day in month_end_candidates(month_of(day))

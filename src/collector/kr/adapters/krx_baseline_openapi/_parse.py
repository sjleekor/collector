"""두 파서가 같이 쓰는 값 변환."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

# 이 표기는 모두 빈 값으로 본다. 2026-10-09 조사 사본에서는 ""만 나왔다.
_EMPTY = frozenset({"", "-"})


def is_empty(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() in _EMPTY)


def to_int(value: object) -> int | None:
    if is_empty(value):
        return None
    text = str(value).strip().replace(",", "")
    try:
        return int(text)
    except ValueError:
        pass
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"정수로 바꿀 수 없는 값: {value!r}") from exc
    if number != number.to_integral_value():
        raise ValueError(f"정수가 아닌 값: {value!r}")
    return int(number)


def to_float(value: object) -> float | None:
    if is_empty(value):
        return None
    try:
        return float(Decimal(str(value).strip().replace(",", "")))
    except InvalidOperation as exc:
        raise ValueError(f"숫자로 바꿀 수 없는 값: {value!r}") from exc


def check_columns(row: dict, columns: tuple[str, ...]) -> None:
    missing = [c for c in columns if c not in row]
    if missing:
        raise KeyError(f"응답 행에 기대 필드가 없습니다: {missing}")


def to_date(value: object) -> date:
    """YYYYMMDD 문자열을 date로 바꿉니다. 형식이 틀리면 ValueError입니다."""
    return datetime.strptime(str(value).strip(), "%Y%m%d").date()

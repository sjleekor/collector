"""무엇을 받을지 정하는 함수들 — 대기 규칙, ``fill``·``reobserve`` 대상.

모두 **완료 manifest가 가리키는 요청 기록**만 본다. 요청 기록이 정본이다.
``last_confirmed_at``도 관측 표가 아니라 여기서 계산한다(R05).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta

import pandas as pd

from collector.kr.baseline.store import BaselineStore

#: 이 날짜 상태나 순자산 0이 있으면, 아직 발표 전일 수 있어 다시 받는다.
PENDING_DAY_KINDS = frozenset({"no_price", "empty", "partial"})
DEFAULT_WINDOW_WEEKDAYS = 10


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def weekdays_between(start: date | str, end: date | str) -> int:
    """``start`` 다음 날부터 ``end``까지(양끝 중 ``end``만 포함) 월~금의 수.

    달력(공휴일)을 보지 않는다. 휴장인지 미발표인지는 원천이 가려주지 못하므로
    평일 전부를 같은 기준으로 센다. ``end <= start``면 0.
    """
    first, last = _as_date(start), _as_date(end)
    count = 0
    day = first + timedelta(days=1)
    while day <= last:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count


def is_pending(
    day_kind: str | None,
    netasst_zero_rows: int | None,
    required_missing: int | None,
    bas_dd: date | str,
    fetched_on: date | datetime | str,
    window_weekdays: int = DEFAULT_WINDOW_WEEKDAYS,
) -> bool:
    """ "대기"인가 — 매일 다시 받아야 하는가. 아니면 확정이다.

    ``day_kind``가 ``no_price``·``empty``·``partial``이거나 순자산 0인 행이 있고,
    ``bas_dd``에서 ``fetched_on``까지 평일 수가 ``window_weekdays`` 이하면 대기다.
    그 밖은 확정이고, 오래된 날짜(백필)는 처음부터 확정이다.

    ``required_missing``은 요청 기록에 남기는 값이고 대기 판단에는 쓰지 않는다
    (계획 §3 "대기와 확정"이 세 조건만 적는다).
    """
    del required_missing
    triggered = day_kind in PENDING_DAY_KINDS or bool(netasst_zero_rows)
    if not triggered:
        return False
    return weekdays_between(bas_dd, fetched_on) <= window_weekdays


def iter_weekdays(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def bas_dd_request_key(day: date) -> str:
    """KRX 날짜 요청 키. 파서·CLI가 같은 이름을 쓰게 한곳에 둔다."""
    return f"bas_dd={day:%Y%m%d}"


def fill_dates(
    store: BaselineStore,
    service: str,
    start: date,
    end: date,
    *,
    window_weekdays: int = DEFAULT_WINDOW_WEEKDAYS,
) -> list[date]:
    """``fill`` 모드 대상 — 범위의 평일 중 완료 관측이 없거나 "대기"인 날짜.

    날짜마다 **성공한 마지막 요청**을 본다. 없으면(실패만 있어도) 받는다. 있으면
    그 요청을 받은 날(``fetched_at``의 UTC 날짜가 아니라 기록된 날짜) 기준으로
    ``is_pending``이 참일 때만 다시 받는다. 대기 창 안에서 받은 마지막 기록이
    여전히 비어 있으면 한 번 더 받아야 하고, 창을 넘겨 받고도 비어 있으면 확정이다.
    """
    log = store.read_fetch_log(service)
    latest: dict[str, pd.Series] = {}
    if not log.empty:
        ok = log[log["result"] != "failed"].sort_values("fetched_at")
        for _, row in ok.drop_duplicates("request_key", keep="last").iterrows():
            latest[row["request_key"]] = row
    out: list[date] = []
    for day in iter_weekdays(start, end):
        row = latest.get(bas_dd_request_key(day))
        if row is None or is_pending(
            row["day_kind"],
            _none_if_na(row["netasst_zero_rows"]),
            _none_if_na(row["required_missing"]),
            day,
            row["fetched_at"].date(),
            window_weekdays,
        ):
            out.append(day)
    return out


def completed_request_keys(store: BaselineStore, service: str, run_id: str) -> set[str]:
    """``reobserve`` 재개용 — 이 ``run_id``가 이미 끝낸(실패 아닌) 요청 키.

    ``obs_seq``가 아니라 **요청 기록**으로 판단한다. 값이 같은 날은 관측이 안
    생기므로 ``obs_seq``로는 끝낸 날과 안 한 날을 가를 수 없다(R02).
    """
    log = store.read_fetch_log(service)
    if log.empty:
        return set()
    done = log[(log["run_id"] == run_id) & (log["result"] != "failed")]
    return set(done["request_key"])


def reobserve_keys(
    store: BaselineStore, service: str, run_id: str, request_keys: list[str]
) -> list[str]:
    """``reobserve`` 대상 — 범위 전체에서 이 ``run_id``가 끝낸 것만 뺀다."""
    done = completed_request_keys(store, service, run_id)
    return [key for key in request_keys if key not in done]


def request_confirmations(store: BaselineStore, service: str | None = None) -> pd.DataFrame:
    """요청 키별 ``first_fetched_at``·``last_confirmed_at``·``confirmations``.

    실패한 요청은 확인으로 세지 않는다. 관측 표에 이 값을 두지 않는 대신 여기서
    계산한다.
    """
    log = store.read_fetch_log(service)
    columns = ["service", "request_key", "first_fetched_at", "last_confirmed_at", "confirmations"]
    if log.empty:
        return pd.DataFrame(columns=columns)
    ok = log[log["result"] != "failed"]
    grouped = ok.groupby(["service", "request_key"], as_index=False).agg(
        first_fetched_at=("fetched_at", "min"),
        last_confirmed_at=("fetched_at", "max"),
        confirmations=("fetched_at", "size"),
    )
    return grouped[columns]


def _none_if_na(value: object) -> int | None:
    return None if pd.isna(value) else int(value)  # type: ignore[arg-type]

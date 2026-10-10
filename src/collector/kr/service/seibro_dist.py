"""SEIBro ETF 분배금 수집 — 주간 증분과 분기 전체 재확인 (계획 04 §3.1 SEIBro 창, R07).

**창 규칙.** 창은 ``weekly_window(오늘, 마지막 완료 창의 끝)``이다. 마지막 완료 창은
저장소의 요청 기록에서 찾는다 — 창이 끝날 때마다 ``request_key="window=YYYYMMDD_YYYYMMDD"``
요약 한 줄을 ``day_kind="complete"|"incomplete"``로 남기기 때문이다. 120일 꺼 두었다
켜도 마지막 완료 끝에서 90일 앞까지 덮으므로 사이 구간이 빠지지 않는다. 한 페이지라도
실패하면 창 전체가 미완료로 남고 그 창은 ``absent`` 근거로 쓰지 않는다.

**원문.** 응답(건수·페이지)마다 받은 바이트를 그대로 ``seibro_dist/window=…/p=NNN``에
둔다. 관측은 기준일(``RGT_STD_DT``)별 scope로 비교한다. 그래서 행의 ``raw_path``는 그
행이 나온 페이지 원문이다.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from collector.kr.adapters.seibro_distribution import (
    EARLIEST_RGT_STD_DT,
    KEY_COLUMNS,
    PARSED_COLUMNS,
    SOURCE_COLUMNS,
    SeibroClient,
    WindowResult,
    fetch_window,
    is_quarterly_full_day,
    parse_rows,
    seibro_enabled,
    weekly_window,
)
from collector.kr.adapters.seibro_distribution.request import LIST_ACTION
from collector.kr.baseline import BaselineStore, BaselineWriter, RawRef, TableSpec
from collector.kr.baseline.writer import WindowResponse
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

SERVICE = "seibro_dist"

_PARSED_TYPES = {
    "stock_code": "string",
    "rgt_std_date": "date32",
    "pay_begin_date": "date32",
    "per_share_amount": "float64",
    "dist_rate_pct": "float64",
    "tax_std_price": "float64",
    "tax_std_is_zero": "bool",
}

SPEC = TableSpec(
    name="etf_distribution",
    key_columns=KEY_COLUMNS,
    source_columns=SOURCE_COLUMNS,
    date_column="RGT_STD_DT",
    parsed_columns={name: _PARSED_TYPES[name] for name in PARSED_COLUMNS},
)

_WINDOW_KEY = re.compile(r"^window=(\d{8})_(\d{8})$")


def window_key(start: date, end: date) -> str:
    return f"window={start:%Y%m%d}_{end:%Y%m%d}"


def last_complete_window_end(store: BaselineStore) -> date | None:
    """요청 기록의 창 요약 줄 중 ``complete``인 것의 가장 늦은 끝. 없으면 ``None``."""
    log = store.read_fetch_log(SERVICE, columns=["request_key", "day_kind"])
    best: date | None = None
    for key, kind in zip(log["request_key"], log["day_kind"], strict=True):
        match = _WINDOW_KEY.match(key)
        if match and kind == "complete":
            end = datetime.strptime(match[2], "%Y%m%d").date()
            best = end if best is None or end > best else best
    return best


def choose_window(
    store: BaselineStore, today: date, *, full: bool = False
) -> tuple[date, date, bool]:
    """``(시작, 끝, 전체 여부)``. ``--full``이나 분기 첫 일요일이면 전체 재확인이다."""
    if full or is_quarterly_full_day(today):
        return EARLIEST_RGT_STD_DT, today, True
    start, end = weekly_window(today, last_complete_window_end(store))
    return start, end, start == EARLIEST_RGT_STD_DT


@dataclass
class SeibroSyncResult:
    enabled: bool = True
    start: date | None = None
    end: date | None = None
    full: bool = False
    complete: bool = False
    rows: int = 0
    responses: int = 0
    http_requests: int = 0
    new_obs: bool = False
    errors: list[str] = field(default_factory=list)


def _request_key(raw_range: tuple[date, date], page: int | None) -> str:
    base = window_key(*raw_range)
    return f"{base}/count" if page is None else f"{base}/p={page:03d}"


def sync(
    *,
    store: BaselineStore,
    client: SeibroClient,
    env: Mapping[str, str],
    today: date | None = None,
    full: bool = False,
    run_id: str | None = None,
) -> SeibroSyncResult:
    """주간(또는 전체) 창을 받아 저장한다. 꺼져 있으면 요청 없이 끝낸다."""
    result = SeibroSyncResult()
    if not seibro_enabled(env):
        logger.info("SEIBro 수집이 꺼져 있습니다(SDC_SEIBRO_ENABLED=0). 요청 없이 끝냅니다.")
        result.enabled = False
        return result

    today = today or now_kst().date()
    start, end, is_full = choose_window(store, today, full=full)
    result.start, result.end, result.full = start, end, is_full

    before = client.counters.http_requests
    window: WindowResult = fetch_window(client, start, end)
    result.http_requests = client.counters.http_requests - before
    result.complete = window.complete
    result.rows = len(window.rows)
    result.responses = len(window.raw_responses)
    result.errors = [c.error for c in window.chunks if c.error]

    responses: list[WindowResponse] = []
    row_raw: dict[tuple[str, ...], RawRef] = {}
    for resp in window.raw_responses:
        rng = (resp.range_start or start, resp.range_end or end)
        ref = store.write_raw(
            SERVICE, _request_key(rng, resp.page), resp.body, resp.fetched_at, ext="xml"
        )
        responses.append(
            WindowResponse(_request_key(rng, resp.page), ref, resp.status_code, None, None)
        )
        if resp.action == LIST_ACTION:
            for row in parse_rows(resp.body):
                row_raw[row.key] = (
                    ref  # 여러 페이지에 나오면 뒤 페이지가 이긴다(fetch_window와 같다)
                )

    rid = run_id or f"seibro_{now_kst():%Y%m%dT%H%M%S}"
    source = f"seibro_full_{today:%Y%m%d}" if is_full else "seibro_weekly"
    writer = BaselineWriter(store, rid, store.next_attempt(rid), source, batch_size=10**9)
    records = [row.to_record() for row in window.rows]
    outcome = writer.add_window(
        service=SERVICE,
        window_key=window_key(start, end),
        spec=SPEC,
        rows=records,
        row_raw=row_raw,
        responses=responses,
        complete=window.complete,
        low=f"{start:%Y%m%d}",
        high=f"{end:%Y%m%d}",
        fetched_at=datetime.now(UTC),
        http_requests=result.http_requests,
        error="; ".join(result.errors)[:300] or None,
    )
    result.new_obs = outcome == "new_obs"
    writer.flush()
    return result

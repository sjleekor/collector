"""Nasdaq 애널리스트 추정치 — 주 1회 축적 한 번의 실행 (source_expansion 04·99 순번 4).

**전진 축적 전용 원천이라 ``us-daily``(하루 단위 raw)·``us-derive``(굳히기)와
다른 자기 명령이다.** 저 둘의 락 도메인(``us``)을 같이 쓰지 않는다 — 원천
일곱을 순서대로 보는 그 실행들과 달리 이 잡은 종목당 1요청이라 전체를 돌면
몇 시간이 걸리는데(04 §4), 같은 도메인을 쓰면 그동안 매일 15:00 raw 수집과
일요일 16:00 derive가 lock 대기에서 막힌다. 건드리는 raw 하위 트리도 겹치지
않는다 — ``raw/nasdaq/analyst_earnings_forecast/``뿐이고 dolt·SEC 등은 안
건드린다.

할 일은 **"이번 주 유니버스 중 아직 못 받은 심볼"**이다 — 예산이 모자라면
``pending``으로 남기고 다음 실행이 이어받는다. 실패한 심볼은 파일을 안 쓰므로
그것도 자연히 다음 실행의 할 일에 다시 들어간다(이어받기 = raw 파일 존재
여부로 정의된다, 04 §2.4와 같은 원칙).
"""

from __future__ import annotations

import datetime as dt
import time

import requests

from collector.lake import DataRoot

#: 6시간. 04 §4 어림(간격 5초 기준 한 바퀴 5.8시간)에 여유를 더했다. 유니버스가
#: 04의 "전체 약 4,200종목"보다 작으므로(target_symbols) 보통은 더 빨리 끝난다.
DEFAULT_BUDGET_SECONDS = 21_600.0

#: 심볼 하나가 실패할 때(타임아웃 등) 다시 시도하는 횟수. 403처럼 재시도로
#: 안 풀리는 실패는 sec.py 와 달리 이 원천에서 아직 관찰되지 않았다 — 04는
#: 브라우저 UA면 200을 받는다고만 적었다.
DEFAULT_MAX_RETRIES = 2


class _Budget:
    def __init__(self, seconds: float | None):
        self.seconds = seconds
        self.started = time.monotonic()

    def spent(self) -> bool:
        return self.seconds is not None and time.monotonic() - self.started > self.seconds


def run_weekly(
    root: DataRoot,
    *,
    today: dt.date | None = None,
    symbols: list[str] | None = None,
    budget_seconds: float | None = DEFAULT_BUDGET_SECONDS,
    interval_seconds: float | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    dry_run: bool = False,
) -> dict[str, object]:
    """이번 주 대상 심볼 중 아직 못 받은 것을 받는다.

    ``symbols``를 주면 유니버스 대신 그것을 쓴다 — 스모크 테스트나 특정
    종목만 다시 받고 싶을 때 쓴다. 기본은 :func:`target_symbols`(유니버스
    최신 멤버)다.
    """
    from collector.us.sources import nasdaq_analyst as src

    today = today or dt.date.today()
    week = src.week_start(today)
    wanted = symbols if symbols is not None else src.target_symbols(root)
    todo = [s for s in wanted if not src.earnings_forecast_path(root, week, s).is_file()]
    already_done = len(wanted) - len(todo)

    result: dict[str, object] = {
        "collected_week": str(week),
        "universe_symbols": len(wanted),
        "already_done": already_done,
        "pending_before": len(todo),
    }
    if dry_run or not todo:
        result.update(
            fetched=0,
            rows_fetched=0,
            failed=[],
            not_found=[],
            pending=len(todo),
            budget_spent=False,
            ok=True,
        )
        return result

    client = src.NasdaqClient(interval_seconds=interval_seconds or src.DEFAULT_INTERVAL_SECONDS)
    budget = _Budget(budget_seconds)
    fetched = 0
    rows_fetched = 0
    failed: list[str] = []
    not_found: list[str] = []

    for symbol in todo:
        if budget.spent():
            break
        last_exc: Exception | None = None
        outcome: dict[str, object] | None = None
        for _attempt in range(max_retries + 1):
            try:
                outcome = src.fetch_one(client, root, week, symbol, skip_existing=False)
                last_exc = None
                break
            except (src.NasdaqError, requests.RequestException) as exc:
                last_exc = exc
                continue
        if last_exc is not None or outcome is None:
            # raw 파일을 안 썼다 — 다음 실행의 todo 계산에 이 심볼이 자연히
            # 다시 들어간다. 여기서 또 손대지 않는다.
            failed.append(f"{symbol}: {last_exc}")
            continue
        fetched += 1
        rows_fetched += int(outcome.get("rows") or 0)
        rcode = outcome.get("status_rcode")
        if rcode is not None and rcode != 200:
            # 200 이면서도 원천이 실패를 알린 경우다(예: "Symbol not exists.").
            # 재시도해도 안 바뀌므로 실패가 아니라 별도로 센다 — raw 는 이미 썼다.
            not_found.append(symbol)

    pending = max(0, len(todo) - fetched - len(failed))
    result.update(
        fetched=fetched,
        rows_fetched=rows_fetched,
        failed=failed,
        not_found=not_found,
        pending=pending,
        budget_spent=budget.spent(),
        ok=not failed,
    )
    return result

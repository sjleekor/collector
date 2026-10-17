"""Nasdaq 실적 캘린더 — **미래 날짜 칸을 관측일마다 쌓는다** (전진 축적 전용).

``raw/nasdaq/earnings_calendar/date=<d>.json``은 날짜가 지난 뒤에 받은 것이라
(2,084개 중 그날 전에 받은 것은 0개, 2026-10-17 실측) 예측 시점에 Nasdaq이
미래 실적일을 어떻게 보여 줬는지는 남아 있지 않다. 그래서 ``days_to_earn``의
PIT 판은 지금까지 만들 수 없었다. 이 모듈은 **관측일 T에 미래 날짜 d의 캘린더가
어떻게 보였는지**를 그대로 쌓는다. 지난 뒤에는 복원할 수 없으므로 오늘부터
쌓는 방법밖에 없다.

* 저장 위치는 ``raw/nasdaq/earnings_calendar_forward/observed=<T>/date=<d>.json``
  이다. 기존 ``earnings_calendar``와 겹치지 않는다.
* 파일은 ``{"fetched_at": <UTC>, "response": <원문>}`` 봉투다
  (``nasdaq_analyst.fetch_one``과 같다). 원문은 고치지 않는다.
* **같은 (T, d) 파일이 있으면 건드리지 않는다.** 같은 날 다시 돌려도 안전하다.
* 표(derived)로 굳히지 않는다. raw 축적만 한다.

요청 예산: 매일 ``--horizon-days 14``는 평일 10요청, 주 1회
``--horizon-days 100``은 평일 약 71요청이다. 합치면 한 주에 약 131요청이다.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

import requests

from collector.lake import DataRoot
from collector.us.sources.nasdaq import (
    EARNINGS_URL,
    NasdaqClient,
    NasdaqError,
    summarize_earnings,
)

#: 요청 간격 1.5초. 이 잡은 날짜당 1요청이고 한 번에 최대 약 71요청이라 간격을
#: 5초로 두어도 6분이면 끝나지만, CLAUDE.md가 Nasdaq 실적 캘린더에 허용한 값이
#: 1.5초이고 같은 호스트의 다른 잡(애널리스트, 종목당 1요청)이 5초로 오래 도는
#: 것과 겹치지 않으므로 1.5초로 낮춘다. 요청 수가 적어 부담이 거의 없다.
DEFAULT_INTERVAL_SECONDS = 1.5

#: 날짜 하나가 실패할 때(타임아웃 등) 다시 시도하는 횟수. 애널리스트 잡과 같다.
DEFAULT_MAX_RETRIES = 2


def forward_path(root: DataRoot, observed: dt.date | str, day: dt.date | str) -> Path:
    """``raw/nasdaq/earnings_calendar_forward/observed=<T>/date=<d>.json``.

    **절대 덮어쓰거나 지우지 않는다** — 관측 시점의 화면은 다시 못 받는다.
    """
    t = observed.isoformat() if isinstance(observed, dt.date) else str(observed)
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    return root.raw / "nasdaq" / "earnings_calendar_forward" / f"observed={t}" / f"date={d}.json"


def target_dates(observed: dt.date, horizon_days: int) -> list[dt.date]:
    """``(observed, observed + horizon_days]`` 안의 평일. 관측일 자신은 뺀다."""
    out = []
    for offset in range(1, horizon_days + 1):
        d = observed + dt.timedelta(days=offset)
        if d.weekday() < 5:
            out.append(d)
    return out


def _check_body(doc: object) -> dict:
    """본문을 본다. ``data``가 null이면 0행으로 유효하지만 키가 없으면 오류다."""
    if not isinstance(doc, dict) or "data" not in doc:
        raise NasdaqError(f"기대한 형식이 아니다(data 키 없음): {str(doc)[:120]!r}")
    return doc


def fetch_forward(
    client: NasdaqClient,
    root: DataRoot,
    observed: dt.date,
    day: dt.date,
    *,
    now: dt.datetime | None = None,
) -> dict[str, object]:
    """그 날짜 칸을 받아 봉투째 굳힌다. 이미 있으면 요청하지 않고 건너뛴다."""
    dest = forward_path(root, observed, day)
    if dest.is_file():
        return {"date": day.isoformat(), "status": "skipped", "path": dest}

    doc = _check_body(client.get_json(EARNINGS_URL, {"date": day.isoformat()}))
    fetched_at = now or dt.datetime.now(dt.UTC)
    envelope = {"fetched_at": fetched_at.isoformat(), "response": doc}
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.tmp{os.getpid()}")
    tmp.write_text(json.dumps(envelope, ensure_ascii=False))
    try:
        # 하드링크는 대상이 있으면 실패한다 — 덮어쓰지 않는 쓰기다.
        os.link(tmp, dest)
    except FileExistsError:
        return {"date": day.isoformat(), "status": "skipped", "path": dest}
    finally:
        tmp.unlink(missing_ok=True)
    return {
        "date": day.isoformat(),
        "status": "fetched",
        "path": dest,
        **summarize_earnings(doc),
    }


def run_forward(
    root: DataRoot,
    *,
    horizon_days: int,
    observed: dt.date | None = None,
    interval_seconds: float | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    dry_run: bool = False,
    client: NasdaqClient | None = None,
) -> dict[str, object]:
    """관측일 기준 앞으로 ``horizon_days``일의 평일을 받는다."""
    observed = observed or dt.date.today()
    dates = target_dates(observed, horizon_days)
    result: dict[str, object] = {
        "observed": observed.isoformat(),
        "horizon_days": horizon_days,
        "dates_requested": len(dates),
        "fetched": 0,
        "skipped": 0,
        "rows": {},
        "errors": [],
        "ok": True,
    }
    if dry_run:
        todo = [d for d in dates if not forward_path(root, observed, d).is_file()]
        result.update(skipped=len(dates) - len(todo), would_fetch=len(todo))
        return result

    client = client or NasdaqClient(interval_seconds=interval_seconds or DEFAULT_INTERVAL_SECONDS)
    rows: dict[str, int] = {}
    errors: list[str] = []
    fetched = skipped = 0
    for day in dates:
        last_exc: Exception | None = None
        outcome: dict[str, object] | None = None
        for _attempt in range(max_retries + 1):
            try:
                outcome = fetch_forward(client, root, observed, day)
                last_exc = None
                break
            except (NasdaqError, requests.RequestException) as exc:
                last_exc = exc
        if last_exc is not None or outcome is None:
            errors.append(f"{day}: {last_exc}")
            continue
        if outcome["status"] == "skipped":
            skipped += 1
        else:
            fetched += 1
            rows[day.isoformat()] = int(outcome.get("rows") or 0)
    result.update(fetched=fetched, skipped=skipped, rows=rows, errors=errors, ok=not errors)
    return result

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
* **막히면 그 자리에서 멈추고 쉰다.** 요청 하나라도 실패하면(HTTP 오류·JSON 아님·
  ``data`` 키 없음, 네트워크 오류는 한 번 더 해 보고) 남은 날짜를 받지 않고 끝내며,
  ``_blocked.json``에 쉬는 기한을 적는다. 기한 전에는 요청 없이 0으로 끝난다.
  다시 막히면 2·4·8·16·30일로 늘린다. 한 실행이 다 성공하면 지운다.
  ``api.nasdaq.com``의 robots.txt가 ``Disallow: /``인데도 사용자가 수집을 정했으므로
  (2026-10-17) 막혔을 때 두드리지 않는 쪽을 코드로 지킨다.

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

#: 요청 간격 5초. ``us-daily``의 실적 캘린더(``NasdaqClient`` 기본값)와 애널리스트 잡이
#: 실제로 쓰는 값과 같다. 문서의 "Nasdaq 1.5초"는 backfill 때 좁힌 값이라 따르지 않는다.
#: 날짜당 1요청이고 매일 약 10요청·주 1회 약 71요청이라 5초여도 1분·6분이면 끝난다.
DEFAULT_INTERVAL_SECONDS = 5.0

#: 네트워크 오류(시간 초과·연결 끊김)만 한 번 더 해 본다. HTTP 오류와 형식 오류는
#: 다시 하지 않는다 — 막힌 것일 수 있다.
DEFAULT_MAX_RETRIES = 1

#: 쉬는 기한을 적는 파일. 앞이 ``_``라 날짜 파일과 섞이지 않는다.
BLOCK_FILE = "_blocked.json"

#: 연달아 막힌 횟수별 쉬는 날수. 마지막 값에서 더 늘리지 않는다.
BLOCK_COOLDOWN_DAYS = (2, 4, 8, 16, 30)


def forward_path(root: DataRoot, observed: dt.date | str, day: dt.date | str) -> Path:
    """``raw/nasdaq/earnings_calendar_forward/observed=<T>/date=<d>.json``.

    **절대 덮어쓰거나 지우지 않는다** — 관측 시점의 화면은 다시 못 받는다.
    """
    t = observed.isoformat() if isinstance(observed, dt.date) else str(observed)
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    return root.raw / "nasdaq" / "earnings_calendar_forward" / f"observed={t}" / f"date={d}.json"


def block_path(root: DataRoot) -> Path:
    """``raw/nasdaq/earnings_calendar_forward/_blocked.json``."""
    return root.raw / "nasdaq" / "earnings_calendar_forward" / BLOCK_FILE


def read_block(root: DataRoot) -> dict | None:
    """쉬는 기한 기록. 없으면 None."""
    path = block_path(root)
    if not path.is_file():
        return None
    return json.loads(path.read_text())


def _write_block(root: DataRoot, now: dt.datetime, reason: str, previous: dict | None) -> dict:
    consecutive = int((previous or {}).get("consecutive", 0)) + 1
    days = BLOCK_COOLDOWN_DAYS[min(consecutive, len(BLOCK_COOLDOWN_DAYS)) - 1]
    record = {
        "blocked_at": now.isoformat(),
        "until": (now + dt.timedelta(days=days)).isoformat(),
        "cooldown_days": days,
        "consecutive": consecutive,
        "reason": reason[:500],
    }
    path = block_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False))
    return record


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
    ignore_block: bool = False,
    client: NasdaqClient | None = None,
    now: dt.datetime | None = None,
) -> dict[str, object]:
    """관측일 기준 앞으로 ``horizon_days``일의 평일을 받는다.

    쉬는 기한 안이면 요청 없이 ``ok``로 끝낸다(``blocked_until``). 요청이 하나라도
    실패하면 남은 날짜를 받지 않고 쉬는 기한을 적은 뒤 ``ok=False``로 끝낸다.
    """
    observed = observed or dt.date.today()
    now = now or dt.datetime.now(dt.UTC)
    dates = target_dates(observed, horizon_days)
    block = read_block(root)
    result: dict[str, object] = {
        "observed": observed.isoformat(),
        "horizon_days": horizon_days,
        "dates_requested": len(dates),
        "fetched": 0,
        "skipped": 0,
        "rows": {},
        "errors": [],
        "ok": True,
        "blocked_until": block["until"] if block else None,
    }
    if dry_run:
        todo = [d for d in dates if not forward_path(root, observed, d).is_file()]
        result.update(skipped=len(dates) - len(todo), would_fetch=len(todo))
        return result
    if block and not ignore_block and now < dt.datetime.fromisoformat(block["until"]):
        result.update(dates_requested=0, skipped_reason="blocked")
        return result

    client = client or NasdaqClient(interval_seconds=interval_seconds or DEFAULT_INTERVAL_SECONDS)
    rows: dict[str, int] = {}
    fetched = skipped = 0
    failure: str | None = None
    for day in dates:
        outcome: dict[str, object] | None = None
        for attempt in range(max_retries + 1):
            try:
                outcome = fetch_forward(client, root, observed, day)
                break
            except requests.RequestException as exc:
                # 네트워크 오류만 다시 해 본다.
                if attempt == max_retries:
                    failure = f"{day}: {type(exc).__name__}: {exc}"
            except NasdaqError as exc:
                # HTTP 오류·JSON 아님·data 키 없음 — 막힌 것일 수 있어 다시 하지 않는다.
                failure = f"{day}: {exc}"
                break
        if failure is not None:
            break
        assert outcome is not None
        if outcome["status"] == "skipped":
            skipped += 1
        else:
            fetched += 1
            rows[day.isoformat()] = int(outcome.get("rows") or 0)

    result.update(fetched=fetched, skipped=skipped, rows=rows)
    if failure is not None:
        record = _write_block(root, now, failure, block)
        result.update(errors=[failure], ok=False, blocked_until=record["until"])
    elif block is not None:
        block_path(root).unlink(missing_ok=True)
        result.update(blocked_until=None, block_cleared=True)
    return result

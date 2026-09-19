"""FRED / ALFRED — vintage가 있어야 거시 피쳐가 PIT다 (04 C5).

미국 계획 01 §1, 03 §3, 06 §1, 연구
[`08_fred_alfred`](../../../../../my/milestones/us/research/data/open_apis/08_fred_alfred.md).

* **`get_series()`를 쓰면 안 된다.** 오늘 값으로 과거 피쳐를 만들게 된다.
  `CPIAUCSL`은 2010-01-15 시점 값과 지금 값이 **겹치는 구간에서 54건 다르다.**
* **realtime 구간당 vintage 2,000개가 API 한도다.** 일별로 나오는 시장 관측값
  (`DGS10`·`VIXCLS`·`SP500`)은 전 구간을 한 번에 못 받는다 —
  `series/vintagedates`로 목록을 받아 **끊어서 받고 이어 붙인다.**
* **이어 붙인 뒤 연속 중복을 접는다.** 끊은 구간의 첫 행은 `realtime_start`가
  구간 시작으로 잘려 오므로, 같은 관측일에서 **값이 안 바뀐 연속 행**을 지운다.
  값이 되돌아간 것(A→B→A)은 남는다 — 연속이 아니다.
"""

from __future__ import annotations

import datetime as dt
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pyar

from collector.lake import DataRoot

#: 거시 exposure 다섯 축 + 물가·고용·생산·통화·지수. 연구 §1의 표 그대로다.
#: **조사 문서가 "15개"라고 적었는데 표에는 18개가 있다** (2026-09-19 확인).
FRED_SERIES: dict[str, str] = {
    "DGS10": "금리",
    "DGS2": "금리",
    "T10Y2Y": "금리",
    "FEDFUNDS": "금리",
    "DCOILWTICO": "유가",
    "DTWEXBGS": "환율",
    "BAMLH0A0HYM2": "신용",
    "DBAA": "신용",
    "BAA10Y": "신용",
    "AAA10Y": "신용",
    "VIXCLS": "변동성",
    "CPIAUCSL": "물가",
    "PPIACO": "물가",
    "PAYEMS": "고용",
    "UNRATE": "고용",
    "GDP": "생산",
    "M2SL": "통화",
    "SP500": "지수",
}

#: API 한도가 2,000이다. 경계에 붙이지 않는다.
MAX_VINTAGES_PER_REQUEST = 1_800

#: 공표된 한도가 없다. FRED 문서가 오래 말해 온 분당 120회의 절반으로 둔다.
DEFAULT_INTERVAL_SECONDS = 1.0

_VINTAGE_CAP = re.compile(r"There are (\d+) vintage dates")
#: ALFRED에 없는 series가 있다 (`SP500`). 개정 이력 자체가 없다는 뜻이다.
_NO_ALFRED = re.compile(r"does not exist in ALFRED")


class FredError(RuntimeError):
    """FRED가 거부했거나 받은 것이 기대한 형식이 아니다."""


def api_key_from_env(env: dict[str, str] | None = None) -> str:
    """``FRED_API_KEY``. 기본값을 두지 않는다 — 이 저장소는 public이다."""
    import os

    env = os.environ if env is None else env
    key = env.get("FRED_API_KEY")
    if not key:
        raise FredError("FRED_API_KEY가 없습니다. .env를 확인하십시오 (README §7).")
    return key


@dataclass
class FredClient:
    """요청 하나를 책임진다. 간격이 클라이언트 안에 있다."""

    api_key: str
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    _fred: object = field(default=None, init=False, repr=False)
    _last_request_at: float = field(default=0.0, init=False, repr=False)
    requests_made: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        from fredapi import Fred

        self._fred = Fred(api_key=self.api_key)

    def _wait(self) -> None:
        gap = time.monotonic() - self._last_request_at
        if self._last_request_at and gap < self.interval_seconds:
            time.sleep(self.interval_seconds - gap)

    def _call(self, name: str, *args, **kw):
        self._wait()
        try:
            return getattr(self._fred, name)(*args, **kw)
        finally:
            self._last_request_at = time.monotonic()
            self.requests_made += 1

    def vintage_dates(self, series_id: str) -> list:
        return list(self._call("get_series_vintage_dates", series_id))

    def all_releases(self, series_id: str) -> list[tuple]:
        """``(realtime_start, date, value)`` 전부. 한도에 걸리면 끊어서 받는다.

        돌려주는 것은 **관측일 오름차순, 같은 관측일 안에서 realtime 오름차순**의
        행 목록이고 연속 중복은 이미 접혀 있다.
        """
        try:
            frame = self._call("get_series_all_releases", series_id)
            return _dedupe(_rows(frame))
        except ValueError as exc:
            if _NO_ALFRED.search(str(exc)):
                # 개정 이력이 없는 series다. 최신 한 벌만 받고 realtime_start를
                # null로 둔다 — **null은 "개정 이력을 못 받았다"는 표시다.**
                series = self._call("get_series", series_id)
                return [
                    (None, _as_date(d), None if _isna(v) else float(v))
                    for d, v in series.items()
                ]
            if not _VINTAGE_CAP.search(str(exc)):
                raise FredError(f"{series_id}: {exc}") from exc

        dates = self.vintage_dates(series_id)
        if not dates:
            raise FredError(f"{series_id}: vintage 목록이 비었다")
        rows: list[tuple] = []
        for i in range(0, len(dates), MAX_VINTAGES_PER_REQUEST):
            chunk = dates[i : i + MAX_VINTAGES_PER_REQUEST]
            frame = self._call(
                "get_series_all_releases",
                series_id,
                realtime_start=str(_as_date(chunk[0])),
                realtime_end=str(_as_date(chunk[-1])),
            )
            rows.extend(_rows(frame))
        return _dedupe(rows)


def _as_date(value) -> dt.date:
    return value.date() if hasattr(value, "date") else value


def _isna(value) -> bool:
    import pandas as pd

    return bool(pd.isna(value))


def _rows(frame) -> list[tuple]:
    """``fredapi``가 전치한 DataFrame을 돌려줘 dtype이 object다.

    원천이 ``.``(휴일·미발표)을 주면 ``NaN``이 되는데, 전치 탓에 ``NaT``로 오는
    열이 섞인다. **`pandas.isna`로 본다** — `math.isnan`은 `NaT`에서 죽는다.
    """
    out = []
    for rt, d, v in zip(frame["realtime_start"], frame["date"], frame["value"], strict=True):
        out.append((_as_date(rt), _as_date(d), None if _isna(v) else float(v)))
    return out


def _dedupe(rows: list[tuple]) -> list[tuple]:
    """같은 관측일에서 **값이 안 바뀐 연속 행**을 지운다.

    끊어 받으면 구간마다 같은 값이 다시 오고 ``realtime_start``가 구간 시작으로
    잘린다. 가장 이른 것만 남기면 된다. **값이 되돌아간 것은 안 지운다** —
    A→B→A에서 마지막 A는 앞 행과 연속이 아니다.
    """
    out: list[tuple] = []
    last: dict = {}
    # realtime_start 가 null 인 행(ALFRED에 없는 series)이 섞일 수 있다
    for rt, d, v in sorted(rows, key=lambda r: (r[1], r[0] or dt.date.min)):
        if d in last and last[d] == v:
            continue
        last[d] = v
        out.append((rt, d, v))
    return out


def load_macro_series(
    root: DataRoot,
    client: FredClient,
    *,
    snapshot_date: dt.date | str,
    series: dict[str, str] | None = None,
    observed_at: dt.datetime | None = None,
) -> dict[str, object]:
    """``macro_series`` 한 장 (03 §4.13). 축 이름을 행에 같이 박는다."""
    from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

    series = series or FRED_SERIES
    observed_at = observed_at or dt.datetime.now(dt.UTC)
    ids, axes, dates, rts, vals = [], [], [], [], []
    per_series: dict[str, int] = {}

    for series_id, axis in series.items():
        rows = client.all_releases(series_id)
        per_series[series_id] = len(rows)
        for rt, d, v in rows:
            ids.append(series_id)
            axes.append(axis)
            dates.append(d)
            rts.append(rt)
            vals.append(v)

    table = pyar.table(
        {
            "series_id": pyar.array(ids, type=pyar.string()),
            "axis": pyar.array(axes, type=pyar.string()),
            "date": pyar.array(dates, type=pyar.date32()),
            "realtime_start": pyar.array(rts, type=pyar.date32()),
            "value": pyar.array(vals, type=pyar.float64()),
            "observed_at": pyar.array(
                [observed_at] * len(ids), type=pyar.timestamp("us", tz="UTC")
            ),
            "source_rev": pyar.array([None] * len(ids), type=pyar.string()),
        }
    )
    dest: Path = snapshot_path(root, "macro_series", snapshot_date)
    write_snapshot_arrow(
        table, "macro_series", dest, unique_on=("series_id", "date", "realtime_start")
    )
    stats = verify_snapshot(
        dest, "macro_series", unique_on=("series_id", "date", "realtime_start")
    )
    return {
        "path": dest,
        "series": len(series),
        "requests": client.requests_made,
        "per_series": per_series,
        **stats,
    }

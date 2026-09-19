"""``api.nasdaq.com`` 실적 캘린더 — 컨센서스가 여기에만 있다.

미국 계획 01 §2.3, 04 C6 §2.1, 06 §1·§5.

* **브라우저 UA가 아니면 응답이 안 온다.** 403도 아니고 **읽기 타임아웃**이다
  (2026-09-19 실측: SEC용 연락처 UA로 30초 대기 후 끊김). 403이면 로그로
  보이지만 타임아웃은 네트워크 탓으로 읽히기 쉽다 — UA를 먼저 본다.
* **``marketCap``은 버린다.** 과거 날짜 행에도 **오늘 값**이 들어 있다
  (01 §2.3). 같은 2015-01-27 행의 AAPL ``marketCap``이 일주일 만에
  4.60조 → 4.92조로 바뀌는 것을 봤다. 행 단위가 아니라 **컬럼 단위로**
  as-of를 봐야 한다.
* **캐시를 걸지 않는다.** ``epsForecast``가 나중에 고쳐지는지 보는 것이
  목적이다 (05 §6.1).
"""

from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

from collector.lake import DataRoot

EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"

#: 브라우저 UA. 비밀이 아니다 — 없으면 응답 자체가 안 온다.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

#: 05 §2의 2,016요청·2.8시간이 이 간격이다. backfill은 더 좁혀 돌렸다 (04 §2.16).
DEFAULT_INTERVAL_SECONDS = 5.0

#: 검정 구간 시작 (07 X3). 그 앞은 가격에 생존편향이 있어 받아도 못 쓴다.
EARNINGS_START = dt.date(2018, 7, 2)

#: 안 쓰는 컬럼. 저장은 원문 그대로 하고 표에서만 뺀다.
DROPPED_FIELDS = ("marketCap",)


class NasdaqError(RuntimeError):
    """Nasdaq이 거부했거나 받은 것이 기대한 형식이 아니다."""


@dataclass
class NasdaqClient:
    """요청 하나를 책임진다. 간격이 클라이언트 안에 있다."""

    user_agent: str = BROWSER_USER_AGENT
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    session: requests.Session = field(default_factory=requests.Session)
    _last_request_at: float = field(default=0.0, init=False, repr=False)

    def _wait(self) -> None:
        gap = time.monotonic() - self._last_request_at
        if self._last_request_at and gap < self.interval_seconds:
            time.sleep(self.interval_seconds - gap)

    def get_json(self, url: str, params: dict[str, str]) -> dict:
        self._wait()
        try:
            resp = self.session.get(
                url,
                params=params,
                headers={"User-Agent": self.user_agent, "Accept": "application/json"},
                timeout=60,
            )
        finally:
            self._last_request_at = time.monotonic()
        if resp.status_code != 200:
            raise NasdaqError(f"{resp.status_code} {url} {params}")
        try:
            return resp.json()
        except ValueError as exc:
            raise NasdaqError(f"JSON이 아니다: {resp.text[:120]!r}") from exc


def earnings_path(root: DataRoot, day: dt.date | str) -> Path:
    """``raw/nasdaq/earnings_calendar/date=<d>.json`` — 원문을 그대로 둔다 (X1)."""
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    return root.raw / "nasdaq" / "earnings_calendar" / f"date={d}.json"


def fetch_earnings(
    client: NasdaqClient,
    root: DataRoot,
    day: dt.date | str,
    *,
    skip_existing: bool = False,
) -> dict[str, object]:
    """그 날짜의 실적 캘린더를 받아 원문 그대로 굳히고 세어 돌려준다.

    ``skip_existing``의 기본값이 **False**다. 같은 URL이 나중에 다른 값을 주는
    원천이라(``epsForecast`` 정정) 있는 파일로 갈음하면 볼 것을 못 본다.
    """
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    dest = earnings_path(root, d)
    if skip_existing and dest.is_file():
        doc = json.loads(dest.read_text())
    else:
        doc = client.get_json(EARNINGS_URL, {"date": d})
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(doc, ensure_ascii=False))
    return {"date": d, "path": dest, **summarize_earnings(doc)}


def summarize_earnings(doc: dict) -> dict[str, object]:
    """행 수와 컬럼별 채움률. **``data``가 null이면 0행이다** (06 §1)."""
    data = doc.get("data") or {}
    rows = data.get("rows") or []
    filled = {
        f"n_{key}": sum(1 for r in rows if (r.get(key) or "").strip() not in ("", "N/A"))
        for key in ("eps", "epsForecast", "surprise", "noOfEsts")
    }
    return {
        "as_of": data.get("asOf"),
        "rows": len(rows),
        "symbols": len({r.get("symbol") for r in rows if r.get("symbol")}),
        **filled,
    }


def sample_dates(
    start: tuple[int, int] = (2018, 3),
    end: tuple[int, int] = (2026, 2),
    weekday: int = 3,
) -> list[dt.date]:
    """분기당 2일. **실적 시즌 한복판을 고른다** (04 C6 §2.1).

    분기 Q의 실적은 다음 분기 첫 6주에 몰린다. 그래서 그 분기의 **첫 달 마지막
    목요일**과 **둘째 달 첫 목요일**을 잡는다 — 1월 말·2월 초, 7월 말·8월 초다.
    한산한 날을 뽑으면 행 수가 적은 것이 원천 구멍인지 그날이 비었던 것인지
    못 가른다.
    """
    out: list[dt.date] = []
    y, q = start
    while (y, q) <= end:
        first_month = 3 * (q - 1) + 1
        out.append(_last_weekday(y, first_month, weekday))
        nxt_y, nxt_m = (y + 1, 1) if first_month == 12 else (y, first_month + 1)
        out.append(_first_weekday(nxt_y, nxt_m, weekday))
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


def _first_weekday(year: int, month: int, weekday: int) -> dt.date:
    d = dt.date(year, month, 1)
    return d + dt.timedelta(days=(weekday - d.weekday()) % 7)


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    nxt = dt.date(year + (month == 12), (month % 12) + 1, 1)
    d = nxt - dt.timedelta(days=1)
    return d - dt.timedelta(days=(d.weekday() - weekday) % 7)


def scan_earnings_sample(
    client: NasdaqClient,
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    dates: list[dt.date] | None = None,
    filings_index: Path | None = None,
) -> dict[str, object]:
    """표본 날짜를 훑어 ``output/scan/``에 CSV로 남긴다 (04 C6 §2.1).

    **전수 backfill의 시작일을 정하려는 것이다.** 행 수만으로는 원천이 얇은
    것인지 그날이 한산한 것인지 못 가르므로, ``filings_index``를 주면 같은
    날짜의 **8-K 항목 2.02(실적 발표)** 건수를 같이 적는다 — 우리가 이미 가진
    공시로 만든 대조군이다. 요청이 더 들지 않는다.
    """
    import csv

    import duckdb

    dates = dates or sample_dates()
    rows = [fetch_earnings(client, root, d) for d in dates]

    if filings_index is not None:
        con = duckdb.connect()
        counts = dict(
            con.execute(
                f"""SELECT filing_date, count(DISTINCT cik) FROM read_parquet('{filings_index}')
                    WHERE form = '8-K' AND items LIKE '%2.02%' GROUP BY 1"""
            ).fetchall()
        )
        for r in rows:
            r["filings_8k_item202"] = counts.get(dt.date.fromisoformat(str(r["date"])), 0)

    out = root.output / "scan" / f"snapshot_date={snapshot_date}"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "nasdaq_earnings_sample.csv"
    fields = [k for k in rows[0] if k != "path"]
    with dest.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return {"path": dest, "dates": len(rows), "rows": rows}


#: ``Dec/2023`` 꼴 회계분기. 2,065일 표본에서 예외가 없었다.
_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def parse_money(text: str | None) -> float | None:
    """``$2.18`` → 2.18, ``($0.02)`` → −0.02, ``N/A`` → None.

    **괄호가 음수다.** 회계 표기라 빼먹으면 적자 종목의 EPS 부호가 뒤집힌다.
    """
    raw = (text or "").strip()
    if not raw or raw.upper() in ("N/A", "NA", "-", "--"):
        return None
    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = raw.strip("()").replace("$", "").replace(",", "").replace("%", "").strip()
    if not cleaned:
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return -value if negative else value


def parse_fiscal_quarter(text: str | None) -> dt.date | None:
    """``Dec/2023`` → 2023-12-31. 그 달의 **마지막 날**로 둔다."""
    raw = (text or "").strip()
    if "/" not in raw:
        return None
    mon, _, year = raw.partition("/")
    month = _MONTHS.get(mon[:3].title())
    if month is None or not year.isdigit():
        return None
    y = int(year)
    return dt.date(y + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1)


def load_earnings_calendar(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
) -> dict[str, object]:
    """``raw/nasdaq/earnings_calendar/``의 JSON들을 ``earnings_calendar`` 한 장으로.

    **`marketCap`은 안 담는다** — 과거 행에도 오늘 값이 들어 있다 (01 §2.3).
    """
    import pyarrow as pyar

    from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

    observed_at = observed_at or dt.datetime.now(dt.UTC)
    src_dir = root.raw / "nasdaq" / "earnings_calendar"
    files = sorted(src_dir.glob("date=*.json"))
    if not files:
        raise NasdaqError(f"{src_dir}에 받아 둔 JSON이 없다. 먼저 받는다.")

    cols: dict[str, list] = {
        k: []
        for k in ("date", "symbol", "name", "eps", "eps_forecast", "surprise_pct",
                  "n_estimates", "fiscal_quarter_ending", "fiscal_period_end", "time_code")
    }
    seen: set[tuple[dt.date, str]] = set()
    duplicate_rows = empty_days = asof_mismatch = 0

    for path in files:
        day = dt.date.fromisoformat(path.stem.split("=", 1)[1])
        doc = json.loads(path.read_text())
        data = doc.get("data") or {}
        rows = data.get("rows") or []
        if not rows:
            empty_days += 1
            continue
        # asOf 는 사람이 읽는 꼴("Thu, Feb 1, 2024")이라 날짜로 다시 파싱하지 않고
        # 연·일만 본다. 다른 날짜가 오면 요청이 조용히 무시된 것이다 (06 §1)
        as_of = (data.get("asOf") or "")
        if as_of and (str(day.year) not in as_of or f" {day.day}," not in as_of):
            asof_mismatch += 1
        for row in rows:
            symbol = (row.get("symbol") or "").strip()
            if not symbol or (day, symbol) in seen:
                duplicate_rows += 1 if symbol else 0
                continue
            seen.add((day, symbol))
            cols["date"].append(day)
            cols["symbol"].append(symbol)
            cols["name"].append((row.get("name") or "").strip() or None)
            cols["eps"].append(parse_money(row.get("eps")))
            cols["eps_forecast"].append(parse_money(row.get("epsForecast")))
            cols["surprise_pct"].append(parse_money(row.get("surprise")))
            n_est = (row.get("noOfEsts") or "").strip()
            cols["n_estimates"].append(int(n_est) if n_est.isdigit() else None)
            fq = (row.get("fiscalQuarterEnding") or "").strip() or None
            cols["fiscal_quarter_ending"].append(fq)
            cols["fiscal_period_end"].append(parse_fiscal_quarter(fq))
            cols["time_code"].append((row.get("time") or "").strip() or None)

    n = len(cols["symbol"])
    table = pyar.table(
        {
            "date": pyar.array(cols["date"], type=pyar.date32()),
            "symbol": pyar.array(cols["symbol"], type=pyar.string()),
            "name": pyar.array(cols["name"], type=pyar.string()),
            "eps": pyar.array(cols["eps"], type=pyar.float64()),
            "eps_forecast": pyar.array(cols["eps_forecast"], type=pyar.float64()),
            "surprise_pct": pyar.array(cols["surprise_pct"], type=pyar.float64()),
            "n_estimates": pyar.array(cols["n_estimates"], type=pyar.int32()),
            "fiscal_quarter_ending": pyar.array(
                cols["fiscal_quarter_ending"], type=pyar.string()
            ),
            "fiscal_period_end": pyar.array(cols["fiscal_period_end"], type=pyar.date32()),
            "time_code": pyar.array(cols["time_code"], type=pyar.string()),
            "observed_at": pyar.array(
                [observed_at] * n, type=pyar.timestamp("us", tz="UTC")
            ),
        }
    )
    dest = snapshot_path(root, "earnings_calendar", snapshot_date)
    write_snapshot_arrow(table, "earnings_calendar", dest, unique_on=("date", "symbol"))
    stats = verify_snapshot(dest, "earnings_calendar", unique_on=("date", "symbol"))
    return {
        "path": dest,
        "files": len(files),
        "empty_days": empty_days,
        "duplicate_rows": duplicate_rows,
        "asof_mismatch": asof_mismatch,
        **stats,
    }

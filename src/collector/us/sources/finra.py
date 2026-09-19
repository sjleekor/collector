"""FINRA — 공매도 잔고와 off-exchange 공매도 거래량 (04 C7).

미국 계획 01 §2.4, 03 §4.4, 06 §1, 연구
[`07_finra`](../../../../../my/milestones/us/research/data/open_apis/07_finra.md).

**두 원천이 성격이 완전히 다르다.**

* **잔고**(`api.finra.org`)는 **날짜당 벌크**다. 한 결제일이 1만 5천\\~1만 7천 행이고
  ``limit`` 상한이 5,000이라 날짜당 4요청이다. 종목당 요청을 걸면 12,500요청이
  되는데 그 길로 가지 않는다 (D6). **``revisionFlag``가 정정을 직접 표시한다.**
* **거래량**(`cdn.finra.org`)은 **날짜당 정적 파일**이다. off-exchange TRF·ADF·ORF
  공개 거래만 담는다 — **전체 시장 short volume이 아니다.** 분모를 시장 전체로
  잡으면 값이 틀린다.

**과거 파일에 ``ShortExemptVolume``이 없다** (연구 §2.1). 최신 형식만 아는
판정기는 과거 파일을 통째로 거절한다 — 머리글을 읽어 맞춘다.

**정정 파일 규칙을 원천이 안 밝혔다** (연구 §2.5). 그래서 거래량 쪽은
``skip_existing``으로 건너뛰되 **원문을 남겨 나중에 sha로 대조할 수 있게 둔다.**
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

from collector.lake import DataRoot

SHORT_INTEREST_URL = (
    "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
)
REGSHO_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{ymd}.txt"

#: 원천이 밝힌 상한. ``record-max-limit`` 헤더로도 온다.
PAGE_LIMIT = 5_000

#: 잔고는 POST API다. 공표된 한도가 없어 직렬 1초로 둔다 — 벌크 조회용 API이고
#: 8년 backfill이 772요청뿐이다. 막히면 429가 오고 이어받기가 되므로 잃는 것이 없다.
API_INTERVAL_SECONDS = 1.0
#: CDN의 정적 텍스트 파일이다.
CDN_INTERVAL_SECONDS = 1.0

#: consolidated 파일 시작일 (연구 §2.6). 그 앞은 venue별 파일이라 형식이 다르다.
REGSHO_START = dt.date(2018, 8, 1)
#: 잔고가 실재하는 첫 결제일 (01 §2.4 실측). 2017-09-15는 0행이다.
SHORT_INTEREST_START = dt.date(2018, 9, 14)


class FinraError(RuntimeError):
    """FINRA가 거부했거나 받은 것이 기대한 형식이 아니다."""


@dataclass
class FinraClient:
    api_interval_seconds: float = API_INTERVAL_SECONDS
    cdn_interval_seconds: float = CDN_INTERVAL_SECONDS
    session: requests.Session = field(default_factory=requests.Session)
    _last: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    requests_made: int = field(default=0, init=False)

    def _wait(self, kind: str, interval: float) -> None:
        prev = self._last.get(kind, 0.0)
        gap = time.monotonic() - prev
        if prev and gap < interval:
            time.sleep(interval - gap)

    def post_csv(self, url: str, body: dict) -> tuple[str, int]:
        """CSV 본문과 ``record-total``. 헤더가 전체 행 수를 준다."""
        self._wait("api", self.api_interval_seconds)
        try:
            resp = self.session.post(
                url, json=body, headers={"Accept": "text/plain"}, timeout=120
            )
        finally:
            self._last["api"] = time.monotonic()
            self.requests_made += 1
        if resp.status_code == 204:
            return "", 0  # 그 날짜에 행이 없다. 실패가 아니다 (06 §1)
        if resp.status_code != 200:
            raise FinraError(f"{resp.status_code} {url} {body} — {resp.text[:200]}")
        total = resp.headers.get("record-total")
        return resp.text, int(total) if total and total.isdigit() else 0

    def get_text(self, url: str) -> str:
        self._wait("cdn", self.cdn_interval_seconds)
        try:
            resp = self.session.get(url, timeout=120)
        finally:
            self._last["cdn"] = time.monotonic()
            self.requests_made += 1
        if resp.status_code == 404:
            raise FileNotFoundError(url)
        if resp.status_code != 200:
            raise FinraError(f"{resp.status_code} {url}")
        return resp.text


# --- 결제일 -----------------------------------------------------------------


def settlement_dates(sessions: list[dt.date], *, start: dt.date, end: dt.date) -> list[dt.date]:
    """공매도 잔고 결제일. **달마다 둘이다 — 15일과 말일.**

    둘 다 거래일이 아니면 **그 앞 거래일**로 당긴다. 2018-09-15가 토요일이라
    실제 결제일이 **2018-09-14**인 것이 이 규칙이다 (01 §2.4 실측).
    """
    trading = sorted(d for d in sessions if d <= end)
    if not trading:
        return []
    out: list[dt.date] = []
    months = sorted({(d.year, d.month) for d in trading})
    for year, month in months:
        mid = dt.date(year, month, 15)
        last_day = dt.date(year + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1)
        for target in (mid, last_day):
            # **아직 안 지난 기준일은 후보로 만들지 않는다.** 만들면 그 달의
            # 마지막 거래일이 결제일인 것처럼 들어와 204를 부른다
            if target > end:
                continue
            prior = [d for d in trading if d <= target]
            if prior:
                out.append(prior[-1])
    return sorted({d for d in out if start <= d <= end})


# --- 공매도 잔고 -------------------------------------------------------------


def short_interest_path(root: DataRoot, day: dt.date | str) -> Path:
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    return root.raw / "finra" / "short_interest" / f"settlement_date={d}.csv"


def fetch_short_interest(
    client: FinraClient,
    root: DataRoot,
    day: dt.date | str,
    *,
    skip_existing: bool = True,
) -> dict[str, object]:
    """한 결제일 전량을 받아 CSV 한 장으로 굳힌다. 페이지를 이어 붙인다.

    ``skip_existing``의 기본값이 **True**다 — 8년 backfill을 나눠 돌리기 때문이다.
    **정정을 보려면 False로 다시 돈다.** ``revisionFlag``가 원천의 표시다.
    """
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    dest = short_interest_path(root, d)
    if skip_existing and dest.is_file():
        rows = sum(1 for _ in dest.open()) - 1
        return {"date": d, "path": dest, "rows": rows, "skipped": True, "pages": 0}

    header: str | None = None
    body_lines: list[str] = []
    offset, total, pages = 0, None, 0
    while True:
        text, record_total = client.post_csv(
            SHORT_INTEREST_URL,
            {
                "limit": PAGE_LIMIT,
                "offset": offset,
                "compareFilters": [
                    {"fieldName": "settlementDate", "fieldValue": d, "compareType": "EQUAL"}
                ],
            },
        )
        pages += 1
        total = record_total if total is None else total
        lines = text.splitlines()
        if not lines:
            break
        if header is None:
            header = lines[0]
        body_lines.extend(lines[1:])
        offset += PAGE_LIMIT
        if offset >= (total or 0) or len(lines) <= 1:
            break

    if not body_lines:
        return {"date": d, "path": None, "rows": 0, "skipped": False, "pages": pages}
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join([header or "", *body_lines]) + "\n")
    return {
        "date": d,
        "path": dest,
        "rows": len(body_lines),
        "record_total": total,
        "skipped": False,
        "pages": pages,
    }


#: 표시가 안 된 것으로 읽을 값. 나머지 비어 있지 않은 값은 전부 표시로 본다.
_FLAG_FALSE = ("", "N", "NO", "FALSE", "0")


def _flag(value: str | None) -> bool:
    """**표시 칸이다. `Y`가 아니라 `R`·`S`로 온다** (2026-09-19 실측).

    ``revisionFlag``는 ``R``(24,753행), ``stockSplitFlag``는 ``S``(5,896행)이고
    나머지는 빈칸이다. ``Y``만 참으로 보면 **정정된 행이 전부 거짓이 된다.**
    빈칸은 표시가 없다는 뜻이므로 ``False``다 — 모른다가 아니다.
    """
    text = (value or "").strip().upper()
    return text not in _FLAG_FALSE


def _num(value: str | None) -> float | None:
    if value is None or value.strip() == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _int(value: str | None) -> int | None:
    n = _num(value)
    return None if n is None else int(n)


def load_short_interest(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
) -> dict[str, object]:
    """``raw/finra/short_interest/``의 CSV들을 ``short_interest`` 한 장으로 (03 §4.4)."""
    import pyarrow as pyar

    from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

    observed_at = observed_at or dt.datetime.now(dt.UTC)
    src_dir = root.raw / "finra" / "short_interest"
    files = sorted(src_dir.glob("settlement_date=*.csv"))
    if not files:
        raise FinraError(f"{src_dir}에 받아 둔 CSV가 없다. 먼저 받는다.")

    cols: dict[str, list] = {
        k: []
        for k in ("settlement_date", "symbol", "current_short_qty", "previous_short_qty",
                  "avg_daily_volume_qty", "days_to_cover", "change_percent",
                  "revision_flag", "stock_split_flag", "market_class")
    }
    seen: set[tuple[dt.date, str]] = set()
    duplicate_rows = 0
    for path in files:
        with path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                day_text = (row.get("settlementDate") or "").strip()
                symbol = (row.get("symbolCode") or "").strip()
                if not day_text or not symbol:
                    continue
                day = dt.date.fromisoformat(day_text)
                if (day, symbol) in seen:
                    duplicate_rows += 1
                    continue
                seen.add((day, symbol))
                cols["settlement_date"].append(day)
                cols["symbol"].append(symbol)
                cols["current_short_qty"].append(_int(row.get("currentShortPositionQuantity")))
                cols["previous_short_qty"].append(_int(row.get("previousShortPositionQuantity")))
                cols["avg_daily_volume_qty"].append(_int(row.get("averageDailyVolumeQuantity")))
                cols["days_to_cover"].append(_num(row.get("daysToCoverQuantity")))
                cols["change_percent"].append(_num(row.get("changePercent")))
                cols["revision_flag"].append(_flag(row.get("revisionFlag")))
                cols["stock_split_flag"].append(_flag(row.get("stockSplitFlag")))
                cols["market_class"].append((row.get("marketClassCode") or "").strip() or None)

    n = len(cols["symbol"])
    table = pyar.table(
        {
            "settlement_date": pyar.array(cols["settlement_date"], type=pyar.date32()),
            "symbol": pyar.array(cols["symbol"], type=pyar.string()),
            "current_short_qty": pyar.array(cols["current_short_qty"], type=pyar.int64()),
            "previous_short_qty": pyar.array(cols["previous_short_qty"], type=pyar.int64()),
            "avg_daily_volume_qty": pyar.array(cols["avg_daily_volume_qty"], type=pyar.int64()),
            "days_to_cover": pyar.array(cols["days_to_cover"], type=pyar.float64()),
            "change_percent": pyar.array(cols["change_percent"], type=pyar.float64()),
            "revision_flag": pyar.array(cols["revision_flag"], type=pyar.bool_()),
            "stock_split_flag": pyar.array(cols["stock_split_flag"], type=pyar.bool_()),
            "market_class": pyar.array(cols["market_class"], type=pyar.string()),
            "observed_at": pyar.array(
                [observed_at] * n, type=pyar.timestamp("us", tz="UTC")
            ),
        }
    )
    dest = snapshot_path(root, "short_interest", snapshot_date)
    write_snapshot_arrow(
        table, "short_interest", dest, unique_on=("settlement_date", "symbol")
    )
    stats = verify_snapshot(dest, "short_interest", unique_on=("settlement_date", "symbol"))
    return {
        "path": dest,
        "files": len(files),
        "settlement_dates": len({d for d, _ in seen}),
        "duplicate_rows": duplicate_rows,
        **stats,
    }


# --- off-exchange 공매도 거래량 ----------------------------------------------


def regsho_path(root: DataRoot, day: dt.date | str) -> Path:
    d = day.isoformat() if isinstance(day, dt.date) else str(day)
    return root.raw / "finra" / "regsho" / f"date={d}.txt"


def fetch_regsho(
    client: FinraClient,
    root: DataRoot,
    day: dt.date,
    *,
    skip_existing: bool = True,
) -> dict[str, object]:
    """그날의 consolidated 파일 하나. 없는 날은 ``missing``으로 돌려준다."""
    dest = regsho_path(root, day)
    if skip_existing and dest.is_file():
        return {"date": day, "path": dest, "skipped": True, "missing": False}
    try:
        text = client.get_text(REGSHO_URL.format(ymd=day.strftime("%Y%m%d")))
    except FileNotFoundError:
        return {"date": day, "path": None, "skipped": False, "missing": True}
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text)
    return {"date": day, "path": dest, "skipped": False, "missing": False, "bytes": len(text)}


def parse_regsho(text: str) -> tuple[list[dict[str, str]], int | None]:
    """``|`` 구분 텍스트. **마지막 줄이 레코드 수**라 데이터로 세면 한 줄 더 잡힌다.

    ``ShortExemptVolume``이 없는 옛 형식도 받는다 — 머리글로 칸을 잡는다 (연구 §2.1).
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return [], None
    trailer = None
    if lines[-1].strip().isdigit():
        trailer = int(lines[-1].strip())
        lines = lines[:-1]
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="|")
    return [r for r in reader if (r.get("Symbol") or "").strip()], trailer


def load_short_volume(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
) -> dict[str, object]:
    """``raw/finra/regsho/``의 TXT들을 ``short_volume`` 한 장으로 (03 §4.16).

    **전체 시장 공매도 거래량이 아니다.** off-exchange TRF·ADF·ORF 공개 거래만
    담는다 — 분모를 시장 전체로 잡으면 값이 틀린다 (연구 §1).
    """
    import pyarrow as pyar
    import pyarrow.parquet as pq

    from collector.us.store.schema import ARROW_SCHEMAS
    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or dt.datetime.now(dt.UTC)
    src_dir = root.raw / "finra" / "regsho"
    files = sorted(src_dir.glob("date=*.txt"))
    if not files:
        raise FinraError(f"{src_dir}에 받아 둔 파일이 없다. 먼저 받는다.")

    schema = ARROW_SCHEMAS["short_volume"]
    dest = snapshot_path(root, "short_volume", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)
    trailer_mismatch: list[str] = []
    no_exempt = 0
    written = 0

    with pq.ParquetWriter(dest, schema, compression="zstd") as writer:
        for path in files:
            day = dt.date.fromisoformat(path.stem.split("=", 1)[1])
            rows, trailer = parse_regsho(path.read_text())
            if trailer is not None and trailer != len(rows):
                trailer_mismatch.append(f"{day}:{trailer}!={len(rows)}")
            if rows and "ShortExemptVolume" not in rows[0]:
                no_exempt += 1
            n = len(rows)
            if not n:
                continue
            writer.write_table(
                pyar.table(
                    {
                        "date": pyar.array([day] * n, type=pyar.date32()),
                        "symbol": pyar.array(
                            [(r.get("Symbol") or "").strip() for r in rows], type=pyar.string()
                        ),
                        "short_volume": pyar.array(
                            [_num(r.get("ShortVolume")) for r in rows], type=pyar.float64()
                        ),
                        "short_exempt_volume": pyar.array(
                            [_num(r.get("ShortExemptVolume")) for r in rows],
                            type=pyar.float64(),
                        ),
                        "total_volume": pyar.array(
                            [_num(r.get("TotalVolume")) for r in rows], type=pyar.float64()
                        ),
                        # consolidated 파일의 Market 은 venue 목록이다 ("B,Q,N").
                        # 단일 코드로 파싱하면 틀린다 (연구 §2.4)
                        "market": pyar.array(
                            [(r.get("Market") or "").strip() or None for r in rows],
                            type=pyar.string(),
                        ),
                        "observed_at": pyar.array(
                            [observed_at] * n, type=pyar.timestamp("us", tz="UTC")
                        ),
                    }
                ).select(schema.names).cast(schema)
            )
            written += n

    stats = verify_snapshot(dest, "short_volume", unique_on=("date", "symbol"))
    return {
        "path": dest,
        "files": len(files),
        "rows_written": written,
        "trailer_mismatch": trailer_mismatch,
        "files_without_short_exempt": no_exempt,
        **stats,
    }

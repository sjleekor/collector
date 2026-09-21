"""Wayback 이 뜬 nasdaqtrader 심볼 디렉터리 — ``is_etf``·``test_issue``의 PIT 원천.

``dolt``의 ``symbol``은 PK가 ``act_symbol`` 하나뿐이라 **오늘 값만** 있다
(03 §5.4). 과거 시점 상장 상태는 이 원문에서만 나온다.

**헤더 이름으로 컬럼을 잡는다. 위치로 잡지 않는다.** 형식이 셋이다:

* ``nasdaqlisted``: ``Symbol|Security Name|Market Category|Test Issue|
  Financial Status|Round Lot Size|ETF|NextShares``
* ``otherlisted`` (현재): ``ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|
  Round Lot Size|Test Issue|NASDAQ Symbol``
* ``otherlisted`` (2008): 위에서 ``NASDAQ Symbol``이 없고 첫 칸이 ``Symbol``이다

``as_of``는 **파일이 스스로 밝힌** 마지막 줄의 ``File Creation Time``이다 —
아카이브 timestamp가 아니다 (03 §3).

> **원문을 텍스트로 읽어 저장하면 안 된다.** 이 디렉터리의 파일 10개가 gzip
> 응답을 UTF-8로 디코딩해 저장한 탓에 ``\\x8b``가 U+FFFD로 바뀌어 있었다
> (2026-09-19에 다시 받아 복구). 되돌릴 수 없는 손상이다 — ``fetch``는 항상
> 바이트를 돌려준다 (02 §2.1).
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from pathlib import Path

#: 원문 헤더 이름 -> 우리 컬럼 이름.
_COLUMN_ALIASES = {
    "symbol": "symbol",
    "act symbol": "symbol",
    "security name": "security_name",
    "exchange": "exchange",
    "market category": "market_category",
    "etf": "is_etf",
    "test issue": "test_issue",
    "financial status": "financial_status",
    "round lot size": "round_lot_size",
}

_CREATION_PREFIX = "File Creation Time:"


class WaybackParseError(ValueError):
    """원문이 기대한 모양이 아니다."""


@dataclass(frozen=True)
class ListingFile:
    """원문 하나. ``kind``와 Wayback ``snapshot``은 파일 이름에서 나온다."""

    path: Path

    @property
    def kind(self) -> str:
        return self.path.stem.split("_", 1)[0]

    @property
    def snapshot(self) -> str:
        return self.path.stem.split("_", 1)[1]


def symdir_dir(root) -> Path:
    """``raw/wayback/symdir/`` — 2026-09-18에 ``derived/``에서 옮겨왔다."""
    return root.raw / "wayback" / "symdir"


def _parse_creation_time(line: str) -> _dt.datetime:
    """``File Creation Time: 0828202621:31`` -> 2026-08-28 21:31."""
    body = line.split(_CREATION_PREFIX, 1)[1].split("|", 1)[0].strip()
    return _dt.datetime.strptime(body, "%m%d%Y%H:%M")


def parse_listing_file(path: Path) -> tuple[_dt.datetime, list[dict[str, str | None]]]:
    """원문 하나를 (as_of, 행들)로 푼다.

    바이트로 읽고 UTF-8로 디코딩하되 **깨진 바이트가 있으면 알린다** —
    조용히 대체 문자로 바꾸면 위 주석의 손상을 또 만든다.
    """
    raw = path.read_bytes()
    if raw[:2] in (b"\x1f\x8b", b"\x1f\xef"):
        raise WaybackParseError(
            f"{path.name}: gzip 바이트로 시작한다. 풀어서 저장하거나, "
            "텍스트로 디코딩해 망가진 파일이면 다시 받는다."
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise WaybackParseError(f"{path.name}: 비어 있다")

    header = [h.strip().lower() for h in lines[0].split("|")]
    if "symbol" not in header and "act symbol" not in header:
        raise WaybackParseError(f"{path.name}: 헤더가 아니다 — {lines[0][:60]!r}")
    index = {_COLUMN_ALIASES[h]: i for i, h in enumerate(header) if h in _COLUMN_ALIASES}

    creation = [ln for ln in lines if ln.startswith(_CREATION_PREFIX)]
    if not creation:
        raise WaybackParseError(f"{path.name}: File Creation Time 줄이 없다")
    as_of = _parse_creation_time(creation[-1])

    rows: list[dict[str, str | None]] = []
    for line in lines[1:]:
        if line.startswith(_CREATION_PREFIX):
            continue
        cells = line.split("|")
        if len(cells) < 2:
            continue
        row: dict[str, str | None] = {}
        for name, i in index.items():
            row[name] = cells[i].strip() if i < len(cells) else None
        if not row.get("symbol"):
            continue
        rows.append(row)
    return as_of, rows


def build_listing_snapshots(
    root,
    *,
    snapshot_date,
    observed_at=None,
) -> dict[str, object]:
    """``raw/wayback/symdir/``의 원문 전부를 ``listing_snapshots`` 한 장으로 (03 §4.9)."""
    import pyarrow as pyar

    from collector.us.store.writer import snapshot_path, write_snapshot_arrow

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    files = sorted(symdir_dir(root).glob("*.txt"))
    if not files:
        raise WaybackParseError(f"{symdir_dir(root)} 에 원문이 없다")

    cols: dict[str, list] = {
        k: []
        for k in (
            "as_of",
            "as_of_time",
            "kind",
            "snapshot",
            "symbol",
            "security_name",
            "exchange",
            "market_category",
            "is_etf",
            "test_issue",
            "financial_status",
            "round_lot_size",
            "observed_at",
            "source_rev",
        )
    }
    per_file: list[tuple[str, int]] = []
    without_etf: list[str] = []

    def _flag(v):
        return None if v in (None, "") else v.upper() == "Y"

    for path in files:
        lf = ListingFile(path)
        as_of, rows = parse_listing_file(path)
        if rows and rows[0].get("is_etf") is None:
            without_etf.append(path.name)
        for r in rows:
            cols["as_of"].append(as_of.date())
            cols["as_of_time"].append(as_of)
            cols["kind"].append(lf.kind)
            cols["snapshot"].append(lf.snapshot)
            cols["symbol"].append(r.get("symbol"))
            cols["security_name"].append(r.get("security_name"))
            cols["exchange"].append(r.get("exchange"))
            cols["market_category"].append(r.get("market_category"))
            cols["is_etf"].append(_flag(r.get("is_etf")))
            cols["test_issue"].append(_flag(r.get("test_issue")))
            cols["financial_status"].append(r.get("financial_status") or None)
            rl = r.get("round_lot_size")
            cols["round_lot_size"].append(int(float(rl)) if rl else None)
            cols["observed_at"].append(observed_at)
            cols["source_rev"].append(path.name)
        per_file.append((path.name, len(rows)))

    tbl = pyar.table(
        {
            "as_of": pyar.array(cols["as_of"], type=pyar.date32()),
            "as_of_time": pyar.array(cols["as_of_time"], type=pyar.timestamp("us")),
            "kind": pyar.array(cols["kind"], type=pyar.string()),
            "snapshot": pyar.array(cols["snapshot"], type=pyar.string()),
            "symbol": pyar.array(cols["symbol"], type=pyar.string()),
            "security_name": pyar.array(cols["security_name"], type=pyar.string()),
            "exchange": pyar.array(cols["exchange"], type=pyar.string()),
            "market_category": pyar.array(cols["market_category"], type=pyar.string()),
            "is_etf": pyar.array(cols["is_etf"], type=pyar.bool_()),
            "test_issue": pyar.array(cols["test_issue"], type=pyar.bool_()),
            "financial_status": pyar.array(cols["financial_status"], type=pyar.string()),
            "round_lot_size": pyar.array(cols["round_lot_size"], type=pyar.int32()),
            "observed_at": pyar.array(cols["observed_at"], type=pyar.timestamp("us", tz="UTC")),
            "source_rev": pyar.array(cols["source_rev"], type=pyar.string()),
        }
    )
    dest = snapshot_path(root, "listing_snapshots", snapshot_date)
    write_snapshot_arrow(tbl, "listing_snapshots", dest, unique_on=("kind", "snapshot", "symbol"))
    empty = [n for n, c in per_file if c == 0]
    return {
        "path": dest,
        "files": len(files),
        "rows": tbl.num_rows,
        "empty_files": empty,
        "files_without_etf_column": len(without_etf),
        "bytes": dest.stat().st_size,
    }


# --- 과거 티커 → CIK 맵 (2026-09-21) ----------------------------------------
#
# **`universe_daily.cik`이 오늘자 맵에서 왔었다.** `raw/sec/company_tickers/`
# 한 벌은 SEC가 **지금** 상장돼 있다고 보는 종목의 맵이라, 상폐·피인수·개명한
# 회사가 구조적으로 빠진다. 2018년 행에 그걸 붙이면 **그 시점에 알 수 없던
# 정보**를 쓰는 것이다 — 끝까지 남은 종목은 98.9%가 붙고 사라진 종목은
# 26.5%만 붙었다 (2026-09-21 실측).
#
# Wayback에 같은 URL의 스냅샷이 562개 있다(2017-08\~). 받아서 `as_of`별로 두고
# **`date >= as_of` 최신**으로 붙이면 결측이 22.94% → 2.86%가 된다.
# 근거는 미국 2차 후속 `03_cik_pit_probe.md`다.
#
# **합집합으로 합치면 안 된다.** 스냅샷 19개 안에서만도 심볼 1,359개(4.34%)가
# 서로 다른 CIK 둘 이상을 가리킨다 — 티커 재사용이다.

#: Wayback CDX. ``collapse=digest``라 **내용이 바뀐 것만** 준다.
CDX_URL = "https://web.archive.org/cdx/search/cdx"

#: 아카이브 대상 URL.
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

#: Wayback 요청 간격. 공표된 한도가 없어 보수적으로 둔다.
WAYBACK_INTERVAL_SECONDS = 1.0


class WaybackFetchError(RuntimeError):
    """Wayback 요청이 실패했다."""


def company_tickers_dir(root) -> Path:
    """``raw/wayback/company_tickers/`` — ``symdir``과 같은 자리다."""
    return root.raw / "wayback" / "company_tickers"


def company_tickers_path(root, timestamp: str) -> Path:
    return company_tickers_dir(root) / f"company_tickers_{timestamp}.json"


@dataclass
class WaybackClient:
    """Wayback 요청 하나. 간격이 클라이언트 안에 있다 (``SecClient``와 같은 꼴)."""

    user_agent: str
    interval_seconds: float = WAYBACK_INTERVAL_SECONDS

    def __post_init__(self) -> None:
        import requests

        self._session = requests.Session()
        self._last_request_at = 0.0

    def get(self, url: str, *, params: dict | None = None) -> bytes:
        """**바이트를 돌려준다.** 디코딩하지 않는다 (모듈 docstring의 교훈).

        ``id_`` 모드는 아카이브된 **원본 바이트**를 그대로 주므로
        ``Content-Encoding: gzip``이던 응답은 **압축된 채로** 온다.
        여기서 풀지 않는다 — 원문을 그대로 굳히고 읽을 때 푼다 (X1).
        """
        import time

        gap = time.monotonic() - self._last_request_at
        if self._last_request_at and gap < self.interval_seconds:
            time.sleep(self.interval_seconds - gap)
        try:
            resp = self._session.get(
                url,
                params=params,
                headers={"User-Agent": self.user_agent},
                timeout=180,
                # 우리가 직접 풀 것이므로 requests 가 풀지 않게 둔다
                stream=False,
            )
        finally:
            self._last_request_at = time.monotonic()
        if resp.status_code != 200:
            raise WaybackFetchError(f"{resp.status_code} {resp.url}")
        return resp.content


def cdx_timestamps(client: WaybackClient, url: str = COMPANY_TICKERS_URL) -> list[str]:
    """그 URL의 **내용이 바뀐** 200 스냅샷 timestamp를 이른 순으로."""
    import json as _j

    body = client.get(
        CDX_URL,
        params={
            "url": url.removeprefix("https://").removeprefix("http://"),
            "output": "json",
            "fl": "timestamp,statuscode",
            "collapse": "digest",
            "limit": "5000",
        },
    )
    rows = _j.loads(body)
    if not rows:
        return []
    return sorted({ts for ts, status in rows[1:] if status == "200"})


def download_company_tickers(
    client: WaybackClient, root, *, timestamps: list[str] | None = None
) -> dict[str, object]:
    """빠진 스냅샷만 받는다. **이미 있는 것은 다시 안 받는다.**"""
    stamps = timestamps if timestamps is not None else cdx_timestamps(client)
    out_dir = company_tickers_dir(root)
    out_dir.mkdir(parents=True, exist_ok=True)
    fetched, skipped, failed = 0, 0, []
    for ts in stamps:
        dest = company_tickers_path(root, ts)
        if dest.is_file() and dest.stat().st_size > 1024:
            skipped += 1
            continue
        try:
            body = client.get(f"https://web.archive.org/web/{ts}id_/{COMPANY_TICKERS_URL}")
        except WaybackFetchError as exc:
            failed.append(f"{ts}: {exc}")
            continue
        part = dest.with_suffix(".json.part")
        part.write_bytes(body)
        part.replace(dest)
        fetched += 1
    return {
        "available": len(stamps),
        "fetched": fetched,
        "skipped": skipped,
        "failed": failed,
        "dir": out_dir,
    }


def parse_company_tickers(path: Path) -> tuple[_dt.date, list[tuple[str, int]]]:
    """``(as_of, [(SYMBOL, cik)])``.

    **``as_of``는 아카이브 timestamp다.** ``symdir``은 파일이 스스로 밝힌
    ``File Creation Time``을 쓰는데(03 §3) 이 JSON에는 그런 필드가 없다.
    아카이브 시각은 "그때 아카이브가 본 것"이므로 PIT로는 안전한 쪽이다.

    **gzip일 수 있다.** 받은 바이트를 그대로 굳혔기 때문이다 — 매직으로 가른다.
    """
    stem = path.stem
    ts = stem.rsplit("_", 1)[-1]
    if len(ts) < 8 or not ts[:8].isdigit():
        raise WaybackParseError(f"파일 이름에서 timestamp를 못 읽었다: {path.name}")
    as_of = _dt.date(int(ts[:4]), int(ts[4:6]), int(ts[6:8]))

    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        import gzip

        raw = gzip.decompress(raw)
    import json as _j

    try:
        obj = _j.loads(raw)
    except ValueError as exc:
        raise WaybackParseError(f"{path.name}: JSON이 아니다 — {exc}") from exc

    rows = obj.values() if isinstance(obj, dict) else obj
    pairs: list[tuple[str, int]] = []
    for v in rows:
        if not isinstance(v, dict):
            continue
        ticker, cik = v.get("ticker"), v.get("cik_str")
        if not ticker or cik is None:
            continue
        pairs.append((str(ticker).upper().strip(), int(cik)))
    if not pairs:
        raise WaybackParseError(f"{path.name}: 티커가 하나도 없다")
    return as_of, pairs


def ticker_cik_map(root) -> list[tuple[str, int, _dt.date]]:
    """``[(symbol, cik, as_of)]`` — 과거 스냅샷 전부. **PIT로 쓰라고 만든 것이다.**

    스냅샷이 하나도 없으면 오늘자 ``raw/sec/company_tickers/company_tickers.json``
    한 벌로 되돌아간다 — **그건 생존편향이 있는 옛 동작이다.** 되돌아갔다는
    사실은 부르는 쪽이 행 수로 알 수 있다(``as_of``가 한 종류다).
    """
    files = sorted(company_tickers_dir(root).glob("company_tickers_*.json"))
    out: list[tuple[str, int, _dt.date]] = []
    seen: set[tuple[str, _dt.date]] = set()
    for path in files:
        as_of, pairs = parse_company_tickers(path)
        for symbol, cik in pairs:
            key = (symbol, as_of)
            if key in seen:  # 같은 스냅샷 안의 중복 티커는 먼저 나온 것을 쓴다
                continue
            seen.add(key)
            out.append((symbol, cik, as_of))
    if out:
        return out

    legacy = root.raw / "sec" / "company_tickers" / "company_tickers.json"
    if not legacy.is_file():
        raise FileNotFoundError(
            f"티커 맵이 없다: {company_tickers_dir(root)} 도 {legacy} 도 비었다. "
            "collector us-tickers sync 를 먼저 돌린다."
        )
    import json as _j

    entries = _j.loads(legacy.read_text())
    as_of = _dt.date.fromtimestamp(legacy.stat().st_mtime)
    return sorted(
        {(str(v["ticker"]).upper(), int(v["cik_str"]), as_of) for v in entries.values()}
    )

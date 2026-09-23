"""SEC 반월 Fails-to-Deliver(FTD) — 수급 원천이자 CUSIP<->심볼 PIT 맵.

연구 [`01_sec_ftd.md`](
../../../../../my/milestones/us/research/data/source_expansion/01_sec_ftd.md).

* **목록 페이지를 파싱해야 한다.** 같은 종류의 파일인데 URL 경로가 네 가지로
  섞여 있다(212·191·6·3개) — ``midas_url``처럼 규칙 하나로 만들 수 없다
  (연구 §3.1). 그래서 이 원천은 ``sec.py``의 분기 ZIP들과 달리 URL을 계산하지
  않고 매번 목록 페이지(``LIST_URL``)의 ``href``를 읽는다.
* **반월(half-month) 파일이다.** 파일 이름 ``cnsfails<YYYY><MM><a|b>.zip``에서
  ``a``는 1\\~15일, ``b``는 16일\\~월말이다. 두 개는 ``_0`` 접미사가 붙는다
  (``cnsfails201910a_0.zip``·``cnsfails202308b_0.zip``) — SEC 쪽 재발행으로
  보이고, 우리는 반월 태그로만 관리하므로 접미사는 버린다.
* **결제일이지 거래일이 아니다.** ``SETTLEMENT DATE``는 T+1/T+2 결제일이고
  ``PRICE``는 그 **직전 거래일** 종가다(연구 §2.1) — 참고값이지 1차 가격
  계열이 아니다.
* **인코딩이 UTF-8이 아니다.** ``DESCRIPTION``에 latin-1 바이트가 섞여 있어
  UTF-8로 통짜 디코딩하면 실패한다(연구 §2). ``wayback.py``의
  ``parse_company_tickers``와 같은 방식으로 UTF-8을 먼저 시도하고 실패하면
  latin-1로 되돌아간다.
* **파일 끝에 Trailer 두 줄이 붙는다** — 행 수와 총 주식 수다. 데이터로 읽으면
  안 된다(연구 §2). ``SETTLEMENT DATE`` 칸이 8자리 숫자가 아닌 줄은 전부
  트레일러/잡음으로 보고 데이터에서 뺀다 — FINRA regsho 파서
  (``finra.parse_regsho``)와 같은 방어적 파싱이다.
* **CUSIP<->심볼이 일대일이 아니다.** CUSIP 하나에 심볼이 여럿(티커 변경)이거나
  심볼 하나에 CUSIP이 여럿(합병·재상장)이다(연구 §6.1) — 그래서
  ``cusip_symbol_pit``는 오늘자 맵이 아니라 **쌍마다 관측 구간**을 남긴다.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path

from collector.lake import DataRoot
from collector.us.sources.sec import BASE, SecAccessError, SecClient, assert_is_zip

#: 목록 페이지. 412개 반월 zip의 href가 여기 있다(연구 §3).
LIST_URL = f"{BASE}/data/foiadocsfailsdatahtm"

#: ``href="...cnsfails202608b.zip"`` 류. 경로는 네 가지가 섞여 있어 경로 자체는
#: 신경 쓰지 않고 파일 이름만 본다.
_HREF_RE = re.compile(r'href\s*=\s*(["\'])([^"\']*cnsfails[^"\']*\.zip)\1', re.IGNORECASE)

#: 파일 이름에서 반월 태그를 뽑는다. ``_0`` 접미사(재발행)는 버린다.
_FILENAME_RE = re.compile(r"cnsfails(\d{4})(\d{2})([ab])(?:_\d+)?\.zip$", re.IGNORECASE)


class FtdError(RuntimeError):
    """FTD 목록·원문이 기대한 모양이 아니다."""


def period_key(year: int, month: int, half: str) -> str:
    """``(2026, 8, "b")`` -> ``"202608b"``. raw 파일명·``source_rev``가 쓰는 태그다."""
    if half not in ("a", "b"):
        raise ValueError(f"half는 'a'·'b'만 된다: {half!r}")
    return f"{year:04d}{month:02d}{half}"


def _absolute_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{BASE}{href}"
    raise FtdError(f"상대 경로를 못 푼다: {href!r}")


@dataclass(frozen=True)
class FtdListing:
    """목록 페이지 파싱 결과. ``duplicates``는 같은 반월에 링크가 둘 이상일 때다."""

    files: dict[str, str]  # period -> url
    duplicates: dict[str, list[str]]
    unparsed: list[str]  # cnsfails 를 담은 href인데 태그를 못 뽑은 것


def parse_listing(html: str) -> FtdListing:
    """목록 페이지 HTML에서 ``{period: url}``을 뽑는다.

    **경로 넷을 하나로 합치지 않는다** — href를 그대로 쓴다. 그래서 SEC가
    다섯째 경로를 새로 쓰기 시작해도 여기는 안 깨진다. 깨지는 것은
    ``_FILENAME_RE``가 파일 이름 규칙을 못 알아볼 때뿐이다.
    """
    hrefs = [m.group(2) for m in _HREF_RE.finditer(html)]
    if not hrefs:
        raise FtdError(
            f"{LIST_URL}: cnsfails zip 링크를 하나도 못 찾았다 — 페이지 구조가 바뀌었나 본다."
        )

    files: dict[str, str] = {}
    duplicates: dict[str, list[str]] = {}
    unparsed: list[str] = []
    for href in hrefs:
        m = _FILENAME_RE.search(href)
        if not m:
            unparsed.append(href)
            continue
        year, month, half = int(m.group(1)), int(m.group(2)), m.group(3)
        tag = period_key(year, month, half)
        url = _absolute_url(href)
        if tag in files and files[tag] != url:
            duplicates.setdefault(tag, [files[tag]]).append(url)
            continue  # 먼저 본 것을 그대로 쓴다 — 결정적이어야 한다
        files[tag] = url
    return FtdListing(files=files, duplicates=duplicates, unparsed=unparsed)


def list_ftd_files(client: SecClient) -> FtdListing:
    """목록 페이지를 받아 파싱한다. 반월당 새 원천 요청 하나가 생겼는지 여기서 안다."""
    resp = client.get(LIST_URL, stream=False)
    return parse_listing(resp.text)


# --- raw ----------------------------------------------------------------


def ftd_raw_dir(root: DataRoot) -> Path:
    """``raw/sec/ftd/`` — 원문 zip을 그대로 둔다(02 §1의 raw 원칙)."""
    return root.raw / "sec" / "ftd"


def ftd_raw_path(root: DataRoot, period: str) -> Path:
    """``raw/sec/ftd/cnsfails<period>.zip``.

    원본 파일 이름에 ``_0`` 접미사가 붙어 있어도 여기서는 반월 태그로 정규화한
    이름을 쓴다 — 접미사는 SEC 쪽 재발행 표시일 뿐 내용을 가르는 키가 아니다.
    """
    return ftd_raw_dir(root) / f"cnsfails{period}.zip"


def download_ftd(
    client: SecClient,
    root: DataRoot,
    period: str,
    url: str,
    *,
    skip_existing: bool = True,
) -> dict[str, object]:
    """반월 zip 하나를 ``raw/``에 굳힌다. **받자마자 열어 본다**(``sec.py``와 같은 이유)."""
    dest = ftd_raw_path(root, period)
    if skip_existing and dest.is_file():
        try:
            entries = assert_is_zip(dest)
            return {
                "period": period,
                "path": dest,
                "skipped": True,
                "entries": len(entries),
                "bytes": dest.stat().st_size,
            }
        except SecAccessError:
            dest.unlink()  # 깨진 것은 다시 받는다

    client.download(url, dest)
    entries = assert_is_zip(dest)
    return {
        "period": period,
        "path": dest,
        "skipped": False,
        "entries": len(entries),
        "bytes": dest.stat().st_size,
    }


def raw_periods(root: DataRoot) -> set[str]:
    """``raw/``에 이미 받아 둔 반월 태그. 이어받기 판단이 여기서 나온다."""
    directory = ftd_raw_dir(root)
    if not directory.is_dir():
        return set()
    out = set()
    for p in directory.glob("cnsfails*.zip"):
        m = re.match(r"cnsfails(\d{4})(\d{2})([ab])\.zip$", p.name)
        if m:
            out.add(period_key(int(m.group(1)), int(m.group(2)), m.group(3)))
    return out


# --- 파싱 -------------------------------------------------------------------

#: 원문 헤더 -> 우리 칸 이름. 공백·괄호를 정규화한 뒤 비교한다.
_HEADER_ALIASES = {
    "settlement date": "settlement_date",
    "cusip": "cusip",
    "symbol": "symbol",
    "quantity (fails)": "quantity",
    "description": "description",
    "price": "price",
}

#: ``SETTLEMENT DATE`` 칸이 이 모양이 아니면 데이터 행이 아니다(Trailer·잡음).
_SETTLEMENT_RE = re.compile(r"^\d{8}$")


def _normalize_header(cell: str) -> str:
    return " ".join(cell.strip().lower().split())


def _parse_settlement(cell: str) -> dt.date | None:
    """``YYYYMMDD`` -> ``date``. **8자리 숫자여도 진짜 날짜가 아니면 트레일러로 본다**
    (``999999999`` 같은 합계 줄이 우연히 8자리일 수 있다)."""
    if not _SETTLEMENT_RE.match(cell):
        return None
    try:
        return dt.datetime.strptime(cell, "%Y%m%d").date()
    except ValueError:
        return None


def _decode(raw: bytes) -> str:
    """UTF-8을 먼저 시도하고 실패하면 latin-1로 되돌아간다(연구 §2 — DESCRIPTION에
    latin-1 바이트가 섞여 있다)."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


@dataclass(frozen=True)
class ParsedRow:
    settlement_date: dt.date
    cusip: str | None
    symbol: str | None
    quantity: str | None
    description: str | None
    price: str | None


@dataclass(frozen=True)
class ParsedFtdFile:
    rows: list[ParsedRow]
    trailer_lines: int
    header: str


def parse_ftd_text(text: str) -> ParsedFtdFile:
    """반월 원문 하나를 (데이터 행들, Trailer 줄 수)로 판다.

    **헤더 이름으로 칸을 잡는다. 위치로 잡지 않는다** — 이 저장소의 다른
    파서(``wayback.parse_listing_file``)와 같은 규칙이다. 헤더가 2009-07부터
    바뀐 적이 없다지만(연구 §4) 규칙은 이름 기준으로 둔다.

    **``SETTLEMENT DATE``가 진짜 날짜로 안 읽히는 줄은 전부 Trailer/잡음으로
    본다** — 파일 끝의 행 수·총 주식 수 두 줄이 이렇게 걸러진다. 정확한
    Trailer 문구를 가정하지 않는다(받아 보지 못해 실측이 없다 — 04 §9.1).
    """
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        raise FtdError("파일이 비어 있다")

    header_cells = [_normalize_header(c) for c in lines[0].split("|")]
    if "settlement date" not in header_cells:
        raise FtdError(f"헤더가 아니다 — {lines[0][:80]!r}")
    index = {_HEADER_ALIASES[h]: i for i, h in enumerate(header_cells) if h in _HEADER_ALIASES}
    missing = set(_HEADER_ALIASES.values()) - set(index)
    if missing:
        raise FtdError(f"칸이 빠졌다: {sorted(missing)} — 헤더: {lines[0][:120]!r}")

    settle_i = index["settlement_date"]
    rows: list[ParsedRow] = []
    trailer = 0
    for line in lines[1:]:
        cells = line.split("|")
        settle_cell = cells[settle_i].strip() if settle_i < len(cells) else ""
        settled = _parse_settlement(settle_cell)
        if settled is None:
            trailer += 1  # Trailer 두 줄(행 수·총 주식 수) 또는 그 밖의 잡음
            continue

        def cell(name: str) -> str | None:
            i = index[name]
            return cells[i].strip() if i < len(cells) else None

        rows.append(
            ParsedRow(
                settlement_date=settled,
                cusip=cell("cusip") or None,
                symbol=cell("symbol") or None,
                quantity=cell("quantity"),
                description=cell("description"),
                price=cell("price"),
            )
        )
    return ParsedFtdFile(rows=rows, trailer_lines=trailer, header=lines[0])


def _member_to_text(zip_path: Path) -> str:
    """ZIP 안의 유일한 원문 멤버를 읽어 디코딩한다."""
    import zipfile

    names = [n for n in assert_is_zip(zip_path) if not n.endswith("/")]
    if len(names) != 1:
        raise FtdError(f"{zip_path}: 원문이 하나가 아니다 — {names}")
    with zipfile.ZipFile(zip_path) as zf, zf.open(names[0]) as fh:
        raw = fh.read()
    return _decode(raw)


# --- derived: ftd_fails · cusip_symbol_pit -----------------------------------


def _num(value: str | None) -> float | None:
    if value is None:
        return None
    v = value.strip()
    if v in ("", "."):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _int(value: str | None) -> int | None:
    n = _num(value)
    return None if n is None else int(n)


def extract_ftd(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
) -> dict[str, object]:
    """``raw/sec/ftd/``의 반월 zip들을 ``ftd_fails``·``cusip_symbol_pit`` 두 장으로.

    한 로더가 표 둘을 낸다 — ``sec_bulk.load_submissions``(``filings_index``·
    ``company_meta``)와 같은 모양이다(``ops/derive.py``의 ``Recipe.tables``).
    맵은 ``ftd_fails``를 다 쓴 뒤 그 스냅샷을 다시 읽어 만든다 — 반월 파일마다
    따로 맵을 만들면 파일 경계에서 끊긴 (cusip, symbol) 관측 구간을 못 잇는다.
    """
    import duckdb
    import pyarrow as pyar
    import pyarrow.parquet as pq

    from collector.us.store.schema import ARROW_SCHEMAS
    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or dt.datetime.now(dt.UTC)
    files = sorted(ftd_raw_dir(root).glob("cnsfails*.zip"))
    if not files:
        raise FtdError(f"{ftd_raw_dir(root)}에 받아 둔 zip이 없다. 먼저 받는다 (us-daily run).")

    schema = ARROW_SCHEMAS["ftd_fails"]
    dest = snapshot_path(root, "ftd_fails", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)

    trailer_counts: dict[str, int] = {}
    price_missing = 0
    written = 0
    periods_ok: list[str] = []
    periods_failed: list[str] = []

    with pq.ParquetWriter(dest, schema, compression="zstd") as writer:
        for path in files:
            m = re.match(r"cnsfails(\d{4})(\d{2})([ab])\.zip$", path.name)
            period = period_key(int(m.group(1)), int(m.group(2)), m.group(3)) if m else path.stem
            try:
                text = _member_to_text(path)
                parsed = parse_ftd_text(text)
            except FtdError as exc:
                periods_failed.append(f"{period}: {exc}")
                continue
            trailer_counts[period] = parsed.trailer_lines
            n = len(parsed.rows)
            if not n:
                periods_ok.append(period)
                continue
            prices = [_num(r.price) for r in parsed.rows]
            price_missing += sum(1 for p in prices if p is None)
            writer.write_table(
                pyar.table(
                    {
                        "settlement_date": pyar.array(
                            [r.settlement_date for r in parsed.rows], type=pyar.date32()
                        ),
                        "cusip": pyar.array([r.cusip for r in parsed.rows], type=pyar.string()),
                        "symbol": pyar.array(
                            [(r.symbol or "").upper() or None for r in parsed.rows],
                            type=pyar.string(),
                        ),
                        "quantity": pyar.array(
                            [_int(r.quantity) for r in parsed.rows], type=pyar.int64()
                        ),
                        "description": pyar.array(
                            [r.description for r in parsed.rows], type=pyar.string()
                        ),
                        "price": pyar.array(prices, type=pyar.float64()),
                        "observed_at": pyar.array(
                            [observed_at] * n, type=pyar.timestamp("us", tz="UTC")
                        ),
                        "source_rev": pyar.array([period] * n, type=pyar.string()),
                    }
                )
                .select(schema.names)
                .cast(schema)
            )
            written += n
            periods_ok.append(period)

    if written == 0:
        dest.unlink(missing_ok=True)
        raise FtdError(
            "행을 하나도 못 썼다 — 받아 둔 zip을 전부 못 읽었거나 전부 빈 반월이다: "
            + "; ".join(periods_failed)
        )

    fails_stats = verify_snapshot(dest, "ftd_fails", unique_on=None)

    # cusip_symbol_pit: 방금 굳힌 ftd_fails 전체에서 (cusip, symbol) 쌍마다
    # 관측 구간을 잰다. 파일 하나가 아니라 전체를 봐야 반월 경계를 안 끊는다.
    map_dest = snapshot_path(root, "cusip_symbol_pit", snapshot_date)
    map_dest.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT cusip,
                   symbol,
                   min(settlement_date)                AS first_seen,
                   max(settlement_date)                 AS last_seen,
                   CAST(count(DISTINCT settlement_date) AS INTEGER) AS n_settlement_dates,
                   CAST(? AS TIMESTAMP WITH TIME ZONE)  AS observed_at,
                   CAST(? AS VARCHAR)                   AS source_rev
            FROM read_parquet('{dest}')
            WHERE cusip IS NOT NULL AND symbol IS NOT NULL
            GROUP BY cusip, symbol
        ) TO '{map_dest}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [observed_at, f"ftd_fails:{len(periods_ok)}periods"],
    )
    map_stats = verify_snapshot(map_dest, "cusip_symbol_pit", unique_on=("cusip", "symbol"))

    return {
        "files": len(files),
        "periods_ok": periods_ok,
        "periods_failed": periods_failed,
        "trailer_lines_by_period": trailer_counts,
        "price_missing_rows": price_missing,
        "ftd_fails": {"path": dest, **fails_stats},
        "cusip_symbol_pit": {"path": map_dest, **map_stats},
    }

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

"""v2 전용 입력 — 상장 목록(``listing_snapshots_v2``)과 입력 스냅샷 해석.

**공유 표(``listing_snapshots``·``universe_daily``)에는 쓰지 않는다.** v1 빌더의
``resolve_inputs`` 가 표마다 최신 스냅샷을 읽기 때문이다 (설계 2.4).

``raw/nasdaqtrader/symdir`` 은 **원문을 재배포하지 않는 조건**으로 쌓는다
(``nasdaqtrader_symdir.NOTICE_TEXT``). 여기서는 읽어서 파생 표(``derived/``)로만
만들고, 원문은 어디에도 복사하지 않는다.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

#: v2 가 읽는 스냅샷. 이름은 레이크 표 이름이다.
V2_INPUT_TABLES: tuple[str, ...] = (
    "prices_daily",
    "listing_snapshots_v2",
    "filings_index",
    "company_meta",
    "filings_sub",
    "cusip_symbol_pit",
    "ftd_fails",
)

#: 어느 표가 없어도 v2 는 못 만든다. 그 표를 만드는 명령을 알려 준다.
_HOW_TO_MAKE = {
    "prices_daily": "collector us-derive run --tables prices-daily",
    "listing_snapshots_v2": "collector us-load listing-snapshots-v2",
    "filings_index": "collector us-derive run --tables submissions",
    "company_meta": "collector us-derive run --tables submissions",
    "filings_sub": "collector us-derive run --tables filings-sub",
    "cusip_symbol_pit": "collector us-derive run --tables ftd",
    "ftd_fails": "collector us-derive run --tables ftd",
}


def resolve_inputs(root) -> dict[str, Path]:
    """``V2_INPUT_TABLES`` 마다 **가장 최근 스냅샷** 경로 (v1 과 같은 규칙)."""
    from collector.us.store.writer import latest_snapshot

    out: dict[str, Path] = {}
    for name in V2_INPUT_TABLES:
        path = latest_snapshot(root, name)
        if path is None:
            raise FileNotFoundError(f"{name} 스냅샷이 없다. 먼저 굳힌다 — {_HOW_TO_MAKE[name]}")
        out[name] = path
    return out


def listing_source_files(root) -> list[Path]:
    """상장 목록 원문 전부 — Wayback 먼저, 그 뒤 매일 받는 nasdaqtrader."""
    from collector.us.sources import nasdaqtrader_symdir, wayback

    files = sorted(wayback.symdir_dir(root).glob("*.txt"))
    files += sorted(nasdaqtrader_symdir.symdir_dir(root).glob("*.txt"))
    return files


def build_listing_snapshots_v2(
    root,
    *,
    snapshot_date,
    observed_at=None,
) -> dict[str, object]:
    """Wayback 원문과 매일 받는 nasdaqtrader 원문을 ``listing_snapshots_v2`` 한 장으로.

    **같은 파서**(``wayback.parse_listing_file``)로 읽는다. 열은 ``listing_snapshots`` 와
    같다. 같은 ``(kind, snapshot, symbol)`` 이 두 번 나오면 안 되므로 파일 이름
    (``<kind>_<timestamp>``)이 겹치면 멈춘다.
    """
    import pyarrow as pyar

    from collector.us.sources.wayback import ListingFile, WaybackParseError, parse_listing_file
    from collector.us.store.writer import snapshot_path, write_snapshot_arrow

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    files = listing_source_files(root)
    if not files:
        raise WaybackParseError(f"{root.raw} 에 상장 목록 원문이 없다")
    stems = [f.stem for f in files]
    if len(set(stems)) != len(stems):
        raise ValueError("같은 이름의 상장 목록 원문이 둘 이상이다 (wayback·nasdaqtrader)")

    names = (
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
    cols: dict[str, list] = {k: [] for k in names}
    per_file: list[tuple[str, int]] = []

    def _flag(v):
        return None if v in (None, "") else v.upper() == "Y"

    for path in files:
        lf = ListingFile(path)
        as_of, rows = parse_listing_file(path)
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
    dest = snapshot_path(root, "listing_snapshots_v2", snapshot_date)
    write_snapshot_arrow(
        tbl, "listing_snapshots_v2", dest, unique_on=("kind", "snapshot", "symbol")
    )
    return {
        "path": dest,
        "files": len(files),
        "rows": tbl.num_rows,
        "empty_files": [n for n, c in per_file if c == 0],
        "bytes": dest.stat().st_size,
    }


def load_ticker_map(root, *, parquet: Path | None = None):
    """티커→cik 지도. ``(rows | None, sources)``.

    기본은 Wayback ``company_tickers`` 스냅샷 전부(PIT)다. ``parquet`` 를 주면
    ``(symbol, cik, as_of)`` 열을 가진 그 파일을 쓴다 — Wayback 스냅샷이 없는 레이크
    (맥 미러)에서 검증 빌드를 돌릴 때만 쓴다. ``sources`` 는 completion 에 sha256 으로 남긴다.
    """
    from collector.us.sources import wayback

    if parquet is not None:
        return None, [Path(parquet)]
    files = sorted(wayback.company_tickers_dir(root).glob("company_tickers_*.json"))
    rows = wayback.ticker_cik_map(root, source_paths=files or None)
    if not files:
        files = [root.raw / "sec" / "company_tickers" / "company_tickers.json"]
    return rows, files

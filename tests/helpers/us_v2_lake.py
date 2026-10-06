"""유니버스 v2 시험용 합성 레이크.

실제 원문(nasdaqtrader·SEC)은 어디에도 복사하지 않는다. 심볼·이름·cik 는 전부 지어낸 값이다.
날짜는 실제 XNYS 거래일이다 — 달력 규칙(다음 거래일부터 쓴다)을 그대로 시험하려는 것이다.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from collector.lake import DataRoot
from collector.us.universe.v2.cal import Calendar

D = dt.date
OBSERVED = dt.datetime(2026, 10, 6, 0, 0, tzinfo=dt.UTC)


class SyntheticLake:
    """가격·상장 목록·공시를 모았다가 ``flush`` 로 스냅샷 파티션에 쓴다."""

    def __init__(self, tmp_path: Path, *, first: dt.date, last: dt.date):
        for layer in ("raw", "derived", "datasets", "output"):
            (tmp_path / layer).mkdir(parents=True, exist_ok=True)
        self.root = DataRoot(tmp_path)
        self.cal = Calendar.xnys(first - dt.timedelta(days=10), last + dt.timedelta(days=400))
        self.first, self.last = first, last
        self.price_rows: list[tuple] = []
        self.listing_rows: list[tuple] = []
        self.filing_rows: list[tuple] = []
        self.meta_rows: list[tuple] = []
        self.sub_rows: list[tuple] = []
        self.cusip_rows: list[tuple] = []
        self.ftd_rows: list[tuple] = []
        self.ticker_files: dict[dt.date, list[tuple[str, int]]] = {}
        self.cik_of: dict[str, int] = {}

    # --- 가격 ---------------------------------------------------------------------------
    def sessions(self, start: dt.date, end: dt.date) -> list[dt.date]:
        return [d for d in self.cal.sessions if start <= d <= end]

    def series(
        self,
        symbol: str,
        start: dt.date,
        end: dt.date,
        *,
        close: float = 20.0,
        volume: int = 200_000,
        skip: set[dt.date] | None = None,
    ) -> None:
        """``start``~``end`` 거래일마다 가격 한 행. 거래대금 = close × volume."""
        for d in self.sessions(start, end):
            if skip and d in skip:
                continue
            self.price_rows.append((symbol, d, close, volume))

    # --- 상장 목록 ----------------------------------------------------------------------------
    def listing(
        self,
        as_of: dt.date,
        rows: list[tuple],
        *,
        kind: str = "nasdaqlisted",
    ) -> None:
        """``rows`` 는 ``(symbol, name[, is_etf[, test]])``."""
        for r in rows:
            symbol, name = r[0], r[1]
            is_etf = r[2] if len(r) > 2 else False
            test = r[3] if len(r) > 3 else False
            self.listing_rows.append((as_of, kind, symbol, name, is_etf, test))

    def listing_monthly(
        self, symbols: list[tuple], *, first: dt.date, last: dt.date, kind: str = "nasdaqlisted"
    ) -> None:
        """매달 15일에 같은 목록을 쌓는다."""
        y, m = first.year, first.month
        while D(y, m, 15) <= last:
            self.listing(D(y, m, 15), symbols, kind=kind)
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)

    # --- SEC ------------------------------------------------------------------------------------
    def filing(
        self,
        cik: int,
        form: str,
        filing_date: dt.date,
        *,
        accepted: dt.datetime | None = None,
        file_number: str | None = None,
        items: str | None = None,
    ) -> None:
        """``accepted`` 는 **미 동부 현지 시각**(naive)이다. 없으면 접수 시각 결측."""
        self.filing_rows.append((cik, form, filing_date, accepted, file_number, items))

    def periodic(self, cik: int, first: dt.date, last: dt.date) -> None:
        """분기마다 10-Q 를 낸다 (접수 오후 5시, 접수 시각 있음)."""
        y, m = first.year, first.month
        while D(y, m, 10) <= last:
            d = D(y, m, 10)
            self.filing(cik, "10-Q", d, accepted=dt.datetime(d.year, d.month, d.day, 17, 0))
            y, m = (y + (m + 3 > 12), (m + 2) % 12 + 1)

    def company(self, cik: int, name: str, sic: str | None = None, formers=None) -> None:
        self.meta_rows.append((cik, name, sic, json.dumps(formers) if formers else None))

    def sic_filing(self, cik: int, filed: dt.date, sic: str) -> None:
        self.sub_rows.append((cik, filed, sic))

    # --- FTD ------------------------------------------------------------------------------------
    def cusip(self, cusip: str, symbol: str, first_seen: dt.date, last_seen: dt.date) -> None:
        self.cusip_rows.append((cusip, symbol, first_seen, last_seen))

    def ftd(self, settlement: dt.date, cusip: str, symbol: str, description: str) -> None:
        self.ftd_rows.append((settlement, cusip, symbol, 1000, description))

    # --- SEC 티커→cik 지도 (Wayback company_tickers JSON) ---------------------------------------
    def tickers(self, as_of: dt.date, pairs: list[tuple[str, int]]) -> None:
        self.ticker_files[as_of] = pairs

    # --- 쓰기 -------------------------------------------------------------------------------------
    def flush(self, snapshot_date: str | None = None) -> str:
        sd = snapshot_date or self.last.isoformat()
        root = self.root

        def put(table: str, tbl: pa.Table) -> Path:
            path = root.derived / "snapshots" / table / f"snapshot_date={sd}" / "part.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, path)
            return path

        cols = list(zip(*self.price_rows, strict=True)) if self.price_rows else [(), (), (), ()]
        put(
            "prices_daily",
            pa.table(
                {
                    "date": pa.array(cols[1], type=pa.date32()),
                    "symbol": pa.array(cols[0], type=pa.string()),
                    "close": pa.array(cols[2], type=pa.float64()),
                    "volume": pa.array(cols[3], type=pa.int64()),
                }
            ),
        )
        lc = list(zip(*self.listing_rows, strict=True)) if self.listing_rows else [()] * 6
        n = len(self.listing_rows)
        listing = pa.table(
            {
                "as_of": pa.array(lc[0], type=pa.date32()),
                "as_of_time": pa.array(
                    [dt.datetime(d.year, d.month, d.day, 18, 0) for d in lc[0]],
                    type=pa.timestamp("us"),
                ),
                "kind": pa.array(lc[1], type=pa.string()),
                "snapshot": pa.array([d.strftime("%Y%m%d") for d in lc[0]], type=pa.string()),
                "symbol": pa.array(lc[2], type=pa.string()),
                "security_name": pa.array(lc[3], type=pa.string()),
                "exchange": pa.array(["NASDAQ"] * n, type=pa.string()),
                "market_category": pa.array(["Q"] * n, type=pa.string()),
                "is_etf": pa.array(lc[4], type=pa.bool_()),
                "test_issue": pa.array(lc[5], type=pa.bool_()),
                "financial_status": pa.array([None] * n, type=pa.string()),
            }
        )
        put("listing_snapshots_v2", listing)
        put("listing_snapshots", listing)

        fc = list(zip(*self.filing_rows, strict=True)) if self.filing_rows else [()] * 6
        m = len(self.filing_rows)
        # acceptance_datetime 은 UTC 로 저장된다. 미 동부 현지 시각을 UTC 로 바꿔 쓴다
        import zoneinfo

        ny = zoneinfo.ZoneInfo("America/New_York")
        acc = [None if a is None else a.replace(tzinfo=ny).astimezone(dt.UTC) for a in fc[3]]
        put(
            "filings_index",
            pa.table(
                {
                    "cik": pa.array(fc[0], type=pa.int64()),
                    "accession": pa.array([f"acc-{i}" for i in range(m)], type=pa.string()),
                    "form": pa.array(fc[1], type=pa.string()),
                    "filing_date": pa.array(fc[2], type=pa.date32()),
                    "acceptance_datetime": pa.array(acc, type=pa.timestamp("us", tz="UTC")),
                    "file_number": pa.array(fc[4], type=pa.string()),
                    "items": pa.array(fc[5], type=pa.string()),
                }
            ),
        )
        mc = list(zip(*self.meta_rows, strict=True)) if self.meta_rows else [()] * 4
        put(
            "company_meta",
            pa.table(
                {
                    "cik": pa.array(mc[0], type=pa.int64()),
                    "name": pa.array(mc[1], type=pa.string()),
                    "sic": pa.array(mc[2], type=pa.string()),
                    "former_names": pa.array(mc[3], type=pa.string()),
                }
            ),
        )
        sc = list(zip(*self.sub_rows, strict=True)) if self.sub_rows else [()] * 3
        put(
            "filings_sub",
            pa.table(
                {
                    "cik": pa.array(sc[0], type=pa.int64()),
                    "filed": pa.array(sc[1], type=pa.date32()),
                    "sic": pa.array(sc[2], type=pa.string()),
                }
            ),
        )
        uc = list(zip(*self.cusip_rows, strict=True)) if self.cusip_rows else [()] * 4
        put(
            "cusip_symbol_pit",
            pa.table(
                {
                    "cusip": pa.array(uc[0], type=pa.string()),
                    "symbol": pa.array(uc[1], type=pa.string()),
                    "first_seen": pa.array(uc[2], type=pa.date32()),
                    "last_seen": pa.array(uc[3], type=pa.date32()),
                    "n_settlement_dates": pa.array([5] * len(uc[0]), type=pa.int32()),
                }
            ),
        )
        tc = list(zip(*self.ftd_rows, strict=True)) if self.ftd_rows else [()] * 5
        put(
            "ftd_fails",
            pa.table(
                {
                    "settlement_date": pa.array(tc[0], type=pa.date32()),
                    "cusip": pa.array(tc[1], type=pa.string()),
                    "symbol": pa.array(tc[2], type=pa.string()),
                    "quantity": pa.array(tc[3], type=pa.int64()),
                    "description": pa.array(tc[4], type=pa.string()),
                }
            ),
        )
        # v1 입력 둘 (v1 빌더가 읽는다)
        put(
            "midas_security_daily",
            pa.table(
                {
                    "date": pa.array([], type=pa.date32()),
                    "ticker": pa.array([], type=pa.string()),
                    "security_type": pa.array([], type=pa.string()),
                    "mcap_rank": pa.array([], type=pa.int32()),
                }
            ),
        )
        tdir = root.raw / "wayback" / "company_tickers"
        tdir.mkdir(parents=True, exist_ok=True)
        for as_of, pairs in self.ticker_files.items():
            body = {
                str(i): {"cik_str": cik, "ticker": sym, "title": f"T{i}"}
                for i, (sym, cik) in enumerate(pairs)
            }
            (tdir / f"company_tickers_{as_of:%Y%m%d}120000.json").write_text(json.dumps(body))
        return sd

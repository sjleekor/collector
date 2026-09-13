"""FDR universe provider — stub.

Uses ``FinanceDataReader.StockListing('KOSPI')`` and
``FinanceDataReader.StockListing('KOSDAQ')`` to fetch the current stock
universe.

Mapping notes (for implementation):
    • FDR returns a DataFrame with columns like ``Symbol``, ``Name``,
      ``Market``, ``ListingDate``, etc.
    • Map ``Symbol`` → ``Stock.ticker``, ``Name`` → ``Stock.name``.
    • ``ListingDate`` may be NaT — handle as ``None``.
    • FDR does not provide historical delisting info; all returned rows
      are assumed ``ACTIVE``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Any

import FinanceDataReader as fdr
import pandas as pd

from collector.kr.domain.enums import ListingStatus, Market, Source
from collector.kr.domain.models import Stock, StockUniverseSnapshot, UniverseResult
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


class FdrUniverseProvider:
    """Fetches the KOSPI / KOSDAQ stock universe via FinanceDataReader.

    Conforms to :class:`~collector.kr.ports.universe.UniverseProvider`.
    """

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve listed stocks from FinanceDataReader.

        Args:
            markets: Market segments to query.
            as_of: Reference date (ignored by FDR — always returns current
                listing).

        Returns:
            ``UniverseResult`` with the snapshot.
        """
        reference_date = as_of or today_kst()
        fetched_at = now_kst()

        try:
            records: list[Stock] = []

            for market in markets:
                logger.info("Fetching FDR universe for market: %s", market.value)
                df = fdr.StockListing(market.value)

                if df.empty:
                    logger.warning("FDR returned empty DataFrame for market: %s", market.value)
                    continue

                records.extend(self._stocks_from_rows(df.iterrows(), market, reference_date))

            snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=reference_date,
                source=Source.FDR,
                fetched_at=fetched_at,
                records=records,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as exc:
            # No pykrx fallback (K-5). It used to switch here automatically, and
            # that is the worst possible moment to do it: FDR wobbles when KRX
            # wobbles, so the fallback fired exactly when KRX was least willing
            # to be scraped, turning an anonymous read into a login-based one
            # without anybody choosing it. `--source pykrx` still exists for
            # someone who decides to.
            logger.exception("FDR universe fetch failed: %s", exc)
            return UniverseResult(error=f"FDR failed: {exc}")

    @staticmethod
    def _parse_listing_date(row: Any) -> date | None:
        """Best-effort parse of the FDR listing date; never raises.

        The column name varies across FDR versions / endpoints, and values
        may be strings, ``Timestamp``s, ``date``s, empty, or ``NaT``. Any
        unparseable value yields ``None`` so poor FDR data cannot break
        universe sync.
        """
        for key in ("ListingDate", "Listing Date", "ListedDate"):
            if key not in row:
                continue
            val = row.get(key)
            if val is None:
                continue
            ts = pd.to_datetime(val, errors="coerce")  # NaT-safe: str/Timestamp/date
            if pd.isna(ts):
                continue
            return ts.date()
        return None

    @staticmethod
    def _stocks_from_rows(
        rows: Any,
        market: Market,
        reference_date: date,
    ) -> list[Stock]:
        records: list[Stock] = []
        for _, row in rows:
            # FDR has used both 'Code' and 'Symbol' across versions / endpoints.
            ticker = str(row.get("Code") or row.get("Symbol") or "").strip()
            if not ticker:
                continue

            records.append(
                Stock(
                    ticker=ticker,
                    market=market,
                    name=str(row.get("Name", "")).strip(),
                    status=ListingStatus.ACTIVE,
                    last_seen_date=reference_date,
                    source=Source.FDR,
                    listing_date=FdrUniverseProvider._parse_listing_date(row),
                )
            )
        return records

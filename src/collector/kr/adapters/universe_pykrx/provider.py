"""pykrx universe providers.

Two implementations of ``UniverseProvider``, split by how they resolve names:

``PykrxUniverseProvider``
    Live universe sync.  One ``get_market_ticker_name`` call per ticker.

``PykrxHistoricalUniverseProvider``
    Past-dated snapshots for the survivorship backfill.  Names come from a
    single ``get_market_price_change_by_ticker`` call per market, because the
    per-ticker path costs ~38 hours across the backfill range.

Both use ``get_market_ticker_list(date, market)``, which accepts a past date —
that is what makes the historical reconstruction possible at all.

Listing date is not available from pykrx and is left as ``None``; it can only be
inferred from the earliest date with OHLCV data.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date

from collector.kr.adapters.pykrx_auth import call_with_session_retry, get_pykrx_stock_module
from collector.kr.domain.enums import ListingStatus, Market, Source
from collector.kr.domain.models import Stock, StockUniverseSnapshot, UniverseResult
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


class PykrxUniverseProvider:
    """Fetches the KOSPI / KOSDAQ stock universe via pykrx.

    Conforms to :class:`~collector.kr.ports.universe.UniverseProvider`.
    """

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve listed stocks from pykrx.

        Args:
            markets: Market segments to query.
            as_of: Reference date.  ``None`` means today (Asia/Seoul).

        Returns:
            ``UniverseResult`` with the snapshot.
        """
        try:
            stock = get_pykrx_stock_module()
            records: list[Stock] = []
            reference_date = as_of or today_kst()
            fetched_at = now_kst()

            # pykrx requires date in 'YYYYMMDD' format
            date_str = reference_date.strftime("%Y%m%d")

            for market in markets:
                logger.info(
                    "Fetching pykrx universe for market: %s as_of: %s", market.value, date_str
                )
                # market argument expects 'KOSPI' or 'KOSDAQ'
                tickers = stock.get_market_ticker_list(date_str, market=market.value)

                if not tickers:
                    logger.warning("pykrx returned empty ticker list for market: %s", market.value)
                    continue

                for ticker in tickers:
                    name = stock.get_market_ticker_name(ticker)

                    records.append(
                        Stock(
                            ticker=ticker,
                            market=market,
                            name=name,
                            status=ListingStatus.ACTIVE,
                            last_seen_date=reference_date,
                            source=Source.PYKRX,
                        )
                    )
            snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=reference_date,
                source=Source.PYKRX,
                fetched_at=fetched_at,
                records=records,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as exc:
            logger.exception("Failed to fetch pykrx universe: %s", exc)
            return UniverseResult(error=str(exc))


class PykrxHistoricalUniverseProvider:
    """Fetches a past-dated universe snapshot with two calls per market.

    Conforms to :class:`~collector.kr.ports.universe.UniverseProvider`, and
    exists alongside :class:`PykrxUniverseProvider` because of the name lookup.

    ``PykrxUniverseProvider`` calls ``get_market_ticker_name`` once per ticker.
    Measured at 0.354s per call, which is fine for one live snapshot and
    hopeless for a backfill: 145 month-ends x 2 markets x ~2,700 tickers is
    about 38 hours of requests.

    ``get_market_price_change_by_ticker(d, d, market)`` returns 종목명 for every
    ticker in ONE call (~0.8s), and its ticker set matched
    ``get_market_ticker_list`` exactly at 2014-06-30, 2016-06-30, 2020-03-31 and
    2024-01-02 with no blank names.  So the whole backfill costs two calls per
    slice — about 580 in total.

    Snapshots produced here carry ``Source.PYKRX_BACKFILL`` so the live
    ``sync_universe`` diff never mistakes a reconstructed snapshot for a
    newly observed one.
    """

    _NAME_COLUMN = "종목명"

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the listed universe as of a past date.

        Args:
            markets: Market segments to query.
            as_of: Reference date.  ``None`` means today (Asia/Seoul).

        Returns:
            ``UniverseResult`` with a ``Source.PYKRX_BACKFILL`` snapshot.
        """
        try:
            stock = get_pykrx_stock_module()
            reference_date = as_of or today_kst()
            fetched_at = now_kst()
            date_str = reference_date.strftime("%Y%m%d")

            records: list[Stock] = []

            for market in markets:
                # An empty ticker list on a trading day is almost always a
                # dropped KRX session rather than an empty market, and pykrx
                # will not notice because its expiry clock still says the
                # session is good. See adapters/pykrx_auth.py.
                tickers = call_with_session_retry(
                    lambda: stock.get_market_ticker_list(date_str, market=market.value),
                    is_empty=lambda result: not result,
                    label=f"ticker list {market.value} {date_str}",
                )
                if not tickers:
                    logger.warning("pykrx returned no tickers for %s on %s", market.value, date_str)
                    continue

                names = self._fetch_names(stock, date_str, market)

                for ticker in tickers:
                    records.append(
                        Stock(
                            ticker=ticker,
                            market=market,
                            # name is NOT NULL in the DDL and is not the point
                            # of this backfill — "was it listed that day" is.
                            # Fall back to the ticker rather than spending a
                            # request per name.
                            name=names.get(ticker) or ticker,
                            status=ListingStatus.ACTIVE,
                            last_seen_date=reference_date,
                            source=Source.PYKRX_BACKFILL,
                        )
                    )

            snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=reference_date,
                source=Source.PYKRX_BACKFILL,
                fetched_at=fetched_at,
                records=records,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as exc:
            logger.exception("Failed to fetch historical pykrx universe: %s", exc)
            return UniverseResult(error=str(exc))

    def _fetch_names(self, stock: object, date_str: str, market: Market) -> dict[str, str]:
        """Return ``{ticker: name}`` for one market, or ``{}`` if unavailable.

        A missing name is not worth failing a snapshot over — the caller falls
        back to the ticker.
        """
        try:
            df = stock.get_market_price_change_by_ticker(  # type: ignore[attr-defined]
                date_str, date_str, market=market.value
            )
        except Exception:
            logger.warning("Name lookup failed for %s on %s", market.value, date_str)
            return {}

        if df is None or df.empty or self._NAME_COLUMN not in df.columns:
            return {}

        return {
            str(ticker): str(name).strip()
            for ticker, name in df[self._NAME_COLUMN].items()
            if str(name).strip()
        }

"""pykrx daily OHLCV price provider — stub.

Uses ``pykrx.stock.get_market_ohlcv_by_date(fromdate, todate, ticker)``
to retrieve daily bars for a single ticker.

Mapping notes (for implementation):
    • pykrx returns a DataFrame indexed by date with columns:
      시가, 고가, 저가, 종가, 거래량 (Open, High, Low, Close, Volume).
    • All prices are integers (KRW).
    • Rate limiting should be applied between consecutive calls to avoid
      being blocked by KRX — use ``RATE_LIMIT_SECONDS`` from settings.
"""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.adapters.pykrx_auth import call_with_session_retry, get_pykrx_stock_module
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import DailyBar, DailyPriceResult
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


class PykrxDailyPriceProvider:
    """Fetches daily OHLCV bars for a single ticker via pykrx.

    Conforms to :class:`~collector.kr.ports.prices.PriceProvider`.
    """

    def fetch_daily_ohlcv(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> DailyPriceResult:
        """Retrieve daily OHLCV bars from pykrx.

        Args:
            ticker: 6-digit KRX ticker code.
            market: Market segment.
            start: First trade date (inclusive).
            end: Last trade date (inclusive).

        Returns:
            ``DailyPriceResult`` containing bars or an error.
        """
        try:
            stock = get_pykrx_stock_module()
            start_str = start.strftime("%Y%m%d")
            end_str = end.strftime("%Y%m%d")

            logger.debug(
                "Fetching OHLCV for %s (%s) from %s to %s", ticker, market.value, start_str, end_str
            )
            # An empty frame is ambiguous here: a delisted or not-yet-listed
            # ticker has no rows in the range, and so does a ticker fetched over
            # a dropped KRX session. The helper only acts once empties cluster,
            # which is what a dead session looks like and scattered absent data
            # does not. See adapters/pykrx_auth.py.
            df = call_with_session_retry(
                lambda: stock.get_market_ohlcv_by_date(start_str, end_str, ticker),
                is_empty=lambda frame: frame is None or frame.empty,
                label=f"ohlcv {ticker} {start_str}-{end_str}",
            )

            if df is None or df.empty:
                return DailyPriceResult(ticker=ticker, bars=[])

            bars: list[DailyBar] = []
            fetched_at = now_kst()

            for trade_date, row in df.iterrows():
                trade_date_val = trade_date.date() if hasattr(trade_date, "date") else trade_date  # type: ignore

                bars.append(
                    DailyBar(
                        ticker=ticker,
                        market=market,
                        trade_date=trade_date_val,
                        open=int(row["시가"]),
                        high=int(row["고가"]),
                        low=int(row["저가"]),
                        close=int(row["종가"]),
                        volume=int(row["거래량"]),
                        source=Source.PYKRX,
                        fetched_at=fetched_at,
                    )
                )

            return DailyPriceResult(ticker=ticker, bars=bars)

        except Exception as exc:
            logger.exception("Failed to fetch daily prices for %s", ticker)
            return DailyPriceResult(ticker=ticker, error=str(exc))

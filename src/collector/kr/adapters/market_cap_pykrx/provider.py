"""pykrx daily market-cap provider.

Uses ``pykrx.stock.get_market_cap_by_ticker(date, market)`` — one call returns
every listed stock on that date.

Response shape (verified live against pykrx 1.2.8, 2026-08-15; see
``docs/dev/20260731_raw_features/02_data_expansion_plan/poc/n1_pykrx_market_cap.md``):

    >>> get_market_cap_by_ticker('20240102', market='KOSPI')
                종가             시가총액       거래량           거래대금       상장주식수
    티커
    005930   79600  475194690980000  17142847  1356958225913  5969782550

Three properties of that response drive the code below.

1. There is no market column — the index is the ticker.  ``market`` therefore
   comes from the call argument.  ``market='ALL'`` is never used: it cannot
   fill the column, and it also folds in KONEX (129 tickers on 2024-01-02).

2. Holidays do NOT return an empty frame.  They return every ticker with the
   price columns zeroed and ``상장주식수`` populated.  ``alternative=False`` is
   passed explicitly because ``alternative=True`` silently substitutes the
   previous session's data under the requested date, which is worse than a
   zero row — it is a wrong row that looks right.

3. pykrx casts blanks to ``0`` (``wrap.py``: ``df.replace("", 0)`` then
   ``astype(np.int64)``), so a real zero and a missing value are the same
   value.  ``종가 == 0`` never happens on a real session — across six sampled
   market-days there were zero occurrences — so it is treated as "this row is
   not real".  A zero ``거래량``/``거래대금`` on a row with a live close IS
   real (a halted name) and is stored as zero, not NULL.
"""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.adapters.pykrx_auth import call_with_session_retry, get_pykrx_stock_module
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import DailyMarketCapResult, DailyMarketCapRow
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

_CLOSE = "종가"
_MARKET_CAP = "시가총액"
_VOLUME = "거래량"
_TRADING_VALUE = "거래대금"
_LISTED_SHARES = "상장주식수"

_REQUIRED_COLUMNS = (_CLOSE, _MARKET_CAP, _VOLUME, _TRADING_VALUE, _LISTED_SHARES)


def _as_int(value: object) -> int | None:
    """Coerce a pykrx cell to ``int``, or ``None`` when it is not a number."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


class PykrxMarketCapProvider:
    """Fetches one ``(trade_date, market)`` slice of market-cap rows via pykrx.

    Conforms to :class:`~collector.kr.ports.market_cap.MarketCapProvider`.
    """

    def fetch_by_date(self, trade_date: date, market: Market) -> DailyMarketCapResult:
        """Retrieve every listed stock's market-cap row for one date and market.

        Args:
            trade_date: Trading date to fetch.
            market: Market segment to fetch (fills the ``market`` column).

        Returns:
            ``DailyMarketCapResult`` with normalised rows, the raw response row
            count, or an error message.  Never raises.
        """
        try:
            stock = get_pykrx_stock_module()
            date_str = trade_date.strftime("%Y%m%d")

            logger.debug("Fetching market cap for %s on %s", market.value, date_str)
            # The caller pre-filters to trading days, so an empty frame here is
            # a dropped KRX session far more often than a closed market, and
            # pykrx cannot tell because its expiry clock still says the session
            # is good. This backfill is ~6,000 slices, long enough that the
            # session will be dropped mid-run. See adapters/pykrx_auth.py.
            df = call_with_session_retry(
                lambda: stock.get_market_cap_by_ticker(
                    date_str, market=market.value, alternative=False
                ),
                is_empty=lambda frame: frame is None or frame.empty,
                label=f"market cap {market.value} {date_str}",
            )

            if df is None or df.empty:
                return DailyMarketCapResult(trade_date=trade_date, market=market, rows=[])

            missing = [col for col in _REQUIRED_COLUMNS if col not in df.columns]
            if missing:
                return DailyMarketCapResult(
                    trade_date=trade_date,
                    market=market,
                    error=f"unexpected response columns; missing {missing}",
                )

            fetched_at = now_kst()
            rows: list[DailyMarketCapRow] = []

            for ticker, row in df.iterrows():
                source_close = _as_int(row[_CLOSE])

                # Zeroed close == holiday zero-fill, not a real session.  Drop
                # the row rather than store a price of 0.
                if not source_close:
                    continue

                rows.append(
                    DailyMarketCapRow(
                        ticker=str(ticker),
                        market=market,
                        trade_date=trade_date,
                        source_close=source_close,
                        market_cap=_as_int(row[_MARKET_CAP]),
                        trading_value=_as_int(row[_TRADING_VALUE]),
                        listed_shares=_as_int(row[_LISTED_SHARES]),
                        volume=_as_int(row[_VOLUME]),
                        source=Source.PYKRX,
                        fetched_at=fetched_at,
                    )
                )

            return DailyMarketCapResult(
                trade_date=trade_date,
                market=market,
                rows=rows,
                response_rows=len(df),
            )

        except Exception as exc:
            logger.exception("Failed to fetch market cap for %s on %s", market.value, trade_date)
            return DailyMarketCapResult(trade_date=trade_date, market=market, error=str(exc))

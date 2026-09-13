"""Daily OHLCV from Naver's chart endpoint, with no KRX contact at all (K-5).

This replaces ``prices_pykrx``, and the reason is not the data — it is identical.
``pykrx.stock.get_market_ohlcv_by_date`` defaults to ``adjusted=True``, and that
branch calls ``pykrx.website.naver``, which fetches exactly the URL below.  KRX
never sees the request.

What KRX *did* see was the import.  ``pykrx/website/comm/webio.py`` runs
``_session = build_krx_session()`` at module scope, so ``from pykrx import
stock`` performs a KRX login — a warmup GET, a login-page GET and a login POST —
before any collector asks for anything.  Every ``prices backfill`` therefore
logged in to KRX to download data from Naver.  Nor can that be dodged by
importing a narrower module: ``pykrx.website.naver.core`` imports ``Get`` from
that same ``webio``.

So the wrapper goes and the request stays.  KRX restricted this host on
2026-08-16 for automated collection; a login that buys nothing is the easiest
KRX traffic to stop sending.

Response shape, verified live on 2026-08-18 (``symbol=005930&count=5``)::

    \n\n<?xml version="1.0" encoding="EUC-KR" ?>
    <protocol><chartdata symbol="005930" name="삼성전자" count="5" timeframe="day">
      <item data="20260814|275000|275500|266000|274500|21669476" />
    </chartdata></protocol>

An unknown ticker answers ``<protocol />`` — no ``chartdata``, still a valid
document — and a delisted one answers with its real history (``001529`` returns
bars ending in 2022).  So "no bars" is normal and must not read as a failure.

Three properties of the response drive the code below.

1. **``count`` counts bars back from today, not from the requested range.**  The
   endpoint takes no start date, so reaching a start in 2015 means asking for
   every bar since.  Calendar days over-count trading days, which is the safe
   direction: ask for more bars than needed and filter.
2. **The declared encoding is EUC-KR**, and ``xml.etree.ElementTree`` refuses
   bytes carrying a multi-byte encoding declaration ("multi-byte encodings are
   not supported").  So the bytes are decoded here — explicitly, rather than
   through ``response.text``, which would guess ISO-8859-1 if the header ever
   lost its charset — and the decoded string is what gets parsed.  Only the
   company name is non-ASCII and this adapter never reads it, so a decode error
   is replaced rather than raised.

3. **Blank lines precede the XML declaration**, which ElementTree rejects
   outright ("XML or text declaration not at start of entity").  The payload is
   stripped before parsing.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import date
from typing import Final

import requests

from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import DailyBar, DailyPriceResult
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

DEFAULT_NAVER_CHART_URL: Final = "https://fchart.stock.naver.com/sise.nhn"
DEFAULT_TIMEOUT_SECONDS: Final = 10.0

#: What the response's own XML declaration says.
_RESPONSE_ENCODING: Final = "euc-kr"

#: Root element of a real answer, empty or not.
_ROOT_TAG: Final = "protocol"

#: Extra bars requested beyond the calendar span, matching what pykrx asked for.
#: The endpoint includes today's (possibly intraday) bar, so the span alone can
#: fall one short of the requested start.
_COUNT_MARGIN_DAYS: Final = 2

#: ``data`` is pipe-separated in this order.
_FIELD_COUNT: Final = 6


def _parse_item(raw: str) -> tuple[date, int, int, int, int, int] | None:
    """Parse one ``data`` attribute, returning ``None`` if it is unusable.

    A malformed row is skipped rather than failing the ticker: one bad bar in a
    ten-year range should not cost the other 2,500.
    """
    parts = raw.split("|")
    if len(parts) < _FIELD_COUNT:
        return None
    try:
        trade_date = date(int(parts[0][0:4]), int(parts[0][4:6]), int(parts[0][6:8]))
        values = [int(part) for part in parts[1:_FIELD_COUNT]]
    except ValueError:
        return None
    return trade_date, values[0], values[1], values[2], values[3], values[4]


class NaverDailyPriceProvider:
    """Fetches adjusted daily OHLCV bars for one ticker from Naver.

    Conforms to :class:`~collector.kr.ports.prices.PriceProvider`.
    """

    def __init__(
        self,
        *,
        url: str = DEFAULT_NAVER_CHART_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        session: requests.Session | None = None,
    ) -> None:
        self._url = url
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        #: Real HTTP requests issued, as opposed to logical fetch calls (K-5).
        self.http_requests = 0

    def fetch_daily_ohlcv(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> DailyPriceResult:
        """Retrieve daily OHLCV bars for *ticker* between *start* and *end*.

        Args:
            ticker: 6-digit KRX ticker code.
            market: Market segment; fills the ``market`` column. The endpoint
                does not report it, so the caller's value is authoritative.
            start: First trade date (inclusive).
            end: Last trade date (inclusive).

        Returns:
            ``DailyPriceResult`` with bars, or with ``error`` set. Never raises
            for an upstream failure.
        """
        if start > end:
            return DailyPriceResult(ticker=ticker, error=f"start {start} is after end {end}")

        try:
            payload = self._fetch_xml(ticker, self._bar_count(start))
        except Exception as exc:
            logger.warning("Naver OHLCV fetch failed for %s: %s", ticker, exc)
            return DailyPriceResult(ticker=ticker, error=str(exc))

        try:
            # `.lstrip()` is load-bearing: the endpoint emits blank lines ahead
            # of the XML declaration, and ElementTree rejects a declaration that
            # is not at the very start of the entity.
            root = ET.fromstring(payload.decode(_RESPONSE_ENCODING, errors="replace").lstrip())
        except ET.ParseError as exc:
            return DailyPriceResult(ticker=ticker, error=f"unparseable response: {exc}")

        if root.tag != _ROOT_TAG:
            # An error page is often well-formed, so parsing alone does not
            # separate "upstream is unhappy" from "this ticker has no bars".
            # The root tag does: an unknown ticker still answers <protocol />,
            # while an HTML error page is rooted at <html>. Reading the latter
            # as an empty range would write a gap that gap-detection then reads
            # as genuinely absent data.
            return DailyPriceResult(
                ticker=ticker,
                error=f"unexpected response root <{root.tag}>, expected <{_ROOT_TAG}>",
            )

        fetched_at = now_kst()
        bars: list[DailyBar] = []
        skipped = 0
        for node in root.iter("item"):
            parsed = _parse_item(node.get("data", ""))
            if parsed is None:
                skipped += 1
                continue
            trade_date, open_, high, low, close, volume = parsed
            if trade_date < start or trade_date > end:
                continue
            bars.append(
                DailyBar(
                    ticker=ticker,
                    market=market,
                    trade_date=trade_date,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    source=Source.NAVER,
                    fetched_at=fetched_at,
                )
            )

        if skipped:
            logger.warning("Naver returned %d unparseable bars for %s", skipped, ticker)

        return DailyPriceResult(ticker=ticker, bars=bars)

    def _bar_count(self, start: date) -> int:
        """How many bars back from today are needed to reach *start*."""
        span_days = (today_kst() - start).days + _COUNT_MARGIN_DAYS
        return max(1, span_days)

    def _fetch_xml(self, ticker: str, count: int) -> bytes:
        """Issue the request, returning raw bytes so the XML declaration holds."""
        self.http_requests += 1
        response = self._session.get(
            self._url,
            params={
                "symbol": ticker,
                "timeframe": "day",
                "count": str(count),
                "requestType": "0",
            },
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        return response.content

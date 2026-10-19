"""KRX regular-session sources for ``fix_regular_session`` (same-day MDC and the combined one).

``daily_market_cap`` (KRX OpenAPI) is the official source but lags one trading day: at day T
20:00 the newest row is T-1. ``MDCSTAT01501`` on ``data.krx.co.kr`` has day T once the market
closes, but its values were still moving at 20:00 (005930 close above its high) and were
consistent at 20:36 (measured 2026-10-19), because the after-market session was still folding
in. Hence the 20:30 guard. Whether post-20:30 MDC equals the official regular session is to be
checked against the OpenAPI row when it arrives; the combined source is built so that the
OpenAPI row always wins and the audit row names the source each value came from.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import date, datetime, time, timedelta
from typing import Any, Protocol

import requests

from collector.kr.adapters.krx_common.client import KrxMdcError
from collector.kr.domain.models import KrxSessionRow
from collector.kr.service.fix_regular_session import FixStorage, MarketCapSource
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

MDC_BLD = "dbms/MDC/STAT/standard/MDCSTAT01501"
MDC_OUTPUT_KEY = "OutBlock_1"
MDC_SOURCE_NAME = "krx_mdc_MDCSTAT01501"
OPENAPI_SOURCE_NAME = "daily_market_cap"
#: KRX mktId -> repo market value. KONEX (KNX) is not collected.
MDC_MARKETS = {"STK": "KOSPI", "KSQ": "KOSDAQ"}
#: Same-day MDC values are only trustworthy after the after-market has settled.
MDC_SAME_DAY_NOT_BEFORE = time(20, 30)

REJECT_NONPOSITIVE = "mdc_nonpositive_or_missing"
REJECT_INCONSISTENT = "mdc_ohlc_inconsistent"
REJECT_TOO_EARLY = "mdc_too_early"
REJECT_MDC_ERROR = "mdc_error"
#: Failures the combined source absorbs (OpenAPI-only for that run); ``mdc`` alone raises.
MDC_ABSORBED_ERRORS = (KrxMdcError, requests.RequestException, TimeoutError, ConnectionError)
MAX_ERROR_MESSAGE = 200


class MdcTooEarlyError(RuntimeError):
    """Same-day MDC was requested before 20:30 KST without the override."""


class MdcClient(Protocol):
    def post_rows(self, bld: str, params: dict[str, Any], *, output_key: str) -> list[Any]: ...


def _to_int(value: Any) -> int | None:
    """``"1,234"`` -> 1234; ``""``, ``"-"``, ``None`` and junk -> ``None``."""
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


class KrxMdcSameDaySource:
    """``RegularSessionSource`` reading MDCSTAT01501 (전종목 시세) for each date and market."""

    name = MDC_SOURCE_NAME

    def __init__(
        self,
        client: MdcClient,
        *,
        allow_early: bool = False,
        now_fn: Callable[[], datetime] = now_kst,
    ) -> None:
        self._client = client
        self._allow_early = allow_early
        self._now_fn = now_fn
        #: ``(ticker/market/date key, reason)`` for every row or date dropped by this source.
        self.rejected: list[tuple[str, str]] = []
        #: Dates to fetch; ``None`` means every date in the requested range.
        self._only_dates: set[date] | None = None

    def restrict_dates(self, dates: Iterable[date]) -> None:
        self._only_dates = set(dates)

    def _check_not_too_early(self, day: date) -> None:
        now = self._now_fn()
        if self._allow_early:
            return
        if day == now.date() and now.time() < MDC_SAME_DAY_NOT_BEFORE:
            raise MdcTooEarlyError(
                f"MDC rows for {day} are not final before "
                f"{MDC_SAME_DAY_NOT_BEFORE:%H:%M} KST (now {now:%Y-%m-%d %H:%M}); "
                "pass the override to run anyway."
            )

    def get_session_rows(self, since: date, until: date) -> list[KrxSessionRow]:
        self.rejected = []
        out: list[KrxSessionRow] = []
        day = since
        while day <= until:
            # Future days have nothing yet (the service reads context after ``end``).
            if (
                day.weekday() < 5
                and day <= self._now_fn().date()
                and (self._only_dates is None or day in self._only_dates)
            ):
                self._check_not_too_early(day)
                for mkt_id, market in MDC_MARKETS.items():
                    out.extend(self._fetch_one(day, mkt_id, market))
            day += timedelta(days=1)
        return out

    def _fetch_one(self, day: date, mkt_id: str, market: str) -> list[KrxSessionRow]:
        params = {
            "locale": "ko_KR",
            "mktId": mkt_id,
            "trdDd": day.strftime("%Y%m%d"),
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
        }
        records = self._client.post_rows(MDC_BLD, params, output_key=MDC_OUTPUT_KEY)
        rows: list[KrxSessionRow] = []
        for rec in records:
            raw = rec.row if hasattr(rec, "row") else rec
            ticker = str(raw.get("ISU_SRT_CD", "")).strip()
            if not ticker:
                continue
            o, h, low, c = (
                _to_int(raw.get(k)) for k in ("TDD_OPNPRC", "TDD_HGPRC", "TDD_LWPRC", "TDD_CLSPRC")
            )
            key = f"{ticker}/{market}/{day}"
            reason = self._sanity(o, h, low, c)
            if reason is not None:
                self.rejected.append((key, reason))
                # Keep the row with no values so the service reports it as krx_invalid
                # instead of silently treating it as "no KRX row".
                rows.append(KrxSessionRow(ticker, market, day, None, None, None, None, self.name))
                continue
            rows.append(KrxSessionRow(ticker, market, day, o, h, low, c, self.name))
        return rows

    @staticmethod
    def _sanity(o: int | None, h: int | None, low: int | None, c: int | None) -> str | None:
        if o is None or h is None or low is None or c is None or min(o, h, low, c) <= 0:
            return REJECT_NONPOSITIVE
        if not (low <= o <= h and low <= c <= h):
            return REJECT_INCONSISTENT
        return None


def _valid(row: KrxSessionRow) -> bool:
    vals = (row.open, row.high, row.low, row.close)
    return all(v is not None and v > 0 for v in vals)


class CombinedSource:
    """OpenAPI (``daily_market_cap``) first; MDC only for dates OpenAPI has not delivered.

    For each ``(ticker, market, date)`` a valid OpenAPI row wins. MDC is asked only for dates
    after the newest ``daily_market_cap`` date in the window (normally just today), so the
    daily rerun over the last N trading days re-fixes older days with OpenAPI values. When MDC
    is refused (before 20:30) the source degrades to OpenAPI only and records the reason.
    """

    name = "combined"

    def __init__(
        self,
        storage: FixStorage,
        mdc: KrxMdcSameDaySource,
        *,
        now_fn: Callable[[], datetime] = now_kst,
    ) -> None:
        self._openapi = MarketCapSource(storage)
        self._mdc = mdc
        self._now_fn = now_fn
        self.rejected: list[tuple[str, str]] = []
        #: ``ClassName: short message`` for each absorbed MDC failure (kept in run params).
        self.errors: list[str] = []

    def get_session_rows(self, since: date, until: date) -> list[KrxSessionRow]:
        self.rejected = []
        self.errors = []
        api_rows = [
            KrxSessionRow(
                r.ticker,
                r.market,
                r.trade_date,
                r.open,
                r.high,
                r.low,
                r.close,
                OPENAPI_SOURCE_NAME,
            )
            for r in self._openapi.get_session_rows(since, until)
        ]
        have = {(r.ticker, r.market, r.trade_date) for r in api_rows if _valid(r)}
        latest_api = max((r.trade_date for r in api_rows), default=None)
        first = since if latest_api is None else max(since, latest_api + timedelta(days=1))
        last = min(until, self._now_fn().date())
        if first > last:
            return api_rows
        self._mdc.restrict_dates(
            d for d in (first + timedelta(days=i) for i in range((last - first).days + 1))
        )
        try:
            mdc_rows = self._mdc.get_session_rows(first, last)
        except MdcTooEarlyError as exc:
            logger.warning("Combined source: %s; using OpenAPI rows only.", exc)
            self.rejected.append((f"{first}..{last}", REJECT_TOO_EARLY))
            return api_rows
        except MDC_ABSORBED_ERRORS as exc:
            message = f"{type(exc).__name__}: {str(exc)[:MAX_ERROR_MESSAGE]}"
            logger.warning(
                "Combined source: MDC failed (%s); using OpenAPI rows only, "
                "same-day rows stay unfixed until the next run.",
                message,
            )
            self.rejected.append((f"{first}..{last}", REJECT_MDC_ERROR))
            self.errors.append(message)
            return api_rows
        self.rejected.extend(self._mdc.rejected)
        # OpenAPI wins: an MDC row is added only where no valid OpenAPI row exists. An OpenAPI
        # row that is present but invalid (NULL, 0) is replaced by a valid MDC row.
        extra = [r for r in mdc_rows if (r.ticker, r.market, r.trade_date) not in have]
        replaced = {(r.ticker, r.market, r.trade_date) for r in extra}
        kept = [r for r in api_rows if (r.ticker, r.market, r.trade_date) not in replaced]
        return kept + extra

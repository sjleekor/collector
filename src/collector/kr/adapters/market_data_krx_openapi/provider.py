"""KRX Open API market-cap and universe providers.

One request returns a whole market-day.  Verified live on 2026-08-18
(``basDd=20260814``): KOSPI 942 rows, KOSDAQ 1,821 rows, 15 fields::

    {"BAS_DD": "20260814", "ISU_CD": "095570", "ISU_NM": "AJ네트웍스",
     "MKT_NM": "KOSPI", "SECT_TP_NM": "", "TDD_CLSPRC": "4520",
     "CMPPREVDD_PRC": "100", "FLUC_RT": "2.26", "TDD_OPNPRC": "4435",
     "TDD_HGPRC": "4545", "TDD_LWPRC": "4435", "ACC_TRDVOL": "74511",
     "ACC_TRDVAL": "334258460", "MKTCAP": "204542470680",
     "LIST_SHRS": "45252759"}

Three properties of that response drive the code below, and each of them
differs from the pykrx path this replaces.

1. **A non-trading day returns zero rows.**  ``basDd=20260817`` (a substitute
   holiday) returned an empty ``OutBlock_1``.  pykrx instead returns every
   ticker with the price columns zeroed, which is why the pykrx adapter has to
   pass ``alternative=False`` and treat ``종가 == 0`` as "not a real row".
   Neither guard is needed here: absence is expressed as absence.

2. **Every value is a string**, including the numeric ones, and a field can be
   an empty string.  Parsing is explicit and an unparseable value becomes
   ``None`` rather than zero — a zero that cannot be told from a real zero is
   the bug the pykrx adapter had to work around.

3. **``MKT_NM`` exists but is not used to fill ``market``.**  The endpoint is
   already market-specific, so the call argument is authoritative and cheaper
   to trust.  ``SECT_TP_NM`` is *not* an industry: it is the KOSDAQ 소속부
   (벤처기업부 / 중견기업부) and is blank for KOSPI.  It is deliberately not
   mapped anywhere near N2's ``induty_code``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta

from collector.kr.adapters.market_data_krx_openapi.client import KrxOpenApiClient
from collector.kr.domain.enums import ListingStatus, Market, Source
from collector.kr.domain.models import (
    DailyMarketCapResult,
    DailyMarketCapRow,
    Stock,
    StockUniverseSnapshot,
    UniverseResult,
)
from collector.kr.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

#: One daily-trade endpoint per market.  There is no combined endpoint, and
#: that is fine: the split is what fills ``market`` without a stock_master join.
DAILY_TRADE_ENDPOINTS: dict[Market, tuple[str, str]] = {
    Market.KOSPI: ("sto", "stk_bydd_trd"),
    Market.KOSDAQ: ("sto", "ksq_bydd_trd"),
}

#: Issue base info, same market split.  Carries LIST_DD / SECUGRP_NM /
#: KIND_STKCERT_TP_NM, which is what a universe filter policy needs.
ISSUE_BASE_ENDPOINTS: dict[Market, tuple[str, str]] = {
    Market.KOSPI: ("sto", "stk_isu_base_info"),
    Market.KOSDAQ: ("sto", "ksq_isu_base_info"),
}

_TICKER = "ISU_CD"
_NAME = "ISU_NM"

# Issue-base-info field names.  ``ISU_CD`` means something *different* here than
# in the daily-trade response: base info returns the 12-character ISIN
# (``KR7095570008``) in ``ISU_CD`` and the 6-digit code in ``ISU_SRT_CD``, while
# daily trade puts the 6-digit code straight in ``ISU_CD``.  Reusing the
# constant across the two would silently fill ``ticker`` with ISINs.
_SHORT_TICKER = "ISU_SRT_CD"
#: Abbreviated name.  Equal to what the daily-trade endpoint calls ``ISU_NM``
#: ("AJ네트웍스"), whereas base info's ``ISU_NM`` is the legal name with the
#: share class appended ("AJ네트웍스보통주").
_ABBREVIATED_NAME = "ISU_ABBRV"
_LISTING_DATE = "LIST_DD"
_SECURITY_GROUP = "SECUGRP_NM"
_SHARE_CLASS = "KIND_STKCERT_TP_NM"
_CLOSE = "TDD_CLSPRC"
_OPEN = "TDD_OPNPRC"
_HIGH = "TDD_HGPRC"
_LOW = "TDD_LWPRC"
_VOLUME = "ACC_TRDVOL"
_TRADING_VALUE = "ACC_TRDVAL"
_MARKET_CAP = "MKTCAP"
_LISTED_SHARES = "LIST_SHRS"

REQUIRED_COLUMNS: tuple[str, ...] = (
    _TICKER,
    _NAME,
    _CLOSE,
    _OPEN,
    _HIGH,
    _LOW,
    _VOLUME,
    _TRADING_VALUE,
    _MARKET_CAP,
    _LISTED_SHARES,
)

REQUIRED_ISSUE_BASE_COLUMNS: tuple[str, ...] = (
    _SHORT_TICKER,
    _ABBREVIATED_NAME,
    _LISTING_DATE,
)


class _NoPublishedData(RuntimeError):
    """The endpoint has nothing for any candidate date in the lookback window."""


def parse_yyyymmdd(value: object) -> date | None:
    """Parse a ``YYYYMMDD`` field, returning ``None`` when it is unusable.

    A listing date is reference data, not a key, so an unparseable one should
    cost that one field rather than the whole universe fetch.
    """
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def parse_int(value: object) -> int | None:
    """Parse a KRX Open API numeric string.

    Values arrive as strings, sometimes empty, sometimes comma-grouped.  A
    value that cannot be parsed becomes ``None``, never ``0``.

    Args:
        value: Raw cell from the response.

    Returns:
        The integer value, or ``None`` when it is blank or unparseable.
    """
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


def _missing_columns(row: dict[str, object]) -> tuple[str, ...]:
    return tuple(column for column in REQUIRED_COLUMNS if column not in row)


class KrxOpenApiMarketCapProvider:
    """Fetches one ``(trade_date, market)`` market-cap slice from the Open API.

    Conforms to :class:`~collector.kr.ports.market_cap.MarketCapProvider`.
    """

    def __init__(self, client: KrxOpenApiClient) -> None:
        self._client = client

    def fetch_by_date(self, trade_date: date, market: Market) -> DailyMarketCapResult:
        """Retrieve every listed stock's market-cap row for one date and market.

        Args:
            trade_date: Trading date to fetch.
            market: Market segment; fills the ``market`` column.

        Returns:
            ``DailyMarketCapResult`` with normalised rows, the raw response row
            count, or an error message.  Never raises for an upstream failure.
        """
        endpoint = DAILY_TRADE_ENDPOINTS.get(market)
        if endpoint is None:
            return DailyMarketCapResult(
                trade_date=trade_date,
                market=market,
                error=f"KRX Open API has no daily-trade endpoint for market {market.value}",
            )

        group, name = endpoint
        try:
            raw_rows = self._client.fetch_rows(
                group, name, {"basDd": trade_date.strftime("%Y%m%d")}
            )
        except Exception as exc:
            logger.exception("KRX Open API market-cap fetch failed: %s", exc)
            return DailyMarketCapResult(trade_date=trade_date, market=market, error=str(exc))

        if raw_rows:
            missing = _missing_columns(raw_rows[0])
            if missing:
                # A renamed field would otherwise turn into a column of NULLs
                # that looks like sparse upstream data.
                return DailyMarketCapResult(
                    trade_date=trade_date,
                    market=market,
                    response_rows=len(raw_rows),
                    error=f"KRX Open API response is missing columns: {', '.join(missing)}",
                )

        fetched_at = now_kst()
        rows: list[DailyMarketCapRow] = []
        for raw in raw_rows:
            ticker = str(raw.get(_TICKER, "")).strip()
            if not ticker:
                continue
            rows.append(
                DailyMarketCapRow(
                    ticker=ticker,
                    market=market,
                    trade_date=trade_date,
                    source_close=parse_int(raw.get(_CLOSE)),
                    market_cap=parse_int(raw.get(_MARKET_CAP)),
                    trading_value=parse_int(raw.get(_TRADING_VALUE)),
                    listed_shares=parse_int(raw.get(_LISTED_SHARES)),
                    volume=parse_int(raw.get(_VOLUME)),
                    source=Source.KRX_OPENAPI,
                    fetched_at=fetched_at,
                    source_open=parse_int(raw.get(_OPEN)),
                    source_high=parse_int(raw.get(_HIGH)),
                    source_low=parse_int(raw.get(_LOW)),
                )
            )

        return DailyMarketCapResult(
            trade_date=trade_date,
            market=market,
            rows=rows,
            response_rows=len(raw_rows),
        )


class KrxOpenApiHistoricalUniverseProvider:
    """Reconstructs a past-dated universe snapshot from the Open API.

    Conforms to :class:`~collector.kr.ports.universe.UniverseProvider`.

    The same daily-trade call that fills market cap already lists exactly the
    tickers that traded that day, with their names, so a snapshot costs one
    request per market rather than the two the pykrx path needed.  Snapshots
    carry ``Source.KRX_OPENAPI_BACKFILL`` so the live ``sync_universe`` diff
    never reads a reconstructed snapshot as a newly observed one.
    """

    def __init__(self, client: KrxOpenApiClient) -> None:
        self._client = client

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the listed universe as of a date.

        Args:
            markets: Market segments to query.
            as_of: Reference date.  ``None`` means today (Asia/Seoul).

        Returns:
            ``UniverseResult`` with a ``Source.KRX_OPENAPI_BACKFILL`` snapshot,
            or an error.  Never raises for an upstream failure.
        """
        reference_date = as_of or today_kst()
        fetched_at = now_kst()
        records: list[Stock] = []

        for market in markets:
            endpoint = DAILY_TRADE_ENDPOINTS.get(market)
            if endpoint is None:
                return UniverseResult(
                    error=f"KRX Open API has no daily-trade endpoint for market {market.value}"
                )
            group, name = endpoint
            try:
                raw_rows = self._client.fetch_rows(
                    group, name, {"basDd": reference_date.strftime("%Y%m%d")}
                )
            except Exception as exc:
                logger.exception("KRX Open API universe fetch failed: %s", exc)
                return UniverseResult(error=str(exc))

            if not raw_rows:
                # A trading day that returns nothing is an upstream problem,
                # not an empty market.  Reporting it as a snapshot would look
                # like every listing was delisted at once.
                return UniverseResult(
                    error=(
                        f"KRX Open API returned no rows for {market.value} on "
                        f"{reference_date.isoformat()}; refusing to record an empty universe"
                    )
                )

            for raw in raw_rows:
                ticker = str(raw.get(_TICKER, "")).strip()
                if not ticker:
                    continue
                records.append(
                    Stock(
                        ticker=ticker,
                        market=market,
                        # The daily-trade response carries ISU_NM, so unlike
                        # the pykrx path there is no second call and no
                        # ticker-as-name fallback to spend requests avoiding.
                        name=str(raw.get(_NAME, "")).strip() or ticker,
                        status=ListingStatus.ACTIVE,
                        last_seen_date=reference_date,
                        source=Source.KRX_OPENAPI_BACKFILL,
                    )
                )

        snapshot = StockUniverseSnapshot(
            snapshot_id=str(uuid.uuid4()),
            as_of_date=reference_date,
            source=Source.KRX_OPENAPI_BACKFILL,
            fetched_at=fetched_at,
            records=records,
        )
        return UniverseResult(snapshot=snapshot)


class KrxOpenApiUniverseProvider:
    """Fetches the current listed universe from issue base info.

    Conforms to :class:`~collector.kr.ports.universe.UniverseProvider`, and
    replaces the FDR path that ``universe sync`` used (K-5).

    The replacement is about permission, not volume.  ``fdr.StockListing`` reads
    its listing rows from a GitHub CSV cache, but it first calls
    ``data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd`` — twice per
    call, the second one a duplicate — purely to learn ``max_work_dt``.  That is
    four anonymous MDC requests per daily run, outside our throttle and outside
    our accounting, for a field the official endpoint gives us as part of the
    answer.  KRX restricted this host for automated collection (K-0), so the
    question is not how much traffic it is; it is which door it goes through.

    Two fields carry real gains over FDR: ``LIST_DD`` is a listing date from the
    exchange rather than FDR's best-effort column, and ``SECUGRP_NM`` /
    ``KIND_STKCERT_TP_NM`` finally make the universe's composition visible.
    Nothing is filtered on them here — narrowing the universe is N3-3's decision
    and would change what every downstream table covers — but the breakdown is
    logged so that decision can be made from counts instead of guesses.

    One behaviour FDR did not need: **this endpoint is dated, and today's file
    is not published all day.**  Measured 2026-08-18 at 16:04 KST, after the
    15:30 close, ``basDd=20260818`` still returned zero rows.  FDR reads a cache
    that always answers, so a daily job never saw this.  When the caller does
    not name a date, the provider therefore asks for the most recent day that
    actually has data and labels the snapshot with *that* date — which is what
    "as of" means.  A named date is used exactly as given.
    """

    def __init__(self, client: KrxOpenApiClient, *, max_lookback_days: int = 10) -> None:
        self._client = client
        # Enough for the longest KRX holiday cluster plus an unpublished day.
        # Bounded so a real outage surfaces as an error instead of quietly
        # resurrecting a universe from an arbitrary distance in the past.
        self._max_lookback_days = max(0, max_lookback_days)

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the listed universe as of a date.

        Args:
            markets: Market segments to query.
            as_of: Reference date, used exactly as given.  ``None`` means the
                most recent day the endpoint has published, searching back from
                today (Asia/Seoul).

        Returns:
            ``UniverseResult`` with a ``Source.KRX_OPENAPI`` snapshot, or an
            error.  Never raises for an upstream failure.
        """
        if not markets:
            return UniverseResult(error="no markets requested")

        fetched_at = now_kst()
        try:
            reference_date, probe_rows = self._resolve_reference_date(markets[0], as_of)
        except _NoPublishedData as exc:
            return UniverseResult(error=str(exc))
        except Exception as exc:
            logger.exception("KRX Open API universe fetch failed: %s", exc)
            return UniverseResult(error=str(exc))

        records: list[Stock] = []
        for index, market in enumerate(markets):
            if index == 0:
                raw_rows = probe_rows
            else:
                endpoint = ISSUE_BASE_ENDPOINTS.get(market)
                if endpoint is None:
                    return UniverseResult(
                        error=f"KRX Open API has no issue-base endpoint for market {market.value}"
                    )
                group, name = endpoint
                try:
                    raw_rows = self._client.fetch_rows(
                        group, name, {"basDd": reference_date.strftime("%Y%m%d")}
                    )
                except Exception as exc:
                    logger.exception("KRX Open API universe fetch failed: %s", exc)
                    return UniverseResult(error=str(exc))

            if not raw_rows:
                # `sync_universe` infers delistings by diffing snapshots, so an
                # empty one is not a small problem: it reads as every listing
                # disappearing at once.
                return UniverseResult(
                    error=(
                        f"KRX Open API returned no rows for {market.value} on "
                        f"{reference_date.isoformat()}; refusing to record an empty universe"
                    )
                )

            missing = tuple(
                column for column in REQUIRED_ISSUE_BASE_COLUMNS if column not in raw_rows[0]
            )
            if missing:
                return UniverseResult(
                    error=(
                        "KRX Open API issue-base response is missing columns: "
                        f"{', '.join(missing)}"
                    )
                )

            self._log_composition(market, raw_rows)

            for raw in raw_rows:
                ticker = str(raw.get(_SHORT_TICKER, "")).strip()
                if not ticker:
                    continue
                records.append(
                    Stock(
                        ticker=ticker,
                        market=market,
                        name=str(raw.get(_ABBREVIATED_NAME, "")).strip() or ticker,
                        status=ListingStatus.ACTIVE,
                        last_seen_date=reference_date,
                        source=Source.KRX_OPENAPI,
                        listing_date=parse_yyyymmdd(raw.get(_LISTING_DATE)),
                    )
                )

        snapshot = StockUniverseSnapshot(
            snapshot_id=str(uuid.uuid4()),
            as_of_date=reference_date,
            source=Source.KRX_OPENAPI,
            fetched_at=fetched_at,
            records=records,
        )
        return UniverseResult(snapshot=snapshot)

    def _resolve_reference_date(
        self,
        probe_market: Market,
        as_of: date | None,
    ) -> tuple[date, list[dict[str, object]]]:
        """Pick the date to snapshot, and return the rows already fetched for it.

        A named *as_of* is honoured with a single request, even if it comes back
        empty — the caller asked about that day, and answering about a different
        one would be a lie. Only the default walks back, and only over the empty
        responses that mean "weekend, holiday, or not published yet".

        Raises:
            _NoPublishedData: nothing found within the lookback window.
        """
        endpoint = ISSUE_BASE_ENDPOINTS.get(probe_market)
        if endpoint is None:
            raise _NoPublishedData(
                f"KRX Open API has no issue-base endpoint for market {probe_market.value}"
            )
        group, name = endpoint

        if as_of is not None:
            rows = self._client.fetch_rows(group, name, {"basDd": as_of.strftime("%Y%m%d")})
            return as_of, rows

        start = today_kst()
        for offset in range(self._max_lookback_days + 1):
            candidate = start - timedelta(days=offset)
            rows = self._client.fetch_rows(group, name, {"basDd": candidate.strftime("%Y%m%d")})
            if rows:
                if offset:
                    logger.info(
                        "KRX Open API has no %s issue base info for %s yet; "
                        "using %s, the most recent published day.",
                        probe_market.value,
                        start.isoformat(),
                        candidate.isoformat(),
                    )
                return candidate, rows

        raise _NoPublishedData(
            f"KRX Open API published no {probe_market.value} issue base info in the "
            f"{self._max_lookback_days + 1} days up to {start.isoformat()}"
        )

    @staticmethod
    def _log_composition(market: Market, raw_rows: list[dict[str, object]]) -> None:
        """Log what the universe is made of, without acting on it (N3-3)."""
        groups: dict[str, int] = {}
        classes: dict[str, int] = {}
        for raw in raw_rows:
            group = str(raw.get(_SECURITY_GROUP, "")).strip() or "(blank)"
            share_class = str(raw.get(_SHARE_CLASS, "")).strip() or "(blank)"
            groups[group] = groups.get(group, 0) + 1
            classes[share_class] = classes.get(share_class, 0) + 1
        logger.info(
            "%s universe composition: %d rows, security groups=%s, share classes=%s",
            market.value,
            len(raw_rows),
            dict(sorted(groups.items(), key=lambda item: -item[1])),
            dict(sorted(classes.items(), key=lambda item: -item[1])),
        )

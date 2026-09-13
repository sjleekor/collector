"""KIS Developers security-flow provider (per-ticker, date-range shaped)."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from collector.kr.adapters.flows_kis.parsers import (
    DAILY_SHORT_SALE_PAGE_ROWS,
    DAILY_SHORT_SALE_PATH,
    DAILY_SHORT_SALE_TR_ID,
    INQUIRE_PRICE_PATH,
    INQUIRE_PRICE_TR_ID,
    INVESTOR_DAILY_PAGE_ROWS,
    INVESTOR_DAILY_PATH,
    INVESTOR_DAILY_TR_ID,
    TRADE_DATE_FIELD,
    parse_foreign_holding_row,
    parse_investor_net_volume_rows,
    parse_short_selling_rows,
    parse_trade_date,
)
from collector.kr.adapters.kis_common.client import KisClient
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import FlowRequestStats, SecurityFlowFetchResult, SecurityFlowLine
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError

logger = logging.getLogger(__name__)

# Only the metric codes the existing schema already carries.  KIS also breaks
# 기관 down into 증권·투신·은행·보험·사모·기금·기타법인 (101 output2 fields), and
# that is a real opportunity — but it is an *expansion*, and mixing it into the
# replacement would make any regression impossible to attribute.
KIS_FLOW_GROUPS: tuple[str, ...] = ("foreign_holding", "investor", "shorting")

# KIS gives short-selling *trades*; the balance is a KRX-only series.
KIS_UNSUPPORTED_METRIC_CODES: list[str] = ["short_selling_balance_quantity"]

# A page returning fewer rows than its size means we reached the far end of the
# ticker's history; walking further would spend requests on nothing.
MAX_PAGES_PER_REQUEST = 200


class KisFlowProvider:
    """Fetch security-flow metrics from KIS domestic-stock quotation APIs."""

    def __init__(self, *, client: KisClient) -> None:
        self._client = client

    def source(self) -> Source:
        return Source.KIS

    def supported_flow_groups(self) -> tuple[str, ...]:
        return KIS_FLOW_GROUPS

    def unsupported_metric_codes(self) -> list[str]:
        return list(KIS_UNSUPPORTED_METRIC_CODES)

    def request_stats(self) -> FlowRequestStats:
        stats = self._client.stats
        return FlowRequestStats(
            http_requests=stats.http_requests,
            http_retries=stats.http_retries,
            pages_fetched=stats.pages_fetched,
            rate_limited_responses=stats.rate_limited_responses,
            auth_token_issued=stats.token_issued,
            auth_token_cache_hits=stats.token_cache_hits,
            throttle_waits=stats.throttle_waits,
            throttle_wait_seconds=stats.throttle_wait_seconds,
            status_counts=dict(stats.status_counts),
        )

    def fetch_foreign_holding(
        self,
        ticker: str,
        market: Market,
        trade_date: date,
    ) -> SecurityFlowFetchResult:
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": str(ticker).zfill(6),
        }
        try:
            response = self._client.get(
                INQUIRE_PRICE_PATH,
                tr_id=INQUIRE_PRICE_TR_ID,
                params=params,
            )
            if response.no_data:
                return SecurityFlowFetchResult(no_data=True)
            rows = response.rows("output")
            if not rows:
                return SecurityFlowFetchResult(no_data=True)
            records = parse_foreign_holding_row(
                rows[0],
                ticker=ticker,
                market=market,
                trade_date=trade_date,
                request=params,
            )
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except (SourceAuthError, SourceQuotaExhaustedError):
            raise
        except Exception as exc:
            logger.warning("KIS foreign holding fetch failed for %s: %s", ticker, exc)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Walk ``investor-trade-by-stock-daily`` backwards from ``end``.

        The endpoint takes only an end date and answers with the 30 sessions at
        or before it, so paging means moving the end date back one day past the
        oldest row we just received.
        """
        return self._walk_backwards(
            ticker=ticker,
            market=market,
            start=start,
            end=end,
            path=INVESTOR_DAILY_PATH,
            tr_id=INVESTOR_DAILY_TR_ID,
            page_rows=INVESTOR_DAILY_PAGE_ROWS,
            build_params=lambda page_end: {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": str(ticker).zfill(6),
                "FID_INPUT_DATE_1": page_end.strftime("%Y%m%d"),
                "FID_ORG_ADJ_PRC": "",
                "FID_ETC_CLS_CODE": "",
            },
            parse_rows=parse_investor_net_volume_rows,
            label="investor",
        )

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Walk ``daily-short-sale`` backwards from ``end`` in 100-row pages."""
        return self._walk_backwards(
            ticker=ticker,
            market=market,
            start=start,
            end=end,
            path=DAILY_SHORT_SALE_PATH,
            tr_id=DAILY_SHORT_SALE_TR_ID,
            page_rows=DAILY_SHORT_SALE_PAGE_ROWS,
            build_params=lambda page_end: {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": str(ticker).zfill(6),
                "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": page_end.strftime("%Y%m%d"),
            },
            parse_rows=parse_short_selling_rows,
            label="shorting",
        )

    def _walk_backwards(
        self,
        *,
        ticker: str,
        market: Market,
        start: date,
        end: date,
        path: str,
        tr_id: str,
        page_rows: int,
        build_params,
        parse_rows,
        label: str,
    ) -> SecurityFlowFetchResult:
        if start > end:
            return SecurityFlowFetchResult(no_data=True)

        records: list[SecurityFlowLine] = []
        rows_seen = 0
        page_end = end
        try:
            for page_index in range(MAX_PAGES_PER_REQUEST):
                params = build_params(page_end)
                response = self._client.get(path, tr_id=tr_id, params=params)
                if response.no_data:
                    break
                rows = response.rows("output2")
                if not rows:
                    break
                rows_seen += len(rows)

                records.extend(
                    parse_rows(
                        rows,
                        ticker=ticker,
                        market=market,
                        start=start,
                        end=end,
                        request=params,
                    )
                )

                oldest = _oldest_row_date(rows)
                if oldest is None or oldest <= start or len(rows) < page_rows:
                    break
                page_end = oldest - timedelta(days=1)
                if page_end < start:
                    break
                if page_index == MAX_PAGES_PER_REQUEST - 1:
                    logger.warning(
                        "KIS %s paging for %s hit the %d-page cap at %s; "
                        "the remaining window will be picked up on the next run",
                        label,
                        ticker,
                        MAX_PAGES_PER_REQUEST,
                        page_end.isoformat(),
                    )
        except (SourceAuthError, SourceQuotaExhaustedError):
            raise
        except Exception as exc:
            logger.warning("KIS %s fetch failed for %s: %s", label, ticker, exc)
            return SecurityFlowFetchResult(records=records, error=str(exc))

        # "Upstream has nothing for this ticker" and "upstream has nothing for
        # this ticker in THIS window" are different answers, and only the first
        # may be tombstoned.
        #
        # The no-data tombstone key is ``group:ticker`` with no date in it, so a
        # single no-data verdict skips that ticker for the whole TTL (7 days by
        # default). This endpoint returns the ticker's most recent traded
        # sessions, so a ticker that did not trade during the requested window
        # answers with thirty good rows, all of them older than it.
        #
        # Measured on prod 2026-08-18: 000880, 001570, 001470, 000300 and
        # 900270 all came back ending 08-14, and daily_ohlcv shows why — volume
        # zero with the price frozen since 08-10, i.e. trading halts. Reading
        # that as no-data would suspend those tickers for a week, and a halted
        # ticker is precisely one whose resumption must not be missed.
        #
        # Rows seen but none in the window therefore returns neither records nor
        # no_data: nothing is written, nothing is tombstoned, and the next run
        # asks again.
        return SecurityFlowFetchResult(records=records, no_data=not records and rows_seen == 0)


def _oldest_row_date(rows: list[dict[str, object]]) -> date | None:
    dates = [
        parsed
        for parsed in (parse_trade_date(row.get(TRADE_DATE_FIELD)) for row in rows)
        if parsed is not None
    ]
    return min(dates) if dates else None

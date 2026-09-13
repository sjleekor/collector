"""Direct KRX MDC security-flow provider."""

from __future__ import annotations

import logging
from datetime import date

from collector.kr.adapters.flows_common import UNSUPPORTED_FLOW_METRIC_CODES
from collector.kr.adapters.flows_krx.codes import KrxStockCodeResolver
from collector.kr.adapters.flows_krx.parsers import (
    FOREIGN_HOLDING_BLD,
    INVESTOR_BLD,
    INVESTOR_BULK_BLD,
    SHORTING_BALANCE_BLD,
    SHORTING_BALANCE_BULK_BLD,
    SHORTING_STATUS_BLD,
    SHORTING_TRADING_BULK_BLD,
    parse_foreign_holding_rows,
    parse_investor_net_volume_bulk_rows,
    parse_investor_net_volume_rows,
    parse_shorting_balance_bulk_rows,
    parse_shorting_rows,
    parse_shorting_trading_bulk_rows,
)
from collector.kr.adapters.krx_common.client import KrxMdcClient, KrxMdcResponseError
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import SecurityFlowFetchResult
from collector.kr.util.pipeline import HumanThrottle

logger = logging.getLogger(__name__)

MARKET_TO_KRX_ID = {
    Market.KOSPI: "STK",
    Market.KOSDAQ: "KSQ",
}
MARKET_TO_KRX_SHORTING_BALANCE_ID = {
    Market.KOSPI: "1",
    Market.KOSDAQ: "2",
}
SHORTING_STOCK_SECURITY_GROUP = "STMFRTSCIFDRFS"
INSTITUTION_INVESTOR_CODE = "7050"
INDIVIDUAL_INVESTOR_CODE = "8000"
FOREIGN_INVESTOR_CODE = "9000"
OTHER_FOREIGN_INVESTOR_CODE = "9001"


class KrxDirectFlowProvider:
    """Fetch security-flow metrics directly from KRX MDC JSON endpoints."""

    def __init__(
        self,
        *,
        client: KrxMdcClient | None = None,
        resolver: KrxStockCodeResolver | None = None,
        timeout_seconds: float = 20.0,
        login_id: str = "",
        login_pw: str = "",
        human_throttle: HumanThrottle | None = None,
    ) -> None:
        self._client = client or KrxMdcClient(
            timeout_seconds=timeout_seconds,
            login_id=login_id,
            login_pw=login_pw,
            human_throttle=human_throttle,
        )
        self._resolver = resolver or KrxStockCodeResolver(self._client)

    def source(self) -> Source:
        return Source.KRX

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        try:
            isin = self._resolver.resolve_isin(ticker, market)
            rows = self._client.post_rows(
                INVESTOR_BLD,
                {
                    "strtDd": start.strftime("%Y%m%d"),
                    "endDd": end.strftime("%Y%m%d"),
                    "isuCd": isin,
                    "inqTpCd": "2",
                    "trdVolVal": "1",
                    "askBid": "3",
                },
                output_key="output",
            )
            records = parse_investor_net_volume_rows(rows, ticker, market)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception("Failed to fetch KRX investor net volume for %s", ticker)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_investor_net_volume_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        try:
            mkt_id = MARKET_TO_KRX_ID.get(market)
            if mkt_id is None:
                raise KrxMdcResponseError(
                    f"Unsupported market for KRX MDC investor bulk: {market.value}"
                )
            common_params = {
                "strtDd": trade_date.strftime("%Y%m%d"),
                "endDd": trade_date.strftime("%Y%m%d"),
                "mktId": mkt_id,
            }
            institution_rows = self._fetch_investor_bulk_rows(
                common_params,
                INSTITUTION_INVESTOR_CODE,
            )
            individual_rows = self._fetch_investor_bulk_rows(
                common_params,
                INDIVIDUAL_INVESTOR_CODE,
            )
            foreign_rows = self._fetch_investor_bulk_rows(
                common_params,
                FOREIGN_INVESTOR_CODE,
            )
            other_foreign_rows = self._fetch_investor_bulk_rows(
                common_params,
                OTHER_FOREIGN_INVESTOR_CODE,
            )
            records = parse_investor_net_volume_bulk_rows(
                individual_rows=individual_rows,
                institution_rows=institution_rows,
                foreign_rows=foreign_rows,
                other_foreign_rows=other_foreign_rows,
                market=market,
                trade_date=trade_date,
                tickers=tickers,
            )
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception(
                "Failed to fetch KRX investor net volume bulk for %s:%s",
                trade_date.isoformat(),
                market.value,
            )
            return SecurityFlowFetchResult(error=str(exc))

    def _fetch_investor_bulk_rows(
        self,
        common_params: dict[str, str],
        investor_code: str,
    ):
        return self._client.post_rows(
            INVESTOR_BULK_BLD,
            {
                **common_params,
                "invstTpCd": investor_code,
            },
            output_key="output",
        )

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        try:
            isin = self._resolver.resolve_isin(ticker, market)
            common_params = {
                "strtDd": start.strftime("%Y%m%d"),
                "endDd": end.strftime("%Y%m%d"),
                "isuCd": isin,
            }
            status_rows = self._client.post_rows(
                SHORTING_STATUS_BLD,
                common_params,
                output_key="OutBlock_1",
            )
            balance_rows = self._client.post_rows(
                SHORTING_BALANCE_BLD,
                common_params,
                output_key="OutBlock_1",
            )
            records = parse_shorting_rows(status_rows, balance_rows, ticker, market)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception("Failed to fetch KRX shorting metrics for %s", ticker)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_shorting_trading_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        try:
            mkt_id = MARKET_TO_KRX_ID.get(market)
            if mkt_id is None:
                raise KrxMdcResponseError(
                    f"Unsupported market for KRX MDC shorting trading: {market.value}"
                )
            rows = self._client.post_rows(
                SHORTING_TRADING_BULK_BLD,
                {
                    "trdDd": trade_date.strftime("%Y%m%d"),
                    "mktId": mkt_id,
                    "inqCond": SHORTING_STOCK_SECURITY_GROUP,
                },
                output_key="OutBlock_1",
            )
            records = parse_shorting_trading_bulk_rows(rows, market, trade_date, tickers)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception(
                "Failed to fetch KRX shorting trading bulk for %s:%s",
                trade_date.isoformat(),
                market.value,
            )
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_shorting_balance_bulk(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        try:
            market_type = MARKET_TO_KRX_SHORTING_BALANCE_ID.get(market)
            if market_type is None:
                raise KrxMdcResponseError(
                    f"Unsupported market for KRX MDC shorting balance: {market.value}"
                )
            rows = self._client.post_rows(
                SHORTING_BALANCE_BULK_BLD,
                {
                    "trdDd": trade_date.strftime("%Y%m%d"),
                    "mktTpCd": market_type,
                },
                output_key="OutBlock_1",
            )
            records = parse_shorting_balance_bulk_rows(rows, market, trade_date, tickers)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception(
                "Failed to fetch KRX shorting balance bulk for %s:%s",
                trade_date.isoformat(),
                market.value,
            )
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        try:
            mkt_id = MARKET_TO_KRX_ID.get(market)
            if mkt_id is None:
                raise KrxMdcResponseError(
                    f"Unsupported market for KRX MDC foreign holding: {market.value}"
                )
            rows = self._client.post_rows(
                FOREIGN_HOLDING_BLD,
                {
                    "searchType": "1",
                    "mktId": mkt_id,
                    "trdDd": trade_date.strftime("%Y%m%d"),
                    "isuLmtRto": "0",
                },
                output_key="output",
            )
            records = parse_foreign_holding_rows(rows, market, trade_date, tickers)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception("Failed to fetch KRX foreign holdings for %s", market.value)
            return SecurityFlowFetchResult(error=str(exc))

    def unsupported_metric_codes(self) -> list[str]:
        return list(UNSUPPORTED_FLOW_METRIC_CODES)

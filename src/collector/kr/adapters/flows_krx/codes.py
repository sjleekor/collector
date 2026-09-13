"""KRX ticker to ISIN resolver."""

from __future__ import annotations

from dataclasses import dataclass

from collector.kr.adapters.krx_common.client import KrxMdcClient
from collector.kr.domain.enums import Market

FINDER_BLD = "dbms/comm/finder/finder_stkisu"


class KrxStockCodeNotFoundError(LookupError):
    """Raised when the KRX finder payload has no matching ticker."""


@dataclass(frozen=True, slots=True)
class KrxStockCode:
    """A listed KRX stock code row from finder_stkisu."""

    ticker: str
    name: str
    isin: str
    market: Market
    market_code: str


class KrxStockCodeResolver:
    """Resolve six-digit KRX tickers to ISIN codes using finder_stkisu."""

    def __init__(self, client: KrxMdcClient) -> None:
        self._client = client
        self._cache: dict[tuple[str, Market], KrxStockCode] | None = None

    def resolve(self, ticker: str, market: Market | None = None) -> KrxStockCode:
        normalized_ticker = str(ticker).strip().zfill(6)
        codes = self._load_codes()

        if market is not None:
            match = codes.get((normalized_ticker, market))
            if match is None:
                raise KrxStockCodeNotFoundError(
                    f"KRX finder has no ISIN for ticker={normalized_ticker}, "
                    f"market={market.value}"
                )
            return match

        matches = [
            code for (code_ticker, _), code in codes.items() if code_ticker == normalized_ticker
        ]
        if not matches:
            raise KrxStockCodeNotFoundError(
                f"KRX finder has no ISIN for ticker={normalized_ticker}"
            )
        return matches[0]

    def resolve_isin(self, ticker: str, market: Market | None = None) -> str:
        """Return the ISIN for a ticker."""
        return self.resolve(ticker, market).isin

    def clear_cache(self) -> None:
        """Drop the process-local finder cache."""
        self._cache = None

    def _load_codes(self) -> dict[tuple[str, Market], KrxStockCode]:
        if self._cache is not None:
            return self._cache

        rows = self._client.post_rows(
            FINDER_BLD,
            {
                "locale": "ko_KR",
                "mktsel": "ALL",
                "searchText": "",
                "typeNo": "0",
            },
            output_key="block1",
        )
        cache: dict[tuple[str, Market], KrxStockCode] = {}
        for item in rows:
            row = item.row
            ticker = str(row.get("short_code", "")).strip().zfill(6)
            isin = str(row.get("full_code", "")).strip()
            market = _parse_market(row)
            if not ticker or not isin or market is None:
                continue
            code = KrxStockCode(
                ticker=ticker,
                name=str(row.get("codeName", "")).strip(),
                isin=isin,
                market=market,
                market_code=str(row.get("marketCode", "")).strip(),
            )
            cache[(ticker, market)] = code

        self._cache = cache
        return cache


def _parse_market(row: dict[str, object]) -> Market | None:
    market_code = str(row.get("marketCode", "")).strip().upper()
    market_eng_name = str(row.get("marketEngName", "")).strip().upper()
    market_name = str(row.get("marketName", "")).strip()
    if market_code == "STK" or market_eng_name.startswith("KOSPI") or market_name == "유가증권":
        return Market.KOSPI
    if market_code == "KSQ" or market_eng_name.startswith("KOSDAQ") or market_name == "코스닥":
        return Market.KOSDAQ
    return None

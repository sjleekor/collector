"""KRX Open API (``data-dbg.krx.co.kr``) market-data adapters.

The official replacement for the pykrx/MDC scraping path that KRX restricted
on 2026-08-16.  One request returns a whole market-day, so N1 (market cap),
N3 (point-in-time universe) and the unadjusted OHLC K-7 needs all come from
the same call.

Response spec verified live on 2026-08-18 — see
``docs/dev/20260731_raw_features/02_data_expansion_plan/poc/krx_open_api.md``
§4.1c.
"""

from collector.kr.adapters.market_data_krx_openapi.client import (
    KrxOpenApiClient,
    KrxOpenApiCounters,
)
from collector.kr.adapters.market_data_krx_openapi.provider import (
    KrxOpenApiHistoricalUniverseProvider,
    KrxOpenApiMarketCapProvider,
)

__all__ = [
    "KrxOpenApiClient",
    "KrxOpenApiCounters",
    "KrxOpenApiHistoricalUniverseProvider",
    "KrxOpenApiMarketCapProvider",
]

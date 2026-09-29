"""KRX Open API index (업종·규모·대표지수) daily-level adapter."""

from collector.kr.adapters.index_krx_openapi.provider import (
    INDEX_ENDPOINTS,
    KRX_INDEX_HISTORY_START,
    KrxIndexDailyRow,
    KrxOpenApiIndexProvider,
    parse_index_rows,
)

__all__ = [
    "INDEX_ENDPOINTS",
    "KRX_INDEX_HISTORY_START",
    "KrxIndexDailyRow",
    "KrxOpenApiIndexProvider",
    "parse_index_rows",
]

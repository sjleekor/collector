"""SEIBro ETF 분배금지급현황 어댑터 (R-4 기준선 #2)."""

from collector.kr.adapters.seibro_distribution.client import (
    SeibroClient,
    SeibroMalformedResponseError,
    SeibroRawResponse,
    SeibroRequestError,
)
from collector.kr.adapters.seibro_distribution.fetch import ChunkResult, WindowResult, fetch_window
from collector.kr.adapters.seibro_distribution.parser import (
    KEY_COLUMNS,
    PARSED_COLUMNS,
    SOURCE_COLUMNS,
    DistributionRow,
    parse_count,
    parse_rows,
)
from collector.kr.adapters.seibro_distribution.settings import seibro_enabled
from collector.kr.adapters.seibro_distribution.window import (
    EARLIEST_RGT_STD_DT,
    is_quarterly_full_day,
    weekly_window,
    year_chunks,
)

__all__ = [
    "ChunkResult",
    "DistributionRow",
    "EARLIEST_RGT_STD_DT",
    "KEY_COLUMNS",
    "PARSED_COLUMNS",
    "SOURCE_COLUMNS",
    "SeibroClient",
    "SeibroMalformedResponseError",
    "SeibroRawResponse",
    "SeibroRequestError",
    "WindowResult",
    "fetch_window",
    "is_quarterly_full_day",
    "parse_count",
    "parse_rows",
    "seibro_enabled",
    "weekly_window",
    "year_chunks",
]

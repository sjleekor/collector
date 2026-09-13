"""Bank of Korea ECOS common feature provider adapter."""

from collector.kr.adapters.common_features_ecos.client import (
    ECOS_STATISTIC_SEARCH_BASE_URL,
    EcosStatisticSearchClient,
    EcosStatisticSearchResult,
)
from collector.kr.adapters.common_features_ecos.provider import EcosCommonFeatureProvider

__all__ = [
    "ECOS_STATISTIC_SEARCH_BASE_URL",
    "EcosCommonFeatureProvider",
    "EcosStatisticSearchClient",
    "EcosStatisticSearchResult",
]

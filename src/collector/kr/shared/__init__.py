"""modeler/가 쓰는 유일한 공개 표면.

여기 없는 것은 modeler/에서 import하지 않는다. 넓히려면 그 판단을 문서에 남긴다.
이 모듈은 순수 데이터와 순수 함수만 내보낸다 — DB도 HTTP도 설정도 건드리지 않는다.

**S3에서 추가된 13개 (2026-09-13).** modeler로 가는 마트 골든 패리티 테스트
(``test_common_build_mart.py`` 등)가 쓰는 픽스처 헬퍼(``_common_fixtures.py``,
``_metric_fixtures.py``)가 순수 데이터클래스·enum을 직접 import하고 있었다.
전부 프로덕션 로직이 없는 dataclass/StrEnum이라 "순수 데이터" 기준에는 맞지만,
research(프로덕션) 코드가 실제로 쓰는 13개보다 훨씬 넓다 — DART 원천 raw 타입
다섯 개(``Dart*Line``)까지 포함한다. 테스트 전용 필요라는 점을 남겨둔다.
"""

from collector.kr.definitions.common_features import (
    default_common_feature_catalog,
    default_common_feature_series,
)
from collector.kr.definitions.industry_groups import (
    MIN_GROUP_SIZE,
    OTHER_GROUP,
    UNKNOWN_GROUP,
    resolve_groups,
)
from collector.kr.definitions.metric_rules import (
    default_metric_catalog,
    default_metric_mapping_rules,
)
from collector.kr.domain.enums import Market, Source
from collector.kr.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
    CommonFeatureObservation,
    CommonFeatureSeries,
    DartCorp,
    DartFinancialStatementLine,
    DartShareCountLine,
    DartShareholderReturnLine,
    DartXbrlFactLine,
    IngestionRun,
    MetricCatalogEntry,
    MetricMappingRule,
    StockMetricFact,
    UpsertResult,
)
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.util.time import now_kst

__all__ = [
    "MIN_GROUP_SIZE",
    "OTHER_GROUP",
    "UNKNOWN_GROUP",
    "CommonFeatureCatalogEntry",
    "CommonFeatureDailyFact",
    "CommonFeatureObservation",
    "CommonFeatureSeries",
    "DartCorp",
    "DartFinancialStatementLine",
    "DartShareCountLine",
    "DartShareholderReturnLine",
    "DartXbrlFactLine",
    "IngestionRun",
    "Market",
    "MetricCatalogEntry",
    "MetricMappingRule",
    "Source",
    "StockMetricFact",
    "UpsertResult",
    "default_common_feature_catalog",
    "default_common_feature_series",
    "default_metric_catalog",
    "default_metric_mapping_rules",
    "get_trading_days",
    "now_kst",
    "resolve_groups",
]

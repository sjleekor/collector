"""Seed the Phase 1 common feature source series into storage.

The pure catalog/series *definitions* live in
``collector.kr.definitions.common_features`` so the DuckDB compute marts can
import them without the service layer (refactor §3.0). Decision 7: only
``common_feature_series`` is still a Postgres table the collector seeds — the
model-facing feature *catalog* is now code-only (read directly by the marts), so
``common seed`` no longer upserts a ``common_feature_catalog`` table.
"""

from __future__ import annotations

from collector.kr.definitions.common_features import (
    default_common_feature_catalog,
    default_common_feature_series,
)
from collector.kr.domain.models import CommonFeatureCatalogSeedResult
from collector.kr.ports.storage import Storage

__all__ = [
    "default_common_feature_catalog",
    "default_common_feature_series",
    "seed_common_feature_catalog",
]


def seed_common_feature_catalog(storage: Storage) -> CommonFeatureCatalogSeedResult:
    """Seed Phase 1 common feature source series (decision 7: series table only)."""
    result = CommonFeatureCatalogSeedResult()
    result.series_upsert = storage.upsert_common_feature_series(default_common_feature_series())
    return result

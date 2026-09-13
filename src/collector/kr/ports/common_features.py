"""Port: common market / macro feature provider interface."""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import Source
from collector.kr.domain.models import CommonFeatureFetchResult, CommonFeatureSeries


@runtime_checkable
class CommonFeatureProvider(Protocol):
    """Fetch raw observations for configured common feature source series."""

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        ...

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        """Fetch raw observations for one catalog series and date range."""
        ...

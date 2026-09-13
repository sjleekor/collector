"""Federal Reserve Economic Data provider for common macro features."""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from collector.kr.adapters.common_features_fred.client import FredSeriesObservationsClient
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureFetchResult,
    CommonFeatureObservation,
    CommonFeatureSeries,
)
from collector.kr.infra.config.settings import get_settings
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


class FredCommonFeatureProvider:
    """Fetch common feature observations via FRED."""

    def __init__(
        self,
        client: FredSeriesObservationsClient | None = None,
        *,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        if client is not None:
            self._client = client
            return

        settings = get_settings()
        self._client = FredSeriesObservationsClient(
            api_key=api_key if api_key is not None else settings.fred_api_key,
            timeout_seconds=(
                timeout_seconds if timeout_seconds is not None else settings.fred_timeout_seconds
            ),
        )

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        return Source.FRED

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        """Fetch one FRED series as raw common feature observations."""
        if series.source != Source.FRED:
            return CommonFeatureFetchResult(
                error=f"FredCommonFeatureProvider cannot fetch source {series.source.value}"
            )

        try:
            fred_series_id = _fred_series_id(series)
            result = self._client.fetch_series_observations(
                series_id=fred_series_id,
                observation_start=start.isoformat(),
                observation_end=end.isoformat(),
            )
            if result.error:
                return CommonFeatureFetchResult(
                    error=result.error,
                    retryable=result.retryable,
                    retry_after_seconds=result.retry_after_seconds,
                )
            if result.no_data:
                return CommonFeatureFetchResult(no_data=True)

            fetched_at = now_kst()
            records: list[CommonFeatureObservation] = []
            for row in result.rows:
                observation = _observation_from_row(
                    row=row,
                    series=series,
                    fred_series_id=fred_series_id,
                    start=start,
                    end=end,
                    fetched_at=fetched_at,
                )
                if observation is not None:
                    records.append(observation)

            if not records:
                return CommonFeatureFetchResult(no_data=True)
            return CommonFeatureFetchResult(records=records)
        except Exception as exc:
            logger.exception("Failed to fetch FRED common feature series %s", series.series_id)
            return CommonFeatureFetchResult(error=str(exc))


def _fred_series_id(series: CommonFeatureSeries) -> str:
    endpoint_series_id = series.endpoint_params.get("series_id")
    if endpoint_series_id:
        return str(endpoint_series_id).strip()
    if series.source_series_key:
        return series.source_series_key.strip()
    raise ValueError(f"Missing FRED series_id for common feature series {series.series_id}")


def _observation_from_row(
    *,
    row: dict[str, object],
    series: CommonFeatureSeries,
    fred_series_id: str,
    start: date,
    end: date,
    fetched_at: datetime,
) -> CommonFeatureObservation | None:
    date_value = str(row.get("date") or "").strip()
    if not date_value:
        return None
    observation_date = date.fromisoformat(date_value)
    if observation_date < start or observation_date > end:
        logger.debug(
            "Skipping FRED common feature row outside requested range: "
            "series=%s date=%s range=%s..%s",
            series.series_id,
            observation_date.isoformat(),
            start.isoformat(),
            end.isoformat(),
        )
        return None

    value_numeric = _to_decimal(row.get("value"))
    if value_numeric is None:
        logger.debug(
            "Skipping FRED common feature row with non-finite value: series=%s date=%s",
            series.series_id,
            observation_date.isoformat(),
        )
        return None

    return CommonFeatureObservation(
        source=Source.FRED,
        series_id=series.series_id,
        observation_date=observation_date,
        period_end_date=observation_date,
        release_date=None,
        available_from_date=None,
        vintage="",
        value_numeric=value_numeric,
        value_text="",
        unit=series.unit,
        frequency=series.frequency,
        source_updated_at=None,
        fetched_at=fetched_at,
        raw_payload={
            "series_id": fred_series_id,
            "source_series_key": series.source_series_key,
            "row": _normalize_row(row),
        },
    )


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None
    if not decimal_value.is_finite():
        return None
    return decimal_value


def _normalize_row(row: dict[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            normalized[str(key)] = value.isoformat()
        elif isinstance(value, date):
            normalized[str(key)] = value.isoformat()
        else:
            normalized[str(key)] = value
    return normalized

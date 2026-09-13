"""Bank of Korea ECOS provider for common macro features."""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from collector.kr.adapters.common_features_ecos.client import EcosStatisticSearchClient
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureFetchResult,
    CommonFeatureObservation,
    CommonFeatureSeries,
)
from collector.kr.infra.config.settings import get_settings
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


class EcosCommonFeatureProvider:
    """Fetch common feature observations via Bank of Korea ECOS."""

    def __init__(
        self,
        client: EcosStatisticSearchClient | None = None,
        *,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        if client is not None:
            self._client = client
            return

        settings = get_settings()
        self._client = EcosStatisticSearchClient(
            api_key=api_key if api_key is not None else settings.ecos_api_key,
            timeout_seconds=(
                timeout_seconds if timeout_seconds is not None else settings.ecos_timeout_seconds
            ),
        )

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        return Source.ECOS

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        """Fetch one ECOS series as raw common feature observations."""
        if series.source != Source.ECOS:
            return CommonFeatureFetchResult(
                error=f"EcosCommonFeatureProvider cannot fetch source {series.source.value}"
            )

        try:
            request = _request_params(series=series, start=start, end=end)
            result = self._client.fetch_statistic_search(**request)
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
                    cycle=request["cycle"],
                    fetched_at=fetched_at,
                )
                if observation is not None:
                    records.append(observation)

            if not records:
                return CommonFeatureFetchResult(no_data=True)
            return CommonFeatureFetchResult(records=records)
        except Exception as exc:
            logger.exception("Failed to fetch ECOS common feature series %s", series.series_id)
            return CommonFeatureFetchResult(error=str(exc))


def _request_params(
    *,
    series: CommonFeatureSeries,
    start: date,
    end: date,
) -> dict[str, object]:
    endpoint_params = series.endpoint_params
    stat_code = str(endpoint_params.get("stat_code") or series.source_series_key).strip()
    if not stat_code:
        raise ValueError(f"Missing ECOS stat_code for common feature series {series.series_id}")

    cycle = str(endpoint_params.get("cycle") or series.frequency).strip().upper()
    item_codes = endpoint_params.get("item_codes")
    if item_codes is None:
        item_codes = [
            endpoint_params.get("item_code1", ""),
            endpoint_params.get("item_code2", ""),
            endpoint_params.get("item_code3", ""),
            endpoint_params.get("item_code4", ""),
        ]
    if isinstance(item_codes, str):
        normalized_item_codes = [item_codes]
    else:
        normalized_item_codes = [str(item_code) for item_code in item_codes if item_code]

    return {
        "stat_code": stat_code,
        "cycle": cycle,
        "start_period": _format_period(start, cycle),
        "end_period": _format_period(end, cycle),
        "item_codes": normalized_item_codes,
    }


def _observation_from_row(
    *,
    row: dict[str, object],
    series: CommonFeatureSeries,
    cycle: str,
    fetched_at: datetime,
) -> CommonFeatureObservation | None:
    time_value = str(row.get("TIME") or "").strip()
    if not time_value:
        return None
    period_end = _parse_period_end(time_value, cycle)
    value_numeric = _to_decimal(row.get("DATA_VALUE"))
    if value_numeric is None:
        logger.debug(
            "Skipping ECOS common feature row with non-finite DATA_VALUE: series=%s time=%s",
            series.series_id,
            time_value,
        )
        return None

    return CommonFeatureObservation(
        source=Source.ECOS,
        series_id=series.series_id,
        observation_date=period_end,
        period_end_date=period_end,
        release_date=None,
        available_from_date=None,
        vintage="",
        value_numeric=value_numeric,
        value_text="",
        unit=str(row.get("UNIT_NAME") or series.unit),
        frequency=series.frequency,
        source_updated_at=None,
        fetched_at=fetched_at,
        raw_payload={"row": _normalize_row(row)},
    )


def _format_period(value: date, cycle: str) -> str:
    if cycle == "D":
        return value.strftime("%Y%m%d")
    if cycle == "M":
        return value.strftime("%Y%m")
    if cycle == "Q":
        quarter = ((value.month - 1) // 3) + 1
        return f"{value.year}Q{quarter}"
    if cycle == "A":
        return f"{value.year}"
    return value.strftime("%Y%m%d")


def _parse_period_end(value: str, cycle: str) -> date:
    if cycle == "D":
        return date.fromisoformat(f"{value[0:4]}-{value[4:6]}-{value[6:8]}")
    if cycle == "M":
        year = int(value[0:4])
        month = int(value[4:6])
        return date(year, month, monthrange(year, month)[1])
    if cycle == "Q":
        normalized = value.upper().replace("-", "")
        year = int(normalized[0:4])
        quarter = int(normalized[-1])
        month = quarter * 3
        return date(year, month, monthrange(year, month)[1])
    if cycle == "A":
        return date(int(value[0:4]), 12, 31)
    return date.fromisoformat(f"{value[0:4]}-{value[4:6]}-{value[6:8]}")


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

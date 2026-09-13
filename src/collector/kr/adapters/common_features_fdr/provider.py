"""FinanceDataReader provider for common global, FX, and commodity features."""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import FinanceDataReader as fdr

from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureFetchResult,
    CommonFeatureObservation,
    CommonFeatureSeries,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

FDR_CLOSE_COLUMNS = ("Close", "close", "종가")


class FdrCommonFeatureProvider:
    """Fetch common feature observations via FinanceDataReader."""

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        return Source.FDR

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        """Fetch one FinanceDataReader series as raw common feature observations."""
        if series.source != Source.FDR:
            return CommonFeatureFetchResult(
                error=f"FdrCommonFeatureProvider cannot fetch source {series.source.value}"
            )

        try:
            symbol = _symbol(series)
            logger.debug(
                "Fetching FDR common feature series=%s symbol=%s range=%s..%s",
                series.series_id,
                symbol,
                start.isoformat(),
                end.isoformat(),
            )
            query_end = end + timedelta(days=1)
            df = fdr.DataReader(symbol, start.isoformat(), query_end.isoformat())

            if df is None or df.empty:
                return CommonFeatureFetchResult(no_data=True)

            close_column = _close_column(df.columns)
            fetched_at = now_kst()
            records: list[CommonFeatureObservation] = []

            for observation_date_raw, row in df.iterrows():
                observation_date = _to_date(observation_date_raw)
                if observation_date < start or observation_date > end:
                    logger.debug(
                        "Skipping FDR common feature row outside requested range: "
                        "series=%s date=%s range=%s..%s",
                        series.series_id,
                        observation_date.isoformat(),
                        start.isoformat(),
                        end.isoformat(),
                    )
                    continue
                value_numeric = _to_decimal(row[close_column])
                if value_numeric is None:
                    logger.debug(
                        "Skipping FDR common feature row with non-finite close: "
                        "series=%s date=%s value=%r",
                        series.series_id,
                        observation_date.isoformat(),
                        row[close_column],
                    )
                    continue
                records.append(
                    CommonFeatureObservation(
                        source=Source.FDR,
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
                            "symbol": symbol,
                            "source_series_key": series.source_series_key,
                            "row": _raw_row(row),
                        },
                    )
                )

            if not records:
                return CommonFeatureFetchResult(no_data=True)

            return CommonFeatureFetchResult(records=records)
        except Exception as exc:
            logger.exception("Failed to fetch FDR common feature series %s", series.series_id)
            return CommonFeatureFetchResult(error=str(exc))


def _symbol(series: CommonFeatureSeries) -> str:
    endpoint_symbol = series.endpoint_params.get("symbol")
    if endpoint_symbol:
        return str(endpoint_symbol)
    if series.source_series_key:
        return series.source_series_key
    raise ValueError(f"Missing FDR symbol for common feature series {series.series_id}")


def _close_column(columns: Any) -> str:
    for column in FDR_CLOSE_COLUMNS:
        if column in columns:
            return column
    raise ValueError(f"FDR result missing close column: {list(columns)}")


def _to_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(_normalize_raw_value(value)))
    except (InvalidOperation, ValueError):
        return None
    if not decimal_value.is_finite():
        return None
    return decimal_value


def _raw_row(row: Any) -> dict[str, object]:
    return {str(key): _normalize_raw_value(value) for key, value in row.to_dict().items()}


def _normalize_raw_value(value: Any) -> object:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Decimal) and not value.is_finite():
        return None
    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        return None
    return value

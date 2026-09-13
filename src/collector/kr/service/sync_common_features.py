"""Use-case: Sync common market / macro feature raw observations."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import date, timedelta

from collector.kr.domain.availability import compute_available_from
from collector.kr.domain.enums import RunStatus, RunType, Source
from collector.kr.domain.models import (
    CommonFeatureObservation,
    CommonFeatureSeries,
    CommonFeatureSyncResult,
    IngestionRun,
)
from collector.kr.infra.calendar.trading_days import get_trading_days, load_holidays
from collector.kr.ports.common_features import CommonFeatureProvider
from collector.kr.ports.storage import Storage
from collector.kr.util.pipeline import (
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    sleep_with_jitter,
)
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

KrxTradingDayProvider = Callable[[date, date], Sequence[date]]

_STRICT_DAILY_COVERAGE_SOURCES = {Source.KRX, Source.PYKRX}
_RELAXED_DAILY_COVERAGE_MIN_NUMERATOR = 9
_RELAXED_DAILY_COVERAGE_MIN_DENOMINATOR = 10
_MARKET_BREADTH_KIND = "market_breadth"
_MARKET_BREADTH_PROGRESS_INTERVAL = 50


def sync_common_features(
    providers: Sequence[CommonFeatureProvider],
    storage: Storage,
    start: date | None,
    end: date,
    sources: list[Source] | None = None,
    series_ids: list[str] | None = None,
    active_only: bool = True,
    force: bool = False,
    rate_limit_seconds: float = 0.0,
    krx_trading_days: KrxTradingDayProvider | None = None,
    incremental: bool = False,
    lookback_days: int = 0,
    max_auto_range_days: int | None = None,
    allow_large_range: bool = False,
) -> CommonFeatureSyncResult:
    """Synchronise configured common feature source series into raw storage."""
    provider_map = {provider.source(): provider for provider in providers}
    source_values = [source.value for source in sources] if sources else None
    provider_source_values = sorted(source.value for source in provider_map)
    run = IngestionRun(
        run_type=RunType.COMMON_FEATURE_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "start": start.isoformat() if start else None,
            "end": end.isoformat(),
            "sources": source_values,
            "series_ids": series_ids,
            "active_only": active_only,
            "force": force,
            "rate_limit_seconds": rate_limit_seconds,
            "provider_sources": provider_source_values,
            "incremental": incremental,
            "lookback_days": lookback_days,
            "max_auto_range_days": max_auto_range_days,
            "allow_large_range": allow_large_range,
        },
    )
    storage.record_run(run)

    result = CommonFeatureSyncResult()
    calendar = krx_trading_days or _default_krx_trading_days()

    try:
        if incremental and start is not None:
            raise ValueError("common sync --incremental cannot be combined with start.")
        if not incremental and start is None:
            raise ValueError("common sync requires start unless incremental=True.")
        if lookback_days < 0:
            raise ValueError("lookback_days must be >= 0")
        if max_auto_range_days is not None and max_auto_range_days <= 0:
            raise ValueError("max_auto_range_days must be positive")

        catalog_rows = storage.get_common_feature_series(
            sources=sources,
            series_ids=series_ids,
            active_only=active_only,
        )
        target_series = _filter_series(catalog_rows, sources=sources, series_ids=series_ids)
        result.series_processed = len(target_series)
        latest_by_series = (
            storage.get_common_feature_observation_max_dates(
                sources=sources,
                series_ids=[series.series_id for series in target_series],
            )
            if incremental
            else {}
        )
        run.params["latest_by_series"] = {
            series_id: latest.isoformat() for series_id, latest in sorted(latest_by_series.items())
        }

        logger.info(
            "Common feature sync started: range=%s..%s series=%d sources=%s force=%s",
            start.isoformat() if start else "<incremental>",
            end.isoformat(),
            len(target_series),
            ",".join(source_values or provider_source_values),
            force,
        )

        for series in target_series:
            provider = provider_map.get(series.source)
            if provider is None:
                result.requests_skipped += 1
                result.errors[series.series_id] = (
                    f"No common feature provider configured for source {series.source.value}"
                )
                continue

            if incremental:
                latest = latest_by_series.get(series.series_id)
                if latest is None:
                    result.requests_skipped += 1
                    result.errors[series.series_id] = (
                        "No stored common_feature_observation_raw baseline for "
                        "incremental sync. Run explicit common sync backfill first."
                    )
                    continue
                effective_start = latest + timedelta(days=1)
                if lookback_days > 0:
                    effective_start = min(effective_start, end - timedelta(days=lookback_days))
            else:
                effective_start = start

            effective_start = _effective_start(effective_start, series)
            if effective_start > end:
                result.requests_skipped += 1
                continue

            auto_range_days = (end - effective_start).days + 1
            if (
                incremental
                and max_auto_range_days is not None
                and auto_range_days > max_auto_range_days
                and not allow_large_range
            ):
                result.requests_skipped += 1
                result.errors[series.series_id] = (
                    f"Resolved incremental range is too large "
                    f"({auto_range_days} days > {max_auto_range_days})."
                )
                continue

            if _is_krx_market_breadth_series(series):
                _sync_market_breadth_series_by_day(
                    provider=provider,
                    storage=storage,
                    series=series,
                    start=effective_start,
                    end=end,
                    force=force,
                    rate_limit_seconds=rate_limit_seconds,
                    krx_trading_days=calendar,
                    result=result,
                )
                continue

            skip_existing_coverage = not incremental or lookback_days == 0
            if (
                skip_existing_coverage
                and not force
                and _has_existing_coverage(
                    storage=storage,
                    series=series,
                    start=effective_start,
                    end=end,
                    krx_trading_days=calendar,
                )
            ):
                result.requests_skipped += 1
                logger.info(
                    "Skipping common feature series with existing coverage: "
                    "source=%s series=%s range=%s..%s",
                    series.source.value,
                    series.series_id,
                    effective_start.isoformat(),
                    end.isoformat(),
                )
                continue

            result.requests_attempted += 1
            request_key = f"{series.source.value}:{series.series_id}"
            logger.info(
                "Common feature series fetch starting: source=%s series=%s "
                "range=%s..%s days=%d incremental=%s lookback_days=%d",
                series.source.value,
                series.series_id,
                effective_start.isoformat(),
                end.isoformat(),
                auto_range_days,
                incremental,
                lookback_days,
            )
            fetch_result = call_with_retry(
                lambda: provider.fetch_series(series=series, start=effective_start, end=end),
                request_label=request_key,
                logger_instance=logger,
                should_retry_result=lambda item: item.retryable,
            )

            if fetch_result.error:
                logger.warning(
                    "Common feature sync failed for %s: %s",
                    request_key,
                    fetch_result.error,
                )
                result.errors[series.series_id] = fetch_result.error
            elif fetch_result.no_data:
                result.no_data_requests += 1
                logger.info(
                    "Common feature series returned no data: source=%s series=%s " "range=%s..%s",
                    series.source.value,
                    series.series_id,
                    effective_start.isoformat(),
                    end.isoformat(),
                )
            elif fetch_result.records:
                logger.info(
                    "Common feature series fetched: source=%s series=%s " "range=%s..%s records=%d",
                    series.source.value,
                    series.series_id,
                    effective_start.isoformat(),
                    end.isoformat(),
                    len(fetch_result.records),
                )
                observations = [
                    _with_service_availability(
                        series=series,
                        observation=observation,
                        krx_trading_days=calendar,
                    )
                    for observation in fetch_result.records
                ]
                upsert = storage.upsert_common_feature_observations(observations)
                result.upsert.updated += upsert.updated
                result.upsert.errors += upsert.errors
                result.rows_upserted += upsert.updated
                logger.info(
                    "Common feature series upserted: source=%s series=%s rows=%d errors=%d",
                    series.source.value,
                    series.series_id,
                    upsert.updated,
                    upsert.errors,
                )

            sleep_with_jitter(rate_limit_seconds, jitter_ratio=0.2)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                series_processed=result.series_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
            ),
            errors=result.errors,
            partial_subject="common feature series",
        )
        return result
    except Exception as exc:
        logger.exception("Common feature sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def _filter_series(
    rows: list[CommonFeatureSeries],
    *,
    sources: list[Source] | None,
    series_ids: list[str] | None,
) -> list[CommonFeatureSeries]:
    source_filter = set(sources or [])
    series_filter = set(series_ids or [])
    return [
        row
        for row in rows
        if (not source_filter or row.source in source_filter)
        and (not series_filter or row.series_id in series_filter)
    ]


def _default_krx_trading_days() -> KrxTradingDayProvider:
    holidays = load_holidays()

    def calendar(start: date, end: date) -> Sequence[date]:
        return get_trading_days(start, end, holidays=holidays)

    return calendar


def _is_krx_market_breadth_series(series: CommonFeatureSeries) -> bool:
    return (
        series.source == Source.KRX
        and str(series.endpoint_params.get("kind") or "").strip() == _MARKET_BREADTH_KIND
    )


def _sync_market_breadth_series_by_day(
    *,
    provider: CommonFeatureProvider,
    storage: Storage,
    series: CommonFeatureSeries,
    start: date,
    end: date,
    force: bool,
    rate_limit_seconds: float,
    krx_trading_days: KrxTradingDayProvider,
    result: CommonFeatureSyncResult,
) -> None:
    trading_days = list(krx_trading_days(start, end))
    if not trading_days:
        result.requests_skipped += 1
        logger.info(
            "Skipping KRX market breadth series with no trading days: series=%s range=%s..%s",
            series.series_id,
            start.isoformat(),
            end.isoformat(),
        )
        return

    existing_dates = (
        set()
        if force
        else storage.get_common_feature_observation_dates(
            source=series.source,
            series_id=series.series_id,
            start=start,
            end=end,
        )
    )
    pending_days = [trade_date for trade_date in trading_days if trade_date not in existing_dates]
    result.requests_skipped += len(existing_dates.intersection(trading_days))

    logger.info(
        "KRX market breadth daily sync starting: series=%s range=%s..%s "
        "trading_days=%d existing=%d pending=%d force=%s",
        series.series_id,
        start.isoformat(),
        end.isoformat(),
        len(trading_days),
        len(existing_dates.intersection(trading_days)),
        len(pending_days),
        force,
    )

    if not pending_days:
        logger.info(
            "Skipping KRX market breadth series with complete stored coverage: "
            "series=%s range=%s..%s",
            series.series_id,
            start.isoformat(),
            end.isoformat(),
        )
        return

    started_at = time.monotonic()
    rows_before = result.rows_upserted
    errors_before = len(result.errors)
    no_data_before = result.no_data_requests

    for index, trade_date in enumerate(pending_days, start=1):
        result.requests_attempted += 1
        request_key = f"{series.source.value}:{series.series_id}:{trade_date.isoformat()}"
        fetch_result = call_with_retry(
            lambda trade_date=trade_date: provider.fetch_series(
                series=series,
                start=trade_date,
                end=trade_date,
            ),
            request_label=request_key,
            logger_instance=logger,
            should_retry_result=lambda item: item.retryable,
        )

        if fetch_result.error:
            logger.warning(
                "KRX market breadth daily sync failed for %s: %s",
                request_key,
                fetch_result.error,
            )
            result.errors[request_key] = fetch_result.error
        elif fetch_result.no_data:
            result.no_data_requests += 1
        elif fetch_result.records:
            observations = [
                _with_service_availability(
                    series=series,
                    observation=observation,
                    krx_trading_days=krx_trading_days,
                )
                for observation in fetch_result.records
            ]
            upsert = storage.upsert_common_feature_observations(observations)
            result.upsert.updated += upsert.updated
            result.upsert.errors += upsert.errors
            result.rows_upserted += upsert.updated
            logger.info(
                "KRX market breadth daily row upserted: series=%s date=%s rows=%d errors=%d",
                series.series_id,
                trade_date.isoformat(),
                upsert.updated,
                upsert.errors,
            )

        if index % _MARKET_BREADTH_PROGRESS_INTERVAL == 0 or index == len(pending_days):
            logger.info(
                "KRX market breadth daily progress: series=%s processed=%d/%d "
                "last_trade_date=%s rows_upserted=%d no_data=%d errors=%d elapsed=%.1fs",
                series.series_id,
                index,
                len(pending_days),
                trade_date.isoformat(),
                result.rows_upserted - rows_before,
                result.no_data_requests - no_data_before,
                len(result.errors) - errors_before,
                time.monotonic() - started_at,
            )

        sleep_with_jitter(rate_limit_seconds, jitter_ratio=0.2)


def _effective_start(start: date, series: CommonFeatureSeries) -> date:
    if series.history_start_date and series.history_start_date > start:
        return series.history_start_date
    return start


def _has_existing_coverage(
    *,
    storage: Storage,
    series: CommonFeatureSeries,
    start: date,
    end: date,
    krx_trading_days: KrxTradingDayProvider,
) -> bool:
    expected_count = _expected_daily_observation_count(
        series=series,
        start=start,
        end=end,
        krx_trading_days=krx_trading_days,
    )
    if expected_count <= 0:
        return False

    counts = storage.count_common_feature_observations(
        series_ids=[series.series_id],
        start=start,
        end=end,
        source=series.source,
    )
    observed_count = counts.get(series.series_id, 0)
    if observed_count >= expected_count:
        return True

    return _has_relaxed_daily_coverage(
        storage=storage,
        series=series,
        start=start,
        end=end,
        observed_count=observed_count,
        expected_count=expected_count,
        krx_trading_days=krx_trading_days,
    )


def _has_relaxed_daily_coverage(
    *,
    storage: Storage,
    series: CommonFeatureSeries,
    start: date,
    end: date,
    observed_count: int,
    expected_count: int,
    krx_trading_days: KrxTradingDayProvider,
) -> bool:
    if series.frequency != "D" or series.source in _STRICT_DAILY_COVERAGE_SOURCES:
        return False
    if observed_count <= 0:
        return False
    if (
        observed_count * _RELAXED_DAILY_COVERAGE_MIN_DENOMINATOR
        < expected_count * _RELAXED_DAILY_COVERAGE_MIN_NUMERATOR
    ):
        return False

    observations = storage.get_common_feature_observations(
        series_ids=[series.series_id],
        start=start,
        end=end,
        source=series.source,
    )
    if not observations:
        return False

    first_observation_date = min(observation.observation_date for observation in observations)
    last_observation_date = max(observation.observation_date for observation in observations)
    max_gap = max(series.max_stale_business_days, 0)
    return (
        _krx_business_day_gap(start, first_observation_date, krx_trading_days) <= max_gap
        and _krx_business_day_gap(last_observation_date, end, krx_trading_days) <= max_gap
    )


def _krx_business_day_gap(
    earlier: date,
    later: date,
    krx_trading_days: KrxTradingDayProvider,
) -> int:
    if earlier >= later:
        return 0
    return sum(1 for day in krx_trading_days(earlier, later) if earlier < day <= later)


def _expected_daily_observation_count(
    *,
    series: CommonFeatureSeries,
    start: date,
    end: date,
    krx_trading_days: KrxTradingDayProvider,
) -> int:
    if series.frequency != "D":
        return 0
    if series.source in {Source.KRX, Source.PYKRX} or series.source_timezone == "Asia/Seoul":
        return len(krx_trading_days(start, end))
    current = start
    count = 0
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current = date.fromordinal(current.toordinal() + 1)
    return count


def _with_service_availability(
    *,
    series: CommonFeatureSeries,
    observation: CommonFeatureObservation,
    krx_trading_days: KrxTradingDayProvider,
) -> CommonFeatureObservation:
    available_from = compute_available_from(
        policy=series.availability_policy,
        observation_date=observation.observation_date,
        period_end_date=observation.period_end_date,
        release_date=observation.release_date,
        source_timezone=series.source_timezone,
        manual_lag_days=series.manual_lag_days,
        krx_trading_days=krx_trading_days,
    )
    return replace(
        observation,
        source=series.source,
        series_id=series.series_id,
        available_from_date=available_from,
        unit=observation.unit or series.unit,
        frequency=observation.frequency or series.frequency,
    )

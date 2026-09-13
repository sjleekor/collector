"""KRX direct provider for common domestic market index features."""

from __future__ import annotations

import logging
import math
import time
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from collector.kr.adapters.krx_common.client import KrxMdcClient, KrxMdcRow
from collector.kr.domain.enums import Source
from collector.kr.domain.models import (
    CommonFeatureFetchResult,
    CommonFeatureObservation,
    CommonFeatureSeries,
)
from collector.kr.infra.calendar.trading_days import get_trading_days
from collector.kr.infra.config.settings import get_settings
from collector.kr.util.pipeline import HumanThrottle, HumanThrottlePolicy
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

KRX_INDEX_OHLCV_BLD = "dbms/MDC/STAT/standard/MDCSTAT00301"
KRX_INDEX_OHLCV_OUTPUT_KEY = "output"
KRX_INDEX_CLOSE_COLUMNS = ("CLSPRC_IDX", "close", "Close")
KRX_MARKET_BREADTH_BLD = "dbms/MDC/STAT/standard/MDCSTAT01501"
KRX_MARKET_BREADTH_OUTPUT_KEY = "OutBlock_1"
MARKET_BREADTH_KIND = "market_breadth"
MARKET_BREADTH_PROGRESS_INTERVAL = 50


class KrxCommonFeatureProvider:
    """Fetch common domestic market index observations via KRX MDC."""

    def __init__(
        self,
        client: KrxMdcClient | None = None,
        *,
        timeout_seconds: float | None = None,
        login_id: str | None = None,
        login_pw: str | None = None,
        human_throttle: HumanThrottle | None = None,
    ) -> None:
        self._market_breadth_rows_cache: dict[tuple[str, str, str, date], list[KrxMdcRow]] = {}
        if client is not None:
            self._client = client
            return

        settings = get_settings()
        self._client = KrxMdcClient(
            timeout_seconds=(
                timeout_seconds if timeout_seconds is not None else settings.krx_mdc_timeout_seconds
            ),
            login_id=login_id if login_id is not None else settings.krx_id,
            login_pw=login_pw if login_pw is not None else settings.krx_pw,
            human_throttle=human_throttle or _default_human_throttle(),
        )

    def source(self) -> Source:
        """Return the provenance source this provider writes."""
        return Source.KRX

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        """Fetch one KRX index series as raw common feature observations."""
        if series.source != Source.KRX:
            return CommonFeatureFetchResult(
                error=f"KrxCommonFeatureProvider cannot fetch source {series.source.value}"
            )

        try:
            if _kind(series) == MARKET_BREADTH_KIND:
                return self._fetch_market_breadth(series=series, start=start, end=end)

            request = _index_request_params(series, start=start, end=end)
            bld = _bld(series)
            output_key = _output_key(series)
            rows = self._client.post_rows(bld, request.params, output_key=output_key)
            if not rows:
                return CommonFeatureFetchResult(no_data=True)

            fetched_at = now_kst()
            records: list[CommonFeatureObservation] = []
            for row in rows:
                observation = _observation_from_row(
                    row=row,
                    series=series,
                    request=request,
                    bld=bld,
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
            logger.exception("Failed to fetch KRX common feature series %s", series.series_id)
            return CommonFeatureFetchResult(error=str(exc))

    def _fetch_market_breadth(
        self,
        *,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        market_id = _market_id(series)
        metric = _metric(series)
        bld = _bld(series)
        output_key = _output_key(series)
        fetched_at = now_kst()
        records: list[CommonFeatureObservation] = []
        trading_days = list(get_trading_days(start, end))
        total_days = len(trading_days)
        started_at = time.monotonic()

        log_progress = total_days > 1
        if log_progress:
            logger.info(
                "KRX market breadth fetch started: series=%s market=%s metric=%s "
                "range=%s..%s trading_days=%d",
                series.series_id,
                market_id,
                metric,
                start.isoformat(),
                end.isoformat(),
                total_days,
            )

        for index, trade_date in enumerate(trading_days, start=1):
            rows = self._fetch_market_breadth_rows(
                bld=bld,
                output_key=output_key,
                market_id=market_id,
                trade_date=trade_date,
            )
            if not rows:
                continue
            observation = _market_breadth_observation_from_rows(
                rows=rows,
                series=series,
                bld=bld,
                output_key=output_key,
                market_id=market_id,
                metric=metric,
                trade_date=trade_date,
                fetched_at=fetched_at,
            )
            if observation is not None:
                records.append(observation)
            if log_progress and (
                index % MARKET_BREADTH_PROGRESS_INTERVAL == 0 or index == total_days
            ):
                logger.info(
                    "KRX market breadth progress: series=%s market=%s metric=%s "
                    "processed=%d/%d last_trade_date=%s records=%d elapsed=%.1fs",
                    series.series_id,
                    market_id,
                    metric,
                    index,
                    total_days,
                    trade_date.isoformat(),
                    len(records),
                    time.monotonic() - started_at,
                )

        if not records:
            return CommonFeatureFetchResult(no_data=True)
        if log_progress:
            logger.info(
                "KRX market breadth fetch completed: series=%s market=%s metric=%s "
                "records=%d elapsed=%.1fs",
                series.series_id,
                market_id,
                metric,
                len(records),
                time.monotonic() - started_at,
            )
        return CommonFeatureFetchResult(records=records)

    def _fetch_market_breadth_rows(
        self,
        *,
        bld: str,
        output_key: str,
        market_id: str,
        trade_date: date,
    ) -> list[KrxMdcRow]:
        cache_key = (bld, output_key, market_id, trade_date)
        if cache_key not in self._market_breadth_rows_cache:
            self._market_breadth_rows_cache[cache_key] = self._client.post_rows(
                bld,
                {"mktId": market_id, "trdDd": trade_date.strftime("%Y%m%d")},
                output_key=output_key,
            )
        return self._market_breadth_rows_cache[cache_key]


class _IndexRequest:
    def __init__(self, *, index_code: str, index_group: str, index_ticker: str) -> None:
        self.index_code = index_code
        self.index_group = index_group
        self.index_ticker = index_ticker
        self.params: dict[str, object] = {}


def _default_human_throttle() -> HumanThrottle:
    settings = get_settings()
    return HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=settings.krx_min_delay_seconds,
            max_delay_seconds=settings.krx_max_delay_seconds,
            long_rest_every=settings.krx_long_rest_every,
            long_rest_min_seconds=settings.krx_long_rest_min_seconds,
            long_rest_max_seconds=settings.krx_long_rest_max_seconds,
            auth_cooldown_seconds=settings.krx_auth_cooldown_seconds,
            error_backoff_min_seconds=settings.krx_error_backoff_min_seconds,
            error_backoff_max_seconds=settings.krx_error_backoff_max_seconds,
        ),
        logger_instance=logger,
    )


def _bld(series: CommonFeatureSeries) -> str:
    value = series.endpoint_params.get("bld")
    if value:
        return str(value).strip()
    if _kind(series) == MARKET_BREADTH_KIND:
        return KRX_MARKET_BREADTH_BLD
    return KRX_INDEX_OHLCV_BLD


def _output_key(series: CommonFeatureSeries) -> str:
    value = series.endpoint_params.get("output_key")
    if value:
        return str(value).strip()
    if _kind(series) == MARKET_BREADTH_KIND:
        return KRX_MARKET_BREADTH_OUTPUT_KEY
    return KRX_INDEX_OHLCV_OUTPUT_KEY


def _kind(series: CommonFeatureSeries) -> str:
    return str(series.endpoint_params.get("kind") or "").strip()


def _market_id(series: CommonFeatureSeries) -> str:
    market_id = str(series.endpoint_params.get("mktId") or series.market or "").strip()
    if market_id in {"KOSPI", "STK"}:
        return "STK"
    if market_id in {"KOSDAQ", "KSQ"}:
        return "KSQ"
    if market_id:
        return market_id
    raise ValueError(f"Missing KRX market id for common feature series {series.series_id}")


def _metric(series: CommonFeatureSeries) -> str:
    metric = str(series.endpoint_params.get("metric") or series.default_transform or "").strip()
    if metric:
        return metric
    raise ValueError(f"Missing KRX market breadth metric for series {series.series_id}")


def _index_request_params(
    series: CommonFeatureSeries,
    *,
    start: date,
    end: date,
) -> _IndexRequest:
    index_code = _index_code(series)
    index_group = str(
        series.endpoint_params.get("indIdx")
        or series.endpoint_params.get("index_group")
        or index_code[0]
    ).strip()
    index_ticker = str(
        series.endpoint_params.get("indIdx2")
        or series.endpoint_params.get("index_ticker")
        or index_code[1:]
    ).strip()
    if not index_group or not index_ticker:
        raise ValueError(
            f"Missing KRX index group/ticker for common feature series {series.series_id}"
        )

    request = _IndexRequest(
        index_code=index_code,
        index_group=index_group,
        index_ticker=index_ticker,
    )
    request.params = {
        "indIdx": index_group,
        "indIdx2": index_ticker,
        "strtDd": start.strftime("%Y%m%d"),
        "endDd": end.strftime("%Y%m%d"),
    }
    return request


def _index_code(series: CommonFeatureSeries) -> str:
    endpoint_code = (
        series.endpoint_params.get("index_code")
        or series.endpoint_params.get("krx_index_code")
        or series.endpoint_params.get("ticker")
    )
    if endpoint_code:
        return str(endpoint_code).strip()
    if series.source_series_key:
        return series.source_series_key.strip()
    raise ValueError(f"Missing KRX index_code for common feature series {series.series_id}")


def _observation_from_row(
    *,
    row: KrxMdcRow,
    series: CommonFeatureSeries,
    request: _IndexRequest,
    bld: str,
    start: date,
    end: date,
    fetched_at: datetime,
) -> CommonFeatureObservation | None:
    observation_date = _date_from_row(row.row)
    if observation_date is None:
        return None
    if observation_date < start or observation_date > end:
        logger.debug(
            "Skipping KRX common feature row outside requested range: "
            "series=%s date=%s range=%s..%s",
            series.series_id,
            observation_date.isoformat(),
            start.isoformat(),
            end.isoformat(),
        )
        return None

    value_numeric = _close_value(row.row)
    if value_numeric is None:
        logger.debug(
            "Skipping KRX common feature row with non-finite close: series=%s date=%s",
            series.series_id,
            observation_date.isoformat(),
        )
        return None

    return CommonFeatureObservation(
        source=Source.KRX,
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
            "bld": bld,
            "index_code": request.index_code,
            "indIdx": request.index_group,
            "indIdx2": request.index_ticker,
            "source_series_key": series.source_series_key,
            "request": _normalize_row(row.request),
            "row": _normalize_row(row.row),
        },
    )


def _market_breadth_observation_from_rows(
    *,
    rows: list[KrxMdcRow],
    series: CommonFeatureSeries,
    bld: str,
    output_key: str,
    market_id: str,
    metric: str,
    trade_date: date,
    fetched_at: datetime,
) -> CommonFeatureObservation | None:
    fluc_counts = Counter(str(row.row.get("FLUC_TP_CD") or "").strip() for row in rows)
    value_numeric = _market_breadth_value(metric=metric, rows=rows, fluc_counts=fluc_counts)
    if value_numeric is None:
        return None

    return CommonFeatureObservation(
        source=Source.KRX,
        series_id=series.series_id,
        observation_date=trade_date,
        period_end_date=trade_date,
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
            "bld": bld,
            "output_key": output_key,
            "kind": MARKET_BREADTH_KIND,
            "market_id": market_id,
            "metric": metric,
            "request": {"mktId": market_id, "trdDd": trade_date.strftime("%Y%m%d")},
            "row_count": len(rows),
            "fluc_counts": dict(sorted(fluc_counts.items())),
        },
    )


def _market_breadth_value(
    *,
    metric: str,
    rows: list[KrxMdcRow],
    fluc_counts: Counter[str],
) -> Decimal | None:
    if metric == "advancers":
        return Decimal(fluc_counts["1"] + fluc_counts["4"])
    if metric == "decliners":
        return Decimal(fluc_counts["2"] + fluc_counts["5"])
    if metric == "unchanged":
        return Decimal(fluc_counts["3"])
    if metric == "total_turnover_value":
        return _sum_decimal(rows, "ACC_TRDVAL")
    if metric == "total_volume":
        return _sum_decimal(rows, "ACC_TRDVOL")
    raise ValueError(f"Unsupported KRX market breadth metric: {metric}")


def _sum_decimal(rows: list[KrxMdcRow], column: str) -> Decimal:
    total = Decimal("0")
    for row in rows:
        value = _to_decimal(row.row.get(column))
        if value is not None:
            total += value
    return total


def _date_from_row(row: dict[str, object]) -> date | None:
    raw_value = row.get("TRD_DD") or row.get("date") or row.get("날짜")
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    compact = text.replace("-", "").replace("/", "").replace(".", "")
    if len(compact) == 8 and compact.isdigit():
        return date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _close_value(row: dict[str, object]) -> Decimal | None:
    for column in KRX_INDEX_CLOSE_COLUMNS:
        if column in row:
            return _to_decimal(row.get(column))
    raise ValueError(f"KRX index OHLCV result missing close column: {sorted(row)}")


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    text = str(value).replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        decimal_value = Decimal(text)
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
        elif hasattr(value, "item"):
            normalized[str(key)] = value.item()
        else:
            normalized[str(key)] = value
    return normalized

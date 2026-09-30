"""Bounded, status-only KIS open-session probe using the existing token cache.

Run inside the collector's configured environment. It emits no market values,
raw responses, credentials or token material. Four logical GETs are made.
"""

from __future__ import annotations

import json

from collector.kr.adapters.kis_common.client import KisClient
from collector.kr.adapters.kis_common.token import KisTokenCache, KisTokenProvider
from collector.kr.adapters.kis_intraday import KisIntradayMarketProvider, public_status_only
from collector.kr.infra.config.settings import get_settings
from collector.kr.util.rate_limit import TokenBucket


def probe() -> dict[str, object]:
    settings = get_settings()
    if not settings.kis_app_key or not settings.kis_app_secret:
        raise RuntimeError("KIS_APP_KEY and KIS_APP_SECRET are not configured")
    token_provider = KisTokenProvider(
        app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        cache=KisTokenCache(
            settings.kis_token_cache_path,
            refresh_margin_seconds=settings.kis_token_refresh_margin_seconds,
        ),
        timeout_seconds=settings.kis_timeout_seconds,
    )
    client = KisClient(
        token_provider=token_provider,
        app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        bucket=TokenBucket(settings.kis_requests_per_second,
                           burst=settings.kis_max_burst_requests),
        timeout_seconds=settings.kis_timeout_seconds,
    )
    snapshot = KisIntradayMarketProvider(client=client).fetch_snapshot()
    status = public_status_only(snapshot)
    status.update({
        "request_started_at": snapshot["request_started_at"],
        "elapsed_ms_including_retries_and_token_lookup": snapshot[
            "elapsed_ms_including_retries_and_token_lookup"
        ],
        "logical_request_count": snapshot["logical_request_count"],
        "client_stat_deltas": snapshot["client_stat_deltas"],
        "source_time_status_counts": {
            kind: sum(row["source_time_status"] == kind for row in snapshot["observations"])
            for kind in sorted({row["source_time_status"] for row in snapshot["observations"]})
        },
    })
    return status


def main() -> int:
    print(json.dumps(probe(), sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

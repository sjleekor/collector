#!/usr/bin/env bash
set -euo pipefail

# N1 — daily market cap / trading value / listed shares, one (date, market)
# slice per request.
#
# Pacing is NOT set here. It comes from the KRX_* settings, the same ones the
# MDC collectors use, because this reaches the same data.krx.co.kr portal they
# do. This wrapper used to pin --rate-limit-seconds 0.4 against the MDC path's
# 1.5-4.0s, and on 2026-08-16 KRX restricted this host's IP for "자동화 수단을
# 통한 비정상 대량 조회". Override per run with MARKET_CAP_MIN_DELAY_SECONDS /
# MARKET_CAP_MAX_DELAY_SECONDS, or for every KRX collector at once by setting
# KRX_MIN_DELAY_SECONDS and friends in .env.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  prices market-cap-backfill
  --market "${MARKET_CAP_MARKET:-all}"
  --max-consecutive-failures "${MARKET_CAP_MAX_CONSECUTIVE_FAILURES:-5}"
)

if [[ -n "${MARKET_CAP_START:-}" ]]; then
  args+=(--start "$MARKET_CAP_START")
fi

if [[ -n "${MARKET_CAP_END:-}" ]]; then
  args+=(--end "$MARKET_CAP_END")
fi

if [[ -n "${MARKET_CAP_MIN_DELAY_SECONDS:-}" ]]; then
  args+=(--min-delay-seconds "$MARKET_CAP_MIN_DELAY_SECONDS")
fi

if [[ -n "${MARKET_CAP_MAX_DELAY_SECONDS:-}" ]]; then
  args+=(--max-delay-seconds "$MARKET_CAP_MAX_DELAY_SECONDS")
fi

if [[ "${MARKET_CAP_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"

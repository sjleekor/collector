#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  prices backfill
  --market all
  --incremental
  --lookback-days "${PRICE_LOOKBACK_DAYS:-0}"
  --max-auto-range-days "${PRICE_MAX_AUTO_RANGE_DAYS:-10}"
)

if [[ -n "${PRICE_NEW_TICKER_START:-}" ]]; then
  args+=(--new-ticker-start "$PRICE_NEW_TICKER_START")
fi

if [[ "${PRICE_ALLOW_NEW_TICKER_BACKFILL:-0}" == "1" ]]; then
  args+=(--allow-new-ticker-backfill)
fi

if [[ "${PRICE_ALLOW_LARGE_RANGE:-0}" == "1" ]]; then
  args+=(--allow-large-range)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"

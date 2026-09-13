#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  ops freshness-report
  --fail-if-stale
  --max-lag-trading-days "${FRESHNESS_MAX_LAG_TRADING_DAYS:-1}"
  --max-market-cap-lag-trading-days "${FRESHNESS_MARKET_CAP_LAG_TRADING_DAYS:-2}"
  --max-lag-calendar-days "${FRESHNESS_MAX_LAG_CALENDAR_DAYS:-14}"
)

# Read-only DB report. It does not call an external source, so no source lock
# or throttle domain is needed.
sdc_run_collector "${args[@]}"

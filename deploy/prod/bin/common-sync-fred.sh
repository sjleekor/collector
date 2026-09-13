#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
LOOKBACK_DAYS="${SDC_COMMON_FRED_LOOKBACK_DAYS:-${SDC_COMMON_DAILY_LOOKBACK_DAYS:-45}}"
RATE_LIMIT_SECONDS="${SDC_COMMON_RATE_LIMIT_SECONDS:-0.2}"

args=(
  common sync
  --sources fred
  --end "$END_DATE"
  --rate-limit-seconds "$RATE_LIMIT_SECONDS"
  --incremental
  --lookback-days "$LOOKBACK_DAYS"
  --max-auto-range-days "${SDC_COMMON_DAILY_MAX_AUTO_RANGE_DAYS:-90}"
)

sdc_run_daily_collector fred "${args[@]}"

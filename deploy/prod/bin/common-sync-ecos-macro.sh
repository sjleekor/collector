#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
LOOKBACK_DAYS="${SDC_COMMON_MACRO_LOOKBACK_DAYS:-540}"
RATE_LIMIT_SECONDS="${SDC_COMMON_RATE_LIMIT_SECONDS:-0.2}"
MACRO_SERIES="${SDC_COMMON_MACRO_SERIES:-macro_cpi,macro_ppi,macro_m2,macro_consumer_sentiment}"

args=(
  common sync
  --sources ecos
  --series "$MACRO_SERIES"
  --end "$END_DATE"
  --rate-limit-seconds "$RATE_LIMIT_SECONDS"
  --incremental
  --lookback-days "$LOOKBACK_DAYS"
  --max-auto-range-days "${SDC_COMMON_MACRO_MAX_AUTO_RANGE_DAYS:-730}"
)

sdc_run_daily_collector ecos "${args[@]}"

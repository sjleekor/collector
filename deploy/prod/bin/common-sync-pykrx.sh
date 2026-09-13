#!/usr/bin/env bash
#
# Opt-in only, and now gated twice. common-features-refresh.sh runs this only
# when SDC_COMMON_ENABLE_PYKRX=1, and pykrx itself refuses to import unless
# ALLOW_KRX_SCRAPING=1 (K-5): the login it performs is the collection path KRX
# restricted this host for. This script has no replacement yet — the Open API
# index endpoints would be one, and that is not built.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
LOOKBACK_DAYS="${SDC_COMMON_PYKRX_LOOKBACK_DAYS:-${SDC_COMMON_DAILY_LOOKBACK_DAYS:-45}}"
RATE_LIMIT_SECONDS="${SDC_COMMON_RATE_LIMIT_SECONDS:-0.2}"

args=(
  common sync
  --sources pykrx
  --end "$END_DATE"
  --rate-limit-seconds "$RATE_LIMIT_SECONDS"
  --incremental
  --lookback-days "$LOOKBACK_DAYS"
  --max-auto-range-days "${SDC_COMMON_DAILY_MAX_AUTO_RANGE_DAYS:-90}"
)

sdc_run_daily_collector krx_marketdata "${args[@]}"

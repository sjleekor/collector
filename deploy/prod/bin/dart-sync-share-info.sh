#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  dart sync-share-info
  --incremental
  --lookback-years "${DART_LOOKBACK_YEARS:-1}"
  --max-attempt-targets "${DART_SHARE_INFO_MAX_ATTEMPT_TARGETS:-10000}"
  --negative-cache-ttl-days "${DART_NEGATIVE_CACHE_TTL_DAYS:-3}"
)

sdc_run_daily_collector opendart "${args[@]}"

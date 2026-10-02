#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  dart sync-xbrl
  --incremental
  --lookback-years "${DART_LOOKBACK_YEARS:-1}"
  --max-attempt-targets "${DART_XBRL_MAX_ATTEMPT_TARGETS:-10000}"
  --negative-cache-ttl-days "${DART_NEGATIVE_CACHE_TTL_DAYS:-3}"
  # Re-ask "no data" slots for N days after a periodic-report receipt confirms them
  # (0 turns it off). See service/dart_receipt_retry.py.
  --receipt-retry-window-days "${DART_RECEIPT_RETRY_WINDOW_DAYS:-7}"
  --receipt-retry-max-slots "${DART_RECEIPT_RETRY_MAX_SLOTS:-3000}"
)

sdc_run_daily_collector opendart "${args[@]}"

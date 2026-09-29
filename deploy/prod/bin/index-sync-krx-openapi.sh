#!/usr/bin/env bash
set -euo pipefail

# KRX index daily levels (업종·규모·대표지수) from the KRX Open API.
#
# Default: incremental. end = yesterday KST (same-day data is not published),
# start = end - KRX_INDEX_LOOKBACK_DAYS. Dates already stored are skipped.
# Set KRX_INDEX_START (and optionally KRX_INDEX_END) for range/backfill mode;
# use KRX_INDEX_MAX_CALLS to split a backfill across days (quota ~10,000/key/day).

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  index sync
  --source krx-openapi
  --groups "${KRX_INDEX_GROUPS:-kospi,kosdaq,krx}"
  --max-consecutive-failures "${KRX_INDEX_MAX_CONSECUTIVE_FAILURES:-5}"
)

if [[ -n "${KRX_INDEX_START:-}" ]]; then
  args+=(--start "$KRX_INDEX_START")
  if [[ -n "${KRX_INDEX_END:-}" ]]; then
    args+=(--end "$KRX_INDEX_END")
  fi
else
  args+=(--incremental --lookback-days "${KRX_INDEX_LOOKBACK_DAYS:-7}")
fi

if [[ -n "${KRX_INDEX_MAX_CALLS:-}" ]]; then
  args+=(--max-calls "$KRX_INDEX_MAX_CALLS")
fi

if [[ "${KRX_INDEX_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"

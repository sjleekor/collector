#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

if [[ -z "${FLOW_START:-}" ]]; then
  echo "FLOW_START is required (YYYY-MM-DD)." >&2
  exit 2
fi

if [[ -z "${FLOW_END:-}" ]]; then
  echo "FLOW_END is required (YYYY-MM-DD)." >&2
  exit 2
fi

args=(
  flows sync
  --start "$FLOW_START"
  --end "$FLOW_END"
)

if [[ -n "${FLOW_TICKERS:-}" ]]; then
  args+=(--tickers "$FLOW_TICKERS")
fi

if [[ -n "${FLOW_MAX_PRICE_RANGE_DAYS:-}" ]]; then
  args+=(--max-price-range-days "$FLOW_MAX_PRICE_RANGE_DAYS")
fi

if [[ "${FLOW_ALLOW_LARGE_RANGE:-0}" == "1" ]]; then
  args+=(--allow-large-range)
fi

if [[ -n "${FLOW_EXCLUDE_GROUPS:-}" ]]; then
  args+=(--exclude-groups "$FLOW_EXCLUDE_GROUPS")
fi

sdc_run_collector_with_lock krx_marketdata "${args[@]}"

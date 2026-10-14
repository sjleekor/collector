#!/usr/bin/env bash
set -euo pipefail

# One-off backfill of DART 거래소공시 (list.json pblntf_ty=I), 2008 onward, market-wide.
#
# ~75 calendar-quarter windows x 150~213 pages = about 12,000~16,000 requests. OpenDART
# allows about 20,000 requests a day per key and other DART jobs share it, so cap each
# run (DART_EXCHANGE_NOTICES_MAX_REQUESTS, default 8000) and rerun until the check
# passes. Finished windows are skipped and an interrupted window resumes at its next
# page. Exit 75 means every key hit its daily limit.
#
# Verify afterwards (DB only, no HTTP):
#   dart check-exchange-notices --from 2008-01-01      # exit 1 on a missing quarter

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  dart backfill-exchange-notices
  --from "${DART_EXCHANGE_NOTICES_FROM:-2008-01-01}"
  --rate-limit-seconds "${DART_EXCHANGE_NOTICES_RATE_LIMIT_SECONDS:-0.3}"
  --max-requests "${DART_EXCHANGE_NOTICES_MAX_REQUESTS:-8000}"
)

if [[ -n "${DART_EXCHANGE_NOTICES_TO:-}" ]]; then
  args+=(--to "$DART_EXCHANGE_NOTICES_TO")
fi

if [[ "${DART_EXCHANGE_NOTICES_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_collector_with_lock opendart "${args[@]}"

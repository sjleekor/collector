#!/usr/bin/env bash
set -euo pipefail

# N2 — OpenDART company profile (industry code, incorporation date, FY end).
#
# One call per corporation, skip-if-present on profile_fetched_at, so a repeat
# run costs nothing. ~3,959 calls under DART_PROFILE_UNIVERSE_SCOPE=historical
# against a 20,000/day per-key limit — a single run finishes it.
#
# Exit code 75 means every OpenDART key hit its daily limit; the next run
# resumes from profile_fetched_at rather than starting over.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  dart sync-corp-profile
  --universe-scope "${DART_PROFILE_UNIVERSE_SCOPE:-historical}"
)

if [[ -n "${DART_PROFILE_TICKERS:-}" ]]; then
  args+=(--tickers "$DART_PROFILE_TICKERS")
fi

if [[ -n "${DART_PROFILE_RATE_LIMIT_SECONDS:-}" ]]; then
  args+=(--rate-limit-seconds "$DART_PROFILE_RATE_LIMIT_SECONDS")
fi

if [[ "${DART_PROFILE_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector opendart "${args[@]}"

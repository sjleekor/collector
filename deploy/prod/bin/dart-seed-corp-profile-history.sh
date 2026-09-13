#!/usr/bin/env bash
set -euo pipefail

# F-1.3 — seed dart_corp_profile_history from dart_corp_master's current
# profiles. Run ONCE, before the first monthly snapshot.
#
# No API calls: this copies rows that dart sync-corp-profile already stored, so
# it takes seconds and needs no OpenDART lock. Re-running inserts nothing
# (ON CONFLICT DO NOTHING on (corp_code, observed_month)), so a retry is free.
#
# DART_PROFILE_HISTORY_MONTH overrides the month the seed rows are stamped with
# (YYYY-MM-DD; the day is forced to the 1st). Default: the current month.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(dart seed-corp-profile-history)

if [[ -n "${DART_PROFILE_HISTORY_MONTH:-}" ]]; then
  args+=(--observed-month "$DART_PROFILE_HISTORY_MONTH")
fi

sdc_run_collector "${args[@]}"

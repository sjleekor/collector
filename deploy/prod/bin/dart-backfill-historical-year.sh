#!/usr/bin/env bash
#
# S-1 remainder — one business year of financials / share-info / XBRL over the
# HISTORICAL universe.
#
# `bin/dart-backfill-all-years.sh` never passed --universe-scope, so it took the
# `current` default and collected the 2,657 corps listed today. 3,959 have ever
# had a ticker, and the ~1,300 that do not appear are exactly the delisted ones
# — the survivorship gap (poc/survivorship_gap.md). Coverage measured on prod
# 2026-08-18: financials and xbrl 2,608, share_count 2,653.
#
# One year per invocation so the caller can stop between years; the daily
# OpenDART chain runs 04:00-04:08 and a backfill must not hold its lock. The
# calling script owns the wall-clock guard.
#
# Skip-if-present per (corp, year, report), so a re-run after exit 75 (every key
# hit its daily limit) resumes instead of restarting.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

year="${DART_HISTORICAL_YEAR:?set DART_HISTORICAL_YEAR}"
reprt_codes="${DART_HISTORICAL_REPRT_CODES:-11011,11012,11013,11014}"
fs_divs="${DART_HISTORICAL_FS_DIVS:-CFS,OFS}"
scope="${DART_HISTORICAL_SCOPE:-historical}"
stages="${DART_HISTORICAL_STAGES:-financials share_info xbrl}"
rate_limit_seconds="${DART_HISTORICAL_RATE_LIMIT_SECONDS:-0.1}"

status=0
for stage in $stages; do
  case "$stage" in
    financials)
      args=(dart sync-financials --bsns-years "$year" --reprt-codes "$reprt_codes"
            --fs-divs "$fs_divs" --universe-scope "$scope"
            --rate-limit-seconds "$rate_limit_seconds")
      ;;
    share_info)
      args=(dart sync-share-info --bsns-years "$year" --reprt-codes "$reprt_codes"
            --universe-scope "$scope" --rate-limit-seconds "$rate_limit_seconds")
      ;;
    xbrl)
      args=(dart sync-xbrl --bsns-years "$year" --reprt-codes "$reprt_codes"
            --universe-scope "$scope" --rate-limit-seconds "$rate_limit_seconds")
      ;;
    *)
      echo "unknown stage: $stage" >&2
      exit 2
      ;;
  esac
  sdc_log "S-1 remainder: year=${year} stage=${stage} scope=${scope} rate_limit=${rate_limit_seconds}s"
  if ! sdc_run_daily_collector opendart "${args[@]}"; then
    rc=$?
    sdc_log "S-1 remainder: year=${year} stage=${stage} FAILED rc=${rc}"
    # 75 is "every OpenDART key hit its daily limit" — a clean stop, not a
    # failure to investigate. Propagate it so the caller can end the run.
    if [[ $rc -eq 75 ]]; then
      exit 75
    fi
    status=1
  fi
done

exit "$status"

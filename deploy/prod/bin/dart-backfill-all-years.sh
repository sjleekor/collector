#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

is_positive_year() {
  [[ "$1" =~ ^[0-9]{4}$ ]]
}

run_dart_sync() {
  if [[ "${SDC_DART_BACKFILL_EXCLUSIVE:-1}" == "1" ]]; then
    sdc_run_collector "$@"
  else
    sdc_run_collector_with_lock opendart "$@"
  fi
}

run_backfill() {
  local start_year end_year default_end_year reprt_codes fs_divs pull_image
  local collect_filings filings_rate current_year filings_end_year
  start_year="${SDC_DART_BACKFILL_START_YEAR:-2015}"
  default_end_year=$(( $(date +%Y) - 1 ))
  if [[ "${SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR:-0}" == "1" ]]; then
    default_end_year="$(date +%Y)"
  fi
  end_year="${SDC_DART_BACKFILL_END_YEAR:-$default_end_year}"

  if ! is_positive_year "$start_year"; then
    printf 'Invalid SDC_DART_BACKFILL_START_YEAR: %s\n' "$start_year" >&2
    exit 2
  fi
  if ! is_positive_year "$end_year"; then
    printf 'Invalid SDC_DART_BACKFILL_END_YEAR: %s\n' "$end_year" >&2
    exit 2
  fi
  if (( start_year > end_year )); then
    printf 'Invalid backfill year range: start_year=%s end_year=%s\n' "$start_year" "$end_year" >&2
    exit 2
  fi

  reprt_codes="${SDC_DART_BACKFILL_REPRT_CODES:-11011,11012,11013,11014}"
  fs_divs="${SDC_DART_BACKFILL_FS_DIVS:-CFS,OFS}"
  pull_image="${SDC_DART_BACKFILL_PULL_IMAGE:-1}"

  # dart_filing_receipt_raw is keyed by *calendar receipt* year, not business
  # year — a FY2025 annual report is filed in calendar 2026 — so the filing
  # stage runs its own range, up to the current calendar year, not to end_year.
  collect_filings="${SDC_DART_BACKFILL_FILINGS:-1}"
  filings_rate="${SDC_DART_BACKFILL_FILINGS_RATE_LIMIT:-0.5}"
  current_year="$(date +%Y)"
  filings_end_year="${SDC_DART_BACKFILL_FILINGS_END_YEAR:-$current_year}"

  if ! is_positive_year "$filings_end_year"; then
    printf 'Invalid SDC_DART_BACKFILL_FILINGS_END_YEAR: %s\n' "$filings_end_year" >&2
    exit 2
  fi

  sdc_log "OpenDART backfill starting"
  sdc_log "app_dir=$SDC_APP_DIR"
  sdc_log "range=${end_year}..${start_year} reprt_codes=$reprt_codes fs_divs=$fs_divs"
  sdc_log "filings=${collect_filings} filings_range=${filings_end_year}..${start_year} rate=${filings_rate}"
  sdc_log "exclusive=${SDC_DART_BACKFILL_EXCLUSIVE:-1}"
  sdc_log "mode=raw-only; derived metric marts are recomputed by bin/parquet-compute-all.sh"

  if [[ "$pull_image" == "1" ]]; then
    sdc_cd_app
    sdc_log "pull: $SDC_DOCKER_COMPOSE_CMD pull $SDC_COLLECTOR_SERVICE"
    sdc_compose pull "$SDC_COLLECTOR_SERVICE"
  fi

  sdc_log "Syncing OpenDART corp master"
  run_dart_sync dart sync-corp

  local year
  for year in $(seq "$end_year" -1 "$start_year"); do
    sdc_log "Backfilling OpenDART financials for ${year}"
    run_dart_sync dart sync-financials \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes" \
      --fs-divs "$fs_divs"

    sdc_log "Backfilling OpenDART share info for ${year}"
    run_dart_sync dart sync-share-info \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes"

    sdc_log "Backfilling OpenDART XBRL for ${year}"
    run_dart_sync dart sync-xbrl \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes"

  done

  # Filing receipts run after the three per-business-year stages: they feed the
  # Phase B SUE original-filing source only, so on a quota exit (75) the metrics
  # every other consumer needs are already in.
  if [[ "$collect_filings" == "1" ]]; then
    local filings_years=()
    for year in $(seq "$filings_end_year" -1 "$start_year"); do
      # A stored past year is skipped forever; the current year is re-fetched on
      # every run by design, so it goes last and ends up the freshest.
      if (( year != current_year )); then
        filings_years+=("$year")
      fi
    done
    if (( filings_end_year >= current_year && start_year <= current_year )); then
      filings_years+=("$current_year")
    fi

    for year in ${filings_years[@]+"${filings_years[@]}"}; do
      sdc_log "Backfilling OpenDART filing receipts for ${year}"
      run_dart_sync dart sync-filings \
        --years "$year" \
        --rate-limit-seconds "$filings_rate"
    done
  fi

  sdc_log "OpenDART backfill completed; run bin/parquet-compute-all.sh when derived marts are needed"
}

if [[ "${SDC_DART_BACKFILL_EXCLUSIVE:-1}" == "1" ]]; then
  sdc_with_source_lock opendart run_backfill
else
  run_backfill
fi

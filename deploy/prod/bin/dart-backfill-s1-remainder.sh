#!/usr/bin/env bash
#
# F-9.3 — S-1 remainder driver: the delisted-corp half of the OpenDART raw.
#
# `bin/dart-backfill-all-years.sh` never passed --universe-scope, so it took the
# `current` default and collected the corps listed today. 3,959 have ever had a
# ticker; the ones that never appear are the delisted names, which is the
# survivorship gap (poc/survivorship_gap.md). This drives
# `dart-backfill-historical-year.sh` (scope=historical) over the whole grid
# until it is closed.
#
# WHY A DRIVER AND NOT ONE LONG RUN
#
# A whole (year, stage) is ~31,700 requests / 4.4h for financials, which is too
# long to stop before the 04:00 chain — measured 2026-08-18, when a year-level
# guard could not hold. So the work unit here is one
# (year, stage, reprt_code, fs_div): ~4,000 requests / ~35 min, short enough
# that a guard checked BETWEEN units is meaningful.
#
# Three guards, in order:
#
#   1. Self lock (non-blocking). Two overlapping triggers must not both drive
#      the queue. The second exits 0 in a second rather than fighting for the
#      opendart lock.
#   2. Blackout 02:30-05:00. The daily chain runs at 04:00. A unit started at
#      02:29 ends about 03:05, so the blackout start leaves ~55 min of margin
#      even for the last unit admitted.
#   3. Deadline 23:00. `sdc_daily_opendart_filings` runs 23:30 and shares both
#      the opendart lock and the API quota.
#
# Each unit takes and releases the opendart lock itself (via
# dart-backfill-historical-year.sh -> sdc_run_daily_collector), so the lock is
# free between units and a daily job can slip in even inside a window.
#
# STOPPING. There is no coverage check here, and none is needed: every stage is
# skip-if-present per (corp, year, report), so once the grid is closed a run
# costs 0 requests and finishes in seconds. The event becomes a cheap no-op
# rather than something that must be deleted on time. Delete it at leisure.
#
# Exit 75 from a unit means every OpenDART key hit its daily limit. That is a
# clean stop, not a fault: it is propagated so the run ends and the next
# trigger resumes.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

start_year="${DART_S1_START_YEAR:-2015}"
end_year="${DART_S1_END_YEAR:-2025}"
stages="${DART_S1_STAGES:-financials share_info xbrl}"
# Annual first: it carries the balance sheet every fin_* family needs, so a run
# that only gets partway through still moves the features that matter most.
reprt_codes="${DART_S1_REPRT_CODES:-11011 11012 11014 11013}"
fs_divs="${DART_S1_FS_DIVS:-CFS OFS}"
rate_limit_seconds="${DART_S1_RATE_LIMIT_SECONDS:-0.1}"
blackout_start="${DART_S1_BLACKOUT_START:-0230}"
blackout_end="${DART_S1_BLACKOUT_END:-0500}"
deadline="${DART_S1_DEADLINE:-2300}"
self_lock="${DART_S1_LOCK_FILE:-$SDC_LOCK_DIR/s1-remainder.lock}"

now_hhmm() { date '+%H%M'; }

# Inside the blackout, or past the deadline? Both are compared as HHMM strings
# stripped of leading zeros, because 10#$x forces base 10 — "0230" is not octal.
window_closed() {
  local now bs be dl
  now=$((10#$(now_hhmm)))
  bs=$((10#$blackout_start))
  be=$((10#$blackout_end))
  dl=$((10#$deadline))
  if (( now >= bs && now < be )); then
    sdc_log "window closed: inside blackout ${blackout_start}-${blackout_end}"
    return 0
  fi
  # The deadline only closes the evening side; after midnight the blackout is
  # what holds until 05:00, so a 00:10 trigger is not blocked by dl=2300.
  if (( now >= dl )); then
    sdc_log "window closed: past deadline ${deadline}"
    return 0
  fi
  return 1
}

run_unit() {
  local year="$1" stage="$2" reprt="$3" fs="$4"
  local label="year=${year} stage=${stage} reprt=${reprt}"
  local -a env_args=(
    "DART_HISTORICAL_YEAR=${year}"
    "DART_HISTORICAL_STAGES=${stage}"
    "DART_HISTORICAL_REPRT_CODES=${reprt}"
    "DART_HISTORICAL_SCOPE=historical"
    "DART_HISTORICAL_RATE_LIMIT_SECONDS=${rate_limit_seconds}"
  )
  if [[ "$stage" == "financials" ]]; then
    env_args+=("DART_HISTORICAL_FS_DIVS=${fs}")
    label="${label} fs_div=${fs}"
  fi

  sdc_log "S-1 unit start: ${label}"
  local rc=0
  env "${env_args[@]}" "$script_dir/dart-backfill-historical-year.sh" || rc=$?
  if (( rc == 75 )); then
    sdc_log "S-1 unit: ${label} -> exit 75 (all OpenDART keys exhausted); ending run"
    return 75
  fi
  if (( rc != 0 )); then
    # A single bad unit is recorded and stepped over; the grid is independent
    # per unit and the next trigger retries it.
    sdc_log "S-1 unit: ${label} FAILED rc=${rc}; continuing to the next unit"
    return 0
  fi
  sdc_log "S-1 unit done: ${label}"
  return 0
}

drive() {
  local units=0
  local year stage reprt fs
  # Newest year first: recent history is what the 2020- feature sample needs.
  for year in $(seq "$end_year" -1 "$start_year"); do
    for stage in $stages; do
      for reprt in $reprt_codes; do
        if [[ "$stage" == "financials" ]]; then
          for fs in $fs_divs; do
            if window_closed; then
              sdc_log "S-1 remainder: stopping after ${units} unit(s)"
              return 0
            fi
            run_unit "$year" "$stage" "$reprt" "$fs" || return 75
            units=$((units + 1))
          done
        else
          if window_closed; then
            sdc_log "S-1 remainder: stopping after ${units} unit(s)"
            return 0
          fi
          run_unit "$year" "$stage" "$reprt" "-" || return 75
          units=$((units + 1))
        fi
      done
    done
  done
  sdc_log "S-1 remainder: walked the whole grid (${units} unit(s) this run)"
  return 0
}

mkdir -p "$SDC_LOCK_DIR"
# The self lock is defence in depth, not the primary mechanism -- the Cronicle
# event carries max_children=1. So a host without flock logs and proceeds
# rather than exiting: silently never running is a much worse failure than
# running without a second guard, and that is what an unconditional
# `flock || exit 0` would do.
s1_lock_held=0
if command -v flock >/dev/null 2>&1; then
  exec {s1_fd}>"$self_lock"
  if flock -n "$s1_fd"; then
    s1_lock_held=1
  else
    sdc_log "S-1 remainder: another run holds $self_lock; exiting cleanly"
    exit 0
  fi
else
  sdc_log "S-1 remainder: WARNING flock unavailable; relying on Cronicle max_children=1"
fi

sdc_log "S-1 remainder: years=${end_year}..${start_year} stages=${stages}"
sdc_log "S-1 remainder: blackout=${blackout_start}-${blackout_end} deadline=${deadline} rate=${rate_limit_seconds}s"
drive
status=$?
if (( s1_lock_held == 1 )); then
  flock -u "$s1_fd" || true
fi
sdc_log "S-1 remainder: run finished status=${status}"
exit "$status"

#!/usr/bin/env bash
#
# F-6.2 — receipt-targeted XBRL backfill so `fin_sue` can exist.
#
# `fin_sue` has never been measured: effective_start 2025-05-02, coverage 0.00,
# all six event cells `insufficient`. The formula was never the problem. XBRL
# captured against *original* periodic receipts is too sparse for any ticker to
# reach the nine consecutive quarterly filings the statistic needs, so F-6.1
# derived the receipts that would close a nine-quarter window and left 22,700 of
# them in a JSON-lines target file (results/sue_backfill_targets_202609.md).
#
# WHY A DRIVER AND NOT ONE LONG RUN
#
# Measured on 50 targets, 2026-09-09 22:58 KST: 28 s, so 1.79 req/s and the
# whole file is about 3 h 31 min at a 0.2 s rate limit.
#
# RATE LIMIT 0.1, NOT 0.2. Of that 0.56 s per target, 0.2 s was the deliberate
# sleep and about 0.26 s the API round trip plus zip parse, so halving the sleep
# takes the file to roughly 2 h 22 min -- 38 minutes of margin against a
# deadline that matters. 0.1 is not a new number: dart-backfill-s1-remainder.sh
# has used it as its default all along, and F-9.3 sustained 131,292 requests
# over 5 h 45 min at 6.34 req/s with zero failures. This backfill peaks around
# 2.8 req/s, well inside that. A throttle answer is retried and rotated across
# the nine keys anyway, so the downside is a slower chunk, not a lost one. That is far too long to stop before a
# scheduled opendart job, and F-9.3 established the rule this driver follows —
# a wall-clock guard only holds if the work unit is shorter than the interval
# between checks. So the unit here is one chunk of DART_SUE_CHUNK_SIZE targets:
# 500 is about 280 s, short enough that a guard checked BETWEEN chunks means
# something.
#
# Three guards, the same three (and the same values) as
# dart-backfill-s1-remainder.sh, because the constraint is identical:
#
#   1. Self lock (non-blocking). Two overlapping triggers must not both drive
#      the queue; the second exits 0 rather than fighting for the opendart lock.
#   2. Blackout 03:40-05:00. The daily chain runs 04:00-04:04, and what has to
#      hold is that no chunk owns the opendart lock when it starts.
#
#      The S-1 driver uses 02:30 for this because its work unit is about 35
#      minutes: a unit admitted at 02:29 ends around 03:05, and the 90-minute
#      gap is what makes that safe. This driver's unit is about 4 minutes, so
#      the same gap is 20x larger than the thing it protects against. 03:40
#      keeps a real margin proportional to the unit -- a chunk admitted at
#      03:39 ends about 03:43, 17 minutes clear -- and the failure mode needs
#      more than that anyway: the chain waits 900 s for the lock before giving
#      up, so a chunk would have to run past 04:15, four times its measured
#      length, to break it.
#
#      Nothing else is scheduled between 00:00 and 04:00 (verified against
#      get_schedule 2026-09-09: the neighbouring jobs are 23:00, 23:30 and
#      04:00), so this opens 00:00-03:40 as one continuous window.
#   3. Deadline 23:00. `sdc_daily_opendart_filings` runs 23:30-23:44 and shares
#      both the opendart lock and the API quota. As in the S-1 driver the
#      deadline closes only the evening side, so a 00:10 trigger is admitted and
#      the 00:00-03:40 stretch is usable.
#
# Each chunk takes and releases the opendart lock itself, so the lock is free
# between chunks and a daily job can slip in even inside an open window.
#
# WHY THIS ONE CALLS docker compose ITSELF
#
# `sdc_run_daily_collector` cannot pass a volume, and the target file has to be
# visible inside the container. The collector service's only mount is
# ./state:/state, which exists solely for the KIS token cache -- repurposing it
# would make that comment false. So the chunk runs under `sdc_with_source_lock`
# (the part that matters) with its own `docker compose run -v`. This is a manual
# backfill, not a scheduled wrapper.
#
# STOPPING. No coverage check is needed. The service checks
# `dart_xbrl_document` keys AND the slice ledger *before* issuing a request, so
# a completed chunk costs zero API calls, and a `no_data` answer is recorded in
# the ledger with no TTL and never retried. That is the F-9.3 failure closed:
# unrecoverable targets retire permanently instead of costing the same requests
# on every run. Once the file is done a full pass is container startup only.
#
# Exit 75 from a chunk means every OpenDART key hit its daily limit. Clean stop,
# propagated so the run ends and the next trigger resumes. With nine keys
# configured the daily budget is 90,000, so this file fits inside one day's
# quota and 75 is not expected here.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

targets_file="${DART_SUE_TARGETS_FILE:-/home/whi/apps/sdc/targets/sue_targets.jsonl}"
chunk_dir="${DART_SUE_CHUNK_DIR:-/home/whi/apps/sdc/targets/chunks}"
chunk_size="${DART_SUE_CHUNK_SIZE:-500}"
rate_limit_seconds="${DART_SUE_RATE_LIMIT_SECONDS:-0.1}"
blackout_start="${DART_SUE_BLACKOUT_START:-0340}"
blackout_end="${DART_SUE_BLACKOUT_END:-0500}"
deadline="${DART_SUE_DEADLINE:-2300}"
self_lock="${DART_SUE_LOCK_FILE:-$SDC_LOCK_DIR/sue-xbrl.lock}"

now_hhmm() { date '+%H%M'; }

# Same comparison as the S-1 driver: 10#$x forces base 10, because "0230" would
# otherwise be read as octal.
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
  if (( now >= dl )); then
    sdc_log "window closed: past deadline ${deadline}"
    return 0
  fi
  return 1
}

# Deterministic, so a resumed run walks the same chunks in the same order and
# the completed ones fall through on skip-if-present.
prepare_chunks() {
  if [[ ! -f "$targets_file" ]]; then
    sdc_log "targets file not found: $targets_file"
    return 1
  fi
  local want
  want=$(( ( $(wc -l < "$targets_file") + chunk_size - 1 ) / chunk_size ))
  local have=0
  if [[ -d "$chunk_dir" ]]; then
    have=$(find "$chunk_dir" -maxdepth 1 -name 'chunk-*.jsonl' | wc -l)
  fi
  if (( have == want )); then
    sdc_log "chunks ready: $have of $chunk_size lines each"
    return 0
  fi
  sdc_log "splitting $targets_file into chunks of $chunk_size (have=$have want=$want)"
  rm -rf "$chunk_dir"
  mkdir -p "$chunk_dir"
  split -l "$chunk_size" -d -a 4 --additional-suffix=.jsonl \
    "$targets_file" "$chunk_dir/chunk-"
  sdc_log "chunks written: $(find "$chunk_dir" -maxdepth 1 -name 'chunk-*.jsonl' | wc -l)"
}

# One work unit. Runs under the opendart lock; the container mounts the chunk
# directory read-only, so nothing in it can be written by the collector.
run_chunk() {
  local chunk="$1"
  local base in_container
  base="$(basename "$chunk")"
  in_container="/targets/chunks/$base"
  sdc_cd_app
  sdc_log "chunk: $base"
  # --name so sdc_reap_orphan_container's guarantee still holds: holding the
  # lock means no sibling can be alive, so a container under this name is
  # provably an orphan. Without it the reaper would be looking for a name this
  # run never creates.
  $SDC_DOCKER_COMPOSE_CMD run --rm --no-deps \
    --name "${SDC_RUN_CONTAINER_NAME:-sdc-sue-xbrl}" \
    -v "$(dirname "$chunk_dir"):/targets:ro" \
    "$SDC_COLLECTOR_SERVICE" \
    dart backfill-xbrl-receipts \
      --targets-file "$in_container" \
      --rate-limit-seconds "$rate_limit_seconds"
}

main() {
  mkdir -p "$SDC_LOCK_DIR"
  exec 9>"$self_lock"
  if ! flock -n 9; then
    sdc_log "another sue-xbrl driver holds $self_lock; exiting 0"
    return 0
  fi

  prepare_chunks || return 1

  # Set once rather than as a prefix on the call: in bash an assignment
  # preceding a *function* invocation persists after it returns, so the prefix
  # form would look scoped and not be.
  export SDC_LOCK_WAIT_SECONDS="${DART_SUE_LOCK_WAIT_SECONDS:-900}"
  export SDC_LOCK_CONFLICT_MODE=fail

  local total processed=0 rc=0 walked_all=1
  total=$(find "$chunk_dir" -maxdepth 1 -name 'chunk-*.jsonl' | wc -l)
  # `for` over a glob, and every child gets </dev/null. F-9.14: a
  # `while read` fed by a heredoc loses its input because `docker compose run`
  # inherits stdin and consumes the rest of it, so the loop silently does one
  # iteration and exits 0. This form is the one to use in a prod driver.
  local chunk
  for chunk in "$chunk_dir"/chunk-*.jsonl; do
    [[ -e "$chunk" ]] || break
    if window_closed; then
      sdc_log "stopping before $(basename "$chunk"); $processed chunk(s) run this pass"
      walked_all=0
      break
    fi
    sdc_with_source_lock opendart run_chunk "$chunk" < /dev/null
    rc=$?
    processed=$((processed + 1))
    if (( rc == 75 )); then
      sdc_log "chunk $(basename "$chunk") exited 75: all OpenDART keys exhausted; stopping"
      return 75
    fi
    if (( rc != 0 )); then
      sdc_log "chunk $(basename "$chunk") exited $rc; stopping so the failure is not buried"
      return "$rc"
    fi
  done

  sdc_log "pass complete: $processed of $total chunk(s) run"
  # Every chunk walked means every target was attempted. Anything still without
  # a document after that is a `no_data` answer, which the ledger has retired
  # permanently -- so this, not a coverage query, is the completion condition,
  # and it is printed as a fixed token so a driving loop can test for it.
  if (( walked_all == 1 && processed == total && total > 0 )); then
    sdc_log "SUE_BACKFILL_COMPLETE: all $total chunk(s) walked"
  fi
  return 0
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

# N3 — month-end historical universe snapshots (survivorship audit).
#
# Writes only stock_master_snapshot / _items; stock_master is never touched, so
# this cannot disturb the live universe or sync_universe's delisting diff.
# Snapshots are tagged with a *_BACKFILL source to keep them distinguishable.
#
# Idempotent on (as_of_date, source): re-running skips dates already captured,
# under either backfill provenance.
#
# The default source is now the KRX Open API (K-4), which needs AUTH_KEYS in
# this host's .env. It paces itself with a token bucket, so the KRX_* delay
# settings and the --min/--max-delay-seconds overrides below apply only to
# `--source pykrx`, which is the scraping path and is gated by
# ALLOW_KRX_SCRAPING (K-5).

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  universe backfill-snapshots
  --markets "${UNIVERSE_SNAPSHOT_MARKETS:-kospi,kosdaq}"
  --max-consecutive-failures "${UNIVERSE_SNAPSHOT_MAX_CONSECUTIVE_FAILURES:-5}"
)

if [[ -n "${UNIVERSE_SNAPSHOT_START:-}" ]]; then
  args+=(--start "$UNIVERSE_SNAPSHOT_START")
fi

if [[ -n "${UNIVERSE_SNAPSHOT_END:-}" ]]; then
  args+=(--end "$UNIVERSE_SNAPSHOT_END")
fi

if [[ -n "${UNIVERSE_SNAPSHOT_MIN_DELAY_SECONDS:-}" ]]; then
  args+=(--min-delay-seconds "$UNIVERSE_SNAPSHOT_MIN_DELAY_SECONDS")
fi

if [[ -n "${UNIVERSE_SNAPSHOT_MAX_DELAY_SECONDS:-}" ]]; then
  args+=(--max-delay-seconds "$UNIVERSE_SNAPSHOT_MAX_DELAY_SECONDS")
fi

if [[ "${UNIVERSE_SNAPSHOT_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"

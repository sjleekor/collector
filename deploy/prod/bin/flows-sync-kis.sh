#!/usr/bin/env bash
#
# KIS security flows — the replacement for flows-sync.sh (K-6f).
#
# Its own lock domain, deliberately. `krx_marketdata` exists so that several
# collectors do not hit KRX at once; KIS is a different company's servers with
# its own quota, so sharing that lock would serialise two sources that have no
# reason to wait for each other — and during the changeover the two must be
# able to run at the same time.
#
# KIS paces at 1 request/second (measured; the documented 20/s is not real), so
# a full universe pass costs roughly 46 minutes per metric group.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  flows sync-kis
  --lookback-days "${KIS_FLOW_LOOKBACK_DAYS:-14}"
)

if [[ -n "${KIS_FLOW_EXCLUDE_GROUPS:-}" ]]; then
  args+=(--exclude-groups "$KIS_FLOW_EXCLUDE_GROUPS")
fi

if [[ -n "${KIS_FLOW_TICKERS:-}" ]]; then
  args+=(--tickers "$KIS_FLOW_TICKERS")
fi

sdc_run_daily_collector kis "${args[@]}"

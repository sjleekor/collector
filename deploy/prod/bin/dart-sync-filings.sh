#!/usr/bin/env bash
set -euo pipefail

# Raw disclosure-receipt history (list.json), standalone and scheduled.
#
# sync-filings previously only ran as the last stage of dart-backfill-all-years.sh,
# so there was no way to run it on its own target set. S-1 needs exactly that:
# 1,302 of the 3,959 corps that ever had a ticker have no receipts at all, and
# they are the delisted ones -- the survivorship gap in the disclosure-activity
# features (N5-7) is precisely this set.
#
# Past years are skip-if-present on (corp_code, year). The current year is
# always refreshed because new and amended filings keep arriving. The regular
# path limits that refresh to a recent window and current listed corps; set the
# environment variables explicitly for a historical backfill. Exit 75 means
# every key hit its daily limit; the next run resumes.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  dart sync-filings
  --universe-scope "${DART_FILINGS_UNIVERSE_SCOPE:-current}"
  --rate-limit-seconds "${DART_FILINGS_RATE_LIMIT_SECONDS:-0.2}"
)

lookback_days="${DART_FILINGS_LOOKBACK_DAYS-14}"
if [[ -n "$lookback_days" ]]; then
  args+=(--lookback-days "$lookback_days")
fi

if [[ -n "${DART_FILINGS_YEARS:-}" ]]; then
  args+=(--years "$DART_FILINGS_YEARS")
fi

if [[ -n "${DART_FILINGS_TICKERS:-}" ]]; then
  args+=(--tickers "$DART_FILINGS_TICKERS")
fi

if [[ "${DART_FILINGS_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector opendart "${args[@]}"

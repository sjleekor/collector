#!/usr/bin/env bash
set -euo pipefail

# Daily incremental of DART 거래소공시 (list.json pblntf_ty=I), market-wide.
#
# Always refetches the last DART_EXCHANGE_NOTICES_LOOKBACK_DAYS (default 7) calendar
# days, so a missed night or a late/corrected filing is picked up. ~230 notices a day
# => about 3 pages a day, ~20 requests for the 7-day window. Needs the table from
# sql/migrations/20261007_dart_exchange_notice.sql (db init does not alter an
# existing server). Exit 75 means every key hit its daily limit; the next run resumes.
#
# Shares the `opendart` source lock with the other DART jobs, so schedule it after
# dart-sync-filings (23:30) or give it its own slot.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

sdc_run_daily_collector opendart \
  dart sync-exchange-notices \
  --lookback-days "${DART_EXCHANGE_NOTICES_LOOKBACK_DAYS:-7}" \
  --rate-limit-seconds "${DART_EXCHANGE_NOTICES_RATE_LIMIT_SECONDS:-0.3}"

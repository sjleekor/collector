#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

# --source krx-openapi since 2026-09-09 (K-5). fdr is not a fallback: it broke.
#
# fdr never read the listing from KRX. It read max_work_dt off
# data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd (twice per
# invocation, the second a duplicate) and then pulled that date's CSV from the
# FinanceData/fdr_krx_data_cache GitHub repo. That cache stopped publishing
# after 2026-09-07.csv, so every run from 2026-09-08 died on `HTTP Error 404`
# — and this event is the head of the daily chain, with no chain_error, so
# prices, flows and krx common did not run either for two sessions.
#
# The official endpoint also removes the four unaccounted anonymous MDC
# requests a day and brings a real LIST_DD instead of FDR's best-effort
# listing-date column. It needs AUTH_KEYS, which prod has had since
# 2026-08-18. It publishes T+1, which the provider absorbs by walking back up
# to 10 days for the latest published day.
#
# Lock domain moves fdr -> krx_marketdata with the source, matching the other
# two krx-openapi wrappers (prices-market-cap-backfill,
# universe-backfill-snapshots). It is the head of the chain, so it takes the
# lock uncontested; the cost is at most the 60s krx_marketdata throttle.
sdc_run_daily_collector krx_marketdata universe sync --source krx-openapi --markets kospi,kosdaq

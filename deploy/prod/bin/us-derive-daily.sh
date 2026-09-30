#!/usr/bin/env bash
# US 일일 scoring에 필요한 raw 뒤 derived snapshot만 갱신한다.
# 유니버스는 월간 판정이므로 여기서 rebuild하지 않는다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

TABLES="${SDC_US_DERIVE_DAILY_TABLES:-prices-daily,corp-actions,volatility-daily,short-interest,short-volume,earnings-calendar,nasdaq-analyst-estimates,fundamentals,submissions,insider,filings-sub,midas,ftd,thirteenf,trading-calendar}"
BUDGET_SECONDS="${SDC_US_DERIVE_DAILY_BUDGET_SECONDS:-7200}"

args=(us-derive run --tables "$TABLES" --budget-seconds "$BUDGET_SECONDS")
args+=("$@")

# 기존 15시 US 수집과 같은 `us` lock을 쓴다. 일일 derive가 raw를 읽는 동안
# 다음 수집이 동시에 원천 파일을 바꾸지 않게 한다.
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-2400}"
sdc_run_daily_collector us "${args[@]}"

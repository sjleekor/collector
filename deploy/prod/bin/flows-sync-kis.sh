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
#
# foreign_holding 요청이 받는 inquire-price 응답에서 관리종목·거래정지·시장경보 플래그도 같이
# kis_stock_status_daily에 쌓는다(추가 요청 0, 10월 계획 4.2-b). 끄려면 KIS_FLOW_NO_STATUS=1.
#
# 컨테이너는 호출자 uid:gid로 돈다. state/kis_token.json과 state/ 디렉터리가 그 사용자 소유여야 하고,
# 아니면 컨테이너를 띄우기 전에 종료 코드 73으로 멈춘다(토큰을 새로 발급하지 않는다).
# 탈출구: SDC_RUN_AS_ROOT=1. 한 번 할 일: deploy/prod/README.md "KIS 토큰 캐시 소유권".
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

sdc_prepare_kis_run || exit $?

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

if [[ "${KIS_FLOW_NO_STATUS:-0}" == "1" ]]; then
  args+=(--no-status)
fi

sdc_run_daily_collector kis "${args[@]}"

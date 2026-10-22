#!/usr/bin/env bash
set -euo pipefail

# 정규장 보정 — daily_ohlcv의 고가·저가·종가를 KRX 정규장 값으로 다시 맞춘다.
#
# 순서가 중요하다. 18:30 `prices-backfill-incremental.sh`는 최근 PRICE_LOOKBACK_DAYS(7)일을
# naver 값으로 다시 받아 덮고, KRX 정규장 원시가(daily_market_cap.source_*)는 20:00
# `prices-market-cap-backfill.sh`가 채운다(하루 늦게 온다: T일 20:00에는 T-1일까지).
# 그래서 이 wrapper는 **20:00 시총 작업이 끝난 뒤**에 돌리고, 최근 N 거래일(기본 10)을
# 매번 다시 맞춘다. 이미 맞은 행은 건드리지 않아 여러 번 돌려도 같다.
# 기본 combined에서는 T일 행도 20:30 KST 뒤 KRX MDC 당일 값으로 그날 고쳐진다(10-20 대조: 10-19
# MDC 20:36 값이 OpenAPI 확정값과 2,761행 모두 같음). market-cap만 쓰면 T+1일 20:00 뒤에 고쳐진다.
#
# 환경:
#   REGULAR_FIX_LAST_TRADING_DAYS  덮을 최근 거래일 수 (기본 10)
#   REGULAR_FIX_DRY_RUN=1          계획만 출력하고 쓰지 않는다
#   REGULAR_FIX_SOURCE             market-cap | mdc | combined (기본 combined)
#                                  combined: 시총(OpenAPI) 우선, 아직 안 온 당일만 KRX MDC.
#                                  당일 MDC는 20:30 KST 전이면 거절되어 시총만 쓴다.
#   REGULAR_FIX_ALLOW_EARLY_MDC=1  20:30 전 당일 MDC를 허용한다 (권하지 않는다)
# 인자는 그대로 `prices fix-regular-session`에 전달된다 (예: --start/--end).
# Cronicle 등록은 별도다(이 wrapper는 등록하지 않는다).

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(prices fix-regular-session)

if [[ $# -eq 0 ]]; then
  args+=(--last-trading-days "${REGULAR_FIX_LAST_TRADING_DAYS:-10}")
else
  args+=("$@")
fi

args+=(--source "${REGULAR_FIX_SOURCE:-combined}")

if [[ "${REGULAR_FIX_ALLOW_EARLY_MDC:-0}" == "1" ]]; then
  args+=(--allow-early-mdc)
fi

if [[ "${REGULAR_FIX_DRY_RUN:-0}" == "1" ]]; then
  args+=(--dry-run)
fi

# market-cap 원천이면 DB만 읽고 쓴다(combined·mdc는 당일 한 번 KRX MDC를 부른다). 18:30 naver 수집·20:00 시총과 같은
# krx_marketdata 락을 쓴다 — 그 작업이 도는 동안 기다려(기본 900초) 서로 덮어쓰지 않게 하고,
# 도메인 기본 간격(60초)이 앞 작업과의 사이를 벌린다. 락을 못 얻으면 75로 끝난다.
sdc_run_daily_collector krx_marketdata "${args[@]}"

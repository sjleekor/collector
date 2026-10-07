#!/usr/bin/env bash
# 가격 vintage 트리거: `listed_shares` 차분(k x p ~ 1)으로 daily_ohlcv 재수집 대상을 골라 목록을 남긴다.
# 근거: my/milestones/kr/research/data/quality/01_price_adjustment_vintage.md §7~§9.
#
# **목록만 남긴다. 재수집은 하지 않는다.** 재수집(`prices backfill --refetch`)은 사람이 목록을 보고
# 승인한 뒤에 따로 돌린다 — 자동 재수집을 만들지 않는 것이 계약이다. 운영 DB는 읽기만 한다
# (원천 호출·소스 락 없음).
#
# 산출물(호스트): <STOCK_DATA_HOST_DIR>/kr/output/price_vintage_trigger/<as_of>/
#   targets.txt   재수집 후보 티커, 한 줄에 하나 (`paste -sd,` 로 --tickers 에 넣는다)
#   events.csv    사건별 판정 (k, p, 상태, 낡은 행 수)
#   summary.json  인자·건수·`refetched_unresolved_tickers`(이미 재수집했는데 안 고쳐진 것)
# 같은 as_of 로 다시 돌리면 덮어쓴다.
#
# 언제 돌리나: 주 1회, 평일 저녁 수집(18:30 체인·20:00 시총)이 끝난 뒤. 시총은 T+1 원천이라 as_of 는
# 마지막으로 적재된 시총 날짜가 된다. Cronicle 등록은 별도다(이 wrapper 는 등록하지 않는다).
#
# 사용: kr-price-vintage-trigger.sh [ops price-vintage-trigger 인자...]
#   예) --lookback-days 30   --as-of 2026-10-02
# 환경: KR_VINTAGE_TRIGGER_DIR  호스트 산출 디렉터리(기본 위 경로)
#
# 소유자: 컨테이너를 호출 사용자 uid:gid 로 돌려 산출물이 그 사용자 소유로 남는다.
# 산출 디렉터리에 쓸 수 없으면 73으로 끝낸다. 탈출구: SDC_RUN_AS_ROOT=1(모든 wrapper 공통).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

host_dir="${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}"
out_dir="${KR_VINTAGE_TRIGGER_DIR:-${host_dir}/kr/output/price_vintage_trigger}"

sdc_assert_host_writable "" "$out_dir" || exit $?
mkdir -p "$out_dir" || {
  sdc_log "cannot create output dir: $out_dir"
  exit 73
}

# 컨테이너가 호스트와 같은 경로로 산출 디렉터리를 쓰도록 마운트한다.
if [[ -z "${SDC_RUN_EXTRA_ARGS:-}" ]]; then
  export SDC_RUN_EXTRA_ARGS="-v ${out_dir}:${out_dir}"
fi
sdc_append_run_as_invoking_user "" || exit $?

sdc_run_collector ops price-vintage-trigger --output-dir "$out_dir" "$@"

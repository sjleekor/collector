#!/usr/bin/env bash
# R-4 기준선 수집 — SEIBro ETF 분배금(분배금지급현황)을 서버 parquet 레이크에 쌓는다
# (계획 04 §3.1 SEIBro 창 · §6, R07).
#
# 창 = min(오늘−90일, 마지막 완료 창의 끝−90일) ~ 오늘. 마지막 완료 창은 저장소의 요청 기록에서
# 찾으므로, 120일 꺼 두었다 켜도 사이 분배금이 빠지지 않는다. 한 페이지라도 실패하면 창이
# 미완료로 남고(종료 코드 1) 다음 주 창이 마지막 완료 창부터 다시 받는다.
# 1·4·7·10월 첫 일요일에는 전체(2003-01-01~) 재확인을 자동으로 한다(약 390요청, 약 6분).
#
# 환경값:
#   SEIBRO_DIST_FULL=1     전체 받기를 강제한다(최초 전체 받기·수동 재확인).
#   SDC_SEIBRO_ENABLED     0이면 요청 없이 정상 종료한다(기본 1). 켜고 끄는 스위치다.
#
# 락 도메인 seibro: KRX 키를 안 쓰므로 krx_baseline 락과 따로다. 주간 sync와 전체 받기가
# 같은 락을 쓴다. 충돌하면 exit 75.
# 소유자·권한 검사는 krx-baseline-sync.sh와 같다(탈출구 SDC_RUN_AS_ROOT=1).
# Cronicle 이벤트(sdc_weekly_seibro_dist, 일요일 10:30)는 memory_limit을 명시하고 이 파일을 exec로 부른다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

if [[ "${SDC_SEIBRO_ENABLED:-1}" == "0" ]]; then
  sdc_log "SDC_SEIBRO_ENABLED=0 — SEIBro 수집이 꺼져 있어 요청 없이 끝냅니다"
  exit 0
fi

args=(seibro-dist sync)
if [[ "${SEIBRO_DIST_FULL:-0}" == "1" ]]; then
  args+=(--full)
fi

stock_data_host="${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}"
sdc_append_run_as_invoking_user "" || exit $?
sdc_assert_host_writable "" "$stock_data_host/kr/raw/krx_baseline" || exit $?
if [[ -n "${SDC_SEIBRO_ENABLED:-}" ]]; then
  SDC_RUN_EXTRA_ARGS="${SDC_RUN_EXTRA_ARGS:+${SDC_RUN_EXTRA_ARGS} }-e SDC_SEIBRO_ENABLED=${SDC_SEIBRO_ENABLED}"
  export SDC_RUN_EXTRA_ARGS
fi

sdc_run_daily_collector seibro "${args[@]}"

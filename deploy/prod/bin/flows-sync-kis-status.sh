#!/usr/bin/env bash
#
# KIS 종목 상태 플래그(관리종목·거래정지·시장경보) 일간 스냅샷 — KR 10월 계획 4.2-b.
#
# inquire-price를 종목별로 한 번씩(약 2,800요청) 불러 kis_stock_status_daily에 쌓는다.
# KIS는 이 계정에서 초당 1건이라(실측) 전 종목이 약 47분 걸린다.
# 다시 돌리면 오늘 이미 저장된 종목은 건너뛰고 빠진 종목만 받는다 — 그것이 재처리 경로다.
#
# lock 도메인은 flows-sync-kis.sh와 같은 `kis`다. KIS 호출 한도(계정 단위 초당 1건)와
# 토큰 캐시(state/kis_token.json)를 같이 쓰므로 둘이 동시에 돌면 안 된다. 락을 잡으면
# 직렬로 돌고, 앞 잡이 길어져도 기다렸다 이어 받는다(기본 3600초; 일일 기본값 900초는
# 19:00 flows가 46분 걸리는 날 이 잡을 실패로 끝낸다).
#
# 권장 일정: 평일 22:15 (Cronicle). 19:00~19:46 sdc_kis_flows_trial, 월요일 20:30~22:02
# sdc_kis_flows_weekly 뒤이고, 23:30 공시 접수(opendart 락)보다 앞서 끝난다.
#
# ── 컨테이너 사용자 ────────────────────────────────────────────────────────────
# 다른 wrapper(US, KR export)는 컨테이너를 호출자 uid:gid로 돌린다. 이 wrapper는 기본이
# root다. 이 잡이 호스트에 남기는 파일이 토큰 캐시 하나뿐이고, 그 파일(state/kis_token.json)이
# root 0600이기 때문이다. whi uid로 돌리면 KisTokenCache.load가 PermissionError를
# "캐시 없음"으로 삼켜 새 토큰을 발급하고(계정 소유자에게 카카오톡 알림), 이어 store가
# state/에 쓰지 못해 죽는다 — 매 실행마다 알림만 가고 수집은 못 한다.
# 토큰 소유권을 whi로 옮기면(flows-sync-kis.sh와 함께) SDC_RUN_AS_ROOT=0으로 바꿔 돌린다.
# 그때는 아래 사전 검사가 컨테이너를 띄우기 전에 읽기·쓰기 불가를 73으로 알려준다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

# 토큰 소유권이 정리되기 전까지 기본은 root. 호출자가 SDC_RUN_AS_ROOT=0을 주면 존중한다.
export SDC_RUN_AS_ROOT="${SDC_RUN_AS_ROOT:-1}"

sdc_append_run_as_invoking_user "" || exit $?

if ! sdc_run_as_root_requested ""; then
  state_dir="${SDC_APP_DIR}/state"
  token_file="${state_dir}/kis_token.json"
  if [[ -e "$token_file" && ! -r "$token_file" ]]; then
    sdc_log "not readable by uid $(id -u): $token_file — 이대로 돌리면 토큰을 새로 발급한다(카카오톡 알림)."
    sdc_not_writable_hint ""
    exit 73
  fi
  if [[ ! -w "$state_dir" ]]; then
    sdc_log "not writable by uid $(id -u): $state_dir — 토큰 캐시를 갱신하지 못한다."
    sdc_not_writable_hint ""
    exit 73
  fi
fi

args=(flows sync-kis-status)
args+=("$@")

SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-3600}"
sdc_run_daily_collector kis "${args[@]}"

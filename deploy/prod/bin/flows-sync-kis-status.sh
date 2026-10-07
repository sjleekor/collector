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
# 평소 19:00 `sdc_kis_flows_trial`(flows-sync-kis.sh)이 같은 inquire-price 응답에서 이 표도 같이
# 채운다. 이 wrapper는 그 잡이 놓친 종목(실패·이미 수급이 저장돼 건너뛴 종목)을 채울 때 쓴다.
#
# 권장 일정: 평일 22:15 (Cronicle). 19:00~19:46 sdc_kis_flows_trial, 월요일 20:30~22:02
# sdc_kis_flows_weekly 뒤이고, 23:30 공시 접수(opendart 락)보다 앞서 끝난다.
#
# ── 컨테이너 사용자 ────────────────────────────────────────────────────────────
# 다른 wrapper처럼 컨테이너를 호출자 uid:gid로 돌린다(flows-sync-kis.sh와 같다).
# state/kis_token.json과 state/ 디렉터리가 그 사용자 소유여야 한다. 아니면 컨테이너를 띄우기 전에
# 종료 코드 73으로 멈춘다(토큰을 새로 발급하지 않는다). 탈출구: SDC_RUN_AS_ROOT=1.
# 한 번 할 일: deploy/prod/README.md "KIS 토큰 캐시 소유권".
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

sdc_prepare_kis_run || exit $?

args=(flows sync-kis-status)
args+=("$@")

SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-3600}"
sdc_run_daily_collector kis "${args[@]}"

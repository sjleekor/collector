#!/usr/bin/env bash
# US universe_daily 를 새 완료 세션만큼 이어 붙인다. 새 세션이 없으면 그대로 끝낸다.
#
# `--if-new` 가 기본이다. 새 XNYS 세션이 없거나 같은 snapshot_date 파티션이 이미
# 있으면 `{"skipped": true}` 를 찍고 exit 0 이다. 그 밖의 오류는 exit 1 로 드러난다.
# 월간 full rebuild(`us-universe rebuild`)는 일정에 걸지 않고 손으로만 돌린다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(us-universe incremental --if-new)
# 손으로 부를 때 `--dry-run` 을 넘길 수 있어야 한다 (us-derive-daily.sh 와 같다).
args+=("$@")


# 컨테이너를 호출 사용자(whi) uid:gid로 돌려 레이크 파일이 root 소유로 안 남게 한다.
# 쓸 수 없는 파일·디렉터리가 레이크에 남아 있으면(예전 root 실행) 컨테이너를 띄우기 전에
# 종료 코드 73으로 끝낸다. 탈출구: SDC_RUN_AS_ROOT=1. `--dry-run`은 검사하지 않는다.
sdc_prepare_us_run "raw derived output" "$@" || exit $?

# 일일 derive(us-derive-daily.sh)와 같은 `us` lock. 뒤에 온 쪽이 기다린다.
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-2400}"
sdc_run_daily_collector us "${args[@]}"

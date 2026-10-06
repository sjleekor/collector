#!/usr/bin/env bash
# US universe_daily_v2(식별 표·증권 마스터·멤버십)를 처음부터 다시 판정한다.
#
# **일정에 걸지 않고 손으로만 돌린다** (월간 `us-universe rebuild` 와 같다). 첫 한 번은
# 이걸로 만들어야 `us-universe-incremental.sh` 가 v1 뒤에 잇는 증분(`--v2`)이 시작된다.
# 규칙을 바꿨을 때도 이걸로 다시 만든다 — 증분은 직전 빌드의 규칙을 그대로 따른다.
#
# 입력 스냅샷은 `us-derive.sh` 가 굳힌 것이다. `listing_snapshots_v2` 가 없으면 먼저
# `collector us-load listing-snapshots-v2` 를 돌린다 (이 래퍼에 `--dry-run` 을 주면 검사만 한다).
#
# 메모리는 `SDC_US_DUCKDB_MEMORY_LIMIT`·`SDC_US_DUCKDB_THREADS` 로 줄인다(v1 과 같은 이름).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(us-universe-v2 rebuild)
args+=("$@")


# 컨테이너를 호출 사용자(whi) uid:gid로 돌려 레이크 파일이 root 소유로 안 남게 한다.
# 쓸 수 없는 파일·디렉터리가 레이크에 남아 있으면(예전 root 실행) 컨테이너를 띄우기 전에
# 종료 코드 73으로 끝낸다. 탈출구: SDC_RUN_AS_ROOT=1. `--dry-run`은 검사하지 않는다.
sdc_prepare_us_run "raw derived output" "$@" || exit $?

# 일일 derive·증분과 같은 `us` lock. 뒤에 온 쪽이 기다린다.
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-2400}"
sdc_run_daily_collector us "${args[@]}"

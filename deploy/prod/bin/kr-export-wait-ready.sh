#!/usr/bin/env bash
# KR raw export 시작 게이트: `collector ops kr-export-readiness`를 deadline까지 반복해 부른다.
#
# 읽기 전용 DB 검사만 한다(원천 호출·락 없음). Cronicle에는 걸지 않는다 — export 앞단에서
# 사람이 또는 이후 승인된 wrapper가 부르는 도구다.
#
# 옵션:
#   --feature-asof-date YYYY-MM-DD   필수. 피쳐 기준일 K.
#   --interval-seconds N             확인 간격(기본 60).
#   --deadline-seconds N             처음 확인 시점부터 최대 대기(기본 3600). 0이면 한 번만 본다.
#   --evidence-dir DIR               증거 JSON을 쓸 호스트 디렉터리
#                                    (기본 ${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}/kr/output/export_readiness).
#                                    파일명은 K=<K>.json. 확인마다 덮어쓰므로 마지막 판정이 남는다.
#   --poll-through-blocked           blocked(exit 1)에도 계속 기다린다. 기본은 즉시 끝낸다.
#   -- ARGS...                       kr-export-readiness에 그대로 넘긴다
#                                    (--min-ticker-ratio, --run-since, --required-run-types 등).
#
# 소유자: 컨테이너를 호출한 사용자의 uid:gid로 돌려 증거 JSON이 그 사용자 소유로 남는다.
# 증거 디렉터리·기존 K=<K>.json이 그 사용자에게 쓸 수 없으면 73으로 끝낸다.
# SDC_RUN_AS_ROOT=1(모든 wrapper 공통) 또는 옛 이름 SDC_KR_EXPORT_RUN_AS_ROOT=1이면 예전처럼 root로 돌린다.
#
# 종료 코드: 0 준비됨 / 75 deadline까지 준비 안 됨(미준비) / 1 blocked(실패·partial 등) /
#            2 사용법 오류 / 73 증거 경로에 쓸 수 없음 / 그 밖은 검사 자체의 오류.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

feature_date=""
interval="${KR_EXPORT_READY_INTERVAL_SECONDS:-60}"
deadline="${KR_EXPORT_READY_DEADLINE_SECONDS:-3600}"
host_dir="${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}"
evidence_dir="${KR_EXPORT_READY_EVIDENCE_DIR:-${host_dir}/kr/output/export_readiness}"
poll_blocked=0
passthrough=()

die_usage() {
  sdc_log "$1"
  exit 2
}

while (($#)); do
  case "$1" in
    --feature-asof-date)
      (($# >= 2)) || die_usage "missing value for --feature-asof-date"
      feature_date="$2"
      shift 2
      ;;
    --interval-seconds)
      (($# >= 2)) || die_usage "missing value for --interval-seconds"
      interval="$2"
      shift 2
      ;;
    --deadline-seconds)
      (($# >= 2)) || die_usage "missing value for --deadline-seconds"
      deadline="$2"
      shift 2
      ;;
    --evidence-dir)
      (($# >= 2)) || die_usage "missing value for --evidence-dir"
      evidence_dir="$2"
      shift 2
      ;;
    --poll-through-blocked)
      poll_blocked=1
      shift
      ;;
    --)
      shift
      passthrough=("$@")
      break
      ;;
    *)
      die_usage "unsupported option: $1"
      ;;
  esac
done

[[ "$feature_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die_usage "--feature-asof-date YYYY-MM-DD is required"
[[ "$interval" =~ ^[0-9]+$ && "$interval" -ge 1 ]] || die_usage "invalid --interval-seconds: $interval"
[[ "$deadline" =~ ^[0-9]+$ ]] || die_usage "invalid --deadline-seconds: $deadline"

evidence_file="${evidence_dir}/K=${feature_date}.json"
sdc_assert_host_writable SDC_KR_EXPORT_RUN_AS_ROOT "$evidence_file" || exit $?
mkdir -p "$evidence_dir" || {
  sdc_log "cannot create evidence dir: $evidence_dir"
  exit 73
}

# 컨테이너가 호스트와 같은 경로로 증거 디렉터리를 쓰도록 마운트한다.
if [[ -z "${SDC_RUN_EXTRA_ARGS:-}" ]]; then
  export SDC_RUN_EXTRA_ARGS="-v ${evidence_dir}:${evidence_dir}"
fi

sdc_append_run_as_invoking_user SDC_KR_EXPORT_RUN_AS_ROOT || exit $?

start_epoch="$(date +%s)"
attempt=0
while :; do
  attempt=$((attempt + 1))
  set +e
  sdc_run_collector ops kr-export-readiness \
    --feature-asof-date "$feature_date" \
    --output "$evidence_file" \
    ${passthrough[@]+"${passthrough[@]}"}
  rc=$?
  set -e

  case "$rc" in
    0)
      sdc_log "kr export ready: K=$feature_date attempt=$attempt evidence=$evidence_file"
      exit 0
      ;;
    75) ;;
    1)
      if ((poll_blocked == 0)); then
        sdc_log "kr export BLOCKED: K=$feature_date attempt=$attempt evidence=$evidence_file"
        exit 1
      fi
      ;;
    *)
      sdc_log "readiness check errored (exit $rc); not retrying"
      exit "$rc"
      ;;
  esac

  now_epoch="$(date +%s)"
  if ((now_epoch + interval - start_epoch > deadline)); then
    sdc_log "kr export NOT READY by deadline: K=$feature_date attempts=$attempt last_exit=$rc evidence=$evidence_file"
    exit "$rc"
  fi
  sdc_log "not ready yet (exit $rc); retry in ${interval}s"
  sleep "$interval"
done

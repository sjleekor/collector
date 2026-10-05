#!/usr/bin/env bash

if [[ "${SDC_WRAPPER_LOADED:-0}" == "1" ]]; then
  return 0
fi
SDC_WRAPPER_LOADED=1

SDC_APP_DIR="${SDC_APP_DIR:-$HOME/apps/sdc}"
SDC_LOCK_DIR="${SDC_LOCK_DIR:-/tmp/sdc-locks}"
SDC_THROTTLE_DIR="${SDC_THROTTLE_DIR:-/tmp/sdc-throttle}"
SDC_DOCKER_COMPOSE_CMD="${SDC_DOCKER_COMPOSE_CMD:-docker compose}"
SDC_COLLECTOR_SERVICE="${SDC_COLLECTOR_SERVICE:-collector}"
SDC_LOCK_CONFLICT_MODE="${SDC_LOCK_CONFLICT_MODE:-fail}"

sdc_log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

sdc_use_daily_lock_defaults() {
  SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-900}"
  SDC_LOCK_CONFLICT_MODE="${SDC_LOCK_CONFLICT_MODE:-fail}"
}

sdc_cd_app() {
  cd "$SDC_APP_DIR"
}

sdc_compose() {
  local compose
  read -r -a compose <<< "$SDC_DOCKER_COMPOSE_CMD"
  "${compose[@]}" "$@"
}

# The lock lives on the host process; the collection lives in a container the
# host process spawns. Killing the wrapper releases the lock and leaves the
# container running, so an aborted backfill kept hitting OpenDART for another
# 20 minutes with nothing holding its slot (observed 2026-08-16 while stopping
# the S-1 backfill ahead of the 04:00 chain). Naming the container after its
# lock domain closes that: holding the lock means no sibling can be alive, so a
# container with that name at lock time is provably an orphan and gets reaped.
sdc_container_name_for_domain() {
  printf 'sdc-collector-%s\n' "$1"
}

sdc_reap_orphan_container() {
  local name="$1"
  sdc_cd_app
  if docker inspect "$name" >/dev/null 2>&1; then
    sdc_log "reaping orphaned container: $name (lock is free, so nothing owns it)"
    docker rm -f "$name" >/dev/null 2>&1 || true
  fi
}

sdc_run_collector() {
  sdc_cd_app
  local -a name_args=()
  if [[ -n "${SDC_RUN_CONTAINER_NAME:-}" ]]; then
    name_args=(--name "$SDC_RUN_CONTAINER_NAME")
    # Covers a graceful abort (Cronicle sends TERM). A SIGKILL cannot be
    # trapped, which is why the reap above exists as well.
    trap 'sdc_log "signal received; stopping ${SDC_RUN_CONTAINER_NAME}"; docker rm -f "${SDC_RUN_CONTAINER_NAME}" >/dev/null 2>&1 || true' INT TERM
  fi
  # Optional: run something other than the `collector` entrypoint (the KR raw
  # export runs bin/raw-parquet-export-all.sh), and pass extra `run` options
  # (e.g. `-e NAME=value`; never put a secret here, it is logged).
  local -a entry_args=() extra_args=()
  if [[ -n "${SDC_RUN_ENTRYPOINT:-}" ]]; then
    entry_args=(--entrypoint "$SDC_RUN_ENTRYPOINT")
  fi
  if [[ -n "${SDC_RUN_EXTRA_ARGS:-}" ]]; then
    read -r -a extra_args <<< "$SDC_RUN_EXTRA_ARGS"
  fi
  sdc_log "run: $SDC_DOCKER_COMPOSE_CMD run --rm ${name_args[*]} ${entry_args[*]} ${extra_args[*]} $SDC_COLLECTOR_SERVICE $*"
  sdc_compose run --rm "${name_args[@]}" "${entry_args[@]}" "${extra_args[@]}" "$SDC_COLLECTOR_SERVICE" "$@"
}

# 컨테이너는 기본이 root라 호스트에 마운트한 경로에 root 소유 파일을 남긴다
# (KR raw snapshot 약 7GB/일·증거 JSON, US 레이크 raw·derived·output). whi가 지우거나
# 순환(rotation)할 수 없어서, 호출한 사용자의 uid:gid로 컨테이너를 돌리는 옵션을
# SDC_RUN_EXTRA_ARGS 뒤에 붙인다. uid/gid는 박지 않고 `id`로 구한다.
# HOME은 uid가 passwd에 없어 `/`가 되므로 /tmp로 준다 (dolt 전역 설정·임시 파일이 거기 생긴다.
# 이미지의 /app·/usr/local/bin은 root 소유라도 읽기·실행만 하면 된다).
#
# 탈출구(릴리스 없이 되돌리기): SDC_RUN_AS_ROOT=1 — 모든 wrapper.
# 인자로 옛 변수 이름을 주면 그것도 같은 뜻으로 받는다
# (KR wrapper는 SDC_KR_EXPORT_RUN_AS_ROOT, 2026-10-05 v0.15.18부터 쓰던 이름).
# 어느 쪽이든 값은 0 또는 1이다.
#
# 종료 코드: 0 / 2 값이 잘못됨. 탈출구가 켜졌으면 SDC_RUN_AS_ROOT_ACTIVE=1을 남긴다.
sdc_run_as_root_requested() {
  local legacy_var="${1:-}" name value
  for name in SDC_RUN_AS_ROOT ${legacy_var:+"$legacy_var"}; do
    value="${!name:-0}"
    if [[ "$value" == "1" ]]; then
      return 0
    elif [[ "$value" != "0" ]]; then
      sdc_log "invalid ${name}: ${value} (expected 0 or 1)"
      return 2
    fi
  done
  return 1
}

sdc_append_run_as_invoking_user() {
  local rc=0
  sdc_run_as_root_requested "${1:-}" || rc=$?
  if [[ "$rc" == "0" ]]; then
    sdc_log "container user: image default (root) by SDC_RUN_AS_ROOT${1:+/$1}=1; host files will be root-owned"
    return 0
  elif [[ "$rc" == "2" ]]; then
    return 2
  fi
  local uid gid
  uid="$(id -u)"
  gid="$(id -g)"
  SDC_RUN_EXTRA_ARGS="${SDC_RUN_EXTRA_ARGS:+${SDC_RUN_EXTRA_ARGS} }--user ${uid}:${gid} -e HOME=/tmp"
  export SDC_RUN_EXTRA_ARGS
}

sdc_not_writable_hint() {
  sdc_log "root가 만든 디렉터리·파일이다. 소유자를 $(id -un)로 바꾸거나(chown -R) 비운 뒤 다시 돌린다."
  sdc_log "급하면 SDC_RUN_AS_ROOT=1${1:+(또는 $1=1)}로 예전처럼 root로 돌릴 수 있다 (파일이 root 소유로 남는다)."
}

# 컨테이너를 호출 사용자로 돌릴 때, 호스트의 출력 경로가 그 사용자에게 쓸 수 있는지 먼저 본다.
# 예전 root 실행이 남긴 디렉터리·파일이 있으면 export는 몇 분 뒤 권한 오류로 죽고
# partial 상태만 남는다. 여기서 바로 끝내고 종료 코드 73(EX_CANTCREAT)을 준다.
# $1 = 옛 탈출구 변수 이름(없으면 ""), 나머지 = 확인할 경로들. 없는 경로는 가장 가까운 기존
# 상위 디렉터리를 본다. 탈출구(root 실행)면 검사하지 않는다.
sdc_assert_host_writable() {
  local legacy_var="${1:-}"
  shift
  if sdc_run_as_root_requested "$legacy_var"; then
    return 0
  fi
  local target probe bad path
  for target in "$@"; do
    probe="$target"
    while [[ ! -e "$probe" && "$probe" != "/" ]]; do
      probe="$(dirname "$probe")"
    done
    bad=""
    if [[ ! -d "$probe" || ! -w "$probe" || ! -x "$probe" ]]; then
      bad="$probe"
    elif [[ -d "$target" ]]; then
      # find -writable은 GNU 전용이라(맥 테스트에서 안 된다) 셸의 -w로 하나씩 본다.
      while IFS= read -r -d '' path; do
        if [[ ! -w "$path" ]]; then
          bad="$path"
          break
        fi
      done < <(find "$target" \( -type d -o -type f \) -print0 2>/dev/null)
    elif [[ -e "$target" && ! -w "$target" ]]; then
      bad="$target"
    fi
    if [[ -n "$bad" ]]; then
      sdc_log "not writable by uid $(id -u): $bad (needed for $target)"
      sdc_not_writable_hint "$legacy_var"
      return 73
    fi
  done
}

# US 레이크용 사전 검사. KR처럼 트리 전체를 파일마다 셸 루프로 보면 레이크가 커질수록
# 느려지므로(US는 파일이 15천 개고 nasdaq 애널리스트만 주 4천 개씩 는다) 비용이 파일 수에
# 거의 안 드는 방식으로 본다:
#   1) find가 "내 uid 소유가 아닌" 항목만 걸러낸다 (C 속도, 정상이면 0건).
#   2) 걸린 항목만 셸의 -w·-r로 본다. 그룹 쓰기로 다른 소유자 파일을 쓰는 경우는 통과한다.
#   3) 내 소유 디렉터리가 소유자 쓰기·탐색(u+wx)을 잃은 것도 본다.
# 파일 소유자를 보므로 0600 root 파일(dolt .dolt/noms)처럼 쓰기 이전에 *읽기*도 안 되는 것까지 잡는다.
# 레이크 루트 $1(보통 ${STOCK_DATA_HOST_DIR}/us), 나머지는 그 아래 하위 트리(없으면 상위 디렉터리를 본다).
# 탈출구(SDC_RUN_AS_ROOT=1)면 검사하지 않는다. root(euid 0)도 건너뛴다.
sdc_assert_us_lake_writable() {
  local lake="$1"
  shift
  if sdc_run_as_root_requested ""; then
    return 0
  fi
  local uid sub target probe bad path
  uid="$(id -u)"
  for sub in "$@"; do
    target="${lake}/${sub}"
    probe="$target"
    while [[ ! -e "$probe" && "$probe" != "/" ]]; do
      probe="$(dirname "$probe")"
    done
    bad=""
    if [[ ! -d "$probe" || ! -w "$probe" || ! -x "$probe" ]]; then
      bad="$probe"
    elif [[ -d "$target" ]]; then
      while IFS= read -r -d '' path; do
        if [[ ! -r "$path" || ! -w "$path" || ( -d "$path" && ! -x "$path" ) ]]; then
          bad="$path"
          break
        fi
      done < <(find "$target" ! -uid "$uid" \( -type d -o -type f \) -print0 2>/dev/null)
      if [[ -z "$bad" ]]; then
        # 내 소유 디렉터리가 u+wx를 잃었다 (find -perm은 BSD·GNU 모두 8진수가 통한다).
        # `| head -n 1`로 자르면 걸린 것이 둘 이상일 때 find가 SIGPIPE를 받고, pipefail·set -e인
        # wrapper가 메시지 없이 죽는다 — find가 첫 건에서 스스로 멈추게 한다 (-quit는 BSD·GNU 공통)
        bad="$(find "$target" -type d -uid "$uid" \( ! -perm -200 -o ! -perm -100 \) -print -quit 2>/dev/null || true)"
      fi
    elif [[ -e "$target" && ! -w "$target" ]]; then
      bad="$target"
    fi
    if [[ -n "$bad" ]]; then
      sdc_log "not writable by uid ${uid}: $bad (needed for $target)"
      sdc_not_writable_hint ""
      return 73
    fi
  done
}

# 인자들에 플래그가 있는지. wrapper가 --dry-run일 때 쓰기 검사를 건너뛰는 데 쓴다.
sdc_has_flag() {
  local flag="$1" a
  shift
  for a in "$@"; do
    [[ "$a" == "$flag" ]] && return 0
  done
  return 1
}

# US wrapper 공통: 컨테이너를 호출 사용자로 돌리고, 레이크 쓰기 가능성을 먼저 본다.
# $1 = 레이크 하위 트리를 공백으로 이은 문자열, 나머지 = wrapper 인자(--dry-run이면 검사 생략).
sdc_prepare_us_run() {
  local subtrees="$1"
  shift
  sdc_append_run_as_invoking_user "" || return $?
  if ! sdc_has_flag --dry-run "$@"; then
    local -a subs
    read -r -a subs <<< "$subtrees"
    sdc_assert_us_lake_writable "${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}/us" "${subs[@]}" || return $?
  fi
}

sdc_run_collector_with_lock() {
  local domain="$1"
  shift
  sdc_with_source_lock "$domain" sdc_run_collector "$@"
}

# The per-source lock defaults to ON. It used to default to OFF, and prod never
# turned it on — not in .env, not in the host env, not in any Cronicle event —
# so /tmp/sdc-locks never even got created and every wrapper that looked like it
# held a lock held nothing. Two jobs hitting the same source concurrently is the
# one thing that gets an IP blocked, so the safety mechanism has to be the
# default and the bypass has to be the thing you type on purpose.
#
# Time separation in the schedule is not a substitute: a run that overruns its
# window (flows has taken 775s against a 5-minute budget, a backfill takes hours)
# silently puts two collectors on the same source with nothing to stop them.
sdc_run_daily_collector() {
  local domain="$1"
  shift
  if [[ "${SDC_DAILY_USE_SOURCE_LOCK:-1}" == "1" ]]; then
    sdc_use_daily_lock_defaults
    sdc_run_collector_with_lock "$domain" "$@"
  else
    sdc_log "source lock DISABLED by SDC_DAILY_USE_SOURCE_LOCK=${SDC_DAILY_USE_SOURCE_LOCK:-1}: domain=$domain"
    sdc_run_collector "$@"
  fi
}

sdc_date_minus_days() {
  python3 - "$1" "$2" <<'PY'
from datetime import date, timedelta
import sys

end = date.fromisoformat(sys.argv[1])
days = int(sys.argv[2])
print((end - timedelta(days=days)).isoformat())
PY
}

sdc_with_source_lock() {
  local domain="$1"
  shift
  local wait_seconds="${SDC_LOCK_WAIT_SECONDS:-0}"
  local conflict_mode="${SDC_LOCK_CONFLICT_MODE:-fail}"
  local lock_file="$SDC_LOCK_DIR/${domain}.lock"

  mkdir -p "$SDC_LOCK_DIR" "$SDC_THROTTLE_DIR"
  if command -v flock >/dev/null 2>&1 && [[ "${SDC_LOCK_BACKEND:-flock}" != "mkdir" ]]; then
    sdc_with_flock "$domain" "$lock_file" "$wait_seconds" "$conflict_mode" "$@"
  else
    sdc_with_mkdir_lock "$domain" "$lock_file.d" "$wait_seconds" "$conflict_mode" "$@"
  fi
}

sdc_with_flock() {
  local domain="$1"
  local lock_file="$2"
  local wait_seconds="$3"
  local conflict_mode="$4"
  shift 4

  exec {sdc_lock_fd}>"$lock_file"
  sdc_log "lock wait: domain=$domain backend=flock wait=${wait_seconds}s file=$lock_file"
  if flock -w "$wait_seconds" "$sdc_lock_fd"; then
    sdc_log "lock acquired: domain=$domain"
    local SDC_RUN_CONTAINER_NAME
    SDC_RUN_CONTAINER_NAME="$(sdc_container_name_for_domain "$domain")"
    export SDC_RUN_CONTAINER_NAME
    sdc_reap_orphan_container "$SDC_RUN_CONTAINER_NAME"
    sdc_throttle "$domain"
    local status
    if "$@"; then
      status=0
    else
      status=$?
    fi
    flock -u "$sdc_lock_fd" || true
    sdc_log "lock released: domain=$domain status=$status"
    return "$status"
  fi
  sdc_lock_conflict "$domain" "$conflict_mode"
}

sdc_with_mkdir_lock() {
  local domain="$1"
  local lock_dir="$2"
  local wait_seconds="$3"
  local conflict_mode="$4"
  shift 4

  sdc_log "lock wait: domain=$domain backend=mkdir wait=${wait_seconds}s dir=$lock_dir"
  local start now elapsed
  start="$(date +%s)"
  while ! mkdir "$lock_dir" 2>/dev/null; do
    now="$(date +%s)"
    elapsed=$((now - start))
    if (( elapsed >= wait_seconds )); then
      sdc_lock_conflict "$domain" "$conflict_mode"
      return $?
    fi
    sleep 1
  done

  sdc_log "lock acquired: domain=$domain"
  local SDC_RUN_CONTAINER_NAME
  SDC_RUN_CONTAINER_NAME="$(sdc_container_name_for_domain "$domain")"
  export SDC_RUN_CONTAINER_NAME
  sdc_reap_orphan_container "$SDC_RUN_CONTAINER_NAME"
  sdc_throttle "$domain"
  local status
  if "$@"; then
    status=0
  else
    status=$?
  fi
  rmdir "$lock_dir" 2>/dev/null || true
  sdc_log "lock released: domain=$domain status=$status"
  return "$status"
}

sdc_lock_conflict() {
  local domain="$1"
  local conflict_mode="$2"
  if [[ "$conflict_mode" == "skip" ]]; then
    sdc_log "lock conflict: domain=$domain mode=skip"
    return 0
  fi
  sdc_log "lock conflict: domain=$domain mode=fail exit=75"
  return 75
}

sdc_throttle() {
  local domain="$1"
  local min_interval marker now last elapsed sleep_seconds
  min_interval="$(sdc_min_interval_seconds "$domain")"
  if ! [[ "$min_interval" =~ ^[0-9]+$ ]] || (( min_interval <= 0 )); then
    sdc_update_throttle_marker "$domain"
    return 0
  fi

  marker="$SDC_THROTTLE_DIR/${domain}.last"
  now="$(date +%s)"
  last=0
  if [[ -r "$marker" ]]; then
    read -r last < "$marker" || last=0
  fi
  if ! [[ "$last" =~ ^[0-9]+$ ]]; then
    last=0
  fi

  elapsed=$((now - last))
  if (( elapsed < min_interval )); then
    sleep_seconds=$((min_interval - elapsed))
    sdc_log "throttle sleep: domain=$domain seconds=$sleep_seconds min_interval=$min_interval"
    sleep "$sleep_seconds"
  else
    sdc_log "throttle pass: domain=$domain elapsed=${elapsed}s min_interval=$min_interval"
  fi
  sdc_update_throttle_marker "$domain"
}

sdc_update_throttle_marker() {
  local domain="$1"
  mkdir -p "$SDC_THROTTLE_DIR"
  date +%s > "$SDC_THROTTLE_DIR/${domain}.last"
}

sdc_min_interval_seconds() {
  local domain="$1"
  local var_name default_value
  case "$domain" in
    krx_marketdata)
      var_name="SDC_KRX_MARKETDATA_MIN_INTERVAL_SECONDS"
      default_value="60"
      ;;
    opendart)
      var_name="SDC_OPENDART_MIN_INTERVAL_SECONDS"
      default_value="5"
      ;;
    fdr)
      var_name="SDC_FDR_MIN_INTERVAL_SECONDS"
      default_value="10"
      ;;
    fred)
      var_name="SDC_FRED_MIN_INTERVAL_SECONDS"
      default_value="10"
      ;;
    ecos)
      var_name="SDC_ECOS_MIN_INTERVAL_SECONDS"
      default_value="10"
      ;;
    *)
      var_name=""
      default_value="0"
      ;;
  esac
  if [[ -n "$var_name" ]]; then
    printf '%s\n' "${!var_name:-$default_value}"
  else
    printf '%s\n' "$default_value"
  fi
}

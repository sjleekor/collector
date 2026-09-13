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
  sdc_log "run: $SDC_DOCKER_COMPOSE_CMD run --rm ${name_args[*]} $SDC_COLLECTOR_SERVICE $*"
  sdc_compose run --rm "${name_args[@]}" "$SDC_COLLECTOR_SERVICE" "$@"
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

#!/usr/bin/env bash
set -euo pipefail
set -m # job control: give each backgrounded table worker its own process group

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_dir="${SDC_APP_DIR:-$(cd "$script_dir/.." && pwd)}"

cd "$app_dir"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

usage() {
  cat <<'EOF'
Usage: bin/raw-parquet-export-all.sh [options]

Export all configured raw/reference PostgreSQL tables to the Parquet lake.

Per-table state is detected before acting (skip tables with a valid completed
manifest, resume tables left with exactly one incomplete checkpoint, error out
if a table has multiple incomplete checkpoints). See
docs/dev/20260730_refactor_dump/00_dual_route_raw_export_plan.md §3.4.1.

Options:
  --snapshot-date YYYY-MM-DD   Output snapshot date. Defaults to today.
  --route local|remote         Which DB to export from (default: local).
                               local  -> source=local_mydb, DSN from DB_DSN.
                               remote -> source=sj2_remote, DSN from
                                         SDC_REMOTE_DSN (see `db with-remote-dsn`).
  --jobs N                     Concurrent table exports (default: 1 local / 3 remote;
                               clamped to [1,4]).
  --force                      Overwrite existing table output directories
                               (ignores/discards any manifest or checkpoints).
  --force-table NAME           Like --force, but only for this table. Repeatable.
                               Required to resolve a table with >1 incomplete
                               checkpoint.
  --no-build                   Skip cargo release build.
  --no-validate                Skip manifest validation after each export.
  --validate-samples           Run source-vs-Parquet sample validation for raw_id tables.
  --dry-run                    Render export plans without writing Parquet files.
  -h, --help                   Show this help.

Environment overrides:
  SDC_RAW_PARQUET_SNAPSHOT_DATE       Same as --snapshot-date.
  SDC_RAW_PARQUET_ROUTE                Same as --route.
  SDC_RAW_PARQUET_JOBS                 Same as --jobs.
  SDC_RAW_PARQUET_FORCE=1             Same as --force.
  SDC_RAW_PARQUET_BUILD_RELEASE=0     Same as --no-build.
  SDC_RAW_PARQUET_VALIDATE=0          Same as --no-validate.
  SDC_RAW_PARQUET_VALIDATE_SAMPLES=1  Same as --validate-samples.
  SDC_RAW_PARQUET_DRY_RUN=1           Same as --dry-run.
  SDC_RAW_PARQUET_BATCH_ROWS          Default: 65536.
  SDC_RAW_PARQUET_MAX_ROWS_PER_FILE   Default: 5000000.
  SDC_RAW_PARQUET_CONFIG              Default: tools/raw-parquet-exporter/config/export_tables.toml.
  SDC_RAW_PARQUET_RUNTIME             Escape hatch: use this runtime TOML verbatim instead of
                                       generating one from --route. Its [source].name and
                                       [output].root must match the route-derived values or the
                                       script fails fast (see docs/dev/20260730_refactor_dump).
  SDC_RAW_PARQUET_OUTPUT_ROOT         Default: \$STOCK_DATA_ROOT/kr/raw/raw_postgres.
  SDC_RAW_PARQUET_BIN                 Exporter binary path override (test injection point).
                                       Default: tools/raw-parquet-exporter/target/release/
                                       raw-parquet-exporter.

On full success (every table skipped/exported/resumed), a completion marker is written to
<output_root>/snapshot_date=<D>/source=<S>/_manifests/_SUCCESS.json — this is what
research.etl.compute_all now requires before it will read the lake (see --allow-incomplete-lake
there). On any table failure, the marker is NOT written; a summary of failed tables is printed
and the script exits non-zero.
EOF
}

is_yyyy_mm_dd() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
}

snapshot_date="${SDC_RAW_PARQUET_SNAPSHOT_DATE:-$(date +%F)}"
route="${SDC_RAW_PARQUET_ROUTE:-local}"
jobs="${SDC_RAW_PARQUET_JOBS:-}"
force="${SDC_RAW_PARQUET_FORCE:-0}"
build_release="${SDC_RAW_PARQUET_BUILD_RELEASE:-1}"
validate="${SDC_RAW_PARQUET_VALIDATE:-1}"
validate_samples="${SDC_RAW_PARQUET_VALIDATE_SAMPLES:-0}"
dry_run="${SDC_RAW_PARQUET_DRY_RUN:-0}"
batch_rows="${SDC_RAW_PARQUET_BATCH_ROWS:-65536}"
max_rows_per_file="${SDC_RAW_PARQUET_MAX_ROWS_PER_FILE:-5000000}"
config_path="${SDC_RAW_PARQUET_CONFIG:-tools/raw-parquet-exporter/config/export_tables.toml}"
custom_runtime_path="${SDC_RAW_PARQUET_RUNTIME:-}"
# --help must still work with no STOCK_DATA_ROOT set, so the guard is skipped
# when -h/--help is anywhere in argv (the while-loop below exits before
# output_root is ever used in that case).
_wants_help=0
for _arg in "$@"; do
  case "$_arg" in -h|--help) _wants_help=1 ;; esac
done
if [ "$_wants_help" -eq 0 ]; then
  : "${STOCK_DATA_ROOT:?STOCK_DATA_ROOT가 없습니다. .envrc를 확인하십시오 (direnv allow).}"
fi
output_root="${SDC_RAW_PARQUET_OUTPUT_ROOT:-${STOCK_DATA_ROOT:-}/kr/raw/raw_postgres}"
bin_path="${SDC_RAW_PARQUET_BIN:-tools/raw-parquet-exporter/target/release/raw-parquet-exporter}"
declare -A force_tables=()

while (($#)); do
  case "$1" in
    --snapshot-date)
      if (($# < 2)); then
        printf 'Missing value for --snapshot-date\n' >&2
        exit 2
      fi
      snapshot_date="$2"
      shift 2
      ;;
    --route)
      if (($# < 2)); then
        printf 'Missing value for --route\n' >&2
        exit 2
      fi
      route="$2"
      shift 2
      ;;
    --jobs)
      if (($# < 2)); then
        printf 'Missing value for --jobs\n' >&2
        exit 2
      fi
      jobs="$2"
      shift 2
      ;;
    --force)
      force=1
      shift
      ;;
    --force-table)
      if (($# < 2)); then
        printf 'Missing value for --force-table\n' >&2
        exit 2
      fi
      force_tables["$2"]=1
      shift 2
      ;;
    --no-build)
      build_release=0
      shift
      ;;
    --no-validate)
      validate=0
      shift
      ;;
    --validate-samples)
      validate_samples=1
      shift
      ;;
    --dry-run)
      dry_run=1
      validate=0
      validate_samples=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! is_yyyy_mm_dd "$snapshot_date"; then
  printf 'Invalid snapshot date: %s\n' "$snapshot_date" >&2
  exit 2
fi

case "$route" in
  local)
    source_name="local_mydb"
    dsn_env="DB_DSN"
    [[ -z "$jobs" ]] && jobs=1
    ;;
  remote)
    source_name="sj2_remote"
    dsn_env="SDC_REMOTE_DSN"
    [[ -z "$jobs" ]] && jobs=3
    ;;
  *)
    printf 'Invalid --route: %s (expected local or remote)\n' "$route" >&2
    exit 2
    ;;
esac

if ! [[ "$jobs" =~ ^[0-9]+$ ]] || ((jobs < 1)); then
  printf 'Invalid --jobs: %s (expected a positive integer)\n' "$jobs" >&2
  exit 2
fi
if ((jobs > 4)); then
  log "Clamping --jobs ${jobs} to 4 (sj2 is a 4-core box; local exports gain little past this)"
  jobs=4
fi

if ! [[ "$batch_rows" =~ ^[0-9]+$ ]] || ((batch_rows <= 0)); then
  printf 'Invalid SDC_RAW_PARQUET_BATCH_ROWS: %s\n' "$batch_rows" >&2
  exit 2
fi

if ! [[ "$max_rows_per_file" =~ ^[0-9]+$ ]] || ((max_rows_per_file <= 0)); then
  printf 'Invalid SDC_RAW_PARQUET_MAX_ROWS_PER_FILE: %s\n' "$max_rows_per_file" >&2
  exit 2
fi

if [[ ! -f "$config_path" ]]; then
  printf 'Config file not found: %s\n' "$config_path" >&2
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  printf 'cargo was not found in PATH. Install the Rust toolchain first.\n' >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  printf 'python3 was not found in PATH (needed for runtime/checkpoint/manifest JSON handling).\n' >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  printf 'jq was not found in PATH (needed for checkpoint state detection).\n' >&2
  exit 2
fi

shopt -s nullglob

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/sdc-raw-parquet-work.XXXXXX")"
chmod 700 "$work_dir"
mkdir -p "$work_dir/status"

cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

# The exporter binary/validator reads [source].name and [output].root from this
# runtime TOML, not from any of this wrapper's own env vars or CLI flags. Those
# two values MUST agree with --route, or the export writes to one source= path
# while this script's manifest/validate math looks at another (see
# docs/dev/20260730_refactor_dump/00_dual_route_raw_export_plan.md §4.1).
validate_custom_runtime() {
  local runtime_file="$1" expected_source="$2" expected_root="$3"
  python3 - "$runtime_file" "$expected_source" "$expected_root" <<'PY'
import sys
import tomllib

runtime_file, expected_source, expected_root = sys.argv[1:4]
with open(runtime_file, "rb") as f:
    data = tomllib.load(f)

actual_source = data.get("source", {}).get("name")
actual_root = data.get("output", {}).get("root")

mismatches = []
if actual_source != expected_source:
    mismatches.append(f"source.name: expected={expected_source!r} actual={actual_source!r}")
if actual_root != expected_root:
    mismatches.append(f"output.root: expected={expected_root!r} actual={actual_root!r}")

if mismatches:
    print(
        f"custom SDC_RAW_PARQUET_RUNTIME ({runtime_file}) does not match --route:",
        file=sys.stderr,
    )
    for m in mismatches:
        print(f"  - {m}", file=sys.stderr)
    sys.exit(1)
PY
}

generate_runtime_toml() {
  local dest="$1" name="$2" dsn_env_name="$3"
  cat > "$dest" <<EOF
[source]
name = "$name"
dsn_env = "$dsn_env_name"
schema = "public"
read_only = true

[output]
root = "$output_root"
snapshot_date = "$snapshot_date"
tmp_root = "$output_root/../_tmp/raw_export"
EOF
  chmod 600 "$dest"
}

if [[ -n "$custom_runtime_path" ]]; then
  if [[ ! -f "$custom_runtime_path" ]]; then
    printf 'Runtime config file not found: %s\n' "$custom_runtime_path" >&2
    exit 2
  fi
  if ! validate_custom_runtime "$custom_runtime_path" "$source_name" "$output_root"; then
    exit 2
  fi
  runtime_path="$custom_runtime_path"
else
  runtime_path="$work_dir/runtime.generated.toml"
  generate_runtime_toml "$runtime_path" "$source_name" "$dsn_env"
fi

raw_id_tables=(
  dart_xbrl_fact_raw
  dart_financial_statement_raw
  dart_shareholder_return_raw
  dart_share_count_raw
  dart_capital_change_raw
  dart_employee_raw
  dart_governance_raw
)

date_month_tables=(
  krx_security_flow_raw
  daily_ohlcv
  daily_market_cap
)

non_resumable_tables=(
  dart_filing_receipt_raw
  dart_xbrl_document
  dart_corp_master
  dart_corp_profile_history
  stock_master
  stock_master_snapshot
  stock_master_snapshot_items
  common_feature_series
  common_feature_observation_raw
)

all_tables=(
  "${raw_id_tables[@]}"
  "${date_month_tables[@]}"
  "${non_resumable_tables[@]}"
)

if [[ "$build_release" == "1" ]]; then
  log "Building raw parquet exporter release binary"
  cargo build --release --manifest-path tools/raw-parquet-exporter/Cargo.toml
elif [[ ! -x "$bin_path" ]]; then
  printf 'Exporter binary not found or not executable: %s\n' "$bin_path" >&2
  printf 'Run without --no-build first, or point SDC_RAW_PARQUET_BIN at a built binary.\n' >&2
  exit 2
fi

common_export_args=(
  --log-level error
  export
  --config "$config_path"
  --runtime "$runtime_path"
  --snapshot-date "$snapshot_date"
  --batch-rows "$batch_rows"
  --max-rows-per-file "$max_rows_per_file"
)

if [[ "$dry_run" == "1" ]]; then
  common_export_args+=(--dry-run)
fi

manifest_for_table() {
  local table="$1"
  printf '%s/snapshot_date=%s/source=%s/_manifests/table_manifests/%s.json' \
    "$output_root" "$snapshot_date" "$source_name" "$table"
}

checkpoints_dir() {
  printf '%s/snapshot_date=%s/source=%s/_manifests/checkpoints' \
    "$output_root" "$snapshot_date" "$source_name"
}

is_in_array() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

table_needs_all_chunks() {
  is_in_array "$1" "${raw_id_tables[@]}"
}

table_is_resumable() {
  is_in_array "$1" "${raw_id_tables[@]}" "${date_month_tables[@]}"
}

has_valid_completed_manifest() {
  local table="$1" manifest report
  manifest="$(manifest_for_table "$table")"
  [[ -f "$manifest" ]] || return 1
  report="$("$bin_path" validate --manifest "$manifest" 2>/dev/null)" || return 1
  printf '%s' "$report" | jq -e '.passed == true' >/dev/null 2>&1
}

incomplete_checkpoints_for_table() {
  local table="$1" dir file
  dir="$(checkpoints_dir)"
  [[ -d "$dir" ]] || return 0
  for file in "$dir/${table}-"*.json; do
    [[ -f "$file" ]] || continue
    if jq -e --arg table "$table" '.table == $table and .completed == false' "$file" \
      >/dev/null 2>&1; then
      printf '%s\n' "$file"
    fi
  done
}

export_table() {
  local table="$1"
  shift

  log "Exporting ${table} ($*)"
  "$bin_path" "${common_export_args[@]}" --tables "$table" "$@" || return 1

  if [[ "$validate" == "1" ]]; then
    local manifest
    manifest="$(manifest_for_table "$table")"
    log "Validating manifest for ${table}"
    "$bin_path" validate --manifest "$manifest" || return 1
  fi
}

resume_table() {
  local table="$1" checkpoint_file="$2"
  log "Resuming ${table} from $(basename "$checkpoint_file")"
  "$bin_path" --log-level error resume \
    --config "$config_path" --runtime "$runtime_path" --checkpoint "$checkpoint_file" || return 1

  if [[ "$validate" == "1" ]]; then
    local manifest
    manifest="$(manifest_for_table "$table")"
    log "Validating manifest for ${table}"
    "$bin_path" validate --manifest "$manifest" || return 1
  fi
}

maybe_validate_samples() {
  local table="$1"
  [[ "$validate_samples" == "1" ]] || return 0
  table_needs_all_chunks "$table" || return 0
  log "Validating PostgreSQL samples for ${table}"
  "$bin_path" validate-samples \
    --runtime "$runtime_path" --manifest "$(manifest_for_table "$table")" || return 1
}

process_table() {
  local table="$1"

  if [[ "${force_tables[$table]:-0}" == "1" ]]; then
    log "Force re-export requested for ${table}"
    if table_needs_all_chunks "$table"; then
      export_table "$table" --force --all-chunks || return 1
    else
      export_table "$table" --force || return 1
    fi
    maybe_validate_samples "$table" || return 1
    return 0
  fi

  if [[ "$force" == "1" ]]; then
    if table_needs_all_chunks "$table"; then
      export_table "$table" --force --all-chunks || return 1
    else
      export_table "$table" --force || return 1
    fi
    maybe_validate_samples "$table" || return 1
    return 0
  fi

  if has_valid_completed_manifest "$table"; then
    log "Skipping ${table} (valid manifest already present)"
    return 0
  fi

  local incomplete=()
  mapfile -t incomplete < <(incomplete_checkpoints_for_table "$table")

  case "${#incomplete[@]}" in
    0)
      if table_needs_all_chunks "$table"; then
        export_table "$table" --all-chunks || return 1
      else
        export_table "$table" || return 1
      fi
      maybe_validate_samples "$table" || return 1
      ;;
    1)
      if table_is_resumable "$table"; then
        resume_table "$table" "${incomplete[0]}" || return 1
        maybe_validate_samples "$table" || return 1
      else
        log "${table}: incomplete checkpoint on a non-resumable table; re-exporting with --force"
        export_table "$table" --force || return 1
      fi
      ;;
    *)
      log "ERROR: ${table} has ${#incomplete[@]} incomplete checkpoints; rerun with" \
        "--force-table ${table} to resolve:"
      printf '  %s\n' "${incomplete[@]}" >&2
      return 1
      ;;
  esac
}

running_pids=()
running_tables=()

reap_finished() {
  local i pid
  local still_pids=() still_tables=()
  for i in "${!running_pids[@]}"; do
    pid="${running_pids[$i]}"
    if kill -0 "$pid" 2>/dev/null; then
      still_pids+=("$pid")
      still_tables+=("${running_tables[$i]}")
    else
      wait "$pid" 2>/dev/null || true
    fi
  done
  running_pids=("${still_pids[@]}")
  running_tables=("${still_tables[@]}")
}

launch_table() {
  local table="$1"
  (
    if process_table "$table"; then
      printf 'ok' > "$work_dir/status/${table}"
    else
      printf 'failed' > "$work_dir/status/${table}"
    fi
  ) &
  running_pids+=("$!")
  running_tables+=("$table")
}

terminate_workers() {
  local pid
  for pid in "${running_pids[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in "${running_pids[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
  done
}

on_signal() {
  local exit_code="$1"
  log "Received termination signal; stopping workers..."
  terminate_workers
  exit "$exit_code"
}
trap 'on_signal 143' TERM
trap 'on_signal 130' INT

log "Raw Parquet full export starting in $app_dir"
log "route=${route} snapshot_date=${snapshot_date} source=${source_name} output_root=${output_root} jobs=${jobs}"
log "runtime_path=${runtime_path}"

started_at="$(TZ=Asia/Seoul date '+%Y-%m-%dT%H:%M:%S%z')"

for table in "${all_tables[@]}"; do
  while ((${#running_pids[@]} >= jobs)); do
    reap_finished
    ((${#running_pids[@]} >= jobs)) && sleep 0.2
  done
  launch_table "$table"
done

while ((${#running_pids[@]} > 0)); do
  reap_finished
  ((${#running_pids[@]} > 0)) && sleep 0.2
done

failed_tables=()
for table in "${all_tables[@]}"; do
  status="$(cat "$work_dir/status/${table}" 2>/dev/null || echo missing)"
  [[ "$status" == "ok" ]] || failed_tables+=("$table")
done

if ((${#failed_tables[@]} > 0)); then
  printf 'Raw Parquet export FAILED for %d table(s):\n' "${#failed_tables[@]}" >&2
  printf '  - %s\n' "${failed_tables[@]}" >&2
  printf 'Rerun this script (same --snapshot-date/--route) to resume.\n' >&2
  exit 1
fi

write_success_marker() {
  local manifests_dir success_path
  manifests_dir="${output_root}/snapshot_date=${snapshot_date}/source=${source_name}/_manifests"
  success_path="${manifests_dir}/_SUCCESS.json"
  mkdir -p "$manifests_dir"
  python3 - "$success_path" "$route" "$jobs" "$started_at" "$finished_at" "$manifests_dir" \
    "${all_tables[@]}" <<'PY'
import json
import os
import sys
from datetime import datetime, timedelta

success_path, route, jobs, started_at, finished_at, manifests_dir, *tables = sys.argv[1:]

table_entries = {}
for table in tables:
    manifest_path = os.path.join(manifests_dir, "table_manifests", f"{table}.json")
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    table_info = manifest["table"]
    schema = table_info.get("schema") or {}
    table_entries[table] = {
        "manifest_path": manifest_path,
        "rows_exported": table_info["rows_exported"],
        "schema_hash": schema.get("hash"),
    }

CHAIN_ANCHORS = [(18, 30), (20, 30), (23, 30), (4, 0)]
start = datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%S%z")
finish = datetime.strptime(finished_at, "%Y-%m-%dT%H:%M:%S%z")

collector_overlap = False
for day_offset in (-1, 0, 1):
    day = start + timedelta(days=day_offset)
    for hh, mm in CHAIN_ANCHORS:
        anchor = day.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if start <= anchor <= finish:
            collector_overlap = True
            break
    if collector_overlap:
        break

payload = {
    "route": route,
    "tables": table_entries,
    "started_at": started_at,
    "finished_at": finished_at,
    "jobs": int(jobs),
    "collector_overlap": collector_overlap,
    "snapshot_policy": "read_committed_per_chunk",
}

tmp_path = success_path + ".tmp"
with open(tmp_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)
os.replace(tmp_path, success_path)
PY
}

finished_at="$(TZ=Asia/Seoul date '+%Y-%m-%dT%H:%M:%S%z')"
if [[ "$dry_run" == "1" ]]; then
  log "Raw Parquet dry-run completed (no _SUCCESS.json written)"
else
  write_success_marker
  log "Raw Parquet full export completed; wrote _SUCCESS.json"
fi

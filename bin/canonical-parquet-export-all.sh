#!/usr/bin/env bash
set -euo pipefail

# DEPRECATED (refactor §5.3, §6). The derived facts (stock_metric_fact,
# common_feature_daily_fact) are no longer exported from Postgres — the DuckDB
# compute marts in research/etl/marts recompute them from the raw lake. Use
# bin/parquet-compute-all.sh instead. This script and
# tools/raw-parquet-exporter/config/export_canonical_tables.toml are retained
# only to re-materialize a one-off pre-drop backup of the legacy canonical lake.
#
# Export canonical/derived PostgreSQL tables to a SEPARATE Parquet lake root
# (data_lake/canonical_postgres) so the strict-raw lake stays pure.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_dir="${SDC_APP_DIR:-$(cd "$script_dir/.." && pwd)}"

cd "$app_dir"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

usage() {
  cat <<'EOF'
Usage: bin/canonical-parquet-export-all.sh [options]

Export canonical/derived PostgreSQL tables to data_lake/canonical_postgres.

Options:
  --snapshot-date YYYY-MM-DD   Output snapshot date. Defaults to today.
  --force                      Overwrite existing table output directories.
  --no-build                   Skip cargo release build.
  --no-validate                Skip manifest validation after each export.
  --dry-run                    Render export plans without writing Parquet files.
  -h, --help                   Show this help.

Environment overrides:
  SDC_CANON_PARQUET_SNAPSHOT_DATE   Same as --snapshot-date.
  SDC_CANON_PARQUET_FORCE=1         Same as --force.
  SDC_CANON_PARQUET_BUILD_RELEASE=0 Same as --no-build.
  SDC_CANON_PARQUET_VALIDATE=0      Same as --no-validate.
  SDC_CANON_PARQUET_DRY_RUN=1       Same as --dry-run.
  SDC_CANON_PARQUET_BATCH_ROWS      Default: 65536.
  SDC_CANON_PARQUET_MAX_ROWS_PER_FILE Default: 5000000.
  SDC_CANON_PARQUET_CONFIG          Default: tools/raw-parquet-exporter/config/export_canonical_tables.toml.
  SDC_CANON_PARQUET_RUNTIME         Default: tools/raw-parquet-exporter/config/canonical.example.toml.
  SDC_CANON_PARQUET_OUTPUT_ROOT     Default: \$STOCK_DATA_ROOT/kr/raw/_legacy_canonical.
  SDC_CANON_PARQUET_SOURCE_NAME     Default: local_mydb.
EOF
}

is_yyyy_mm_dd() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
}

snapshot_date="${SDC_CANON_PARQUET_SNAPSHOT_DATE:-$(date +%F)}"
force="${SDC_CANON_PARQUET_FORCE:-0}"
build_release="${SDC_CANON_PARQUET_BUILD_RELEASE:-1}"
validate="${SDC_CANON_PARQUET_VALIDATE:-1}"
dry_run="${SDC_CANON_PARQUET_DRY_RUN:-0}"
batch_rows="${SDC_CANON_PARQUET_BATCH_ROWS:-65536}"
max_rows_per_file="${SDC_CANON_PARQUET_MAX_ROWS_PER_FILE:-5000000}"
config_path="${SDC_CANON_PARQUET_CONFIG:-tools/raw-parquet-exporter/config/export_canonical_tables.toml}"
runtime_path="${SDC_CANON_PARQUET_RUNTIME:-tools/raw-parquet-exporter/config/canonical.example.toml}"
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
output_root="${SDC_CANON_PARQUET_OUTPUT_ROOT:-${STOCK_DATA_ROOT:-}/kr/raw/_legacy_canonical}"
source_name="${SDC_CANON_PARQUET_SOURCE_NAME:-local_mydb}"

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
    --force)
      force=1
      shift
      ;;
    --no-build)
      build_release=0
      shift
      ;;
    --no-validate)
      validate=0
      shift
      ;;
    --dry-run)
      dry_run=1
      validate=0
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

if ! [[ "$batch_rows" =~ ^[0-9]+$ ]] || ((batch_rows <= 0)); then
  printf 'Invalid SDC_CANON_PARQUET_BATCH_ROWS: %s\n' "$batch_rows" >&2
  exit 2
fi

if ! [[ "$max_rows_per_file" =~ ^[0-9]+$ ]] || ((max_rows_per_file <= 0)); then
  printf 'Invalid SDC_CANON_PARQUET_MAX_ROWS_PER_FILE: %s\n' "$max_rows_per_file" >&2
  exit 2
fi

if [[ ! -f "$config_path" ]]; then
  printf 'Config file not found: %s\n' "$config_path" >&2
  exit 2
fi

if [[ ! -f "$runtime_path" ]]; then
  printf 'Runtime config file not found: %s\n' "$runtime_path" >&2
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  printf 'cargo was not found in PATH. Install the Rust toolchain first.\n' >&2
  exit 2
fi

# All canonical tables use the full_table strategy (one-shot, no chunking).
tables=(
  stock_metric_fact
  common_feature_daily_fact
  metric_catalog
  metric_mapping_rule
  common_feature_catalog
)

bin_path="tools/raw-parquet-exporter/target/release/raw-parquet-exporter"

if [[ "$build_release" == "1" ]]; then
  log "Building raw parquet exporter release binary"
  cargo build --release --manifest-path tools/raw-parquet-exporter/Cargo.toml
elif [[ ! -x "$bin_path" ]]; then
  printf 'Release binary not found: %s\n' "$bin_path" >&2
  printf 'Run without --no-build first, or build it manually.\n' >&2
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

if [[ "$force" == "1" ]]; then
  common_export_args+=(--force)
fi

if [[ "$dry_run" == "1" ]]; then
  common_export_args+=(--dry-run)
fi

manifest_for_table() {
  local table="$1"
  printf '%s/snapshot_date=%s/source=%s/_manifests/table_manifests/%s.json' \
    "$output_root" "$snapshot_date" "$source_name" "$table"
}

export_table() {
  local table="$1"
  shift

  log "Exporting ${table}"
  "$bin_path" "${common_export_args[@]}" --tables "$table" "$@"

  if [[ "$validate" == "1" ]]; then
    local manifest
    manifest="$(manifest_for_table "$table")"
    log "Validating manifest for ${table}"
    "$bin_path" validate --manifest "$manifest"
  fi
}

log "Canonical Parquet export starting in $app_dir"
log "snapshot_date=${snapshot_date} source=${source_name} output_root=${output_root}"
if [[ "$force" == "1" ]]; then
  log "Existing table output directories will be overwritten"
fi

for table in "${tables[@]}"; do
  export_table "$table"
done

log "Canonical Parquet export completed"

#!/usr/bin/env bash
set -euo pipefail

# 전체 피처/테이블 프로파일링을 한 번에 다시 실행하는 wrapper.
# 실제 Python 엔트리포인트는 `collector profile ...` CLI이며,
# 여기서는 전체 실행(all) -> 직전 실행 대비 diff -> 선택적 docs publish 순서로 묶는다.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_dir="${SDC_APP_DIR:-$(cd "$script_dir/.." && pwd)}"

cd "$app_dir"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

usage() {
  cat <<'EOF'
Usage: bin/feature-profile-all.sh [options]

Run the full automated feature/table profiling pipeline.

Options:
  --target local|sj2           DB target. Defaults to local.
  --out-dir DIR                Output root. Defaults to reports/feature_profiles.
  --run-key KEY                Run directory/id. Defaults to YYYYMMDD_HHMMSS_<target>.
  --weight full,light          Catalog weights to include. Defaults to full,light.
  --role LIST                  Table roles to include (raw,derived,reference,operational).
                               Defaults to raw: profiling inspects collected data, so
                               only externally-sourced raw tables are profiled here.
  --formats LIST               Output formats. Defaults to ipynb,md,html,json,parquet.
  --sample-policy POLICY       auto, full, or sample. Defaults to auto.
  --sample-pct FLOAT           Override TABLESAMPLE percentage for expensive checks.
  --query-timeout-sec FLOAT    Per-query timeout. Defaults to 180.
  --drilldown                  Generate per-feature/per-metric drilldown files. Default.
  --no-drilldown               Skip drilldown files.
  --no-execute                 Write notebooks without executing them.
  --diff                       Run profile diff against previous latest. Default.
  --no-diff                    Skip profile diff.
  --baseline PATH              Explicit baseline _run_manifest.json for diff.
  --publish                    Publish lightweight Markdown/manifest artifacts to docs.
  --no-publish                 Do not publish to docs. Default.
  --docs-dir DIR               Override profile publish destination root.
  --dry-run                    Print commands without executing them.
  -h, --help                   Show this help.

Environment overrides:
  SDC_FEATURE_PROFILE_TARGET
  SDC_FEATURE_PROFILE_OUT_DIR
  SDC_FEATURE_PROFILE_RUN_KEY
  SDC_FEATURE_PROFILE_WEIGHT
  SDC_FEATURE_PROFILE_ROLE
  SDC_FEATURE_PROFILE_FORMATS
  SDC_FEATURE_PROFILE_SAMPLE_POLICY
  SDC_FEATURE_PROFILE_SAMPLE_PCT
  SDC_FEATURE_PROFILE_QUERY_TIMEOUT_SEC
  SDC_FEATURE_PROFILE_DRILLDOWN=0|1
  SDC_FEATURE_PROFILE_EXECUTE_NOTEBOOKS=0|1
  SDC_FEATURE_PROFILE_DIFF=0|1
  SDC_FEATURE_PROFILE_BASELINE
  SDC_FEATURE_PROFILE_PUBLISH=0|1
  SDC_FEATURE_PROFILE_DOCS_DIR
  SDC_FEATURE_PROFILE_DRY_RUN=0|1
  SDC_FEATURE_PROFILE_CMD          Default: uv run --extra analysis collector
EOF
}

is_number() {
  [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

is_safe_run_key() {
  [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]
}

print_command() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
}

run_command() {
  print_command "$@"
  if [[ "$dry_run" == "1" ]]; then
    return 0
  fi
  "$@"
}

target="${SDC_FEATURE_PROFILE_TARGET:-local}"
out_dir="${SDC_FEATURE_PROFILE_OUT_DIR:-reports/feature_profiles}"
run_key="${SDC_FEATURE_PROFILE_RUN_KEY:-}"
weight="${SDC_FEATURE_PROFILE_WEIGHT:-full,light}"
# Default to raw-only: profiling is for inspecting the data we *collect*, so the
# automated wrapper scopes to externally-sourced raw tables. derived/reference/
# operational tables (stock_metric_fact, *_catalog, ingestion_runs, ...) are
# pipeline outputs, not collected data, so they are intentionally excluded here.
# Pass --role / SDC_FEATURE_PROFILE_ROLE to widen (e.g. raw,derived) when needed.
role="${SDC_FEATURE_PROFILE_ROLE:-raw}"
formats="${SDC_FEATURE_PROFILE_FORMATS:-ipynb,md,html,json,parquet}"
sample_policy="${SDC_FEATURE_PROFILE_SAMPLE_POLICY:-auto}"
sample_pct="${SDC_FEATURE_PROFILE_SAMPLE_PCT:-}"
query_timeout_sec="${SDC_FEATURE_PROFILE_QUERY_TIMEOUT_SEC:-180}"
drilldown="${SDC_FEATURE_PROFILE_DRILLDOWN:-1}"
execute_notebooks="${SDC_FEATURE_PROFILE_EXECUTE_NOTEBOOKS:-1}"
run_diff="${SDC_FEATURE_PROFILE_DIFF:-1}"
baseline_manifest="${SDC_FEATURE_PROFILE_BASELINE:-}"
publish_docs="${SDC_FEATURE_PROFILE_PUBLISH:-0}"
docs_dir="${SDC_FEATURE_PROFILE_DOCS_DIR:-}"
dry_run="${SDC_FEATURE_PROFILE_DRY_RUN:-0}"
collector_cmd_raw="${SDC_FEATURE_PROFILE_CMD:-uv run --extra analysis collector}"

while (($#)); do
  case "$1" in
    --target)
      if (($# < 2)); then
        printf 'Missing value for --target\n' >&2
        exit 2
      fi
      target="$2"
      shift 2
      ;;
    --out-dir)
      if (($# < 2)); then
        printf 'Missing value for --out-dir\n' >&2
        exit 2
      fi
      out_dir="$2"
      shift 2
      ;;
    --run-key)
      if (($# < 2)); then
        printf 'Missing value for --run-key\n' >&2
        exit 2
      fi
      run_key="$2"
      shift 2
      ;;
    --weight)
      if (($# < 2)); then
        printf 'Missing value for --weight\n' >&2
        exit 2
      fi
      weight="$2"
      shift 2
      ;;
    --role)
      if (($# < 2)); then
        printf 'Missing value for --role\n' >&2
        exit 2
      fi
      role="$2"
      shift 2
      ;;
    --formats)
      if (($# < 2)); then
        printf 'Missing value for --formats\n' >&2
        exit 2
      fi
      formats="$2"
      shift 2
      ;;
    --sample-policy)
      if (($# < 2)); then
        printf 'Missing value for --sample-policy\n' >&2
        exit 2
      fi
      sample_policy="$2"
      shift 2
      ;;
    --sample-pct)
      if (($# < 2)); then
        printf 'Missing value for --sample-pct\n' >&2
        exit 2
      fi
      sample_pct="$2"
      shift 2
      ;;
    --query-timeout-sec)
      if (($# < 2)); then
        printf 'Missing value for --query-timeout-sec\n' >&2
        exit 2
      fi
      query_timeout_sec="$2"
      shift 2
      ;;
    --drilldown)
      drilldown=1
      shift
      ;;
    --no-drilldown)
      drilldown=0
      shift
      ;;
    --no-execute)
      execute_notebooks=0
      shift
      ;;
    --diff)
      run_diff=1
      shift
      ;;
    --no-diff)
      run_diff=0
      shift
      ;;
    --baseline)
      if (($# < 2)); then
        printf 'Missing value for --baseline\n' >&2
        exit 2
      fi
      baseline_manifest="$2"
      run_diff=1
      shift 2
      ;;
    --publish)
      publish_docs=1
      shift
      ;;
    --no-publish)
      publish_docs=0
      shift
      ;;
    --docs-dir)
      if (($# < 2)); then
        printf 'Missing value for --docs-dir\n' >&2
        exit 2
      fi
      docs_dir="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=1
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

case "$target" in
  local|sj2) ;;
  *)
    printf 'Invalid target: %s (expected local or sj2)\n' "$target" >&2
    exit 2
    ;;
esac

case "$sample_policy" in
  auto|full|sample) ;;
  *)
    printf 'Invalid sample policy: %s (expected auto, full, or sample)\n' "$sample_policy" >&2
    exit 2
    ;;
esac

if [[ -z "$weight" ]]; then
  printf 'Invalid weight: must not be empty\n' >&2
  exit 2
fi

if [[ -z "$role" ]]; then
  printf 'Invalid role: must not be empty\n' >&2
  exit 2
fi

if [[ -z "$formats" ]]; then
  printf 'Invalid formats: must not be empty\n' >&2
  exit 2
fi

if [[ -n "$sample_pct" ]] && ! is_number "$sample_pct"; then
  printf 'Invalid sample pct: %s\n' "$sample_pct" >&2
  exit 2
fi

if ! is_number "$query_timeout_sec"; then
  printf 'Invalid query timeout seconds: %s\n' "$query_timeout_sec" >&2
  exit 2
fi

if [[ -z "$run_key" ]]; then
  run_key="$(date '+%Y%m%d_%H%M%S')_${target}"
fi

if ! is_safe_run_key "$run_key"; then
  printf 'Invalid run key: %s (allowed: letters, digits, dot, underscore, hyphen)\n' \
    "$run_key" >&2
  exit 2
fi

read -r -a collector_cmd <<< "$collector_cmd_raw"
if ((${#collector_cmd[@]} == 0)); then
  printf 'SDC_FEATURE_PROFILE_CMD must not be empty\n' >&2
  exit 2
fi

if ! command -v "${collector_cmd[0]}" >/dev/null 2>&1; then
  printf 'Command not found: %s\n' "${collector_cmd[0]}" >&2
  exit 2
fi

target_dir="$out_dir/$target"

# diff 기준은 실행 전에 잡아둔다. profile all 완료 후 latest가 새 run을 가리키기 때문이다.
if [[ -z "$baseline_manifest" && -f "$target_dir/latest/_run_manifest.json" ]]; then
  baseline_manifest="$(cd "$target_dir/latest" && pwd -P)/_run_manifest.json"
fi

if [[ -n "$baseline_manifest" && ! -f "$baseline_manifest" ]]; then
  printf 'Baseline manifest not found: %s\n' "$baseline_manifest" >&2
  exit 2
fi

profile_all_args=(
  profile all
  --target "$target"
  --weight "$weight"
  --role "$role"
  --formats "$formats"
  --out-dir "$out_dir"
  --run-id "$run_key"
  --run-date "$run_key"
  --sample-policy "$sample_policy"
  --query-timeout-sec "$query_timeout_sec"
)

# long-format 테이블(metric_code/feature_code/series_id)은 기본적으로 세부 파일까지 생성한다.
if [[ "$drilldown" == "1" ]]; then
  profile_all_args+=(--drilldown)
fi

if [[ "$execute_notebooks" == "0" ]]; then
  profile_all_args+=(--no-execute)
fi

if [[ -n "$sample_pct" ]]; then
  profile_all_args+=(--sample-pct "$sample_pct")
fi

log "Full feature profiling starting in $app_dir"
log "target=${target} run_key=${run_key} out_dir=${out_dir} role=${role} formats=${formats}"

run_command "${collector_cmd[@]}" "${profile_all_args[@]}"

if [[ "$run_diff" == "1" ]]; then
  if [[ -z "$baseline_manifest" ]]; then
    log "No previous baseline manifest found; skipping profile diff"
  elif [[ "$(basename "$(dirname "$baseline_manifest")")" == "$run_key" ]]; then
    log "Baseline is the current run_key; skipping profile diff"
  else
    # 직전 latest와 이번 실행 결과의 manifest를 비교해 drift_report.md를 생성한다.
    log "Generating profile diff against $baseline_manifest"
    run_command "${collector_cmd[@]}" profile diff \
      --target "$target" \
      --out-dir "$out_dir" \
      --baseline "$baseline_manifest" \
      --candidate "$run_key"
  fi
fi

if [[ "$publish_docs" == "1" ]]; then
  publish_args=(
    profile publish
    --target "$target"
    --out-dir "$out_dir"
    --run-id "$run_key"
  )
  if [[ -n "$docs_dir" ]]; then
    publish_args+=(--docs-dir "$docs_dir")
  fi

  # docs에는 대형 html/ipynb/parquet가 아니라 Markdown/manifest 같은 경량 산출물만 반영한다.
  log "Publishing lightweight profiling artifacts to docs"
  run_command "${collector_cmd[@]}" "${publish_args[@]}"
fi

log "Full feature profiling completed: ${target_dir}/${run_key}"

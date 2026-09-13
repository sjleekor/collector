#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fake_bin="$repo_root/tests/shell/fixtures/fake-raw-parquet-exporter.py"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

run_wrapper() {
  local capture="$1"
  shift
  SDC_APP_DIR="$repo_root" \
  SDC_RAW_PARQUET_BIN="$fake_bin" \
  SDC_RAW_PARQUET_BUILD_RELEASE=0 \
  SDC_RAW_PARQUET_OUTPUT_ROOT="$tmp_dir/data_lake/raw_postgres" \
  SDC_RAW_PARQUET_SNAPSHOT_DATE="2026-07-30" \
  FAKE_EXPORTER_RUNTIME_CAPTURE="$capture" \
  "$repo_root/bin/raw-parquet-export-all.sh" --dry-run "$@"
}

# -- --route local derives local_mydb / DB_DSN -------------------------------
local_capture="$tmp_dir/local-runtime.toml"
run_wrapper "$local_capture" --route local >"$tmp_dir/local.out" 2>&1

if ! grep -q 'name = "local_mydb"' "$local_capture"; then
  echo "expected generated runtime to have source.name=local_mydb" >&2
  cat "$local_capture" >&2
  exit 1
fi
if ! grep -q 'dsn_env = "DB_DSN"' "$local_capture"; then
  echo "expected generated runtime to have dsn_env=DB_DSN" >&2
  cat "$local_capture" >&2
  exit 1
fi

# -- --route remote derives sj2_remote / SDC_REMOTE_DSN ----------------------
remote_capture="$tmp_dir/remote-runtime.toml"
run_wrapper "$remote_capture" --route remote >"$tmp_dir/remote.out" 2>&1

if ! grep -q 'name = "sj2_remote"' "$remote_capture"; then
  echo "expected generated runtime to have source.name=sj2_remote" >&2
  cat "$remote_capture" >&2
  exit 1
fi
if ! grep -q 'dsn_env = "SDC_REMOTE_DSN"' "$remote_capture"; then
  echo "expected generated runtime to have dsn_env=SDC_REMOTE_DSN" >&2
  cat "$remote_capture" >&2
  exit 1
fi

# -- generated runtime dir is cleaned up on exit (trap) ----------------------
runtime_path="$(grep -o 'runtime_path=.*' "$tmp_dir/local.out" | head -1 | cut -d= -f2-)"
if [[ -z "$runtime_path" ]]; then
  echo "expected wrapper to log runtime_path" >&2
  exit 1
fi
if [[ -e "$runtime_path" ]]; then
  echo "expected generated runtime dir to be removed after exit: $runtime_path" >&2
  exit 1
fi

# -- custom SDC_RAW_PARQUET_RUNTIME mismatching --route fails fast -----------
bad_runtime="$tmp_dir/bad-runtime.toml"
cat > "$bad_runtime" <<EOF
[source]
name = "totally_wrong_name"
dsn_env = "DB_DSN"
schema = "public"
read_only = true

[output]
root = "$tmp_dir/data_lake/raw_postgres"
snapshot_date = "2026-07-30"
tmp_root = "$tmp_dir/data_lake/_tmp/raw_export"
EOF

set +e
SDC_APP_DIR="$repo_root" \
SDC_RAW_PARQUET_BIN="$fake_bin" \
SDC_RAW_PARQUET_BUILD_RELEASE=0 \
SDC_RAW_PARQUET_OUTPUT_ROOT="$tmp_dir/data_lake/raw_postgres" \
SDC_RAW_PARQUET_SNAPSHOT_DATE="2026-07-30" \
SDC_RAW_PARQUET_RUNTIME="$bad_runtime" \
"$repo_root/bin/raw-parquet-export-all.sh" --dry-run --route local >"$tmp_dir/mismatch.out" 2>&1
status=$?
set -e

if [[ "$status" == "0" ]]; then
  echo "expected mismatched custom runtime to fail fast, got exit 0" >&2
  cat "$tmp_dir/mismatch.out" >&2
  exit 1
fi
if ! grep -q "does not match --route" "$tmp_dir/mismatch.out"; then
  echo "expected mismatch error message" >&2
  cat "$tmp_dir/mismatch.out" >&2
  exit 1
fi
if ! grep -q "expected='local_mydb'" "$tmp_dir/mismatch.out"; then
  echo "expected mismatch message to name the expected value" >&2
  cat "$tmp_dir/mismatch.out" >&2
  exit 1
fi

echo "raw-parquet-export-route ok"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fake_bin="$repo_root/tests/shell/fixtures/fake-raw-parquet-exporter.py"
wrapper="$repo_root/bin/raw-parquet-export-all.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

lake_root="$tmp_dir/data_lake/raw_postgres"
manifests_dir="$lake_root/snapshot_date=2026-07-30/source=local_mydb/_manifests"
success_marker="$manifests_dir/_SUCCESS.json"

run_wrapper() {
  SDC_APP_DIR="$repo_root" \
  SDC_RAW_PARQUET_BIN="$fake_bin" \
  SDC_RAW_PARQUET_BUILD_RELEASE=0 \
  SDC_RAW_PARQUET_OUTPUT_ROOT="$lake_root" \
  SDC_RAW_PARQUET_SNAPSHOT_DATE="2026-07-30" \
  "$wrapper" --route local "$@"
}

assert_exit() {
  local expected="$1" actual="$2" label="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "FAIL ($label): expected exit $expected, got $actual" >&2
    exit 1
  fi
}

# -- scenario 1: one table fails -> others complete, non-zero, no _SUCCESS.json ---
set +e
FAKE_EXPORTER_FAIL_TABLES="dart_share_count_raw" run_wrapper --jobs 3 \
  >"$tmp_dir/run1.out" 2>&1
status=$?
set -e
assert_exit 1 "$status" "scenario1 initial failure"
if ! grep -q "FAILED for 1 table" "$tmp_dir/run1.out"; then
  echo "FAIL: expected failure summary naming 1 table" >&2
  cat "$tmp_dir/run1.out" >&2
  exit 1
fi
if ! grep -q "dart_share_count_raw" "$tmp_dir/run1.out"; then
  echo "FAIL: expected failed table to be named" >&2
  exit 1
fi
if [[ -f "$success_marker" ]]; then
  echo "FAIL: _SUCCESS.json must not exist after a failed run" >&2
  exit 1
fi

# -- scenario 2: rerun -> completed tables skip, failed table retried and lands ---
run_wrapper --jobs 3 >"$tmp_dir/run2.out" 2>&1
skip_count="$(grep -c "Skipping" "$tmp_dir/run2.out" || true)"
if ((skip_count != 12)); then
  echo "FAIL: expected 12 tables to be skipped on rerun, got $skip_count" >&2
  cat "$tmp_dir/run2.out" >&2
  exit 1
fi
if ! grep -q "Exporting dart_share_count_raw" "$tmp_dir/run2.out"; then
  echo "FAIL: expected the previously-failed table to be re-exported" >&2
  exit 1
fi
if [[ ! -f "$success_marker" ]]; then
  echo "FAIL: expected _SUCCESS.json after a fully successful run" >&2
  exit 1
fi
if ! python3 -c "
import json, sys
with open('$success_marker') as f:
    data = json.load(f)
expected = {
    'dart_xbrl_fact_raw', 'dart_financial_statement_raw', 'dart_shareholder_return_raw',
    'dart_share_count_raw', 'krx_security_flow_raw', 'daily_ohlcv', 'dart_xbrl_document',
    'dart_corp_master', 'stock_master', 'stock_master_snapshot', 'stock_master_snapshot_items',
    'common_feature_series', 'common_feature_observation_raw',
}
assert set(data['tables']) == expected, data['tables'].keys()
assert data['route'] == 'local'
assert data['jobs'] == 3
sys.exit(0)
"; then
  echo "FAIL: _SUCCESS.json table set did not match the expected 13 tables" >&2
  exit 1
fi

rm -rf "$tmp_dir/data_lake"

# -- scenario 3: resumable table (date_month) left with incomplete checkpoint ----
FAKE_EXPORTER_LEAVE_CHECKPOINT="daily_ohlcv" run_wrapper --jobs 3 \
  >"$tmp_dir/run3.out" 2>&1 || true

run_wrapper --jobs 3 --no-validate >"$tmp_dir/run4.out" 2>&1
if ! grep -q "Resuming daily_ohlcv" "$tmp_dir/run4.out"; then
  echo "FAIL: expected daily_ohlcv to resume from its checkpoint" >&2
  cat "$tmp_dir/run4.out" >&2
  exit 1
fi
if grep -q "^\[.*\] Exporting daily_ohlcv " "$tmp_dir/run4.out"; then
  echo "FAIL: daily_ohlcv should resume, not export fresh" >&2
  exit 1
fi

rm -rf "$tmp_dir/data_lake"

# -- scenario 4: non-resumable table left with incomplete checkpoint -> --force ---
FAKE_EXPORTER_LEAVE_CHECKPOINT="stock_master" run_wrapper --jobs 3 \
  >"$tmp_dir/run5.out" 2>&1 || true

run_wrapper --jobs 3 >"$tmp_dir/run6.out" 2>&1
if ! grep -q "non-resumable table; re-exporting with --force" "$tmp_dir/run6.out"; then
  echo "FAIL: expected non-resumable-table fallback message for stock_master" >&2
  cat "$tmp_dir/run6.out" >&2
  exit 1
fi
if ! grep -q "Exporting stock_master (--force)" "$tmp_dir/run6.out"; then
  echo "FAIL: expected stock_master to be re-exported with --force" >&2
  exit 1
fi

# -- scenario 5: two stale incomplete checkpoints for one table -> error ----------
rm -rf "$tmp_dir/data_lake"
checkpoints_dir="$manifests_dir/checkpoints"
mkdir -p "$checkpoints_dir"
cat > "$checkpoints_dir/stock_master-stale-1.json" <<'EOF'
{"version":1,"strategy":"full_table","run_id":"stock_master-stale-1","completed":false,
 "table":"stock_master","source":{"name":"local_mydb","schema":"public",
 "snapshot_date":"2026-07-30","snapshot_policy":"full_table_read_committed"},
 "extract_predicate":"stale","extract_start_raw_id":0,"final_exclusive_end":0,
 "next_raw_id":0,"chunk_rows":0,"batch_rows":65536,"max_rows_per_file":5000000,
 "chunks_planned":0,"chunks_completed":0,"rows_exported":0,"files":[],"schema":null,
 "partitions":[],"manifest_file":null,"updated_at_unix_seconds":1}
EOF
cp "$checkpoints_dir/stock_master-stale-1.json" "$checkpoints_dir/stock_master-stale-2.json"
python3 -c "
import json
p = '$checkpoints_dir/stock_master-stale-2.json'
d = json.load(open(p))
d['run_id'] = 'stock_master-stale-2'
json.dump(d, open(p, 'w'))
"

set +e
run_wrapper --jobs 3 >"$tmp_dir/run7.out" 2>&1
status=$?
set -e
assert_exit 1 "$status" "scenario5 multiple checkpoints"
if ! grep -q "ERROR: stock_master has 2 incomplete checkpoints" "$tmp_dir/run7.out"; then
  echo "FAIL: expected multi-checkpoint error naming stock_master" >&2
  cat "$tmp_dir/run7.out" >&2
  exit 1
fi
if ! grep -q -- "--force-table stock_master" "$tmp_dir/run7.out"; then
  echo "FAIL: expected the error to suggest --force-table stock_master" >&2
  exit 1
fi

# -- scenario 6: --force-table resolves the multi-checkpoint state ---------------
run_wrapper --jobs 3 --force-table stock_master >"$tmp_dir/run8.out" 2>&1
if ! grep -q "Force re-export requested for stock_master" "$tmp_dir/run8.out"; then
  echo "FAIL: expected --force-table to force re-export stock_master" >&2
  cat "$tmp_dir/run8.out" >&2
  exit 1
fi
if [[ ! -f "$success_marker" ]]; then
  echo "FAIL: expected a fresh _SUCCESS.json after --force-table resolved the run" >&2
  exit 1
fi

# -- scenario 7: SIGTERM mid-run stops all workers, no zombies, exit 143 ---------
# Runs the script directly (not via the run_wrapper function) so $! below is the
# script's own PID, not an extra function-call subshell's PID -- otherwise SIGTERM
# would hit the subshell and never reach the script's own TERM trap.
rm -rf "$tmp_dir/data_lake"
SDC_APP_DIR="$repo_root" \
SDC_RAW_PARQUET_BIN="$fake_bin" \
SDC_RAW_PARQUET_BUILD_RELEASE=0 \
SDC_RAW_PARQUET_OUTPUT_ROOT="$lake_root" \
SDC_RAW_PARQUET_SNAPSHOT_DATE="2026-07-30" \
FAKE_EXPORTER_SLEEP_SECONDS=8 \
"$wrapper" --route local --jobs 3 \
  >"$tmp_dir/run9.out" 2>"$tmp_dir/run9.err" &
wrapper_pid=$!
sleep 1.5
kill -TERM "$wrapper_pid"
set +e
wait "$wrapper_pid"
status=$?
set -e
assert_exit 143 "$status" "scenario7 SIGTERM"
sleep 0.5
if pgrep -f "fake-raw-parquet-exporter" >/dev/null 2>&1; then
  echo "FAIL: fake exporter process(es) still running after SIGTERM" >&2
  pgrep -fl "fake-raw-parquet-exporter" >&2
  exit 1
fi
if [[ -f "$success_marker" ]]; then
  echo "FAIL: _SUCCESS.json must not exist after a SIGTERM'd run" >&2
  exit 1
fi

echo "raw-parquet-export-resume ok"

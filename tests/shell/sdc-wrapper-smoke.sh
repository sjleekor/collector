#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$repo_root/deploy/prod/bin/lib/sdc-wrapper.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

export SDC_LOCK_DIR="$tmp_dir/locks"
export SDC_THROTTLE_DIR="$tmp_dir/throttle"
export SDC_LOCK_BACKEND=mkdir
export SDC_LOCK_WAIT_SECONDS=0
export SDC_LOCK_CONFLICT_MODE=fail

mkdir -p "$SDC_LOCK_DIR" "$SDC_THROTTLE_DIR"

mkdir "$SDC_LOCK_DIR/alpha.lock.d"
set +e
sdc_with_source_lock alpha true >"$tmp_dir/alpha.out" 2>&1
status=$?
set -e
if [[ "$status" != "75" ]]; then
  echo "expected alpha lock conflict to exit 75, got $status" >&2
  exit 1
fi

export SDC_LOCK_CONFLICT_MODE=skip
sdc_with_source_lock alpha true >"$tmp_dir/alpha-skip.out" 2>&1
if ! grep -q "mode=skip" "$tmp_dir/alpha-skip.out"; then
  echo "expected skip conflict log" >&2
  exit 1
fi

export SDC_LOCK_CONFLICT_MODE=fail
sdc_with_source_lock beta true >"$tmp_dir/beta.out" 2>&1
if [[ -d "$SDC_LOCK_DIR/beta.lock.d" ]]; then
  echo "expected beta lock directory to be released" >&2
  exit 1
fi

export SDC_FDR_MIN_INTERVAL_SECONDS=1
date +%s > "$SDC_THROTTLE_DIR/fdr.last"
sdc_with_source_lock fdr true >"$tmp_dir/throttle.out" 2>&1
if ! grep -q "throttle sleep: domain=fdr" "$tmp_dir/throttle.out"; then
  echo "expected fdr throttle sleep log" >&2
  exit 1
fi

echo "sdc wrapper smoke ok"

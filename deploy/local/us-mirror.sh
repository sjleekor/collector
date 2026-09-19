#!/usr/bin/env bash
# sj2-server 의 미국 레이크를 이 맥으로 당긴다 (D18의 2단 구조).
#
# **한 방향이다.** 서버가 정본이고 맥은 읽기용 사본이다. `--delete` 를 쓰므로
# 맥에서 고친 것은 사라진다 — 고칠 일이 있으면 서버에서 고친다.
#
# 기본은 derived/ 만 당긴다. modeler 가 읽는 것이 스냅샷이고, raw/ 22GB 중
# 14GB 가 dolt clone 이라 맥에서 쓸 일이 없다.
set -euo pipefail

REMOTE="${SDC_REMOTE_HOST:-whi@sj2-server}"
REMOTE_DIR="${SDC_US_REMOTE_DIR:-/home/whi/data/stock_data/us}"
LOCAL_DIR="${STOCK_DATA_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../stock_data" && pwd)}/us"

paths=(derived/ datasets/ output/)
rsync_opts=(-a --delete --info=progress2 --human-readable)

for arg in "$@"; do
  case "$arg" in
    --all)     paths=(derived/ datasets/ output/ raw/) ;;
    --raw)     paths=(raw/) ;;
    --dry-run) rsync_opts+=(--dry-run) ;;
    *) printf 'usage: %s [--all|--raw] [--dry-run]\n' "$0" >&2; exit 2 ;;
  esac
done

printf '%s -> %s\n' "$REMOTE:$REMOTE_DIR" "$LOCAL_DIR"
for p in "${paths[@]}"; do
  mkdir -p "$LOCAL_DIR/$p"
  printf '\n=== %s ===\n' "$p"
  rsync "${rsync_opts[@]}" "$REMOTE:$REMOTE_DIR/$p" "$LOCAL_DIR/$p"
done

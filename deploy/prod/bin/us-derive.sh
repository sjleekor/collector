#!/usr/bin/env bash
# 미국 raw 를 derived 스냅샷으로 굳힌다. **주 1회다** (미국 계획 04 C8 · 05 §4.1).
#
# `us-daily.sh` 는 raw 만 받는다. 굳히는 자리가 없어서 `dolt pull` 은 매일 도는데
# `prices_daily` 스냅샷은 2026-09-09 에서 멈춰 있었다 (2026-09-21 확인).
#
# **락 도메인을 `us` 로 같이 쓴다.** 15:00 수집과 겹치면 반쯤 받은 raw 를 굳히게
# 된다. 같은 도메인이면 뒤에 온 쪽이 기다린다.
#
# **무엇을 굳힐지는 일정이 아니라 입력이 정한다** — dolt 는 커밋 해시, 나머지는
# raw 의 mtime 을 스냅샷과 비교한다. 그래서 주 1회로 걸어도 분기짜리 SEC 표는
# 분기에 한 번만 새로 쌓인다.
#
# **유니버스는 여기 없다.** 월 1회 재판정이라 `collector us-universe rebuild`
# 가 따로 한다 (03 §5.3).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

# 예산을 넉넉히 준다. `fundamentals` 가 가장 크고 그것만 수 분이다.
BUDGET_SECONDS="${SDC_US_DERIVE_BUDGET_SECONDS:-7200}"

args=(us-derive run --budget-seconds "$BUDGET_SECONDS")
if [[ -n "${SDC_US_DERIVE_TABLES:-}" ]]; then
  args+=(--tables "$SDC_US_DERIVE_TABLES")
fi
# 손으로 부를 때 `--dry-run`·`--force` 를 넘길 수 있어야 한다. 안 넘기면
# dry-run 인 줄 알고 실제 실행을 돌리게 된다 — 2026-09-21 에 실제로 그랬다.
args+=("$@")

# 15:00 수집이 예산 30분을 다 쓰고 있을 수 있다.
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-2400}"
sdc_run_daily_collector us "${args[@]}"

# **티커 맵을 최신으로 유지한다** (2026-09-21). `universe_daily.cik` 이 이제
# PIT 맵을 쓴다 — 안 갱신하면 새로 상장한 종목이 `cik` 을 못 받는다.
# Wayback 은 늦게 따라오므로 SEC 의 지금 맵도 `as_of=오늘` 로 같이 굳힌다.
#
# **굳히기(derive) 뒤에 둔다.** 여기서 실패해도 derive 는 이미 끝나 있고,
# 실패는 잡 exit code 로 그대로 드러난다 — 조용히 삼키지 않는다.
if [[ "${SDC_US_SKIP_TICKERS:-0}" != "1" ]]; then
  sdc_run_collector_with_lock us us-tickers sync
fi

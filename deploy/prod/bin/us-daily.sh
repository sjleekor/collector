#!/usr/bin/env bash
# 미국 상시 운영 한 번 (미국 계획 04 C8 · D18).
#
# 한국 래퍼들과 같은 모양이다 — 락·throttle·고아 컨테이너 수거를 lib 에서 받는다.
# 락 도메인이 `us` 하나인 이유는 **원천 일곱을 한 명령이 순서대로 보기 때문**이다.
# 겹치면 안 되는 것은 원천끼리가 아니라 이 실행끼리다.
#
# 예산을 둔다. 며칠 밀렸으면 할 일이 수백 건인데 한 번에 다 하려다 죽으면
# 어디까지 했는지 모른다. 남은 것은 다음 실행이 이어서 한다 (04 §2.4).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

BUDGET_SECONDS="${SDC_US_BUDGET_SECONDS:-1800}"

args=(us-daily run --budget-seconds "$BUDGET_SECONDS")
if [[ -n "${SDC_US_SOURCES:-}" ]]; then
  args+=(--sources "$SDC_US_SOURCES")
fi

# 락 대기를 넉넉히 준다. 예산이 30분이라 앞 실행이 아직 돌고 있을 수 있다.
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-2400}"
sdc_run_daily_collector us "${args[@]}"

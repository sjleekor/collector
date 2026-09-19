#!/usr/bin/env bash
# 미국 상시 운영 한 번 (04 C8 · 05 §4.1).
#
# launchd 가 이 파일을 부른다. **direnv 로 감싼다** — STOCK_DATA_ROOT ·
# SEC_USER_AGENT · FRED_API_KEY 가 없으면 즉시 에러로 끝나는 편이 낫다.
# launchd 는 로그인 셸 환경을 물려주지 않으므로 여기서 직접 만든다.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log_dir="${COLLECTOR_US_LOG_DIR:-$HOME/Library/Logs/collector-us}"
mkdir -p "$log_dir"

# 예산을 둔다. 맥이 며칠 꺼져 있었으면 할 일이 수백 건이라 한 번에 못 끝낸다 —
# 남은 것은 다음 실행이 이어서 한다 (04 §2.4).
budget="${COLLECTOR_US_BUDGET_SECONDS:-1800}"

exec >>"$log_dir/us-daily.log" 2>&1
echo "=== $(date -u '+%Y-%m-%dT%H:%M:%SZ') us-daily 시작 ==="
cd "$repo_dir"
# set -e 를 끄고 받는다. 안 그러면 실패했을 때 끝 표시를 못 남기고 죽는다
set +e
direnv exec . uv run collector us-daily run --budget-seconds "$budget"
status=$?
set -e
echo "=== $(date -u '+%Y-%m-%dT%H:%M:%SZ') us-daily 끝 (exit $status) ==="
exit "$status"

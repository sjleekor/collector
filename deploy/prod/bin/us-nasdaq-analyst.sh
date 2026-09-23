#!/usr/bin/env bash
# Nasdaq 애널리스트 추정치 — 주 1회 전진 축적 전용 (source_expansion 04·99 순번 4).
#
# 이 원천은 과거를 복원 못 한다(historicalConsensus 13개월·`earnings-forecast`에
# asOf 없음) — 오늘부터 쌓는 것 말고는 방법이 없다.
#
# **`us` 락 도메인을 같이 쓰지 않는다.** `us-daily.sh`(매일 15:00)·`us-derive.sh`
# (일 16:00)는 원천 일곱을 순서대로 보는 한 실행이라 서로 겹치면 안 되지만,
# 이 잡은 종목당 1요청이라 전체를 돌면 몇 시간이 걸린다(04 §4). 같은 도메인을
# 쓰면 그동안 저 둘이 lock 대기에서 막힌다. 건드리는 raw 하위 트리도 겹치지
# 않는다 — `raw/nasdaq/analyst_earnings_forecast/`뿐이고 dolt·SEC 등은 안
# 건드린다. 그래서 자기 도메인(`us_nasdaq_analyst`)을 쓴다.
#
# **같은 주에 다시 돌려도 안전하다.** 파티션 키가 ISO 주 월요일이고(`week_start`),
# 이미 받은 심볼은 건너뛰고 이어받는다 — 예산이 모자라면 다음 실행이 마저 한다.
# 예산 안에서 끝내지 못하면 사람이 손으로 다시 돌려도 된다: 이미 받은 심볼은
# 다시 안 건드리므로 비용이 거의 없다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

# 8.5시간. 처음에는 6시간이었는데 **첫 실행(2026-09-24)에서 모자랐다** —
# 유니버스 4,081종목 · 실측 종목당 약 6초(간격 5초 + 응답)라 한 바퀴가 약
# 6.8시간이다. 예산을 넘기면 남은 종목은 **그 주에 다시 못 받는다** — 다음
# 토요일은 새 ISO 주 파티션이다. 그리고 순서가 알파벳이라 **매주 같은 뒤쪽
# 종목이 빠진다.** 토 09:00 시작이면 17:30에 끝난다. 15:00 `sdc_daily_us` 와는
# 락 도메인도 원천 호스트도 달라 겹쳐도 된다.
BUDGET_SECONDS="${SDC_US_NASDAQ_ANALYST_BUDGET_SECONDS:-30600}"

args=(us-nasdaq-analyst run --budget-seconds "$BUDGET_SECONDS")
# 손으로 부를 때 `--dry-run`·`--symbols`·`--today` 등을 넘길 수 있어야 한다.
# Cronicle 은 인자 없이 부른다 (us-daily.sh·us-derive.sh 와 같은 이유).
args+=("$@")

# 자기 lock 도메인. throttle 은 `sdc_min_interval_seconds`의 기본 분기(0s)를
# 그대로 쓴다 — 원천 간격은 이미 Python 클라이언트 안에 있다(5초, us 도메인의
# Nasdaq 실적 캘린더와 같은 값).
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-60}"
sdc_run_daily_collector us_nasdaq_analyst "${args[@]}"

#!/usr/bin/env bash
# Nasdaq 실적 캘린더 — 미래 날짜 칸을 관측일마다 쌓는 전진 축적 전용.
#
# `earnings_calendar`는 날짜가 지난 뒤에 받은 것뿐이라 예측 시점에 미래 실적일이
# 어떻게 보였는지 남아 있지 않다. 이 잡이 `raw/nasdaq/earnings_calendar_forward/
# observed=<T>/date=<d>.json`으로 그것을 쌓는다. 지난 뒤에는 복원할 수 없다.
#
# 같은 날 다시 돌려도 안전하다 — 이미 있는 (T, d) 파일은 건드리지 않는다.
# Cronicle에는 두 번 건다: 매일 `--horizon-days 14`(기본값), 주 1회 `--horizon-days 100`.
#
# `us` 락 도메인을 같이 쓰지 않는다. 애널리스트 잡과 같은 이유다 — 다른 raw
# 하위 트리를 건드리고, 15:00 `sdc_daily_us`와 일요일 derive를 막을 까닭이 없다.
# 요청은 매일 약 10건·주 1회 약 71건이라 몇 분이면 끝난다.
#
# 막히면 두드리지 않는다. 요청 하나라도 실패하면 그 실행은 남은 날짜를 받지 않고 끝나며
# (종료 코드 1), `earnings_calendar_forward/_blocked.json`에 쉬는 기한을 적는다. 기한 안의
# 실행은 요청 없이 0으로 끝난다. 연달아 막히면 2·4·8·16·30일로 늘고, 한 번 다 성공하면
# 지운다. Cronicle 쪽 재시도는 0으로 둔다 — 재시도는 코드가 정한다(네트워크 오류만 한 번).
# 손으로 확인할 때만 `--ignore-block`.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

HORIZON_DAYS="${SDC_US_EARNINGS_FORWARD_HORIZON_DAYS:-14}"

args=(us-nasdaq-earnings-forward run --horizon-days "$HORIZON_DAYS")
# 뒤에 오는 인자가 이긴다(argparse) — Cronicle이 `--horizon-days 100`을 넘기면 그 값이 쓰인다.
args+=("$@")

sdc_prepare_us_run "raw/nasdaq" "$@" || exit $?

SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-60}"
sdc_run_daily_collector us_nasdaq_earnings_forward "${args[@]}"

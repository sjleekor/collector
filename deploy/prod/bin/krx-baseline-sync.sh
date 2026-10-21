#!/usr/bin/env bash
# R-4 기준선 수집 — KRX Open API의 ETF 일봉·채권지수·파생상품지수(코스피 200 TR)를
# 서버 parquet 레이크($STOCK_DATA_ROOT/kr/raw/krx_baseline)에 쌓는다 (계획 04 §3.1·§6).
#
# 모드(KRX_BASELINE_MODE, 기본 sync):
#   sync            정기 잡. 어제(KST)까지 최근 평일 20일 창 안의 빈·대기 날짜만 받는다.
#                   창보다 오래된 빈 날짜는 받지 않는다(그것은 backfill의 일이다, R04).
#                   KRX_BASELINE_SERVICES(쉼표, 기본 셋 모두), KRX_BASELINE_MAX_CALLS(기본 60,
#                   실제 HTTP 수)
#   backfill        역사 백필. KRX_BASELINE_SERVICES는 **서비스 하나**여야 한다
#                   (etf_bydd_trd · bon_dd_trd · drvprod_dd_trd).
#                   KRX_BASELINE_FILL_MODE=fill(기본, 빈·대기 날짜만) | reobserve
#                   (범위 전부 다시 받음, KRX_BASELINE_RUN_ID 필수 — 같은 ID로 이어 받는다).
#                   KRX_BASELINE_START·KRX_BASELINE_END(기본: 서비스 시작일~어제),
#                   KRX_BASELINE_MAX_CALLS(기본 없음, 실제 HTTP 수)
#   import-research ETF 조사 사본을 obs_seq=1로 들인다(HTTP 0). KRX_BASELINE_IMPORT_PATH
#                   (**호스트 경로**, manifest_files.tsv가 있는 디렉터리이고 gz는 그 아래 raw/)와
#                   KRX_BASELINE_EXPECT_SHA256(manifest_files.tsv의 sha256)이 필수.
#   verify          계획 §8 검사. 락을 잡지 않는다(완료 manifest만 읽는다). KRX_BASELINE_START·
#                   KRX_BASELINE_END·KRX_BASELINE_SERVICES를 줄 수 있다. 실패하면 종료 코드 1.
#
# 조사 사본 경로: compose가 컨테이너에 주는 볼륨은 ${STOCK_DATA_HOST_DIR}:/stock_data 하나뿐이다.
# KRX_BASELINE_IMPORT_PATH가 그 아래면 /stock_data/... 로 바꿔 넘기고, 밖이면(예: research_refs/)
# `-v <호스트 경로>:/import:ro`를 이번 실행에만 붙여 /import로 넘긴다. compose 파일은 안 바꾼다.
#
# 락 도메인 krx_baseline: 정기 sync·backfill·import가 모두 같은 락을 쓴다. 그래서 일회성
# 이벤트와 정기 이벤트가 겹치지 않는다(Cronicle max_children=1은 이벤트 하나 안에서만 막는다).
# 충돌하면 exit 75, 재시도 없음. KRX 키는 컨테이너의 AUTH_KEYS(.env)에서 읽는다.
#
# 소유자: 컨테이너를 호출 사용자(whi) uid:gid로 돌려 레이크 파일이 root 소유로 안 남게 한다.
# 쓸 수 없는 경로가 있으면 컨테이너를 띄우기 전에 종료 코드 73. 탈출구: SDC_RUN_AS_ROOT=1.
#
# Cronicle 이벤트(sdc_daily_krx_baseline, 평일 20:40)는 memory_limit을 비우지 말고 명시한다.
# 이 파일을 exec로 부른다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

mode="${KRX_BASELINE_MODE:-sync}"
stock_data_host="${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}"
args=(krx-baseline)

case "$mode" in
  sync)
    args+=(sync --max-calls "${KRX_BASELINE_MAX_CALLS:-60}")
    if [[ -n "${KRX_BASELINE_SERVICES:-}" ]]; then
      args+=(--services "$KRX_BASELINE_SERVICES")
    fi
    ;;
  backfill)
    if [[ -z "${KRX_BASELINE_SERVICES:-}" || "$KRX_BASELINE_SERVICES" == *,* ]]; then
      sdc_log "backfill은 KRX_BASELINE_SERVICES에 서비스 하나가 필요합니다 (etf_bydd_trd|bon_dd_trd|drvprod_dd_trd)"
      exit 2
    fi
    fill_mode="${KRX_BASELINE_FILL_MODE:-fill}"
    if [[ "$fill_mode" == "reobserve" && -z "${KRX_BASELINE_RUN_ID:-}" ]]; then
      sdc_log "reobserve는 KRX_BASELINE_RUN_ID가 필요합니다 (재개를 이 ID로 판단합니다)"
      exit 2
    fi
    args+=(backfill --service "$KRX_BASELINE_SERVICES" --mode "$fill_mode")
    [[ -n "${KRX_BASELINE_START:-}" ]] && args+=(--start "$KRX_BASELINE_START")
    [[ -n "${KRX_BASELINE_END:-}" ]] && args+=(--end "$KRX_BASELINE_END")
    [[ -n "${KRX_BASELINE_RUN_ID:-}" ]] && args+=(--run-id "$KRX_BASELINE_RUN_ID")
    [[ -n "${KRX_BASELINE_MAX_CALLS:-}" ]] && args+=(--max-calls "$KRX_BASELINE_MAX_CALLS")
    ;;
  import-research)
    if [[ -z "${KRX_BASELINE_IMPORT_PATH:-}" || -z "${KRX_BASELINE_EXPECT_SHA256:-}" ]]; then
      sdc_log "import-research는 KRX_BASELINE_IMPORT_PATH와 KRX_BASELINE_EXPECT_SHA256이 필요합니다"
      exit 2
    fi
    import_path="$KRX_BASELINE_IMPORT_PATH"
    if [[ "$import_path" == "$stock_data_host"/* ]]; then
      container_path="/stock_data/${import_path#"$stock_data_host"/}"
    else
      container_path="/import"
      SDC_RUN_EXTRA_ARGS="${SDC_RUN_EXTRA_ARGS:+${SDC_RUN_EXTRA_ARGS} }-v ${import_path}:/import:ro"
      export SDC_RUN_EXTRA_ARGS
    fi
    args+=(import-research --path "$container_path" --expect-manifest-sha256 "$KRX_BASELINE_EXPECT_SHA256")
    ;;
  verify)
    args+=(verify)
    [[ -n "${KRX_BASELINE_START:-}" ]] && args+=(--start "$KRX_BASELINE_START")
    [[ -n "${KRX_BASELINE_END:-}" ]] && args+=(--end "$KRX_BASELINE_END")
    [[ -n "${KRX_BASELINE_SERVICES:-}" ]] && args+=(--service "$KRX_BASELINE_SERVICES")
    ;;
  *)
    sdc_log "알 수 없는 KRX_BASELINE_MODE: $mode (sync|backfill|import-research|verify)"
    exit 2
    ;;
esac

sdc_append_run_as_invoking_user "" || exit $?
if [[ "$mode" != "verify" ]]; then
  sdc_assert_host_writable "" "$stock_data_host/kr/raw/krx_baseline" || exit $?
  sdc_run_daily_collector krx_baseline "${args[@]}"
else
  # verify는 완료 manifest만 읽으므로 락을 잡지 않는다.
  sdc_run_collector "${args[@]}"
fi

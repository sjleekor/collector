#!/usr/bin/env bash
# R-4 기준선 수집 #6 — ETF 총보수(금투협 DIS "펀드별 보수비용비교", 월말 기준일)를 서버 parquet
# 레이크($STOCK_DATA_ROOT/kr/raw/krx_baseline)에 쌓는다 (계획 04 §2 #6·§2.1·§3.1).
#
# 이 wrapper는 **다음 release(10-21 release 다음)에 들어간다. 10-21 release에는 없다.**
# Cronicle 이벤트(sdc_weekly_etf_fee_dis)는 **등록하더라도 꺼진 채** 둔다. 켜는 순서:
#   연구용 사본 import → verify → 이벤트 켜기 (아래 모드).
#
# 모드(ETF_FEE_DIS_MODE, 기본 sync):
#   sync            정기 잡. 저장된 마지막 달 뒤에 새 월말 기준일이 올라왔는지 보고 받는다.
#                   ETF_FEE_DIS_DISCOVERY=latest(기본, 최근 기준일 한 번 조회 — 새 달이 없으면
#                   요청 1건) | direct(다음 달 월말을 직접 물음, 없으면 요청 4건).
#                   ETF_FEE_DIS_MAX_CALLS(기본 30, 실제 HTTP 수)
#   backfill        달 범위 백필. ETF_FEE_DIS_START·ETF_FEE_DIS_END(YYYY-MM, 필수).
#                   ETF_FEE_DIS_FILL_MODE=fill(기본, 빈 달만) | reobserve(저장된 달도 다시 받음,
#                   ETF_FEE_DIS_RUN_ID 필수 — 같은 ID로 이어 받는다). ETF_FEE_DIS_MAX_CALLS(기본 없음)
#   import-research 연구용 월말 이력 사본(2026-10-11, 280개 달)을 obs_seq=1로 들인다(HTTP 0).
#                   ETF_FEE_DIS_IMPORT_PATH(**호스트 경로**, manifest.jsonl이 있고 gz는 그 아래 raw/)와
#                   ETF_FEE_DIS_EXPECT_SHA256(manifest.jsonl의 sha256)이 필수.
#   verify          완결성·무결성 검사. 락을 잡지 않는다(완료 manifest만 읽는다). 실패하면 종료 코드 1.
#
# **주기 권고: 주 1회**(예: 일요일 10:40). 기준일이 월 1회라 새 달 값은 어느 날 돌려도 같은 주에
# 잡힌다. latest 방식은 새 달이 없으면 요청 1건이라 **매일 돌려도 가볍다** — 매일 얻는 것은
# 공개 지연의 해상도(최근 기준일이 언제 바뀌었는지가 요청 기록에 일 단위로 남는다)뿐이다.
# 공개 지연을 일 단위로 재고 싶으면 매일, 아니면 주 1회. 사용자 결정(README 참고).
# 10-10·11 조회 때 최근 기준일은 2026-08-31이었다(9월 말 값 없음) — 새 달이 처음 보인 시각이
# 관측의 fetched_at이다.
#
# 락 도메인 kofia_dis: DIS는 KRX 키를 안 쓰므로 krx_baseline·seibro 락과 따로다. sync·backfill·
# import가 같은 락을 쓴다. 충돌하면 exit 75, 재시도 없음. 요청 간격 3초는 클라이언트 안에 있다.
# 소유자·권한 검사는 krx-baseline-sync.sh와 같다(탈출구 SDC_RUN_AS_ROOT=1).
# Cronicle 이벤트는 memory_limit을 명시하고(호스트 Python 이벤트 메모리 제한 1GB) 이 파일을 exec로 부른다.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

mode="${ETF_FEE_DIS_MODE:-sync}"
stock_data_host="${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}"
args=(etf-fee-dis)

case "$mode" in
  sync)
    discovery="${ETF_FEE_DIS_DISCOVERY:-latest}"
    if [[ "$discovery" != "latest" && "$discovery" != "direct" ]]; then
      sdc_log "ETF_FEE_DIS_DISCOVERY는 latest|direct 입니다: $discovery"
      exit 2
    fi
    args+=(sync --discovery "$discovery" --max-calls "${ETF_FEE_DIS_MAX_CALLS:-30}")
    ;;
  backfill)
    if [[ -z "${ETF_FEE_DIS_START:-}" || -z "${ETF_FEE_DIS_END:-}" ]]; then
      sdc_log "backfill은 ETF_FEE_DIS_START와 ETF_FEE_DIS_END(YYYY-MM)가 필요합니다"
      exit 2
    fi
    fill_mode="${ETF_FEE_DIS_FILL_MODE:-fill}"
    if [[ "$fill_mode" == "reobserve" && -z "${ETF_FEE_DIS_RUN_ID:-}" ]]; then
      sdc_log "reobserve는 ETF_FEE_DIS_RUN_ID가 필요합니다 (재개를 이 ID로 판단합니다)"
      exit 2
    fi
    args+=(backfill --from "$ETF_FEE_DIS_START" --to "$ETF_FEE_DIS_END" --mode "$fill_mode")
    [[ -n "${ETF_FEE_DIS_RUN_ID:-}" ]] && args+=(--run-id "$ETF_FEE_DIS_RUN_ID")
    [[ -n "${ETF_FEE_DIS_MAX_CALLS:-}" ]] && args+=(--max-calls "$ETF_FEE_DIS_MAX_CALLS")
    ;;
  import-research)
    if [[ -z "${ETF_FEE_DIS_IMPORT_PATH:-}" || -z "${ETF_FEE_DIS_EXPECT_SHA256:-}" ]]; then
      sdc_log "import-research는 ETF_FEE_DIS_IMPORT_PATH와 ETF_FEE_DIS_EXPECT_SHA256이 필요합니다"
      exit 2
    fi
    import_path="$ETF_FEE_DIS_IMPORT_PATH"
    if [[ "$import_path" == "$stock_data_host"/* ]]; then
      container_path="/stock_data/${import_path#"$stock_data_host"/}"
    else
      container_path="/import"
      SDC_RUN_EXTRA_ARGS="${SDC_RUN_EXTRA_ARGS:+${SDC_RUN_EXTRA_ARGS} }-v ${import_path}:/import:ro"
      export SDC_RUN_EXTRA_ARGS
    fi
    args+=(import-research --path "$container_path" --expect-manifest-sha256 "$ETF_FEE_DIS_EXPECT_SHA256")
    ;;
  verify)
    args+=(verify)
    ;;
  *)
    sdc_log "알 수 없는 ETF_FEE_DIS_MODE: $mode (sync|backfill|import-research|verify)"
    exit 2
    ;;
esac

sdc_append_run_as_invoking_user "" || exit $?
if [[ "$mode" != "verify" ]]; then
  sdc_assert_host_writable "" "$stock_data_host/kr/raw/krx_baseline" || exit $?
  sdc_run_daily_collector kofia_dis "${args[@]}"
else
  # verify는 완료 manifest만 읽으므로 락을 잡지 않는다.
  sdc_run_collector "${args[@]}"
fi

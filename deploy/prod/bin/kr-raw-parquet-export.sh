#!/usr/bin/env bash
# KR raw 20개 표를 sj2 DB에서 parquet로 바로 내보낸다 (일일 브리핑의 KR 입력).
#
# collector 이미지 안의 bin/raw-parquet-export-all.sh 와 Rust 바이너리
# (/usr/local/bin/raw-parquet-exporter)를 쓴다. compose 네트워크의 `db` 서비스에
# DB_* 환경변수로 붙으므로 DSN이 명령줄·로그에 나오지 않는다.
#
# 출력: 컨테이너 /stock_data/kr/raw/raw_postgres/snapshot_date=<D>/source=sj2_remote
#       호스트   ${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}/kr/raw/raw_postgres/...
# route=remote·source=sj2_remote는 맥 경로와 같아 modeler 계약(_SUCCESS.json)이 안 바뀐다.
#
# 고정값: --jobs 1 (DB 읽기 연결 1개), --no-force(기존 표는 건너뛰거나 이어받음),
# manifest validation 켬. 이 wrapper는 --force·--route·--jobs·--no-validate를 받지 않는다.
#
# 옵션:
#   --snapshot-date YYYY-MM-DD   기본은 오늘(KST). SDC_KR_EXPORT_SNAPSHOT_DATE로도 준다.
#   --dry-run                    계획만 만든다(파일·marker 없음).
#   --consistent-snapshot        한 PostgreSQL snapshot(REPEATABLE READ)을 모든 표가 읽는다.
#                                SDC_KR_EXPORT_CONSISTENT_SNAPSHOT=1로도 켠다. 기본은 꺼짐
#                                (read committed per chunk). 켜면 _SUCCESS.json에
#                                snapshot_policy=repeatable_read_exported_snapshot과
#                                pg_snapshot_id가 남고, 재시도는 같은 snapshot id일 때만
#                                표를 건너뛴다. 읽는 쪽(modeler verify_raw)이 새 policy를
#                                받는 버전이어야 한다.
#
# 호스트 경로 모드(기본, SDC_KR_EXPORT_HOST_PATHS=1): _SUCCESS.json의 manifest_path는
# export가 본 경로로 적힌다. 운영 KR prepare(modeler kr_live_prepare)는 sj2 호스트에서
# 호스트 경로(raw_root/_manifests/...)와 정확히 같은지 보므로, 호스트 디렉터리를
# 같은 경로로 한 번 더 마운트하고 출력 루트를 호스트 경로로 잡는다.
# SDC_KR_EXPORT_HOST_PATHS=0이면 컨테이너 경로(/stock_data/...)로 적는다.
#
# 락 도메인 kr_raw_export: 같은 출력 디렉터리에 export 둘이 겹치는 것만 막는다.
# DB는 읽기 전용이라 수집 락(krx_marketdata·opendart 등)과는 공유하지 않는다.
# 수집과 겹치지 않게 하는 것은 일정이다 (README의 제안 참고).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

snapshot_date="${SDC_KR_EXPORT_SNAPSHOT_DATE:-$(TZ=Asia/Seoul date +%F)}"
dry_run=0
consistent_snapshot="${SDC_KR_EXPORT_CONSISTENT_SNAPSHOT:-0}"

while (($#)); do
  case "$1" in
    --snapshot-date)
      if (($# < 2)); then
        sdc_log "missing value for --snapshot-date"
        exit 2
      fi
      snapshot_date="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    --consistent-snapshot)
      consistent_snapshot=1
      shift
      ;;
    *)
      sdc_log "unsupported option: $1 (allowed: --snapshot-date, --dry-run, --consistent-snapshot)"
      exit 2
      ;;
  esac
done

if ! [[ "$snapshot_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  sdc_log "invalid snapshot date: $snapshot_date (expected YYYY-MM-DD)"
  exit 2
fi

args=(--route remote --direct-db --jobs 1 --no-build --snapshot-date "$snapshot_date")
if [[ "$dry_run" == "1" ]]; then
  args+=(--dry-run)
fi

if [[ "$consistent_snapshot" == "1" ]]; then
  args+=(--consistent-snapshot)
elif [[ "$consistent_snapshot" != "0" ]]; then
  sdc_log "invalid SDC_KR_EXPORT_CONSISTENT_SNAPSHOT: $consistent_snapshot (expected 0 or 1)"
  exit 2
fi

export SDC_RUN_ENTRYPOINT=/app/bin/raw-parquet-export-all.sh

if [[ "${SDC_KR_EXPORT_HOST_PATHS:-1}" == "1" ]]; then
  host_dir="${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}"
  export SDC_RUN_EXTRA_ARGS="-v ${host_dir}:${host_dir} -e SDC_RAW_PARQUET_OUTPUT_ROOT=${host_dir}/kr/raw/raw_postgres"
fi

# 전체 export는 약 35분이다. 앞선 export나 수집이 락을 잡고 있으면 오래 기다리지 않는다.
SDC_LOCK_WAIT_SECONDS="${SDC_LOCK_WAIT_SECONDS:-60}"
sdc_run_daily_collector kr_raw_export "${args[@]}"

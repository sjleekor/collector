#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
REQUIRED_SOURCES="${SDC_COMMON_REQUIRED_SOURCES:-fdr,fred,ecos,krx}"

sdc_log "common feature refresh starting"
sdc_log "app_dir=$SDC_APP_DIR"
sdc_log "end_date=$END_DATE"
sdc_log "required_sources=$REQUIRED_SOURCES"
sdc_log "pykrx_common_enabled=${SDC_COMMON_ENABLE_PYKRX:-0}"

"$script_dir/common-seed-catalog.sh"
"$script_dir/common-sync-fdr.sh"
"$script_dir/common-sync-fred.sh"
"$script_dir/common-sync-ecos-daily.sh"
"$script_dir/common-sync-ecos-macro.sh"
"$script_dir/common-sync-krx.sh"

if [[ "${SDC_COMMON_ENABLE_PYKRX:-0}" == "1" ]]; then
  "$script_dir/common-sync-pykrx.sh"
fi

sdc_log "common feature raw refresh complete; run bin/parquet-compute-all.sh on the compute node for derived marts/readiness"

#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

sdc_log "deprecated: common build-daily moved to Parquet/DuckDB compute; run bin/parquet-compute-all.sh on the compute node"
exit 2

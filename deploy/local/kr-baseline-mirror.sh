#!/usr/bin/env bash
# sj2-server 의 R-4 기준선 레이크(krx_baseline)를 이 맥으로 당긴다.
#
# 서버 kr/raw/krx_baseline/ -> 맥 $STOCK_DATA_ROOT/kr/raw/krx_baseline/
#
# KR raw export(raw-parquet-export-all.sh · export 20개 표)와 무관하다 (결정 ⑮).
# 서버가 정본이지만 us-mirror.sh 와 달리 **--delete 를 쓰지 않는다.** 이 데이터는
# 추가만 되는 관측이고, 원천에서 다시 못 받는 것(과거 시점의 응답 원문·최초
# 관측본)이 섞여 있다. 서버에서 파일이 사라져도 맥 사본은 남겨야 한다.
#
# 덮어쓰기: --ignore-existing 을 골랐다. 서버 파일은 한 번 쓰면 안 바뀌는 계약
# (04_collection_plan §3.1)이라, 맥에 이미 있는 파일은 다시 받을 이유가 없다.
# 다만 같은 경로인데 내용이 다르면 계약이 깨진 것이므로 조용히 넘기지 않고
# --checksum 비교 한 번으로 찾아 경고한다(덮지는 않는다).
#
# 순서: 원문·parquet 을 먼저, _manifest/ 를 마지막에 받는다. 맥에서 manifest 가
# 가리키는 파일이 늦게 도착하는 일이 없게 하려는 것이다. 읽는 쪽은 완료 manifest
# 가 가리키는 파일만 쓰므로, 도중에 멈춰도 반쯤 받은 묶음은 안 읽힌다.
#
# 평일에는 이 맥을 수집·미러에 쓰지 않는다. 일요일(KR export 날)에 돌린다.
# 스크립트가 막지는 않는다.
set -euo pipefail

REMOTE="${SDC_REMOTE_HOST:-whi@sj2-server}"
REMOTE_DIR="${SDC_KR_BASELINE_REMOTE_DIR:-/home/whi/data/stock_data/kr/raw/krx_baseline}"
LOCAL_DIR="${STOCK_DATA_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../stock_data" && pwd)}/kr/raw/krx_baseline"

dry_run=0
progress=0
for arg in "$@"; do
  case "$arg" in
    --dry-run)  dry_run=1 ;;
    --progress) progress=1 ;;
    -h|--help)
      printf 'usage: %s [--dry-run] [--progress]\n' "$0"
      printf '  서버 %s\n  -> 맥  %s\n' "$REMOTE_DIR" "$LOCAL_DIR"
      printf '  환경변수: SDC_REMOTE_HOST, SDC_KR_BASELINE_REMOTE_DIR, STOCK_DATA_ROOT\n'
      exit 0 ;;
    *) printf 'usage: %s [--dry-run] [--progress]\n' "$0" >&2; exit 2 ;;
  esac
done

# --delete 없음, --ignore-existing 으로 기존 파일 보호, 쓰는 중인 .tmp 는 제외
rsync_opts=(-a --ignore-existing --exclude '*.tmp' --stats --human-readable)
[ "$dry_run" -eq 1 ] && rsync_opts+=(--dry-run)
[ "$progress" -eq 1 ] && rsync_opts+=(--info=progress2)

printf '%s -> %s%s\n' "$REMOTE:$REMOTE_DIR" "$LOCAL_DIR" "$([ "$dry_run" -eq 1 ] && echo ' (dry-run)')"
[ "$dry_run" -eq 1 ] || mkdir -p "$LOCAL_DIR"

log="$(mktemp "${TMPDIR:-/tmp}/kr-baseline-mirror.XXXXXX")"
trap '/bin/rm -f "$log"' EXIT

# 1단: _manifest/ 를 뺀 전부
printf '\n=== 1/2 원문·parquet (_manifest 제외) ===\n'
rsync "${rsync_opts[@]}" --exclude '/_manifest/' "$REMOTE:$REMOTE_DIR/" "$LOCAL_DIR/" | tee -a "$log"

# 2단: _manifest/ 만, 마지막에
printf '\n=== 2/2 _manifest ===\n'
rsync "${rsync_opts[@]}" --include '/_manifest/' --include '/_manifest/*.json' --exclude '*' \
  "$REMOTE:$REMOTE_DIR/" "$LOCAL_DIR/" | tee -a "$log"

# 요약: 두 단의 합계
printf '\n=== 요약%s ===\n' "$([ "$dry_run" -eq 1 ] && echo ' (dry-run: 받을 예정)')"
awk -F': ' '
  /^Number of regular files transferred/ { n += $2 + 0 }
  /^Total transferred file size/ { split($2, a, " "); b = a[1]; sub(/,/, "", b); size = size " " $2 }
  END { printf "받은 파일 %d개\n", n; printf "단계별 바이트:%s\n", size }
' "$log" | sed 's/Total transferred file size: //'

# 같은 경로인데 내용이 다른 기존 파일 — 계약 위반이므로 경고만 한다
printf '\n=== 기존 파일 변경 점검 (덮지 않음) ===\n'
changed="$(rsync -rn --checksum --existing --exclude '*.tmp' --out-format='%n' \
  "$REMOTE:$REMOTE_DIR/" "$LOCAL_DIR/" 2>/dev/null | grep -v '/$' || true)"
if [ -n "$changed" ]; then
  printf '경고: 서버 파일이 맥 사본과 다릅니다(%s개). 한 번 쓴 파일은 안 바뀌는 계약 위반입니다. 맥 쪽은 덮지 않았습니다.\n' \
    "$(printf '%s\n' "$changed" | wc -l | tr -d ' ')" >&2
  printf '%s\n' "$changed" | head -20 >&2
else
  printf '없음\n'
fi

[ "$dry_run" -eq 1 ] && exit 0

# 맥 쪽 점검: _manifest/*.json 이 가리키는 파일이 모두 있고 크기·sha256 이 맞는지
printf '\n=== manifest 점검 ===\n'
python3 -I - "$LOCAL_DIR" <<'PY'
import hashlib, json, sys
from pathlib import Path

base = Path(sys.argv[1])
manifests = sorted((base / "_manifest").glob("*.json"))
bad, total = [], 0
for m in manifests:
    for e in json.loads(m.read_text("utf-8")).get("files", []):
        total += 1
        p = base / e["path"]
        if not p.is_file():
            bad.append(f"{m.name}: {e['path']} 없음")
        elif "bytes" in e and p.stat().st_size != e["bytes"]:
            bad.append(f"{m.name}: {e['path']} 크기 불일치")
        elif "sha256" in e and hashlib.sha256(p.read_bytes()).hexdigest() != e["sha256"]:
            bad.append(f"{m.name}: {e['path']} sha256 불일치")
print(f"manifest {len(manifests)}개, 가리키는 파일 {total}개, 문제 {len(bad)}개")
for line in bad[:50]:
    print("  " + line)
sys.exit(1 if bad else 0)
PY

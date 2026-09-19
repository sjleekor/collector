#!/usr/bin/env bash
# launchd 등록/해제. **사람이 직접 돌린다 — 자동으로 걸지 않는다.**
#
#   deploy/local/install-launchd.sh install     등록하고 즉시 로드
#   deploy/local/install-launchd.sh uninstall   내리고 plist 삭제
#   deploy/local/install-launchd.sh status      상태만 본다
#
# plist 를 저장소에 커밋하지 않고 여기서 만든다 — 경로가 기계마다 다르고
# 이 저장소는 public 이다.
set -euo pipefail

label="local.collector.us-daily"
plist="$HOME/Library/LaunchAgents/$label.plist"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
runner="$repo_dir/deploy/local/us-daily.sh"
log_dir="${COLLECTOR_US_LOG_DIR:-$HOME/Library/Logs/collector-us}"

# 15:00 KST. DoltHub 가 05:30 UTC = 14:30 KST 에 전일 데이터를 커밋한다 (05 §4.1).
hour=15
minute=0

case "${1:-status}" in
  install)
    mkdir -p "$(dirname "$plist")" "$log_dir"
    cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>-lc</string>
    <string>$runner</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>$hour</integer>
    <key>Minute</key><integer>$minute</integer>
  </dict>
  <!-- 맥이 꺼져 있어 거른 실행을 깨어나자마자 한 번 돌린다. 그래도 놓친
       날짜는 실행이 raw/ 를 보고 스스로 메꾼다 (04 §2.4) -->
  <key>RunAtLoad</key><false/>
  <key>StandardOutPath</key><string>$log_dir/launchd.out.log</string>
  <key>StandardErrorPath</key><string>$log_dir/launchd.err.log</string>
</dict>
</plist>
PLIST
    launchctl unload "$plist" 2>/dev/null || true
    launchctl load "$plist"
    echo "등록했다: $plist (매일 $(printf '%02d:%02d' "$hour" "$minute") KST)"
    ;;
  uninstall)
    launchctl unload "$plist" 2>/dev/null || true
    rm -f "$plist"
    echo "내렸다: $plist"
    ;;
  status)
    launchctl list | grep -F "$label" || echo "등록돼 있지 않다: $label"
    ;;
  *)
    echo "usage: $0 {install|uninstall|status}" >&2
    exit 2
    ;;
esac

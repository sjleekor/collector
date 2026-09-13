#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"
docker compose pull collector

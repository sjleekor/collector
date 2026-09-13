#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"
docker compose run --rm collector db init

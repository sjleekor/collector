#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROD_DIR="$SCRIPT_DIR/prod"
REMOTE_HOST="whi@sj2-server"
REMOTE_DIR="/home/whi/apps/sdc"

echo "Deploying from $PROD_DIR to $REMOTE_HOST:$REMOTE_DIR"

# Ensure target bin directory exists
ssh "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/bin"

# Sync compose.yaml and bin/ directory using rsync
rsync -avz --delete "$PROD_DIR/compose.yaml" "$REMOTE_HOST:$REMOTE_DIR/"
rsync -avz --delete "$PROD_DIR/bin/" "$REMOTE_HOST:$REMOTE_DIR/bin/"

echo "Deployment complete."

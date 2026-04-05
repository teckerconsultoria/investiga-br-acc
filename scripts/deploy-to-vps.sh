#!/bin/bash
# Deploy code changes from local repo to VPS
# Usage: bash scripts/deploy-to-vps.sh
set -euo pipefail

VPS_USER="root"
VPS_HOST="82.25.65.4"
VPS_DIR="/opt/br-acc"

echo "=========================================="
echo "Deploy to VPS: ${VPS_HOST}"
echo "=========================================="

# Sync code to VPS (NO --delete to preserve .gitignore files like Dockerfiles, src/, etc.)
echo ""
echo "Step 1: Syncing code..."
rsync -avz \
  --exclude='.git' \
  --exclude='node_modules' \
  --exclude='__pycache__' \
  --exclude='.venv' \
  --exclude='.uv' \
  --exclude='data/' \
  --exclude='.env' \
  ./ ${VPS_USER}@${VPS_HOST}:${VPS_DIR}/

echo ""
echo "Step 2: Rebuilding API and ETL containers..."
ssh ${VPS_USER}@${VPS_HOST} "cd ${VPS_DIR} && docker compose build api etl"

echo ""
echo "Step 3: Restarting services..."
ssh ${VPS_USER}@${VPS_HOST} "cd ${VPS_DIR} && docker compose up -d --remove-orphans"

echo ""
echo "Step 4: Health check..."
sleep 10
HEALTH=$(ssh ${VPS_USER}@${VPS_HOST} "curl -sf http://localhost:8000/health" 2>/dev/null || echo "failed")
if [ "$HEALTH" != "failed" ]; then
  echo "✅ Health check passed: ${HEALTH}"
else
  echo "❌ Health check failed. Check logs:"
  ssh ${VPS_USER}@${VPS_HOST} "cd ${VPS_DIR} && docker compose logs --tail=20 api"
  exit 1
fi

echo ""
echo "=========================================="
echo "Deploy complete!"
echo "=========================================="

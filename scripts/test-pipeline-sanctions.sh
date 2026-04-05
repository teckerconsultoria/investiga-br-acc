#!/bin/bash
# Test pipeline: sanctions (download + ETL)
set -euo pipefail

echo "=========================================="
echo "TEST: Sanctions Pipeline (Download + ETL)"
echo "=========================================="

# Environment
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-changeme}"
NEO4J_URI="bolt://neo4j:7687"
DATA_DIR="/workspace/data"

echo ""
echo "Step 1: Downloading sanctions data..."
echo "------------------------------------------"
python etl/scripts/download_sanctions.py

echo ""
echo "Step 2: Verifying downloaded data..."
echo "------------------------------------------"
ls -lh ${DATA_DIR}/sanctions/ 2>/dev/null || echo "No data found!"
find ${DATA_DIR}/sanctions/ -type f -name "*.csv" 2>/dev/null | head -5

echo ""
echo "Step 3: Running ETL pipeline..."
echo "------------------------------------------"
bracc-etl run \
  --source sanctions \
  --neo4j-uri ${NEO4J_URI} \
  --neo4j-user neo4j \
  --neo4j-password ${NEO4J_PASSWORD} \
  --data-dir ${DATA_DIR}

echo ""
echo "Step 4: Verifying data in Neo4j..."
echo "------------------------------------------"
cypher-shell -u neo4j -p ${NEO4J_PASSWORD} -a ${NEO4J_URI} \
  "MATCH (n:Sanction) RETURN count(n) AS sanction_count;" 2>/dev/null || echo "Could not query Neo4j"

echo ""
echo "=========================================="
echo "TEST COMPLETE"
echo "=========================================="

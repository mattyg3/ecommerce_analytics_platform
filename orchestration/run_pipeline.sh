#!/usr/bin/env bash

set -e

SIM_HOURS=${1:-24}
SIM_START=${2:-"`date "+%Y-%m-%d %H:%M:%S"`"}

: "${PYTHON_BIN:=python3}"

command -v $PYTHON_BIN >/dev/null || {
  echo "❌ Python not found"
  exit 1
}

echo "🚀 Starting orchestration pipeline..."

START_TIME=$SECONDS

bash orchestration/scripts/run_generator_ingestion.sh $SIM_HOURS $SIM_START

bash orchestration/scripts/run_bronze.sh

bash orchestration/scripts/run_dbt.sh

END_TIME=$SECONDS
ELAPSED=$((END_TIME - START_TIME))

MINS=$((ELAPSED / 60))
SECS=$((ELAPSED % 60))

echo "🎉 Orchestration pipeline completed successfully!"
echo "⏱️ Total runtime: ${ELAPSED}s (${MINS}m ${SECS}s)"
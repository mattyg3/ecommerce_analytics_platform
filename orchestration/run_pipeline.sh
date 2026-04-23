#!/usr/bin/env bash

set -e

SIM_HOURS=${1:-24}

: "${PYTHON_BIN:=python3}"

command -v $PYTHON_BIN >/dev/null || {
  echo "❌ Python not found"
  exit 1
}

echo "🚀 Starting orchestration pipeline..."

bash orchestration/scripts/run_generator_ingestion.sh $SIM_HOURS

bash orchestration/scripts/run_bronze.sh

bash orchestration/scripts/run_dbt.sh

echo "🎉 Orchestration pipeline completed successfully!"
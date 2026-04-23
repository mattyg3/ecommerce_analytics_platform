#!/bin/bash
set -e

# ----------------------------------------
# Define repo root and log file
# ----------------------------------------
: "${PYTHON_BIN:=python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/../..")"
mkdir -p "$REPO_ROOT/logs"
LOG_FILE=${2:-$REPO_ROOT/logs/dbt_pipeline.log}
# exec > "$LOG_FILE" 2>&1
exec > >(tee -a "$LOG_FILE") 2>&1

command -v $PYTHON_BIN >/dev/null || {
  echo "❌ Python not found"
  exit 1
}


# # ----------------------------------------
# # Safety checks
# # ----------------------------------------
# if [ ! -d "$REPO_ROOT/data-lake/bronze" ]; then
#   echo "❌ Bronze data not found. Run ingestion and bronze layer first."
#   exit 1
# fi

# ----------------------------------------
# Run DBT Spark job
# ----------------------------------------
echo "⚙️ Running dbt_runner.py..."
$PYTHON_BIN spark_jobs/dbt_runner.py

# ----------------------------------------
# Success
# ----------------------------------------
echo "✅ Silver layer completed successfully."
echo "✅ Gold layer completed successfully."
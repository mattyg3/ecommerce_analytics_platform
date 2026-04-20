#!/bin/bash
set -e

# ----------------------------------------
# Define repo root and log file
# ----------------------------------------
: "${PYTHON_BIN:=python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/../..")"
mkdir -p "$REPO_ROOT/logs"
LOG_FILE=${2:-$REPO_ROOT/logs/bronze_pipeline.log}
# exec > "$LOG_FILE" 2>&1
exec > >(tee -a "$LOG_FILE") 2>&1

command -v $PYTHON_BIN >/dev/null || {
  echo "❌ Python not found"
  exit 1
}

# ----------------------------------------
# Move to repo root
# ----------------------------------------
cd "$REPO_ROOT"
echo "📂 Repo root: $REPO_ROOT"

# ----------------------------------------
# Activate Python environment
# ----------------------------------------
# VENV_PATH="$REPO_ROOT/.venv"

# if [ ! -f "$VENV_PATH/bin/activate" ]; then
#     echo "❌ .venv not found. Create it first with: python3 -m venv .venv && pip install -r requirements.txt"
#     exit 1
# fi

# source "$VENV_PATH/bin/activate"

# ----------------------------------------
# Safety checks
# ----------------------------------------
if [ ! -d "$REPO_ROOT/data-lake/landing" ]; then
  echo "❌ Landing data not found. Run ingestion first."
  exit 1
fi

# ----------------------------------------
# Run Bronze Spark job
# ----------------------------------------
echo "⚙️ Running bronze.py..."
$PYTHON_BIN spark_jobs/bronze.py

# ----------------------------------------
# Success
# ----------------------------------------
echo "✅ Bronze layer completed successfully."
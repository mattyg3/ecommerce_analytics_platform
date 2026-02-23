#!/bin/bash
set -e

# ----------------------------------------
# Define repo root and log file
# ----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/../..")"
mkdir -p "$REPO_ROOT/logs"
LOG_FILE=${2:-$REPO_ROOT/logs/silver_pipeline.log}
exec > "$LOG_FILE" 2>&1

# ----------------------------------------
# Move to repo root
# ----------------------------------------
cd "$REPO_ROOT"
echo "📂 Repo root: $REPO_ROOT"

# ----------------------------------------
# Activate Python environment
# ----------------------------------------
VENV_PATH="$REPO_ROOT/.venv"

if [ ! -f "$VENV_PATH/bin/activate" ]; then
    echo "❌ .venv not found. Create it first with: python3 -m venv .venv && pip install -r requirements.txt"
    exit 1
fi

source "$VENV_PATH/bin/activate"

# ----------------------------------------
# Safety checks
# ----------------------------------------
if [ ! -d "data-lake/bronze" ]; then
  echo "❌ Bronze data not found. Run ingestion and bronze layer first."
  exit 1
fi

# ----------------------------------------
# Run Silver Spark job
# ----------------------------------------
echo "⚙️ Running silver.py..."
python spark_jobs/silver.py

# ----------------------------------------
# Success
# ----------------------------------------
echo "✅ Silver layer completed successfully."
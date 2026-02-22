#!/bin/bash
set -euo pipefail

# ----------------------------------------
# Usage:
#   ./run_pipeline.sh [SIM_HOURS]
# ----------------------------------------
SIM_HOURS=${1:-24}

# ----------------------------------------
# Paths
# ----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/../..")"

LOG_DIR="$REPO_ROOT/logs"
CONTROL_DIR="$REPO_ROOT/control"
VENV_PATH="$REPO_ROOT/.venv"

STOP_FILE="$CONTROL_DIR/clickstream.stop"
PID_FILE="$CONTROL_DIR/pipeline.pid"
LOG_FILE="$LOG_DIR/ingestion_pipeline.log"

mkdir -p "$LOG_DIR" "$CONTROL_DIR"

exec > "$LOG_FILE" 2>&1

echo "========================================"
echo "🚀 Starting pipeline"
echo "⏱️  Simulation hours: $SIM_HOURS"
echo "========================================"

# ----------------------------------------
# Prevent double-start
# ----------------------------------------
if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "❌ Pipeline already running (PID $OLD_PID)"
    exit 1
  else
    echo "⚠️ Stale PID file found, cleaning up"
    rm -f "$PID_FILE"
  fi
fi

echo $$ > "$PID_FILE"

# ----------------------------------------
# Activate virtualenv
# ----------------------------------------
if [[ ! -f "$VENV_PATH/bin/activate" ]]; then
  echo "❌ .venv not found. Create it first."
  exit 1
fi

source "$VENV_PATH/bin/activate"

# ----------------------------------------
# Cleanup control files
# ----------------------------------------
rm -f "$STOP_FILE"

export SIMULATION_HOURS="$SIM_HOURS"

# ----------------------------------------
# Graceful shutdown handler
# ----------------------------------------
cleanup() {
  echo "🛑 Shutdown requested"

  touch "$STOP_FILE"

  [[ -n "${GENERATOR_PID:-}" ]] && wait "$GENERATOR_PID"
  [[ -n "${STREAM_PID:-}" ]] && wait "$STREAM_PID"

  rm -f "$STOP_FILE" "$PID_FILE"

  echo "✅ Pipeline shutdown complete"
  exit 0
}

trap cleanup SIGINT SIGTERM

# ======================================================
# PHASE 0 — Full Refresh
# ======================================================
echo "🗑️ Full Refresh"
python ingestion/helper_functions/clear_old_data.py

# ======================================================
# PHASE 1 — Start generator + STREAM ingest
# ======================================================
PHASE_START=$(date +%s)
echo "🟢 PHASE 1: Starting generator + STREAM ingest"

python -u producers/linked_clickstream_order_generator.py &
GENERATOR_PID=$!
echo "   Generator PID: $GENERATOR_PID"

python ingestion/streaming_ingest.py --mode stream &
STREAM_PID=$!
echo "   Stream PID: $STREAM_PID"

PHASE_END=$(date +%s)
echo "⏱️ PHASE 1 started: $(date -d @$PHASE_START) | Generator + Stream launched"

# ======================================================
# PHASE 2 — Wait for generator to finish
# ======================================================
PHASE_START=$(date +%s)
echo "⏳ PHASE 2: Waiting for generator to finish"
wait "$GENERATOR_PID"
PHASE_END=$(date +%s)
echo "✅ PHASE 2: Generator finished | Duration: $((PHASE_END - PHASE_START)) seconds"

# ======================================================
# PHASE 3 — Stop STREAM ingest
# ======================================================
PHASE_START=$(date +%s)
echo "🛑 PHASE 3: Stopping STREAM ingest"
touch "$STOP_FILE"
wait "$STREAM_PID"
PHASE_END=$(date +%s)
echo "✅ PHASE 3: STREAM ingest stopped | Duration: $((PHASE_END - PHASE_START)) seconds"

# ======================================================
# PHASE 4 — BACKFILL ingest (fast)
# ======================================================
PHASE_START=$(date +%s)
echo "🚀 PHASE 4: Running BACKFILL ingest"
python ingestion/streaming_ingest.py --mode backfill
PHASE_END=$(date +%s)
echo "✅ PHASE 4: BACKFILL ingest complete | Duration: $((PHASE_END - PHASE_START)) seconds"

# ======================================================
# PHASE 5 — Batch ingest (Silver / Orders)
# ======================================================
PHASE_START=$(date +%s)
echo "🟢 PHASE 5: Running batch ingest"
python ingestion/batch_ingest.py
PHASE_END=$(date +%s)
echo "✅ PHASE 5: Batch ingest complete | Duration: $((PHASE_END - PHASE_START)) seconds"

# ----------------------------------------
# Final cleanup
# ----------------------------------------
rm -f "$STOP_FILE" "$PID_FILE"

echo "========================================"
echo "🎉 Pipeline completed successfully"
echo "========================================"
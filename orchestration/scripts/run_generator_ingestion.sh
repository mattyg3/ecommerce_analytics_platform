#!/bin/bash
set -e

# ----------------------------------------
# Usage:
#   ./run_pipeline.sh [SIM_HOURS] [LOG_FILE]
# Example:
#   ./run_pipeline.sh 24 pipeline.log
# ----------------------------------------
SIM_HOURS=${1:-24}  # default to 24 hours
LOG_FILE=${2:-pipeline.log}

# ----------------------------------------
# Determine repo root and paths
# ----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/../..")"
VENV_PATH="$REPO_ROOT/.venv"
CONTROL_DIR="$REPO_ROOT/control"
mkdir -p "$CONTROL_DIR"
STOP_FILE="$CONTROL_DIR/clickstream.stop"
PID_FILE="$CONTROL_DIR/streaming_ingest.pid"

# ----------------------------------------
# Prevent double-start
# ----------------------------------------
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "❌ Streaming ingest already running (PID $OLD_PID). Exiting."
    exit 1
  else
    echo "⚠️ Stale PID file found. Cleaning up."
    rm -f "$PID_FILE"
  fi
fi

echo $$ > "$PID_FILE"
echo "🧷 PID locked: $$"

# ----------------------------------------
# Activate Python environment
# ----------------------------------------
if [ ! -f "$VENV_PATH/bin/activate" ]; then
    echo "❌ .venv not found. Create it first with: python3 -m venv .venv && pip install -r requirements.txt"
    exit 1
fi

source "$VENV_PATH/bin/activate"

# ----------------------------------------
# Clean stop file at start
# ----------------------------------------
if [ -f "$STOP_FILE" ]; then
    echo "🧹 Removing old stop file..."
    rm "$STOP_FILE"
fi

# ----------------------------------------
# Export simulation hours for generator
# ----------------------------------------
export SIMULATION_HOURS=$SIM_HOURS
echo "⏱️ Simulation set for $SIM_HOURS hours."

# ----------------------------------------
# Graceful shutdown trap
# ----------------------------------------
graceful_shutdown() {
  echo "🛑 Shutdown signal received"
  echo "📄 Creating stop file: $STOP_FILE"
  touch "$STOP_FILE"

  if [[ -n "$GENERATOR_PID" ]]; then
    echo "⏳ Waiting for generator ($GENERATOR_PID)..."
    wait "$GENERATOR_PID"
  fi

  if [[ -n "$SPARK_PID" ]]; then
    echo "⏳ Waiting for streaming ingest ($SPARK_PID)..."
    wait "$SPARK_PID"
  fi

  rm -f "$PID_FILE"

  echo "✅ Graceful shutdown complete."
  exit 0
}

trap graceful_shutdown SIGINT SIGTERM

# ----------------------------------------
# Start clickstream & order session generator
# ----------------------------------------
echo "🟢 Starting streaming session generator..."
python producers/linked_clickstream_order_generator.py &
GENERATOR_PID=$!
echo "💡 Generator PID: $GENERATOR_PID"

# ----------------------------------------
# Start streaming ingest
# ----------------------------------------
echo "🟢 Starting streaming ingest..."
python ingestion/streaming_ingest.py &
SPARK_PID=$!
echo "💡 Spark PID: $SPARK_PID"

# ----------------------------------------
# Wait for generator to finish
# ----------------------------------------
wait $GENERATOR_PID
echo "✅ Generator finished."

# ----------------------------------------
# Signal streaming ingest to stop (if not already)
# ----------------------------------------
if [ ! -f "$STOP_FILE" ]; then
    touch "$STOP_FILE"
    echo "🛑 Stop file created to terminate streaming ingest."
fi
wait $SPARK_PID
echo "✅ Streaming ingest stopped."

# ----------------------------------------
# Clean stop file after streaming ends
# ----------------------------------------
if [ -f "$STOP_FILE" ]; then
    rm "$STOP_FILE"
    rm -f "$PID_FILE"
    echo "✅ Clean shutdown complete"
fi

# ----------------------------------------
# Batch Ingest Orders
# ----------------------------------------
echo "🟢 Starting batch ingest..."
python ingestion/batch_ingest.py 
echo "✅ Batch ingest completed."

# ----------------------------------------
# Spark job auto-stops via inactivity_timeout
# ----------------------------------------
# Optionally wait a few seconds to ensure all files are picked up
sleep 5
echo "✅ Pipeline completed. All streams landed." 

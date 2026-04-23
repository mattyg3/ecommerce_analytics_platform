from pathlib import Path
import duckdb
import time
import argparse
import shutil
import datetime

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_LAKE = Path("/data-lake")

INPUT_PATH = DATA_LAKE / "raw" / "clickstream"
OUTPUT_PATH = DATA_LAKE / "landing" / "clickstream"

CHECKPOINT_FILE = DATA_LAKE / "checkpoints" / "clickstream_processed_files.txt"
STOP_FILE = BASE_DIR / "control" / "clickstream.stop"
STOP_FILE.parent.mkdir(parents=True, exist_ok=True)

NO_NEW_FILES_TIMEOUT = 15
CHECK_INTERVAL = 1

MIN_FILES_TO_COMPACT = 1   # threshold to trigger compaction


# -------------------------
# CLI
# -------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["stream", "backfill"], required=True)
    return parser.parse_args()


# -------------------------
# checkpoint
# -------------------------
def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        return set(CHECKPOINT_FILE.read_text().splitlines())
    return set()


def save_checkpoint(processed_files):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text("\n".join(sorted(processed_files)))


# -------------------------
# file discovery
# -------------------------
def list_new_files(processed_files):
    all_files = sorted(INPUT_PATH.glob("**/*.json"))
    return [str(f) for f in all_files if str(f) not in processed_files]


# -------------------------
# DuckDB ingestion (core logic)
# -------------------------
def ingest_files(con, files, processed_files, source_system):
    if not files:
        return 0

    total = 0

    for f in files:
        json_path = str(f)

        batch_id = int(datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S%f"))

        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

        con.execute(f"""
            COPY (
                SELECT
                    event_id,
                    event_type,
                    CAST(user_id as STRING) as user_id,
                    CAST(session_id as STRING) as session_id,
                    CAST(product_id AS STRING) as product_id,
                    TRY_CAST(event_time AS TIMESTAMP) AS event_time,
                    TRY_CAST(ingest_time AS TIMESTAMP) AS source_ingested_at,
                    CURRENT_TIMESTAMP AS pipeline_ingested_at,
                    strftime(ingest_time, '%Y-%m-%d') AS ingest_date,
                    version,
                    device,
                    country,
                    user_agent,
                    referrer,
                    experiment_id,
                    '{source_system}' AS source_system,
                    {batch_id} AS batch_id
                FROM read_json_auto('{json_path}', 
                    columns={{
                        'event_id': 'VARCHAR',
                        'event_type': 'VARCHAR',
                        'user_id': 'VARCHAR',
                        'session_id': 'VARCHAR',
                        'product_id': 'VARCHAR',
                        'event_time': 'TIMESTAMP',
                        'ingest_time': 'TIMESTAMP',
                        'version': 'VARCHAR',
                        'device': 'VARCHAR',
                        'country': 'VARCHAR',
                        'user_agent': 'VARCHAR',
                        'referrer': 'VARCHAR',
                        'experiment_id': 'VARCHAR'
                    }}
                )
            )
            TO '{OUTPUT_PATH}'
            (FORMAT PARQUET, PARTITION_BY (ingest_date, batch_id), OVERWRITE_OR_IGNORE 1)
        """)

        processed_files.add(json_path)
        total += 1

    return total

# -------------------------
# Compact one date partition
# -------------------------
def compact_date_partition(con, date_partition: Path):
    # print(f"🔧 Compacting {date_partition}")

    compacted_file = date_partition / "compacted.parquet"

    # Merge all parquet files under this date
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{date_partition}/**/*.parquet')
        )
        TO '{compacted_file}'
        (FORMAT PARQUET)
    """)

    # Remove batch folders
    for batch_dir in date_partition.glob("batch_id=*"):
        if batch_dir.is_dir():
            shutil.rmtree(batch_dir)

    # print(f"✅ Compacted → {compacted_file}")


# -------------------------
# Main compaction runner
# -------------------------
def run_compaction():
    print("🚀 Starting compaction job")

    con = duckdb.connect()

    # today = datetime.date.today().isoformat()

    for date_partition in OUTPUT_PATH.glob("ingest_date=*"):
        if not date_partition.is_dir():
            continue

        # # Skip today's partition (avoid conflict with streaming writes)
        # if today in date_partition.name:
        #     print(f"⏭ Skipping active partition {date_partition}")
        #     continue

        # Count parquet files recursively
        parquet_files = list(date_partition.glob("**/*.parquet"))

        if len(parquet_files) < MIN_FILES_TO_COMPACT:
            print(f"⏭ Skipping {date_partition} (only {len(parquet_files)} files)")
            continue

        compact_date_partition(con, date_partition)

    con.close()

    print("🎉 Compaction job complete")


# -------------------------
# backfill
# -------------------------
def run_backfill():
    con = duckdb.connect()

    processed_files = load_checkpoint()
    new_files = list_new_files(processed_files)

    print(f"🚀 Backfill: processing {len(new_files)} files")

    count = ingest_files(con, new_files, processed_files, "clickstream_generator")

    save_checkpoint(processed_files)

    print(f"✅ Backfill complete: {count} files ingested")

    con.close()


# -------------------------
# streaming loop
# -------------------------
def run_stream():
    con = duckdb.connect()

    processed_files = load_checkpoint()
    last_new_file_time = time.time()

    print("🔁 Streaming mode started")

    file_count = 0

    while True:
        if STOP_FILE.exists():
            print("🛑 Stop file detected. Exiting stream.")
            print(f"📥 Ingested {file_count} files")
            break

        new_files = list_new_files(processed_files)

        if new_files:
            count = ingest_files(con, new_files, processed_files, "clickstream_generator")
            save_checkpoint(processed_files)
            file_count+=count
            last_new_file_time = time.time()

        else:
            if time.time() - last_new_file_time > NO_NEW_FILES_TIMEOUT:
                print(f"📥 Ingested {file_count} files")
                print("⏹ No new files. Stopping stream.")
                break

        time.sleep(CHECK_INTERVAL)

    con.close()


# -------------------------
# main
# -------------------------
if __name__ == "__main__":
    args = parse_args()

    if args.mode == "backfill":
        run_backfill()
        run_compaction()
    else:
        run_stream()
from pathlib import Path
import os
import time
import argparse
from delta import configure_spark_with_delta_pip # type: ignore
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, to_date, current_timestamp, lit # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, TimestampType # type: ignore
from helper_functions.backfill_progress import log_backfill_progress
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] += ":" + str(Path(os.environ["JAVA_HOME"]) / "bin")
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

BASE_DIR = Path(__file__).resolve().parents[1]
CLICKSTREAM_CHECKPOINT = Path("/home/surff/spark_data/checkpoints/clickstream_ingest") #WSL path

STOP_FILE = BASE_DIR / "control" / "clickstream.stop"
STOP_FILE.parent.mkdir(parents=True, exist_ok=True)

NO_NEW_FILES_TIMEOUT = 15  # seconds with no new data before stopping
CHECK_INTERVAL = 1         # polling frequency

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("ingest_time", TimestampType(), False),
    StructField("version", StringType(), False),
    StructField("device", StringType(), False),
    StructField("country", StringType(), False),
    StructField("user_agent", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("experiment_id", StringType(), True),
])

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["stream", "backfill"],
        required=True,
        help="stream = continuous ingest, backfill = process all available files and exit",
    )
    return parser.parse_args()

def main(
    mode: str,
    input_path  = Path("/home/surff/spark_data/clickstream/raw"),
    output_path: Path = BASE_DIR / "data" / "landing" / "clickstream",
    checkpoint_path = Path("/home/surff/spark_data/checkpoints/clickstream_ingest"),
    source_system: str = "order_generator"
):
    builder = (
        SparkSession.builder
        .appName("StreamingIngestClickstream")
        .config("spark.master", "local[10]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.default.parallelism", "10")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    read_options = {}

    if mode == "stream":
        read_options["maxFilesPerTrigger"] = 50
    else:
        # backfill: let Spark go wide, no maxFilesPerTrigger
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        spark.conf.set("spark.default.parallelism", "200")

    raw_events = (
        spark.readStream
        .schema(CLICKSTREAM_SCHEMA)
        .options(**read_options)
        .format("json")
        .load(str(input_path))
    )

    landed = (
        raw_events
        .withColumnRenamed("ingest_time", "source_ingested_at")
        .withColumn("pipeline_ingested_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("source_ingested_at")))
        .withColumn("source_system", lit(source_system))
    )

    writer = (
        landed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .partitionBy("ingest_date")
    )

    if mode == "backfill":
        print("🚀 Running in BACKFILL mode (availableNow)")
        query = (
            writer
            .trigger(availableNow=True)
            .start(str(output_path))
        )
        log_backfill_progress(query)
        query.awaitTermination()
        print("✅ Backfill complete")

    else:
        print("🔁 Running in STREAM mode (continuous)")
        query = (
            writer
            .trigger(processingTime="30 seconds")
            .start(str(output_path))
        )

        while query.isActive:
            if STOP_FILE.exists():
                print("🛑 Stop file detected. Stopping streaming immediately")
                query.stop()  # Gracefully stops after finishing the current batch
                break
            time.sleep(1)

if __name__ == "__main__":
    args = parse_args()
    main(mode=args.mode)
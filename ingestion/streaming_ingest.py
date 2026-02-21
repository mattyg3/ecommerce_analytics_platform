from pathlib import Path
import os
import time
BASE_DIR = Path(__file__).resolve().parents[1]
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] += ":" + str(Path(os.environ["JAVA_HOME"]) / "bin")
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

STOP_FILE = BASE_DIR / "control" / "clickstream.stop"
STOP_FILE.parent.mkdir(parents=True, exist_ok=True)

NO_NEW_FILES_TIMEOUT = 15  # seconds with no new data before stopping
CHECK_INTERVAL = 1         # polling frequency

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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

def main(
    input_path  = Path("/home/surff/spark_data/clickstream/raw"),
    output_path: Path = BASE_DIR / "data" / "landing" / "clickstream",
    checkpoint_path = Path("/home/surff/spark_data/checkpoints/clickstream_ingest"),
    source_system: str = "order_generator"
):
    builder = (
        SparkSession.builder
        .appName("StreamingIngestClickstream")
        .config("spark.master", "local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    raw_events = (
        spark.readStream
        .schema(CLICKSTREAM_SCHEMA)
        .option("maxFilesPerTrigger", 10)
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

    query = (
        landed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .partitionBy("ingest_date")
        .trigger(processingTime="5 seconds")
        .start(str(output_path))
    )
    last_data_time = time.time()
    stop_requested = False

    while query.isActive:
        # Check for external stop signal
        if STOP_FILE.exists():
            if not stop_requested:
                print("🟡 Stop file detected. Entering drain mode...")
                stop_requested = True

        progress = query.lastProgress

        if progress:
            input_rows = progress.get("numInputRows", 0)

            if input_rows > 0:
                last_data_time = time.time()
                print(f"📥 Processed {input_rows} rows")

        # If stopping AND no new data for timeout window → stop
        if stop_requested:
            idle_time = time.time() - last_data_time
            if idle_time >= NO_NEW_FILES_TIMEOUT:
                print(f"🛑 No new files for {NO_NEW_FILES_TIMEOUT}s. Stopping stream.")
                query.stop()
                break

        time.sleep(CHECK_INTERVAL)

    

if __name__ == "__main__":
    main()
from pathlib import Path
import os
BASE_DIR = Path(__file__).resolve().parents[1]

# os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
# os.environ["HADOOP_HOME"] = r"C:\hadoop"
# os.environ["PYSPARK_PYTHON"] = str(BASE_DIR / ".venv" / "Scripts" / "python.exe")
# os.environ["PYSPARK_DRIVER_PYTHON"] = str(BASE_DIR / ".venv" / "Scripts" / "python.exe")
# os.environ["PATH"] += ";" + str(Path(os.environ["HADOOP_HOME"]) / "bin")
# os.environ["PATH"] += ";" + str(Path(os.environ["JAVA_HOME"]) / "bin")
# os.environ["HADOOP_OPTS"] = ""
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
# os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
# os.environ["PATH"] += ";" + str(Path(os.environ["HADOOP_HOME"]) / "bin")
os.environ["PATH"] += ";" + str(Path(os.environ["JAVA_HOME"]) / "bin")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# import glob
# files = glob.glob(str(BASE_DIR / "data" / "clickstream" / "raw" / "*.json"))

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
    # input_path: Path = BASE_DIR / "data" / "clickstream" / "raw",
    input_path = Path.home() / "spark_data" / "clickstream" / "raw",
    output_path: Path = BASE_DIR / "data" / "landing" / "clickstream",
    checkpoint_path: Path = BASE_DIR / "checkpoints" / "clickstream_ingest",
    source_system: str = "order_generator"
):
    builder = (
        SparkSession.builder
        .appName("StreamingIngestClickstream")
        .config("spark.master", "local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # .config("spark.driver.host", "127.0.0.1")
        # .config("spark.driver.bindAddress", "127.0.0.1")
        # .config("spark.executor.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        # .config("spark.hadoop.native.lib", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    raw_events = (
        spark.readStream
        .schema(CLICKSTREAM_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .format("json")
        .load(str(input_path))
        # .load([f"file:///{f.replace(os.sep, '/')}" for f in files])
        # .load(f"file:///{input_path.as_posix()}")
        # .json(f"file:///{input_path.replace(os.sep, '/')}")
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
        .trigger(once=True)   # process current files then exit
        .start(str(output_path))
        .awaitTermination()
    )

        
    print("Streaming query started")
    print("Status:", query.status)
    print("Recent progress:", query.recentProgress)

    query.awaitTermination()

if __name__ == "__main__":
    main()
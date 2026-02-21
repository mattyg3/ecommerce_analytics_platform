from pathlib import Path
import os
BASE_DIR = Path(__file__).resolve().parents[1]

# os.environ["JAVA_HOME"] = r"C:\Users\surff\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
# os.environ["HADOOP_HOME"] = r"C:\hadoop"
# os.environ["PYSPARK_PYTHON"] = str(BASE_DIR / ".venv" / "Scripts" / "python.exe")
# os.environ["PYSPARK_DRIVER_PYTHON"] = str(BASE_DIR / ".venv" / "Scripts" / "python.exe")
# os.environ["PATH"] += ";" + str(Path(os.environ["HADOOP_HOME"]) / "bin")
# os.environ["PATH"] += ";" + str(Path(os.environ["JAVA_HOME"]) / "bin")
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

# import glob
# files = glob.glob(str(BASE_DIR / "data" / "orders" / "raw" / "*.json"))

CART_SCHEMA = ArrayType(
    StructType([
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DoubleType(), False),
    ])
)

ORDER_SCHEMA = StructType([
    StructField("session_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("items", CART_SCHEMA, False),
    StructField("order_status", StringType(), False),
    StructField("order_time", TimestampType(), False),
    StructField("ingest_time", TimestampType(), False),
])

def main(
        input_path: Path = BASE_DIR / 'data' / 'orders' / 'raw', 
        output_path: Path = BASE_DIR / 'data' / 'landing' / 'orders',
        source_system: str = "order_generator"
):
    
    builder = (
        SparkSession.builder
        .appName("BatchIngestOrders")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    raw_orders = (
        spark.read
        .schema(ORDER_SCHEMA)
        # .json([f"file:///{f.replace(os.sep, '/')}" for f in files]) #bypass Hadoop filesystem
        .json(str(input_path))
    )

    landed = (
        raw_orders
        .withColumnRenamed("ingest_time", "source_ingested_at")
        .withColumn("pipeline_ingested_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("source_ingested_at")))
        .withColumn("source_system", lit(source_system))
    )

    (
        landed.write
        .format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(str(output_path))
    )

if __name__ == "__main__":
    main()
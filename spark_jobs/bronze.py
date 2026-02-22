from pathlib import Path
import os
from helper_functions import validate_delta
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import to_timestamp, current_timestamp # type: ignore
from delta import configure_spark_with_delta_pip # type: ignore

BASE_DIR = Path(__file__).resolve().parents[1]

LANDING = BASE_DIR / "data" / "landing"
BRONZE = BASE_DIR / "data" / "bronze"
CHECKPOINTS = BASE_DIR / "checkpoints" / "bronze"

VALIDATE = os.getenv("BRONZE_VALIDATE", "true").lower() == "true"

def get_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def bronze_clickstream(spark):
    df = spark.read.format("delta").load(str(LANDING / "clickstream"))

    bronze_df = (
        df
        .withColumn("event_time", to_timestamp("event_time"))
        .withColumn("source_ingested_at", to_timestamp("source_ingested_at"))
        .withColumn("bronze_ingested_at", current_timestamp())
    )

    (
        bronze_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(str(BRONZE / "clickstream"))
    )

def bronze_orders(spark):
    df = spark.read.format("delta").load(str(LANDING / "orders"))

    bronze_df = (
        df
        .withColumn("order_time", to_timestamp("order_time"))
        .withColumn("bronze_ingested_at", current_timestamp())
    )

    (
        bronze_df
        .write
        .format("delta")
        .mode("append")
        .save(str(BRONZE / "orders"))
    )

if __name__ == "__main__":
    spark = get_spark("BronzeLayer")

    bronze_clickstream(spark)
    validate_delta(spark, str(BRONZE / "clickstream"), "Bronze Clickstream")
    bronze_orders(spark)
    validate_delta(spark, str(BRONZE / "orders"), "Bronze Orders")

    spark.stop()
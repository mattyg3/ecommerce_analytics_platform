from pathlib import Path
# import os
from helper_functions import validate_delta
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import to_timestamp, current_timestamp # type: ignore
# from delta import configure_spark_with_delta_pip # type: ignore

# BASE_DIR = Path(__file__).resolve().parents[1]
DATA_LAKE = Path("/data-lake")
# DATA_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "/data-lake"))

# LANDING = BASE_DIR / "data-lake" / "landing"
# BRONZE = BASE_DIR / "data-lake" / "bronze"
LANDING = DATA_LAKE / "landing"
BRONZE = DATA_LAKE / "bronze"
# CHECKPOINTS = BASE_DIR / "checkpoints" / "bronze"

# VALIDATE = os.getenv("BRONZE_VALIDATE", "true").lower() == "true"

def build_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/data-lake/warehouse")
        # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    builder.sparkContext.setLogLevel("ERROR")
    # return configure_spark_with_delta_pip(builder).getOrCreate()

    builder.sql("CREATE DATABASE IF NOT EXISTS bronze")

    return builder

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

    (
        bronze_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .saveAsTable("bronze.clickstream")
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
        .partitionBy("ingest_date")
        .save(str(BRONZE / "orders"))
    )

    (
        bronze_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .saveAsTable("bronze.orders")
    )

if __name__ == "__main__":
    spark = build_spark("BronzeLayer")

    bronze_clickstream(spark)
    validate_delta(spark, str(BRONZE / "clickstream"), "Bronze Clickstream")
    bronze_orders(spark)
    validate_delta(spark, str(BRONZE / "orders"), "Bronze Orders")

    spark.stop()
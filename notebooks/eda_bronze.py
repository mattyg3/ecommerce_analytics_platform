from pyspark.sql import SparkSession # type: ignore
from delta import configure_spark_with_delta_pip # type: ignore
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE = BASE_DIR / "data" / "bronze"

builder = (
    SparkSession.builder
    .appName("InspectBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load clickstream
clickstream_df = spark.read.format("delta").load(str(BRONZE / "clickstream")).toPandas()

print(clickstream_df["ingest_date"].min())
print(clickstream_df["ingest_date"].max()) 

print(clickstream_df["source_ingested_at"].min())
print(clickstream_df["source_ingested_at"].max()) 
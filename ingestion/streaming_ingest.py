from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

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
        input_path: str = 'data/clickstream/raw',
        output_path: str = "data/landing/clickstream",
        source_system: str = "order_generator"
):
    spark = (
        SparkSession.builder
        .appName("ClickstreamStreamingIngest")
        .getOrCreate()
    )

    raw_events = (
        spark.readStream
        .schema(CLICKSTREAM_SCHEMA)
        .json(input_path)
    )

    landed = (
        raw_events
        .withColumnRenamed("ingest_time", "source_ingested_at")
        .withColumn("pipeline_ingested_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("source_ingested_at")))
        .withColumn("source_system", lit(source_system))
    )

    (
        landed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "checkpoints/clickstream_ingest")
        .partitionBy("ingest_date")
        .start(output_path)
        .awaitTermination()
    )

if __name__ == "__main__":
    main()
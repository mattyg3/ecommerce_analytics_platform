from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

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
        input_path: str = 'data/orders/raw',
        output_path: str = "data/landing/orders",
        source_system: str = "order_generator"
):
    spark = (
        SparkSession.builder
        .appName("BatchIngestOrders")
        .getOrCreate()
    )

    raw_orders = (
        spark.read
        .schema(ORDER_SCHEMA)
        .parquet(input_path)
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
        .save(output_path)
    )

if __name__ == "__main__":
    main()
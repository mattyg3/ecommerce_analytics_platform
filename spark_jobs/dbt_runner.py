from pathlib import Path
import os
from typing import List

from pyspark.sql import SparkSession  # type: ignore
from dbt.cli.main import dbtRunner, dbtRunnerResult # type: ignore

from helper_functions import validate_delta


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_LAKE = Path("/data-lake")
DBT_DIR = BASE_DIR / "dbt_project"

env_vars = os.environ.copy()
env_vars["DBT_GOLD_PATH"] = str(DATA_LAKE / "gold")
env_vars["DBT_SILVER_PATH"] = str(DATA_LAKE / "silver")
env_vars["DBT_BRONZE_PATH"] = str(DATA_LAKE / "bronze")


# -------------------------
# Spark Session
# -------------------------
def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[10]")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/data-lake/warehouse")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    return spark


# -------------------------
# dbt runner (NO subprocess)
# -------------------------
dbt = dbtRunner()


def run_dbt(args: List[str]) -> None:
    print(f"▶ Running dbt: {' '.join(args)}")

    result: dbtRunnerResult = dbt.invoke(
        args,
        project_dir=str(DBT_DIR),
        profiles_dir=str(DBT_DIR),
        env=env_vars,
    )

    if not result.success:
        raise RuntimeError(f"dbt command failed: {' '.join(args)}")


# -------------------------
# Main pipeline
# -------------------------
def main():
    print("🚀 Starting Silver & Gold layer build (SESSION MODE)")

    spark = build_spark("DBTLayerBuild")

    try:
        # Install dependencies
        run_dbt(["deps"])

        # Run staging models
        run_dbt([
            "run",
            "--select", "staging+",
            "--full-refresh"
        ])

        # Run tests
        run_dbt([
            "test",
            "--select", "staging+"
        ])

        # Validate Delta outputs
        validate_delta(
            spark,
            env_vars["DBT_SILVER_PATH"] + "/stg_clickstream_events",
            "Silver Clickstream Events"
        )

        validate_delta(
            spark,
            env_vars["DBT_SILVER_PATH"] + "/stg_clickstream_sessions",
            "Silver Clickstream Sessions"
        )

        validate_delta(
            spark,
            env_vars["DBT_SILVER_PATH"] + "/stg_orders",
            "Silver Orders"
        )

        validate_delta(
            spark,
            env_vars["DBT_SILVER_PATH"] + "/stg_order_items",
            "Silver Order Items"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# from pathlib import Path
# import subprocess
# import sys
# import os
# # from delta import configure_spark_with_delta_pip # type: ignore
# from pyspark.sql import SparkSession # type: ignore
# from helper_functions import validate_delta
# from typing import List

# BASE_DIR = Path(__file__).resolve().parents[1]
# DBT_DIR = BASE_DIR / "dbt"
# env_vars = os.environ.copy()
# env_vars["DBT_GOLD_PATH"] = str(BASE_DIR / "data-lake" / "gold")
# env_vars["DBT_SILVER_PATH"] = str(BASE_DIR / "data-lake" / "silver")
# env_vars["DBT_BRONZE_PATH"] = str((BASE_DIR / "data-lake" / "bronze").resolve())

# # os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
# # os.environ["PATH"] += ":" + str(Path(os.environ["JAVA_HOME"]) / "bin")
# # os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
# # os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

# def build_spark(app_name: str) -> SparkSession:
#     builder = (
#         SparkSession.builder
#         .appName(app_name)
#         .config("spark.master", "local[10]")
#         .config("spark.sql.shuffle.partitions", "200")
#         .config("spark.default.parallelism", "200")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#         .config("spark.sql.warehouse.dir", "/app/data-lake/warehouse")
#         # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
#         .config("spark.hadoop.fs.defaultFS", "file:///")
#         .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
#         .config("spark.driver.memory", "4g")
#         .config("spark.executor.memory", "4g")
#         .getOrCreate()
#     )

#     # return configure_spark_with_delta_pip(builder).getOrCreate()

#     builder.sql("CREATE DATABASE IF NOT EXISTS silver")
#     builder.sql("CREATE DATABASE IF NOT EXISTS gold")
#     return builder

# # def run_dbt(cmd: list[str]) -> None:
# def run_dbt(cmd: List[str]) -> None:
#     print(f"▶ Running: {' '.join(cmd)}")

#     result = subprocess.run(
#         cmd,
#         cwd=DBT_DIR,
#         env=env_vars,
#         stdout=sys.stdout,
#         stderr=sys.stderr,
#     )

#     if result.returncode != 0:
#         raise RuntimeError(f"dbt command failed: {' '.join(cmd)}")


# def main():
#     print("🚀 Starting Silver & Gold layer build")

#     spark = build_spark("DBTLayerBuild")

#     try:
#         run_dbt(["dbt", "deps", "--project-dir", str(DBT_DIR)])
        
#         run_dbt([
#             "dbt", "run", "--project-dir", str(DBT_DIR),
#             "--select", "staging+", "--full-refresh" #only needed if first run, or want to refresh data completely
#         ])

#         run_dbt([
#             "dbt", "test", "--project-dir", str(DBT_DIR),
#             "--select", "staging+"
#         ])

#         # Validate silver tables

#         validate_delta(spark, env_vars["DBT_SILVER_PATH"] + "/stg_clickstream_events", "Silver Clickstream Events")
#         validate_delta(spark, env_vars["DBT_SILVER_PATH"] + "/stg_clickstream_sessions", "Silver Clickstream Sessions")
#         validate_delta(spark, env_vars["DBT_SILVER_PATH"] + "/stg_orders", "Silver Orders")
#         validate_delta(spark, env_vars["DBT_SILVER_PATH"] + "/stg_order_items", "Silver Order Items")

#     finally:
#         spark.stop()


# if __name__ == "__main__":
#     main()
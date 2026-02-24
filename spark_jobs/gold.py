from pathlib import Path
import subprocess
import sys
import os
from delta import configure_spark_with_delta_pip # type: ignore
from pyspark.sql import SparkSession # type: ignore

BASE_DIR = Path(__file__).resolve().parents[1]
DBT_DIR = BASE_DIR / "dbt"
print(f"DBT_DIR: {DBT_DIR}")
env_vars = os.environ.copy()
env_vars["DBT_TARGET_PATH"] = str(BASE_DIR / "data-lake" / "gold")
env_vars["DBT_BRONZE_PATH"] = str((BASE_DIR / "data-lake" / "bronze").resolve())

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] += ":" + str(Path(os.environ["JAVA_HOME"]) / "bin")
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.master", "local[10]")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_dbt(cmd: list[str]) -> None:
    print(f"▶ Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=DBT_DIR,
        env=env_vars,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(cmd)}")


def main():
    print("🚀 Starting Gold layer build")

    spark = build_spark("GoldLayerBuild")

    try:
        run_dbt(["dbt", "deps"])
        
        run_dbt([
            "dbt", "run",
            "--select", "marts+", "--full-refresh" #only needed if first run, or want to refresh data completely
        ])

        run_dbt([
            "dbt", "test",
            "--select", "marts"
        ])

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
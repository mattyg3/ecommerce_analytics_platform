from pathlib import Path
import duckdb
# from datetime import datetime
from helper_functions import validate_table

DATA_LAKE = Path("/data-lake")
LANDING = DATA_LAKE / "landing"
DUCKDB_PATH = DATA_LAKE / "warehouse.duckdb"


def build_duckdb():
    con = duckdb.connect(str(DUCKDB_PATH))
    # con = duckdb.connect("/app/data-lake/warehouse.duckdb")

    # Create schemas
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    return con


def bronze_clickstream(con):
    landing_path = LANDING / "clickstream" / "**" / "*.parquet"

    query = f"""
        INSERT INTO bronze.clickstream
        SELECT
            * EXCLUDE (event_time, source_ingested_at),
            CAST(event_time AS TIMESTAMP) AS event_time,
            CAST(source_ingested_at AS TIMESTAMP) AS source_ingested_at,
            CURRENT_TIMESTAMP AS bronze_ingested_at
        FROM read_parquet('{landing_path}', hive_partitioning=true)
        WHERE ingest_date NOT IN (
            SELECT DISTINCT ingest_date FROM bronze.clickstream
        )
    """

    # Create table if not exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS bronze.clickstream AS
        SELECT
            * EXCLUDE (event_time, source_ingested_at),
            CAST(event_time AS TIMESTAMP) AS event_time,
            CAST(source_ingested_at AS TIMESTAMP) AS source_ingested_at,
            CURRENT_TIMESTAMP AS bronze_ingested_at
        FROM read_parquet('{landing_path}', hive_partitioning=true)
        WHERE 1=0
    """)

    con.execute(query)


def bronze_orders(con):
    landing_path = LANDING / "orders" / "**" / "*.parquet"

    query = f"""
        INSERT INTO bronze.orders
        SELECT
            * EXCLUDE (order_time),
            CAST(order_time AS TIMESTAMP) AS order_time,
            CURRENT_TIMESTAMP AS bronze_ingested_at
        FROM read_parquet('{landing_path}', hive_partitioning=true)
        WHERE ingest_date NOT IN (
            SELECT DISTINCT ingest_date FROM bronze.orders
        )
    """

    # Create table if not exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS bronze.orders AS
        SELECT
            * EXCLUDE (order_time),
            CAST(order_time AS TIMESTAMP) AS order_time,
            CURRENT_TIMESTAMP AS bronze_ingested_at
        FROM read_parquet('{landing_path}', hive_partitioning=true)
        WHERE 1=0
    """)

    con.execute(query)


if __name__ == "__main__":
    con = build_duckdb()

    bronze_clickstream(con)
    validate_table(con, "bronze.clickstream", "Bronze Clickstream")

    bronze_orders(con)
    validate_table(con, "bronze.orders", "Bronze Orders")

    con.close()
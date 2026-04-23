from pathlib import Path
import duckdb

DATA_LAKE = Path("/data-lake")

def main(
    input_path = DATA_LAKE / "raw" / "orders",
    output_path = DATA_LAKE / "landing" / "orders",
    source_system: str = "order_generator"
):
    con = duckdb.connect()

    json_path = str(input_path / "**/*.json")

    output_path.mkdir(parents=True, exist_ok=True)

    # DuckDB reads + transforms
    df = con.execute(f"""
        SELECT
            CAST(session_id AS STRING) as session_id,
            CAST(order_id AS STRING) as order_id, 
            CAST(user_id AS STRING) as user_id,
            items,
            order_status,
            CAST(order_time AS TIMESTAMP) AS order_time,
            CAST(ingest_time AS TIMESTAMP) AS source_ingested_at,
            CURRENT_TIMESTAMP AS pipeline_ingested_at,
            strftime(CAST(ingest_time AS TIMESTAMP), '%Y-%m-%d') AS ingest_date,
            '{source_system}' AS source_system
        FROM read_json_auto('{json_path}')
    """).df()

    # partitioned write
    for ingest_date, partition_df in df.groupby("ingest_date"):

        partition_dir = output_path / f"ingest_date={ingest_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        file_path = partition_dir / "part.parquet"

        partition_df.to_parquet(file_path, index=False)

    # print(f"✅ Landing Orders written to {output_path}")

if __name__ == "__main__":
    main()
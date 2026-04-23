from pathlib import Path
import os
from typing import List
import duckdb
from helper_functions import validate_table

from dbt.cli.main import dbtRunner, dbtRunnerResult  # type: ignore


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_LAKE = Path("/data-lake")
DBT_DIR = BASE_DIR / "dbt_project"

# DuckDB database (shared across layers)
DUCKDB_PATH = DATA_LAKE / "warehouse.duckdb"

os.environ["DBT_DUCKDB_PATH"] = str(DUCKDB_PATH)


# -------------------------
# DuckDB connection (for validation only)
# -------------------------
def get_connection():
    return duckdb.connect(str(DUCKDB_PATH))


# -------------------------
# dbt runner
# -------------------------
dbt = dbtRunner()


def run_dbt(args: List[str]) -> None:
    print(f"▶ Running dbt: {' '.join(args)}")

    result: dbtRunnerResult = dbt.invoke(
        args,
        project_dir=str(DBT_DIR),
        profiles_dir=str(DBT_DIR),
        env={**os.environ, "DBT_DUCKDB_PATH": str(DUCKDB_PATH)},
    )

    if not result.success:
        raise RuntimeError(f"dbt command failed: {' '.join(args)}")

# -------------------------
# Main pipeline
# -------------------------
def main():
    print("🚀 Starting Silver build (DuckDB + dbt)")

    con = get_connection()

    try:
        # Install dependencies
        run_dbt(["deps"])

        # Run staging (silver)
        run_dbt([
            "run",
            "--select", "staging",
            "--full-refresh"
        ])

        # Run tests
        run_dbt([
            "test",
            "--select", "staging"
        ])

        # -------------------------
        # Validate outputs
        # -------------------------
        validate_table(con, "staging.stg_clickstream_events", "Silver Clickstream Events")
        validate_table(con, "staging.stg_clickstream_sessions", "Silver Clickstream Sessions")
        validate_table(con, "staging.stg_orders", "Silver Orders")
        validate_table(con, "staging.stg_order_items", "Silver Order Items")

        # -------------------------
        # Run gold models
        # -------------------------
        print("🚀 Starting Gold build (DuckDB + dbt)")
        run_dbt([
            "run",
            "--select", "marts+"
        ])

        run_dbt([
            "test",
            "--select", "marts+"
        ])

    finally:
        con.close()


if __name__ == "__main__":
    main()
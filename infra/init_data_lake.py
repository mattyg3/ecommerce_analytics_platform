def init_data_lake():
    from pathlib import Path

    # base = Path("/data-lake") #when run with airflow

    project_root = Path(__file__).resolve().parents[1] #manually on WSL
    base = project_root / "data-lake"

    for p in [
        base / "warehouse" / "analytics_bronze.db",
        base / "warehouse" / "analytics_silver.db",
        base / "warehouse" / "analytics_gold.db",
        base / "bronze" / "clickstream",
        base / "bronze" / "orders",
    ]:
        p.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    init_data_lake()
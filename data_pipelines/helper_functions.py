def validate_table(con, table_name: str, label: str):
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"✅ {label}: {count} rows")
    except Exception as e:
        print(f"❌ Validation failed for {label}: {e}")
        raise
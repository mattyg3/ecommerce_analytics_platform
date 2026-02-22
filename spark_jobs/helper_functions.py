def validate_delta(spark, path, name):
    df = spark.read.format("delta").load(path)
    print(f"\n📊 {name}")
    df.printSchema()
    print(f"Rows: {df.count()}")
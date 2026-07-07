from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date

spark = SparkSession.builder \
    .appName("silver-to-gold") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/opt/airflow/data/lakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver = spark.table("local.logsearch_db.silver_search_events")

# Chỉ tính category='enter' (hành động tìm kiếm thật), loại dòng keyword rỗng
gold_keywords = silver \
    .filter((col("category") == "enter") & col("keyword").isNotNull()) \
    .withColumn("event_date", to_date(col("event_time"))) \
    .groupBy("event_date", "keyword") \
    .agg(count("*").alias("search_count")) \
    .orderBy(col("search_count").desc())

gold_keywords.writeTo("local.logsearch_db.gold_keyword_trends").createOrReplace()

print("Top 10 từ khoá được tìm nhiều nhất:")
gold_keywords.show(10, truncate=False)

spark.stop()
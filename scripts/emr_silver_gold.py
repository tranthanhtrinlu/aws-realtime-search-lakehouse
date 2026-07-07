from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, count, abs as sql_abs, datediff

BUCKET = "thanhtri-logsearch-lakehouse-dev"

spark = SparkSession.builder \
    .appName("emr-bronze-silver-gold") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Đọc toàn bộ 28 ngày cùng lúc — Spark tự nhận diện cột "date" từ tên folder partition (date=YYYY-MM-DD)
bronze = spark.read.parquet(f"s3://{BUCKET}/raw/logsearch/")
print("Tổng dòng Bronze (28 ngày):", bronze.count())

# Làm sạch: parse datetime, so với NGÀY THẬT của partition (thay vì 1 ngày cố định như bản local)
# — cách này tổng quát hơn, xử lý đúng cho cả 28 ngày cùng lúc
silver = bronze \
    .withColumn("event_time", to_timestamp(col("datetime"))) \
    .withColumn("partition_date", to_date(col("date"))) \
    .filter(col("event_time").isNotNull()) \
    .filter(sql_abs(datediff(col("event_time"), col("partition_date"))) <= 2) \
    .dropDuplicates(["eventID"])

print("Tổng dòng Silver (sau làm sạch):", silver.count())

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.logsearch_db")
silver.writeTo("glue_catalog.logsearch_db.silver_search_events").createOrReplace()

# Gold: từ khoá tìm nhiều nhất, gộp theo ngày
gold_keywords = silver \
    .filter((col("category") == "enter") & col("keyword").isNotNull()) \
    .withColumn("event_date", col("partition_date")) \
    .groupBy("event_date", "keyword") \
    .agg(count("*").alias("search_count")) \
    .orderBy(col("search_count").desc())

gold_keywords.writeTo("glue_catalog.logsearch_db.gold_keyword_trends").createOrReplace()

print("Top 20 từ khoá toàn bộ 28 ngày:")
gold_keywords.show(20, truncate=False)

print("HOÀN THÀNH — Bronze/Silver/Gold đã ghi vào Glue Catalog trên S3.")
spark.stop()
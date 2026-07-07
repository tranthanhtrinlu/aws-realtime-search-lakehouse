from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

spark = SparkSession.builder \
    .appName("bronze-to-silver") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/opt/airflow/data/lakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet("/opt/airflow/data/bronze/logsearch")

# Làm sạch: parse datetime đúng kiểu, loại các dòng có giờ vô lý (đồng hồ thiết bị lỗi/lịch khác),
# loại trùng lặp theo eventID (Kafka có thể gửi lặp trong 1 số trường hợp - "at least once delivery")
silver = bronze \
    .withColumn("event_time", to_timestamp(col("datetime"))) \
    .filter(col("event_time").between("2022-01-01", "2022-12-31")) \
    .dropDuplicates(["eventID"])

spark.sql("CREATE DATABASE IF NOT EXISTS local.logsearch_db")
silver.writeTo("local.logsearch_db.silver_search_events").createOrReplace()

print("Số dòng Bronze:", bronze.count())
print("Số dòng Silver (sau khi làm sạch):", silver.count())

spark.sql("SELECT * FROM local.logsearch_db.silver_search_events LIMIT 5").show(truncate=False)

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder \
    .appName("kafka-consumer-test") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema khớp với dữ liệu producer đang gửi (xem lại lúc test_spark.py in ra printSchema())
schema = StructType([
    StructField("eventID", StringType()),
    StructField("datetime", StringType()),
    StructField("user_id", StringType()),
    StructField("keyword", StringType()),
    StructField("category", StringType()),
    StructField("proxy_isp", StringType()),
    StructField("platform", StringType()),
    StructField("networkType", StringType()),
    StructField("action", StringType()),
    StructField("userPlansMap", ArrayType(StringType())),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "search-events") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = parsed.writeStream \
    .format("parquet") \
    .option("path", "/opt/airflow/data/bronze/logsearch") \
    .option("checkpointLocation", "/opt/airflow/data/bronze/_checkpoint/logsearch") \
    .outputMode("append") \
    .start()

query.awaitTermination()
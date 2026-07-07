from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test-spark-local") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet(r"E:\DataSetDataEngineer\log_search\20220601")  # đổi thành đường dẫn file 20220601 như mọi lần
df.printSchema()
df.show(5, truncate=False)
print("Tổng số dòng:", df.count())

spark.stop()
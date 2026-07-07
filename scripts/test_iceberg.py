from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test-iceberg-local") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "data/lakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE TABLE IF NOT EXISTS local.test_db.hello (id INT, msg STRING) USING iceberg")
spark.sql("INSERT INTO local.test_db.hello VALUES (1, 'xin chao iceberg')")
spark.sql("SELECT * FROM local.test_db.hello").show(truncate=False)

spark.stop()
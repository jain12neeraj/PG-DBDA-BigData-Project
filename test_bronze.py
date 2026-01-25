from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

df = spark.read.parquet("hdfs://localhost:9000/data_lake/bronze/artists/batch_id=1")

print("Rows:", df.count())
df.show(50, truncate=False)

spark.stop()

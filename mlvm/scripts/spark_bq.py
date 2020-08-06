from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mlvmTest").getOrCreate()

table = "bigquery-public-data.samples.shakespeare"
df = spark.read.format("bigquery").option("table", table).load()

df.take(1)

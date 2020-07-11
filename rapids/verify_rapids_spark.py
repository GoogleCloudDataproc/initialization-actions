from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("NYTaxiXGBoost4j")
conf.set('spark.submit.pyFiles', '/usr/lib/spark/jars/xgboost4j-spark_3.0-1.0.0-0.1.0.jar')
conf.set("spark.executor.instances", "1")
conf.set("spark.executor.cores", "1") # spark.executor.cores times spark.executor.instances should equal total cores.
conf.set("spark.task.cpus", "1")
conf.set("spark.executor.memory", "2g")
conf.set("spark.task.resource.gpu.amount", "1")
conf.set("spark.plugins", "com.nvidia.spark.SQLPlugin")
spark = SparkSession.builder \
                    .config(conf=conf) \
                    .getOrCreate()

print("CREATED SPARK SESSION")
sc = spark.sparkContext
df1 = sc.parallelize([[x] for x in range(0,1000)]).toDF()
df2 = sc.parallelize([[x] for x in range(0,1000)]).toDF()
out = df1.join(df2, df1._1 == df2._1)

print("************ Join count:", out.count())
out.explain()

print("SUCCESS")
sc.stop()

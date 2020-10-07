from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType
from ml.dmlc.xgboost4j.scala.spark import XGBoostClassificationModel, XGBoostClassifier

conf = SparkConf().setAppName("RAPIDS_Accelerator_Spark_XGBoost_test")
conf.set("spark.executor.instances", "1")
conf.set("spark.executor.cores", "1")
conf.set("spark.task.cpus", "1")
conf.set("spark.executor.memory", "2g")
conf.set("spark.task.resource.gpu.amount", "1")
conf.set("spark.plugins", "com.nvidia.spark.SQLPlugin")
conf.set("spark.rapids.memory.gpu.pooling.enabled", "false")
spark = SparkSession.builder \
                    .config(conf=conf) \
                    .getOrCreate()

label = 'l'
schema = StructType([
    StructField('c0', FloatType()),
    StructField('c1', FloatType()),
    StructField(label, IntegerType()),
])

features = [ x.name for x in schema if x.name != label ]
df = spark.createDataFrame(
    [
        (1.05, 9.05, 0), # create your data here, be consistent in the types.
        (2.95, 1.95, 1),
    ], schema)

params = {
    'missing': 0.0,
    'treeMethod': 'gpu_hist',
    'maxDepth': 2,
    'objective':'binary:logistic',
    'nthread': 1,
    'numRound': 100,
    'numWorkers': 1,
}

classifier = XGBoostClassifier(**params).setLabelCol(label).setFeaturesCols(features)
model = classifier.fit(df)

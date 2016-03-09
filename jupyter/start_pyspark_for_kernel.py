#import findspark
#
#findspark.init()
#
#from pyspark import SparkContext, SparkConf
#from pyspark.sql import HiveContext
#
#conf = SparkConf().setMaster('yarn-client')
#sc = SparkContext(conf=conf)
#sqlContext = HiveContext(sc)
#
import os
import sys
spark_home = '/usr/lib/spark/'
os.environ["SPARK_HOME"] = spark_home
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.9-src.zip'))
#execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))


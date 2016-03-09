import findspark

findspark.init()

import pyspark
from pyspark.sql import HiveContext

sc = pyspark.SparkContext()
sqlContext = HiveContext(sc)


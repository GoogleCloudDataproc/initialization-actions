import pyspark
import sys

sc = pyspark.SparkContext()
distData = sc.parallelize(range(100))
python_distros = distData.map(lambda x: sys.executable).distinct().collect()

print(python_distros)

import pyspark
import sys
import os

pyhashseed = os.environ['PYTHONHASHSEED']
print(pyhashseed)
print(type(pyhashseed))
print(sys.version)

if sys.version >= '3.3' and 'PYTHONHASHSEED' not in os.environ:
    raise Exception(
        "Randomness of hash of string should be disabled via PYTHONHASHSEED")

else:
    sc = pyspark.SparkContext()
    distData = sc.parallelize(range(100))
    python_distros = distData.map(
        lambda x: sys.executable).distinct().collect()
    print(python_distros)

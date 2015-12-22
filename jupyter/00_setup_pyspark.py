# To be copied to ~/.ipython/profile_default/startup/
# See associated README.md

import os
import sys

spark_home = '/usr/lib/spark/'
os.environ["SPARK_HOME"] = spark_home
sys.path.insert(0, os.path.join(spark_home, 'python'))

# # If PySpark isn't specified, use currently running Python binary:
# pyspark_python = None
# pyspark_python = pyspark_python or sys.executable
# os.environ['PYSPARK_PYTHON'] = pyspark_python

# Launch PySpark Shell
spark_shell_path = os.path.join(spark_home, 'python/pyspark/shell.py')
# attempt to execute first via Python 2
try:
    execfile(spark_shell_path)
# and if not, Python 3
except NameError:
    exec(open(spark_shell_path).read())


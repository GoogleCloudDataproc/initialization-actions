#!/bin/bash

set -euxo pipefail

readonly H20_VERSION=3.26.7

SPARK_VERSION=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\.[0-9]\+\+\).*/\1/p' | head -n1)
readonly SPARK_VERSION

pip install requests tabulate future colorama scikit-learn google-cloud-bigquery google-cloud-storage
pip install "h2o_pysparkling_${SPARK_VERSION%.*}==${H20_VERSION}"

echo "Tuning Spark configuration in spark-defaults.conf"
sed -i 's/spark.dynamicAllocation.enabled=true/spark.dynamicAllocation.enabled=false/g' /usr/lib/spark/conf/spark-defaults.conf
sed -i 's/spark.executor.instances=10000/# spark.executor.instances=10000/g' /usr/lib/spark/conf/spark-defaults.conf
sed -i 's/spark.executor.cores.*/# removing unnecessary limits to executor cores/g' /usr/lib/spark/conf/spark-defaults.conf
sed -i 's/spark.executor.memory.*/# removing unnecessary limits to executor memory/g' /usr/lib/spark/conf/spark-defaults.conf
echo "Successfully tuned Spark configuration in spark-defaults.conf"

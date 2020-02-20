#!/bin/bash

set -euxo pipefail


echo "BEGIN Stage 1 : Install H2O libraries and dependencies"
apt-get update



## Set Spark and Sparkling water versions
H2O_SPARKLING_WATER_VERSION="3.28.0.1-1"
SPARK_VERSION=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
echo $SPARK_VERSION



## Download and unzip Sparking water Scala libraries
SPARKLING_WATER="http://h2o-release.s3.amazonaws.com/sparkling-water/spark-$SPARK_VERSION/$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION/sparkling-water-$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION.zip"
echo $SPARKLING_WATER
PYSPARKLING_WATER="h2o_pysparkling_$SPARK_VERSION"
echo $PYSPARKLING_WATER

cd /usr/lib/
wget $SPARKLING_WATER
unzip "sparkling-water-$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION.zip"

H2O_SPARKLING_WATER_HOME="/usr/lib/sparkling-water-$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION"
echo $H2O_SPARKLING_WATER_HOME



## Update $TOPDIR variable in sparkling scripts and add home variable
sed -i "2iH2O_SPARKLING_WATER_HOME=\"$H2O_SPARKLING_WATER_HOME\"" $H2O_SPARKLING_WATER_HOME/bin/sparkling-shell
sed -i 's/TOPDIR=$(cd "$(dirname "$0")\/.."; pwd)/TOPDIR=$H2O_SPARKLING_WATER_HOME/g' $H2O_SPARKLING_WATER_HOME/bin/sparkling-shell

sed -i "2iH2O_SPARKLING_WATER_HOME=\"$H2O_SPARKLING_WATER_HOME\"" $H2O_SPARKLING_WATER_HOME/bin/pysparkling
sed -i 's/TOPDIR=$(cd "$(dirname "$0")\/.."; pwd)/TOPDIR=$H2O_SPARKLING_WATER_HOME/g' $H2O_SPARKLING_WATER_HOME/bin/pysparkling

## create soft link entries for default
ln -s $H2O_SPARKLING_WATER_HOME/bin/sparkling-shell /opt/conda/default/bin/sparkling-shell
ln -s $H2O_SPARKLING_WATER_HOME/bin/pysparkling /opt/conda/default/bin/pysparkling


## Install python packages
PY_PATH=`ls -al /opt/conda/default`
echo $PY_PATH

if [[ $PY_PATH == *"anaconda"* ]]; then
        echo "anaconda"
        /opt/conda/default/bin/pip install -U requests tabulate future colorama scikit-learn google-cloud-bigquery google-cloud-storage
        /opt/conda/default/bin/pip install -U h2o $PYSPARKLING_WATER
        # conda install failing : "Solving environment: ...working... failed"
        # /opt/conda/default/bin/conda install tabulate future colorama scikit-learn
        # /opt/conda/default/bin/conda install --override-channels -c main -c conda-forge google-cloud-bigquery google-cloud-storage
        # /opt/conda/default/bin/conda install -c h2o $PYSPARKLING_WATER

else
        echo "miniconda"
        /opt/conda/default/bin/pip install -U requests tabulate future colorama scikit-learn google-cloud-bigquery google-cloud-storage
        /opt/conda/default/bin/pip install -U h2o $PYSPARKLING_WATER
fi

echo "END Stage 1 : Successfully Installed H2O libraries and dependencies"



# Tune Spark defaults for H2o Sparkling water
echo "BEGIN Stage 2 : Tuning Spark configuration in spark-defaults.conf"

echo "###### BEGIN : H2O specefic properties ######" >> /usr/lib/spark/conf/spark-defaults.conf
sed -i 's/spark.driver.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/#spark.driver.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/g' /usr/lib/spark/conf/spark-defaults.conf
sed -i 's/spark.executor.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/#spark.executor.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/g' /usr/lib/spark/conf/spark-defaults.conf
echo "spark.dynamicAllocation.enabled=false" >> /usr/lib/spark/conf/spark-defaults.conf
echo "###### END : H2O specefic properties ######" >> /usr/lib/spark/conf/spark-defaults.conf

echo "END Stage 2 : Successfully tuned Spark configuration in spark-defaults.conf"


#!/bin/bash

set -euxo pipefail
apt-get update

## Set Spark and Sparkling water versions
readonly DEFAULT_H2O_SPARKLING_WATER_VERSION="3.28.0.3-1"
readonly H2O_SPARKLING_WATER_VERSION="$(/usr/share/google/get_metadata_value attributes/H2O_SPARKLING_WATER_VERSION || echo ${DEFAULT_H2O_SPARKLING_WATER_VERSION})"
echo $H2O_SPARKLING_WATER_VERSION
readonly SPARK_VERSION=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
echo $SPARK_VERSION
readonly SPARKLING_WATER="http://h2o-release.s3.amazonaws.com/sparkling-water/spark-$SPARK_VERSION/$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION/sparkling-water-$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION.zip"
echo $SPARKLING_WATER
readonly PYSPARKLING_WATER="h2o_pysparkling_$SPARK_VERSION"
echo $PYSPARKLING_WATER
readonly H2O_SPARKLING_WATER_HOME="/usr/lib/sparkling-water-$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION"
echo $H2O_SPARKLING_WATER_HOME


# Install Scala packages for H2o Sparkling Water
function install_sparkling_water(){
  ## Download and unzip Sparking water Scala libraries
  cd /usr/lib/
  wget $SPARKLING_WATER
  unzip "sparkling-water-$H2O_SPARKLING_WATER_VERSION-$SPARK_VERSION.zip"

  ## Update $TOPDIR variable in sparkling scripts and add home variable
  sed -i "2iH2O_SPARKLING_WATER_HOME=\"$H2O_SPARKLING_WATER_HOME\"" $H2O_SPARKLING_WATER_HOME/bin/sparkling-shell
  sed -i 's/TOPDIR=$(cd "$(dirname "$0")\/.."; pwd)/TOPDIR=$H2O_SPARKLING_WATER_HOME/g' $H2O_SPARKLING_WATER_HOME/bin/sparkling-shell

  sed -i "2iH2O_SPARKLING_WATER_HOME=\"$H2O_SPARKLING_WATER_HOME\"" $H2O_SPARKLING_WATER_HOME/bin/pysparkling
  sed -i 's/TOPDIR=$(cd "$(dirname "$0")\/.."; pwd)/TOPDIR=$H2O_SPARKLING_WATER_HOME/g' $H2O_SPARKLING_WATER_HOME/bin/pysparkling

  ## create Symlink entries for default
  ln -s $H2O_SPARKLING_WATER_HOME/bin/sparkling-shell /opt/conda/default/bin/sparkling-shell
  ln -s $H2O_SPARKLING_WATER_HOME/bin/pysparkling /opt/conda/default/bin/pysparkling
}


# Install python packages for H2o Sparkling Water
function install_pysparkling_water(){
  PY_PATH=`ls -al /opt/conda/default`
  echo $PY_PATH

  # identify python flavor --for future use
  if [[ $PY_PATH == *"anaconda"* ]]; then
          echo "anaconda"
  else
          echo "miniconda"
  fi
  /opt/conda/default/bin/pip install -U requests tabulate future colorama scikit-learn google-cloud-bigquery google-cloud-storage
  /opt/conda/default/bin/pip install -U h2o $PYSPARKLING_WATER
}


# Tune Spark defaults for H2o Sparkling water
function tune_spark_defaults(){
  echo "###### BEGIN : H2O specefic properties ######" >> /usr/lib/spark/conf/spark-defaults.conf
  sed -i 's/spark.driver.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/#spark.driver.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/g' /usr/lib/spark/conf/spark-defaults.conf
  sed -i 's/spark.executor.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/#spark.executor.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/g' /usr/lib/spark/conf/spark-defaults.conf
  echo "spark.dynamicAllocation.enabled=false" >> /usr/lib/spark/conf/spark-defaults.conf
  echo "###### END : H2O specefic properties ######" >> /usr/lib/spark/conf/spark-defaults.conf
}


function main() {
  echo "BEGIN Stage 1 : Install H2O libraries and dependencies"
  install_sparkling_water
  install_pysparkling_water
  echo "END Stage 1 : Successfully Installed H2O libraries and dependencies"
  echo "BEGIN Stage 2 : Tuning Spark configuration in spark-defaults.conf"
  tune_spark_defaults
  echo "END Stage 2 : Successfully tuned Spark configuration in spark-defaults.conf"
}


main

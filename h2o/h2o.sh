#!/bin/bash

set -euxo pipefail

## Set Spark and Sparkling water versions
readonly DEFAULT_H2O_SPARKLING_WATER_VERSION="3.28.0.3-1"
readonly H2O_SPARKLING_WATER_VERSION="$(/usr/share/google/get_metadata_value attributes/H2O_SPARKLING_WATER_VERSION || echo ${DEFAULT_H2O_SPARKLING_WATER_VERSION})"

readonly SPARK_VERSION=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)

readonly SPARKLING_WATER_NAME="sparkling-water-${H2O_SPARKLING_WATER_VERSION}-${SPARK_VERSION}"
readonly SPARKLING_WATER_URL="http://h2o-release.s3.amazonaws.com/sparkling-water/spark-${SPARK_VERSION}/${H2O_SPARKLING_WATER_VERSION}-${SPARK_VERSION}/${SPARKLING_WATER_NAME}.zip"

readonly PYSPARKLING_WATER="h2o_pysparkling_${SPARK_VERSION}"

# Install Scala packages for H2O Sparkling Water
function install_sparkling_water() {
  local tmp_dir
  tmp_dir=$(mktemp -d -t init-action-h2o-XXXX)

  ## Download and unzip Sparking water Scala libraries
  wget -nv --timeout=30 --tries=5 --retry-connrefused "$SPARKLING_WATER_URL" -P "$tmp_dir"
  unzip -q "${tmp_dir}/${SPARKLING_WATER_NAME}.zip" -d /usr/lib/
  ln -s "/usr/lib/${SPARKLING_WATER_NAME}" /usr/lib/sparkling-water

  ## Fix $TOPDIR variable resolution in Sparkling scripts
  sed -i 's|TOPDIR=.*|TOPDIR=$(cd "$(dirname "$(readlink -f "$0")")/.."; pwd)|g' \
    /usr/lib/sparkling-water/bin/sparkling-shell \
    /usr/lib/sparkling-water/bin/pysparkling

  ## Create Symlink entries for default
  ln -s /usr/lib/sparkling-water/bin/sparkling-shell /usr/bin/
  ln -s /usr/lib/sparkling-water/bin/pysparkling /usr/bin/
}

# Install Python packages for H2O Sparkling Water
function install_pysparkling_water() {
  pip install -U \
    requests tabulate future colorama scikit-learn google-cloud-bigquery google-cloud-storage \
    h2o "$PYSPARKLING_WATER"
}

# Tune Spark defaults for H2O Sparkling water
function tune_spark_defaults() {
  sed -i 's/spark.driver.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/#spark.driver.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/g' /usr/lib/spark/conf/spark-defaults.conf
  sed -i 's/spark.executor.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/#spark.executor.extraJavaOptions=-Dflogger.backend_factory=com.google.cloud.hadoop.repackaged.gcs.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance/g' /usr/lib/spark/conf/spark-defaults.conf

  cat >>/usr/lib/spark/conf/spark-defaults.conf <<EOF

###### BEGIN : H2O specific properties ######
spark.dynamicAllocation.enabled=false
###### END   : H2O specific properties ######
EOF
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

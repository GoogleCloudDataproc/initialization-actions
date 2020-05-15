#!/bin/bash

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This initialization action will download a set of frequently-used Machine Learning
# libraries onto a Dataproc cluster, as well as GPU support and connectors for
# Google Cloud Storage, BigQuery and Spark-Bigquery. 

set -euxo pipefail

readonly JARS_DIR=/usr/lib/spark/jars
readonly CONNECTORS_DIR=/usr/local/share/google/dataproc/lib

readonly DEFAULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO ||
  echo ${DEFAULT_INIT_ACTIONS_REPO})"
readonly INIT_ACTIONS_BRANCH="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_BRANCH ||
  echo 'master')"
readonly INIT_ACTIONS_DIR=/opt/init-actions

mkdir -p ${JARS_DIR}
mkdir -p ${CONNECTORS_DIR}
mkdir -p ${INIT_ACTIONS_DIR}

function download_spark_jar() {
  local -r url=$1

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
  -P "${JARS_DIR}" "${url}" 

}

function download_init_actions() {
  # Download initialization actions locally.
  if [[ ${INIT_ACTIONS_REPO} == gs://* ]]; then
    gsutil -m rsync -r "${INIT_ACTIONS_REPO}" "${INIT_ACTIONS_DIR}"
  else
    git clone -b "${INIT_ACTIONS_BRANCH}" --single-branch "${INIT_ACTIONS_REPO}" "${INIT_ACTIONS_DIR}"
  fi
  find "${INIT_ACTIONS_DIR}" -name '*.sh' -exec chmod +x {} \;
}

function install_connectors() {
  "${INIT_ACTIONS_DIR}/connectors/connectors.sh"
}

function install_pip_packages() {
  "${INIT_ACTIONS_DIR}/python/pip-install.sh"    
}

r_packages=(
  "xgboost"
  "ggplot2"
  "caret"
  "nnet"
  "rpy2"
  "randomForest"
  "sparkbq"
  "sparklyr"
)

function install_rapids() {
  "${INIT_ACTIONS_DIR}/rapids/rapids.sh"
}

function install_gpu() {
  "${INIT_ACTIONS_DIR}/gpu/install_gpu_driver.sh"
}

function install_h2o_sparkling_water() {
  "${INIT_ACTIONS_DIR}/h2o/h2o.sh"
}

function install_spark_nlp() {
  local -r name="spark-nlp"
  local -r repo_url="http://dl.bintray.com/spark-packages/maven/JohnSnowLabs/"
  local -r version="2.4.3"
  
  download_spark_jar "${repo_url}/${name}/${version}/${name}-${version}.jar"
}

function install_r_packages() {
  # Install R system dependencies
  apt-get install -y libcurl4-openssl-dev libssl-dev libxml2-dev

  for package in "${r_packages[@]}"; do
    echo "Installing '${package}'..."
    Rscript -e 'install.packages("${package}", repo="https://cran.rstudio.com")'
    echo "Successfully installed '${package}'"
  done
}

function main () {
  # Download initialization actions
  download_init_actions

  # Install Python packages
  install_pip_packages

  # Install Spark Libraries
  install_spark_nlp
  install_h2o_sparkling_water

  # Install GCP Connectors
  install_connectors

  # Install R packages
  install_r_packages

  # Install RAPIDS and GPU Drivers
  install_rapids
  install_gpu
}

main
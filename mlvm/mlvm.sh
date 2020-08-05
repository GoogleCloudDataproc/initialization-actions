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
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/init-actions-repo ||
  echo ${DEFAULT_INIT_ACTIONS_REPO})"
readonly INIT_ACTIONS_DIR=$(mktemp -d -t dataproc-init-actions-XXXX)

readonly INCLUDE_GPUS="$(/usr/share/google/get_metadata_value attributes/include-gpus || true)"
readonly SPARK_BIGQUERY_VERSION="$(/usr/share/google/get_metadata_value attributes/spark-bigquery-connector-version ||
  echo "0.17.0")"

readonly R_VERSION="$(R --version | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' )"
readonly TENSORFLOW_VERSION="2.2.0"
readonly SPARK_NLP_VERSION="2.5.5"

CONDA_PACKAGES=(
  "matplotlib"
  "mxnet" 
  "nltk"
  "rpy2"
  "r-essentials=${R_VERSION}"
  "r-xgboost"
  "r-sparklyr"
  "scikit-learn"
  "spark-nlp=${SPARK_NLP_VERSION}"
  "pytorch"
  "torchvision" 
)

readonly CONDA_PACKAGES

PIP_PACKAGES=(
  "google-api-python-client" 
  "google-cloud-bigquery"
  "google-cloud-bigtable"
  "google-cloud-datalabeling"
  "google-cloud-dataproc"
  "google-cloud-storage"
  "sparksql-magic" 
  "tensorflow-datasets==3.2.1"
  "tensorflow-estimator==${TENSORFLOW_VERSION}" 
  "tensorflow-hub==0.8.0"
  "tensorflow-io==0.14.0"
  "tensorflow-probability==0.10.1" 
  "xgboost"
)

if [[ -n ${INCLUDE_GPUS} ]]; then
  PIP_PACKAGES+=("tensorflow-gpu==${TENSORFLOW_VERSION}")
else
  PIP_PACKAGES+=("tensorflow==${TENSORFLOW_VERSION}")
fi

if [ "$(echo "$DATAPROC_VERSION >= 2.0" | bc)" -eq 1 ]; then 
  PIP_PACKAGES+=("spark-tensorflow-distributor")
fi

readonly PIP_PACKAGES

mkdir -p ${JARS_DIR}
mkdir -p ${CONNECTORS_DIR}

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  echo "Cmd \"${cmd}\" failed."
  return 1
}

function download_spark_jar() {
  local -r url=$1

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
  -P "${JARS_DIR}" "${url}" 
}

function download_init_actions() {
  # Download initialization actions locally.
  mkdir "${INIT_ACTIONS_DIR}/rapids/"
  mkdir "${INIT_ACTIONS_DIR}/gpu/"
  
  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/rapids/" "${INIT_ACTIONS_DIR}/rapids/"
  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/gpu/" "${INIT_ACTIONS_DIR}/gpu/"

  find "${INIT_ACTIONS_DIR}" -name '*.sh' -exec chmod +x {} \;
}

function install_gpu_drivers() {
  "${INIT_ACTIONS_DIR}/gpu/install_gpu_driver.sh"
}

function install_conda_packages() {  
  local -r extra_packages="$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || true)"
  local -r extra_channels="$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || true)"

  conda config --add channels pytorch
  conda config --add channels johnsnowlabs

  conda install -y "${CONDA_PACKAGES[@]}"

  if [[ -n "${extra_channels}" ]]; then
    for channel in ${extra_channels}; do
      conda config --add channels "${channel}"
    done
  fi
    
  if [[ -n "${extra_packages}" ]]; then
    conda install -y "${extra_packages[@]}"
  fi
}

function install_pip_packages() {
  local -r extra_packages="$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)"

  execute_with_retries "pip install ${PIP_PACKAGES[*]}"

  if [[ -n "${extra_packages}" ]]; then
    execute_with_retries "pip install ${extra_packages[*]}"
  fi 
}

function install_spark_nlp() {
  local -r name="spark-nlp"
  local -r repo_url="http://dl.bintray.com/spark-packages/maven/JohnSnowLabs/"
  
  download_spark_jar "${repo_url}/${name}/${SPARK_NLP_VERSION}/${name}-${SPARK_NLP_VERSION}.jar"
}

function install_connectors() {
  local -r url="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-${SPARK_BIGQUERY_VERSION}.jar" 
  
  gsutil cp "${url}" "${CONNECTORS_DIR}/"

  local -r jar_name=${url##*/}

  # Update or create version-less connector link
  ln -s -f "${CONNECTORS_DIR}/${jar_name}" "${CONNECTORS_DIR}/spark-bigquery-connector.jar"
}

function install_rapids() {
  # Only install RAPIDS if "rapids-runtime" metadata exists and GPUs requested.
  local rapids_runtime
  rapids_runtime="$(/usr/share/google/get_metadata_value attributes/rapids-runtime || true)"
  
  if [[ -n ${rapids_runtime} ]]; then
    if [[ -n ${INCLUDE_GPUS} ]]; then
      "${INIT_ACTIONS_DIR}/rapids/rapids.sh"
    else
      echo "RAPIDS runtime declared but GPUs not included. Exiting."
      return 1
    fi
  fi
}

function main () {
  # Download initialization actions
  echo "Downloading initialization actions"
  download_init_actions

  # Install GPU Drivers
  echo "Installing GPU drivers"
  install_gpu_drivers

  # Install Conda packages
  echo "Installing Conda packages"
  install_conda_packages

  # Install Pip packages
  echo "Installing Pip Packages"
  install_pip_packages

  # Install Spark Libraries
  echo "Installing Spark-NLP jars"
  install_spark_nlp

  # Install GCP Connectors
  echo "Installing GCP Connectors"
  install_connectors

  # Install RAPIDS
  echo "Installing rapids"
  install_rapids
}

main
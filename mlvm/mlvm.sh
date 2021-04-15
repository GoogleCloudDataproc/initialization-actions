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
# Google Cloud Storage, BigQuery and Spark-BigQuery.

set -euxo pipefail

readonly SPARK_JARS_DIR=/usr/lib/spark/jars
readonly CONNECTORS_DIR=/usr/local/share/google/dataproc/lib

readonly DEFAULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/init-actions-repo ||
  echo ${DEFAULT_INIT_ACTIONS_REPO})"
readonly INIT_ACTIONS_DIR=$(mktemp -d -t dataproc-init-actions-XXXX)

readonly RAPIDS_RUNTIME="$(/usr/share/google/get_metadata_value attributes/rapids-runtime || echo "")"
readonly INCLUDE_GPUS="$(/usr/share/google/get_metadata_value attributes/include-gpus || echo "")"
readonly SPARK_BIGQUERY_VERSION="$(/usr/share/google/get_metadata_value attributes/spark-bigquery-connector-version ||
  echo "0.18.1")"

R_VERSION="$(R --version | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p')"
readonly R_VERSION
readonly SPARK_NLP_VERSION="2.7.2" # Must include subminor version here

CONDA_PACKAGES=(
  "r-dplyr=1.0"
  "r-essentials=${R_VERSION}"
  "r-sparklyr=1.5"
  "scikit-learn=0.24"
  "pytorch=1.7"
  "torchvision=0.8"
  "xgboost=1.3"
)

# rapids-xgboost (part of the RAPIDS library) requires a custom build of
# xgboost that is incompatible with r-xgboost. As such, r-xgboost is not
# installed into the MLVM if RAPIDS support is desired.
if [[ -z ${RAPIDS_RUNTIME} ]]; then
  CONDA_PACKAGES+=("r-xgboost=1.3")
fi

PIP_PACKAGES=(
  "mxnet==1.6.*"
  "rpy2==3.4.*"
  "spark-nlp==${SPARK_NLP_VERSION}"
  "sparksql-magic==0.0.*"
  "tensorflow-datasets==4.2.*"
  "tensorflow-hub==0.11.*"
)

if [[ "$(echo "$DATAPROC_VERSION >= 2.0" | bc)" -eq 1 ]]; then
  PIP_PACKAGES+=(
    "spark-tensorflow-distributor==0.1.0"
    "tensorflow==2.4.*"
    "tensorflow-estimator==2.4.*"
    "tensorflow-io==0.17"
    "tensorflow-probability==0.12.*"
  )
else
  CONDA_PACKAGES+=(
    "protobuf=3.15"
  )
  PIP_PACKAGES+=(
    "tensorflow==2.3.*"
    "tensorflow-estimator==2.3.*"
    "tensorflow-io==0.16"
    "tensorflow-probability==0.11.*"
  )
fi
readonly CONDA_PACKAGES
readonly PIP_PACKAGES

mkdir -p ${SPARK_JARS_DIR} ${CONNECTORS_DIR}

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  echo "Cmd '${cmd}' failed."
  return 1
}

function download_spark_jar() {
  local -r url=$1
  local -r jar_name=${url##*/}
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${url}" -o "${SPARK_JARS_DIR}/${jar_name}"
}

function download_init_actions() {
  # Download initialization actions locally.
  mkdir "${INIT_ACTIONS_DIR}"/{gpu,rapids,dask}

  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/rapids/" "${INIT_ACTIONS_DIR}/rapids/"
  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/gpu/" "${INIT_ACTIONS_DIR}/gpu/"
  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/dask/" "${INIT_ACTIONS_DIR}/dask/"

  find "${INIT_ACTIONS_DIR}" -name '*.sh' -exec chmod +x {} \;
}

function install_gpu_drivers() {
  "${INIT_ACTIONS_DIR}/gpu/install_gpu_driver.sh"
}

function install_conda_packages() {
  local base
  base=$(conda info --base)
  local -r mamba_env_name=mamba
  local -r mamba_env=${base}/envs/mamba
  local -r extra_packages="$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || echo "")"
  local -r extra_channels="$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || echo "")"

  conda config --add channels pytorch
  conda config --add channels conda-forge

  # Create a separate environment with mamba.
  # Mamba provides significant decreases in installation times.
  conda create -y -n ${mamba_env_name} mamba

  execute_with_retries "${mamba_env}/bin/mamba install -y ${CONDA_PACKAGES[*]} -p ${base}"

  if [[ -n "${extra_channels}" ]]; then
    for channel in ${extra_channels}; do
      "${mamba_env}/bin/conda" config --add channels "${channel}"
    done
  fi

  if [[ -n "${extra_packages}" ]]; then
    execute_with_retries "${mamba_env}/bin/mamba install -y ${extra_packages[*]} -p ${base}"
  fi

  # Clean up environment
  "${mamba_env}/bin/mamba" clean -y --all

  # Remove mamba env when done
  conda env remove -n ${mamba_env_name}
}

function install_pip_packages() {
  local -r extra_packages="$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || echo "")"

  execute_with_retries "pip install ${PIP_PACKAGES[*]}"

  if [[ -n "${extra_packages}" ]]; then
    execute_with_retries "pip install ${extra_packages[*]}"
  fi
}

function install_dask() {
  "${INIT_ACTIONS_DIR}/dask/dask.sh"
}

function install_spark_nlp() {
  local -r name="spark-nlp"
  local -r repo_url="http://dl.bintray.com/spark-packages/maven/JohnSnowLabs"
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
  if [[ -n ${RAPIDS_RUNTIME} ]]; then
    if [[ -n ${INCLUDE_GPUS} ]]; then
      "${INIT_ACTIONS_DIR}/rapids/rapids.sh"
    else
      echo "RAPIDS runtime declared but GPUs not included. Exiting."
      return 1
    fi
  fi
}

function main() {
  # Download initialization actions
  echo "Downloading initialization actions"
  download_init_actions

  # Install GPU Drivers
  echo "Installing GPU drivers"
  install_gpu_drivers

  # Install Dask
  install_dask

  # Install Spark Libraries
  echo "Installing Spark-NLP jars"
  install_spark_nlp

  # Install GCP Connectors
  echo "Installing GCP Connectors"
  install_connectors

  # Install RAPIDS
  echo "Installing rapids"
  install_rapids

  # Install Conda packages
  echo "Installing Conda packages"
  install_conda_packages

  # Install Pip packages
  echo "Installing Pip Packages"
  install_pip_packages
}

main

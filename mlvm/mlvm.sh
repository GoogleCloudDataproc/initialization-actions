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
readonly include_gpus="$(/usr/share/google/get_metadata_value attributes/include-gpus || true)"

BASE_R_PACKAGES=(
  "r-essentials=3.6.0"
  "r-xgboost=0.90.0.2"
  "r-sparklyr=1.0.0"
)

BASE_PIP_PACKAGES=(
  "google-cloud-bigquery==1.26.1" 
  "google-cloud-datalabeling==0.4.0"
  "google-cloud-storage==1.30.0"
  "google-cloud-bigtable==1.4.0" 
  "google-cloud-dataproc==1.0.1" 
  "google-api-python-client==1.10.0" 
  "matplotlib==3.3.0"
  "mxnet==1.6.0" 
  "nltk==3.5"
  "numpy==1.18.4" 
  "rpy2==3.3.3"
  "scikit-learn==0.23.1" 
  "sparksql-magic==0.0.3" 
  "tensorflow==2.3.0" 
  "tensorflow-datasets==3.2.1"
  "tensorflow-estimator==2.3.0"
  "tensorflow-hub==0.8.0"
  "tensorflow-io==0.14.0"
  "tensorflow-probability==0.10.1" 
  "torch==1.5.1" 
  "torchvision==0.6.1" 
  "xgboost==1.1.0"
)

if [ "$(echo "$DATAPROC_VERSION >= 2.0" | bc)" -eq 1 ]; then 
  BASE_PIP_PACKAGES+=("spark-tensorflow-distributor==0.1.0")
fi

mkdir -p ${JARS_DIR}
mkdir -p ${CONNECTORS_DIR}
mkdir -p ${INIT_ACTIONS_DIR}

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
  readonly EXTRA_PIP_PACKAGES="$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)"

  # Installing all at once is flaky. Added this here. 
  for package in "${BASE_PIP_PACKAGES[@]}"; do
    execute_with_retries "pip install $package"
  done
  
  if [[ -n "${EXTRA_PIP_PACKAGES}" ]]; then
    for package in "${BASE_PIP_PACKAGES[@]}"; do
      execute_with_retries "pip install $package"
    done
  fi 
}

function install_rapids() {
  # Only install RAPIDS if "rapids-runtime" metadata exists and GPUs requested.
  local rapids_runtime
  rapids_runtime="$(/usr/share/google/get_metadata_value attributes/rapids-runtime || true)"
  
  if [[ -n ${rapids_runtime} ]]; then
    if [[ -n ${include_gpus} ]]; then
      "${INIT_ACTIONS_DIR}/rapids/rapids.sh"
    else
      echo "RAPIDS runtime declared but GPUs not included. Exiting."
      return 1
    fi
  fi
}

function install_horovod() {
  readonly MPI_VERSION="4.0.3"
  readonly MPI_URL="https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-${MPI_VERSION}.tar.gz"

  tmp_dir=$(mktemp -d -t mlvm-horovod-mpi-XXXX)
  wget -nv --timeout=30 --tries=5 --retry-connrefused -P ${tmp_dir} "${MPI_URL}" 
  gunzip -c "${tmp_dir}/openmpi-${MPI_VERSION}.tar.gz" | tar xf - -C ${tmp_dir}

  mv /usr/bin/mpirun /usr/bin/bk_mpirun
  mv /usr/bin/mpirun.openmpi /usr/bin/bk_mpirun.openmpi

  cur_dir=$(pwd)  
  cd "${tmp_dir}/openmpi-${MPI_VERSION}"
  ./configure --prefix=/usr/local
  make all install
  ldconfig
  cd ${cur_dir}

  # TensorFlow requires g++-4.8.5
  apt-get install -y g++-4.8
  update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 50
  update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-7 50
  
  # Change g++ version when installing Horovod, then change back to default
  update-alternatives --set g++ /usr/bin/g++-4.8
  pip install --no-cache-dir horovod==0.19.4
  update-alternatives --set g++ /usr/bin/g++-7
}

function install_gpu_drivers() {
  "${INIT_ACTIONS_DIR}/gpu/install_gpu_driver.sh"
}

function install_spark_nlp() {
  local -r name="spark-nlp"
  local -r repo_url="http://dl.bintray.com/spark-packages/maven/JohnSnowLabs/"
  local -r version="2.4.3"
  
  pip install -U "spark-nlp==2.5.1" 
  download_spark_jar "${repo_url}/${name}/${version}/${name}-${version}.jar"
}

function install_r_packages() {  
  readonly EXTRA_R_PACKAGES="$(/usr/share/google/get_metadata_value attributes/R_PACKAGES || true)"
  
  conda install -y -c r "${BASE_R_PACKAGES[@]}"
  
  if [[ -n "${EXTRA_R_PACKAGES}" ]]; then
    conda install -y -c r "${EXTRA_R_PACKAGES[@]}"
  fi
}

function main () {
  # Download initialization actions
  echo "Downloading initialization actions"
  download_init_actions

  # Install GPU Drivers
  echo "Installing GPU drivers"
  install_gpu_drivers

  # Install Python packages
  echo "Installing pip packages"
  install_pip_packages

  # Install R packages
  echo "Installing R Packages"
  install_r_packages

  # Install Spark Libraries
  echo "Installing Spark-NLP"
  install_spark_nlp

  # Install GCP Connectors
  echo "Installing GCP Connectors"
  install_connectors

  # Install RAPIDS
  echo "Installing rapids"
  install_rapids
}

main
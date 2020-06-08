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
readonly include_gpus="$(/usr/share/google/get_metadata_value attributes/include_gpus || true)"

BASE_R_PACKAGES=(
  "xgboost==1.0.0.2"
  "ggplot2==3.3.0"
  "caret==6.0-86"
  "nnet==7.3-14"
  "randomForest==4.6-14"
  "sparkbq==0.1.1"
  "sparklyr==1.2.0"
)

BASE_PIP_PACKAGES=(
  "google-cloud-bigquery==1.24.0" 
  "google-cloud-datalabeling==0.4.0"
  "google-cloud-storage==1.28.1"
  "google-cloud-bigtable==1.2.1" 
  "google-cloud-dataproc==0.8.0" 
  "google-api-python-client==1.8.4" 
  "mxnet==1.6.0" 
  "tensorflow==2.2.0" 
  "numpy==1.18.4" 
  "scikit-learn==0.23.1" 
  "keras==2.3.1"
  "rpy2==3.3.3" 
  "spark-nlp==2.5.1" 
  "xgboost==1.1.0" 
  "torch==1.5.0" 
  "torchvision==0.6.0" 
)

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

  pip install --upgrade "${BASE_PIP_PACKAGES[@]}"
  
  if [[ -n "${EXTRA_PIP_PACKAGES}" ]]; then
    pip install --upgrade "${EXTRA_PIP_PACKAGES}"
  fi 
}

function install_rapids() {
  # Only install RAPIDS if "rapids-runtime" metadata exists as well as GPU hardware detected.
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

  tmp_dir=$(mktemp -d -t ml-vm-horovod-mpi-XXXX)
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
  local r_packages
  local split
  local name
  local version
  local tmp_dir
  local pwd

  # Install R system dependencies
  readonly CURL_REPO="https://github.com/curl/curl/releases/download/curl-7_70_0/curl-7.70.0.tar.gz"


  # libcurl4 requires building curl from source
  tmp_dir=$(mktemp -d -t curl-XXX)
  wget -nv --timeout=30 --tries=5 --retry-connrefused -P ${tmp_dir} "${CURL_REPO}" 
  gunzip -c "${tmp_dir}/curl-7.70.0.tar.gz" | tar xf - -C ${tmp_dir}
  
  cur_dir=$(pwd)
  cd ${tmp_dir}/curl-7.70.0
  ./configure
  make 
  make install
  cd ${cur_dir}

  apt-get install -y libcurl4-openssl-dev libssl-dev libxml2-dev
  
  readonly EXTRA_R_PACKAGES="$(/usr/share/google/get_metadata_value attributes/R_PACKAGES || true)"

  r_packages=("${BASE_R_PACKAGES[@]}" "${EXTRA_R_PACKAGES[@]}")
  for package in "${r_packages[@]}"; do    
    split=($(echo ${package} | tr "==" "\n"))
    name="${split[0]}"
    set +u
    version="${split[1]}"
    set -u

    echo "Installing '${package}'..."
    if [[ -n "${version}" ]]; then
      execute_with_retries "Rscript -e 'install.packages(\"${name}\", version=\"${version}\", repo=\"https://cran.rstudio.com\")'"
    else
      execute_with_retries "Rscript -e 'install.packages(\"${name}\", repo=\"https://cran.rstudio.com\")'"
    fi
    echo "Successfully installed '${package}'"
  done
}

function echo_wrap () {
  echo "BRADBRAD:"$1
}

function main () {
  # Download initialization actions
  echo_wrap "Downloading initialization actions"
  download_init_actions

  # Install GPU Drivers
  echo_wrap "Installing GPU drivers"
  install_gpu_drivers

  # Install Python packages
  echo_wrap "Installing pip packages"
  install_pip_packages

  # Install R packages
  echo_wrap "Installing R Packages"
  install_r_packages

  # Install Spark Libraries
  echo_wrap "Installing Spark-NLP"
  install_spark_nlp

  echo_wrap "Installing H20"
  install_h2o_sparkling_water

  # Install GCP Connectors
  echo_wrap "Installing GCP Connectors"
  install_connectors

  # Install RAPIDS
  echo_wrap "Installing rapids"
  install_rapids

  # Install Horovod
  # echo_wrap "Installing Horovod"
  # install_horovod
}

main
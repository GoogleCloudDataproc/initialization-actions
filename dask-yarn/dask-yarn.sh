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

# This initialization action script will install Dask, Dask-Yarn and other
# relevant libraries on a Dataproc cluster. Please see dask.org and 
# yarn.dask.org for more information.

set -euxo pipefail

readonly DASK_CONFIG_LOCATION=/etc/dask/
readonly DASK_CONFIG_FILE=${DASK_CONFIG_LOCATION}/config.yaml
readonly CONDA_ENV=dask
readonly CONDA_ENV_LOCATION=/opt/envs
readonly CONDA_ENV_PATH=${CONDA_ENV_LOCATION}/${CONDA_ENV}.tar.gz
readonly CONDA_EXTRA_PACKAGES="$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || echo "")"
readonly CONDA_EXTRA_CHANNELS="$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || echo "")"
readonly INCLUDE_RAPIDS="$(/usr/share/google/get_metadata_value attributes/include-rapids || echo "")"
readonly RAPIDS_VERSION="$(/usr/share/google/get_metadata_value attributes/rapids-version || echo "0.14")"
readonly CUDA_VERSION="$(/usr/share/google/get_metadata_value attributes/cuda-version || echo "10.2")"
readonly DASK_VERSION="$(/usr/share/google/get_metadata_value attributes/dask-version || echo "0.8.1")"

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly ROLE

mkdir -p ${CONDA_ENV_LOCATION} ${DASK_CONFIG_LOCATION}

readonly BASE_CONDA_CHANNELS=(
    "conda-forge"
)

readonly BASE_CONDA_PACKAGES=(
    "dask-yarn=${DASK_VERSION}"
    "dask>=2.6.0"
    "dill"
    "ipykernel"
    "fastparquet"
    "gcsfs"
    "pyarrow"
)

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

function install_conda_kernel() {
  conda install -y nb_conda_kernels
  # Restart Jupyter service to pickup RAPIDS environment.
  service jupyter restart || true
}

function configure_dask() {
  # Minimal custom configuarion is required for this
  # setup. Please see https://yarn.dask.org/en/latest/quickstart.html#usage
  # for information on tuning Dask-Yarn environments.

  cat <<EOF >"${DASK_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: ${CONDA_ENV_PATH}

  worker: 
    count: 2
EOF
}

function create_dask_env() {
  if [[ -n ${INCLUDE_RAPIDS} ]]; then
    CONDA_CHANNELS=(
      "rapidsai/label/xgboost"
      "rapidsai"
      "nvidia"
      "${BASE_CONDA_CHANNELS[@]}"
    )
    
    CONDA_PACKAGES=(
      "cudatoolkit=${CUDA_VERSION}"
      "rapids=${RAPIDS_VERSION}"
      "${BASE_CONDA_PACKAGES[@]}"
    )
  else
    CONDA_CHANNELS=("${BASE_CONDA_CHANNELS[@]}")
    CONDA_PACKAGES=("${BASE_CONDA_PACKAGES[@]}")
  fi

  # Add all new channels to the front. 
  if [[ -n ${CONDA_EXTRA_CHANNELS} ]]; then
    CONDA_CHANNELS=(
        "${CONDA_EXTRA_CHANNELS[@]}"
        "${CONDA_CHANNELS[@]}"
    )
  fi
  readonly CONDA_CHANNELS

  if [[ -n ${CONDA_EXTRA_PACKAGES} ]]; then
    CONDA_PACKAGES=(
        "${CONDA_PACKAGES[@]}"
        "${CONDA_EXTRA_PACKAGES[@]}"
    )
  fi
  readonly CONDA_PACKAGES

  local channels=()
  for channel in "${CONDA_CHANNELS[@]}"; do
    channels+="-c $channel"
  done

  # Update conda
  conda update -y conda

  # Install conda-pack which packages a conda environment.
  conda install -y -c conda-forge conda-pack

  # Create a conda environment.
  conda create -y "${channels[@]}" -n "${CONDA_ENV}" "${CONDA_PACKAGES[@]}"

  # conda-pack compresses the environment.
  conda pack -n "${CONDA_ENV}" -o "${CONDA_ENV_PATH}"
}

function main() {
  if [[ "${ROLE}" == "Master" ]]; then
    # Create Dask env
    create_dask_env

    # Configure notebook kernel for usage with Jupyter Notebooks
    install_conda_kernel

    # Create Dask config file.
    configure_dask
    
    echo 'Dask successfully installed.'
  else
    echo 'Dask can be installed only on master node - skipped for worker node.'
  fi
}

main
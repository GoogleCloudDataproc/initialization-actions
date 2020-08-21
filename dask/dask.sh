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

readonly DASK_CONFIG_DIR=/etc/dask/
readonly DASK_CONFIG_FILE=${DASK_CONFIG_DIR}/config.yaml

readonly CONDA_ENV_DIR=/opt/envs
readonly CONDA_ENV_NAME=dask
readonly CONDA_ENV_PACK=${CONDA_ENV_DIR}/${CONDA_ENV_NAME}.tar.gz

readonly CONDA_EXTRA_PACKAGES="$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || echo "")"
readonly CONDA_EXTRA_CHANNELS="$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || echo "")"

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

mkdir -p ${CONDA_ENV_DIR} ${DASK_CONFIG_DIR}

readonly CONDA_BASE_CHANNELS=(
    "conda-forge"
)

readonly CONDA_BASE_PACKAGES=(
    "dask-yarn=0.8.1"
    "dask>=2.6.0"
    "dill"
    "ipykernel"
    "fastparquet"
    "gcsfs"
    "pyarrow"
)

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
  environment: ${CONDA_ENV_PACK}

  worker: 
    count: 2
EOF
}

function configure_cluster_env() {
  if (anaconda -V); then
    local mngr="anaconda"
  else
    local mngr="miniconda3"
  fi

  local -r dask_env=/opt/conda/${mngr}/envs/dask
  
  # Create convenience symlink to dask-python environment
  ln -s ${dask_env}/bin/python /usr/local/bin/dask-python

  # Expose DASK_ENV and DASK_ENV_TAR to all users
  cat <<EOF >"/etc/environment"
DASK_ENV=${dask_env}
DASK_ENV_PACK=${CONDA_ENV_PACK}
EOF
  
  source "/etc/environment"
}

function create_dask_env() {
  local conda_channels
  local conda_packages

  # Add all new channels to the front. 
  if [[ -n ${CONDA_EXTRA_CHANNELS} ]]; then
    conda_channels=(
        "${CONDA_EXTRA_CHANNELS[@]}"
        "${CONDA_BASE_CHANNELS[@]}"
    )
  else
    conda_channels=("${CONDA_BASE_CHANNELS[@]}")
  fi

  if [[ -n ${CONDA_EXTRA_PACKAGES} ]]; then
    conda_packages=(
        "${CONDA_BASE_PACKAGES[@]}"
        "${CONDA_EXTRA_PACKAGES[@]}"
    )
  else
    conda_packages=("${CONDA_BASE_PACKAGES[@]}")
  fi


  local channels_cmd=()
  for channel in "${conda_channels[@]}"; do
    channels_cmd+=("-c $channel")
  done

  # Update conda
  conda update -y conda

  # Install conda-pack to package a conda environment
  conda install -y -c conda-forge conda-pack

  # Create a conda environment
  cmd="conda create -y ${channels_cmd[*]} -n ${CONDA_ENV_NAME} ${conda_packages[*]}"
  $cmd
  
  # conda-pack compresses the environment
  conda pack -n "${CONDA_ENV_NAME}" -o "${CONDA_ENV_PACK}"
}

function main() {
  if [[ "${ROLE}" == "Master" ]]; then
    # Create Dask env
    create_dask_env

    # Configure notebook kernel for usage with Jupyter Notebooks
    install_conda_kernel

    # Create Dask config file
    configure_dask

    # Configure cluster environment
    configure_cluster_env
    
    echo 'Dask successfully installed.'
  else
    echo 'Dask can be installed only on master node - skipped for worker node.'
  fi
}

main
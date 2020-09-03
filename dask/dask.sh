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

# This initialization action script will install Dask and other relevant
# libraries on a Dataproc cluster. This is supported for either "yarn" or
# "standalone" runtimes Please see dask.org and yarn.dask.org for more
# information.

set -euxo pipefail

if (which anaconda); then
  CONDA_MANAGER=anaconda
else
  CONDA_MANAGER=miniconda3
fi
readonly CONDA_MANAGER

readonly DASK_CONFIG_DIR=/etc/dask/
readonly DASK_CONFIG_FILE=${DASK_CONFIG_DIR}/config.yaml

readonly DASK_ENV_DIR=/opt/envs
readonly DASK_ENV_NAME=dask
readonly DASK_ENV_PACK=${DASK_ENV_DIR}/${DASK_ENV_NAME}.tar.gz
readonly DASK_ENV=/opt/conda/${CONDA_MANAGER}/envs/${DASK_ENV_NAME}

readonly DASK_RUNTIME="$(/usr/share/google/get_metadata_value attributes/dask-runtime || echo "yarn")"
readonly RUN_WORKER_ON_MASTER="$(/usr/share/google/get_metadata_value attributes/dask-worker-on-master || echo "true")"

readonly CONDA_EXTRA_PACKAGES="$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || echo "")"
readonly CONDA_EXTRA_CHANNELS="$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || echo "")"

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

# Dask 'standalone' config
readonly DASK_LAUNCHER='/usr/local/bin/dask-launcher.sh'
readonly DASK_SERVICE='dask-cluster'

mkdir -p ${DASK_ENV_DIR} ${DASK_CONFIG_DIR}

readonly CONDA_BASE_CHANNELS=(
    "conda-forge"
)

CONDA_BASE_PACKAGES=(
    "dask>=2.6.0"
    "dill"
    "ipykernel"
    "fastparquet"
    "gcsfs"
    "pyarrow"
)

if [[ "${DASK_RUNTIME}" == "yarn" ]]; then
  CONDA_BASE_PACKAGES+=("dask-yarn~=0.8")
fi
readonly CONDA_BASE_CHANNELS

function install_conda_kernel() {
  conda install -y nb_conda_kernels
  # Restart Jupyter service to pickup Dask environment.
  service jupyter restart || true
}

function configure_dask_yarn() {
  # Minimal custom configuarion is required for this
  # setup. Please see https://yarn.dask.org/en/latest/quickstart.html#usage
  # for information on tuning Dask-Yarn environments.
  cat <<EOF >"${DASK_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: ${DASK_ENV_PACK}

  worker: 
    count: 2
EOF
}

function configure_cluster_env() {
  # Configure cluster for improved user experience and expose variables to
  # allow modifications from other initialization actions.

  # Create convenience symlink to dask-python environment.
  ln -s ${DASK_ENV}/bin/python /usr/local/bin/dask-python
  
  # Expose DASK_ENV, DASK_LAUNCHER and DASK_SERVICE.
  cat <<EOF >"/etc/environment"
DASK_ENV=${DASK_ENV}
DASK_LAUNCHER=${DASK_LAUNCHER}
DASK_SERVICE=${DASK_SERVICE}
EOF

  # Expose DASK_ENV_PACK in "yarn" runtime.
  if [[ "${DASK_RUNTIME}" == "yarn" ]]; then
    cat <<EOF >"/etc/environment"
DASK_ENV_PACK=${DASK_ENV_PACK}
EOF
  fi
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

  # Install conda-pack to package a conda environment
  conda install -y -c conda-forge conda-pack

  # Create a conda environment
  conda create -y ${channels_cmd[*]} -n ${DASK_ENV_NAME} ${conda_packages[*]}

  
  # conda-pack compresses the environment
  conda pack -n "${DASK_ENV_NAME}" -o "${DASK_ENV_PACK}"
}

function install_systemd_dask_service() {
  echo "Installing systemd Dask service..."
  local -r dask_worker_local_dir="/tmp/dask"
  local -r dask_env_bin=${DASK_ENV}/bin

  mkdir -p "${dask_worker_local_dir}"

  if [[ "${ROLE}" == "Master" ]]; then
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${RUN_WORKER_ON_MASTER}" == true ]]; then
  echo "dask-worker starting, logging to /var/log/dask-worker.log."
  ${dask_env_bin}/dask-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-worker.log 2>&1 &
fi
echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log."
${dask_env_bin}/dask-scheduler > /var/log/dask-scheduler.log 2>&1
EOF
  else
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
echo "dask-worker starting, logging to /var/log/dask-worker.log."
${dask_env_bin}/dask-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-worker.log 2>&1
EOF
  fi
  chmod 750 "${DASK_LAUNCHER}"

  local -r dask_service_file=/usr/lib/systemd/system/${DASK_SERVICE}.service
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Cluster Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload
  systemctl enable "${DASK_SERVICE}"
}

function main() {
  if [[ "${DASK_RUNTIME}" == "yarn" ]]; then
    # Dask-Yarn only requires setup on Master node
    if [[ "${ROLE}" == "Master" ]]; then
      # Create Dask env
      create_dask_env

      # Create Dask config file
      configure_dask_yarn

      # Configure cluster environment
      configure_cluster_env
    else
      echo 'Dask-Yarn can be installed only on master node - skipped for worker node.'
    fi
  elif [[ "${DASK_RUNTIME}" == "standalone" ]]; then
    # Standalone requires set up on all nodes
    
    # Create Dask service
    install_systemd_dask_service

    # Create Dask env on all nodes
    create_dask_env

    # Configure cluster environment
    configure_cluster_env
   
    echo "Starting Dask 'standalone' cluster..."
    systemctl start "${DASK_SERVICE}"
  else
    echo "Unsupported Dask Runtime: ${DASK_RUNTIME}"
    exit 1
  fi

  if [[ "${ROLE}" == "Master" ]]; then
    # Configure notebook kernel for usage with Jupyter Notebooks
    install_conda_kernel
  fi

  echo "Dask for ${DASK_RUNTIME} successfully initialized."
}

main
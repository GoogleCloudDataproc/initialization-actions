#!/bin/bash

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

readonly RUN_WORKER_ON_MASTER=$(get_metadata_attribute 'dask-cuda-worker-on-master' 'true')

readonly CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '10.0')

readonly DASK_LAUNCHER='/usr/local/bin/dask-launcher.sh'
readonly DASK_SERVICE='dask-cluster'
readonly RAPIDS_ENV='RAPIDS'
readonly RAPIDS_ENV_BIN="/opt/conda/anaconda/envs/${RAPIDS_ENV}/bin"

BUILD_DIR=$(mktemp -d -t rapids-init-action-XXXX)
readonly BUILD_DIR

function create_conda_env() {
  echo "Create RAPIDS Conda environment..."
  # For use with Anaconda component
  local -r conda_env_file="${BUILD_DIR}/conda-environment.yaml"
  cat <<EOF >"${conda_env_file}"
channels:
  - rapidsai/label/xgboost
  - rapidsai
  - nvidia
  - conda-forge
dependencies:
  - cudatoolkit=${CUDA_VERSION}
  - dask-cuda=0.7.*
  - cudf=0.7.*
  - pyarrow=0.12.1
  - arrow-cpp=0.12.1
  - dask-cudf=0.7.*
  - cuml=0.7.*
  - dask-cuml=0.7.*
  - cugraph=0.7.*
  - rapidsai/label/xgboost::xgboost=0.90.*
  - rapidsai/label/xgboost::dask-xgboost=0.2.*
  - gcsfs
  - dill
  - ipykernel
EOF
  conda env create --name "${RAPIDS_ENV}" --file "${conda_env_file}"
}

function install_conda_kernel() {
  /opt/conda/anaconda/bin/conda install -y nb_conda_kernels
  # Restart Jupyter service to pickup RAPIDS environment.
  service jupyter restart || true
}

install_systemd_dask_service() {
  echo "Installing systemd Dask service..."

  if [[ "${ROLE}" == "Master" ]]; then
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${RUN_WORKER_ON_MASTER}" == true ]]; then
  echo "dask-cuda-worker starting, logging to /var/log/dask-cuda-worker.log."
  $RAPIDS_ENV_BIN/dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-worker.log 2>&1 &
fi
echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log."
$RAPIDS_ENV_BIN/dask-scheduler > /var/log/dask-scheduler.log 2>&1
EOF
  else
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
$RAPIDS_ENV_BIN/dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-worker.log 2>&1
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
  create_conda_env
  if [[ "${ROLE}" == "Master" ]]; then
    install_conda_kernel
  fi
  install_systemd_dask_service

  echo "Starting Dask cluster..."
  systemctl start "${DASK_SERVICE}"
  echo "Dask cluster instantiation successful"
}

main

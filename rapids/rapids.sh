#!/bin/bash

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

readonly PLATFORM=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
readonly RUN_WORKER_ON_MASTER=$(get_metadata_attribute 'dask-cuda-worker-on-master' 'true')

readonly CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '10.0')
readonly CUDF_VERSION=$(get_metadata_attribute 'cudf-version' '0.9.2')

readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' '1.0.0-Beta4')
readonly RAPIDS_SPARK_VERSION=$(get_metadata_attribute 'rapids-spark-version' '2.x')

readonly DASK_LAUNCHER='/usr/local/bin/dask-launcher.sh'
readonly DASK_SERVICE='dask-cluster'
readonly RAPIDS_ENV='RAPIDS'
readonly RAPIDS_ENV_BIN="/opt/conda/anaconda/envs/${RAPIDS_ENV}/bin"

BUILD_DIR=$(mktemp -d -t rapids-init-action-XXXX)
readonly BUILD_DIR

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_xgboost4j() {
  local -r repo_url='https://repo1.maven.org/maven2/ai/rapids'
  if [[ "${CUDA_VERSION}" == "10.0" ]]; then
    CUDF_CUDA_VERSION="10"
  else
    CUDF_CUDA_VERSION="${CUDA_VERSION//\./-}"
  fi

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${repo_url}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}/${RAPIDS_VERSION}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${repo_url}/xgboost4j_${RAPIDS_SPARK_VERSION}/${RAPIDS_VERSION}/xgboost4j_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${repo_url}/cudf/${CUDF_VERSION}/cudf-${CUDF_VERSION}-cuda${CUDF_CUDA_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  cp /usr/lib/spark/jars/xgboost4j-spark_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar /usr/lib/spark/python/lib/
}

function create_conda_env() {
  echo "Create RAPIDS Conda environment..."
  # For use with Anaconda component
  local -r conda_env_file="${BUILD_DIR}/conda-environment.yaml"
  cat <<EOF >"${conda_env_file}"
channels:
  - rapidsai
  - nvidia
  - conda-forge
  - defaults
dependencies:
  - rapids=0.12
  - python=3.7
  - gcsfs
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
  export DEBIAN_FRONTEND=noninteractive

  install_xgboost4j

  if [[ "${PLATFORM}" == "DASK" ]]; then
    create_conda_env
    if [[ "${ROLE}" == "Master" ]]; then
      install_conda_kernel
    fi
    install_systemd_dask_service
    echo "Starting Dask cluster..."
    systemctl start "${DASK_SERVICE}"
    echo "Dask cluster instantiation successful"
  elif [[ "${PLATFORM}" != "SPARK" ]]; then
    echo 'unsupported RAPIDS Runtime'
  else
    echo 'deployed Spark Based RAPIDS'
  fi
}

main

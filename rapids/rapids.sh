#!/bin/bash

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly SPARK_VERSION_ENV=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)

if [[ "${SPARK_VERSION_ENV}" == "3"* ]]; then
    readonly DEFAULT_CUDF_VERSION="0.14"
    readonly DEFAULT_SPARK_RAPIDS_VERSION="0.1.0"
    readonly SPARK_VERSION="${SPARK_VERSION_ENV}"
else
    readonly DEFAULT_CUDF_VERSION="0.9.2"
    readonly DEFAULT_SPARK_RAPIDS_VERSION="Beta5"
    readonly SPARK_VERSION="2.x"
fi

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

readonly RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'DASK')
readonly RUN_WORKER_ON_MASTER=$(get_metadata_attribute 'dask-cuda-worker-on-master' 'true')

# RAPIDS config
readonly DEFAULT_CUDA_VERSION="10.2"
readonly CUDA_VERSION=$(get_metadata_attribute 'cuda-version' ${DEFAULT_CUDA_VERSION})
readonly CUDF_VERSION=$(get_metadata_attribute 'cudf-version' ${DEFAULT_CUDF_VERSION})
readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' '0.14')

# SPARK config
readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' '1.0.0')

# DASK config
readonly DASK_LAUNCHER='/usr/local/bin/dask-launcher.sh'
readonly DASK_SERVICE='dask-cluster'
readonly RAPIDS_ENV='RAPIDS'
readonly RAPIDS_ENV_BIN="/opt/conda/anaconda/envs/${RAPIDS_ENV}/bin"

# Dataproc configurations
readonly SPARK_CONF_DIR='/etc/spark/conf'

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

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'

  if [[ "${CUDA_VERSION}" == "10.0" ]]; then
    local -r cudf_cuda_version="10"
  else
    local -r cudf_cuda_version="${CUDA_VERSION//\./-}"
  fi

  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  else
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  fi
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${rapids_repo_url}/cudf/${CUDF_VERSION}/cudf-${CUDF_VERSION}-cuda${cudf_cuda_version}.jar" \
    -P /usr/lib/spark/jars/
}

function configure_spark() {
  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
spark.rapids.sql.concurrentGpuTasks=2
spark.executor.resource.gpu.amount=1
spark.executor.cores=2
spark.task.cpus=1
spark.task.resource.gpu.amount=0.5
spark.rapids.memory.pinnedPool.size=2G
spark.executor.memoryOverhead=2G
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.extraJavaOptions='-Dai.rapids.cudf.prefer-pinned=true'
spark.locality.wait=0s
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.sql.shuffle.partitions=48
spark.sql.files.maxPartitionBytes=512m
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}.jar
spark.dynamicAllocation.enabled=false
spark.shuffle.service.enabled=false
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  else
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${SPARK_RAPIDS_VERSION}.jar
spark.dynamicAllocation.enabled=false
spark.shuffle.service.enabled=false
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  fi
}

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
  - rapids=${RAPIDS_VERSION}
  - gcsfs
  - dill
  - ipykernel
EOF
  conda env create --name "${RAPIDS_ENV}" --file "${conda_env_file}"
}

function install_conda_kernel() {
  conda install -y nb_conda_kernels
  # Restart Jupyter service to pickup RAPIDS environment.
  service jupyter restart || true
}

install_systemd_dask_service() {
  echo "Installing systemd Dask service..."
  local -r dask_worker_local_dir="/tmp/rapids"
  local -r mem_total=$(free -m | grep -oP '\d+' | head -n1)

  if [[ "${ROLE}" == "Master" ]]; then
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${RUN_WORKER_ON_MASTER}" == true ]]; then
  nvidia-smi -c DEFAULT
  echo "dask-cuda-worker starting, logging to /var/log/dask-cuda-worker.log."
  $RAPIDS_ENV_BIN/dask-cuda-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-cuda-worker.log 2>&1 &
fi
echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log."
$RAPIDS_ENV_BIN/dask-scheduler > /var/log/dask-scheduler.log 2>&1
EOF
  else
    nvidia-smi -c DEFAULT
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
$RAPIDS_ENV_BIN/dask-cuda-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-cuda-worker.log 2>&1
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
  if [[ "${RUNTIME}" == "DASK" ]]; then
    create_conda_env
    if [[ "${ROLE}" == "Master" ]]; then
      install_conda_kernel
    fi
    install_systemd_dask_service
    echo "Starting Dask cluster..."
    systemctl start "${DASK_SERVICE}"
    echo "RAPIDS initialized with Dask runtime"
  elif [[ "${RUNTIME}" == "SPARK" ]]; then
    install_spark_rapids
    configure_spark

    if [[ "${ROLE}" == "Master" ]]; then
      systemctl restart hadoop-yarn-resourcemanager.service
    else
      systemctl restart hadoop-yarn-nodemanager.service
    fi
    echo "RAPIDS initialized with Spark runtime"
  else
    echo "Unsupported RAPIDS Runtime: ${RUNTIME}"
    exit 1
  fi
}

main

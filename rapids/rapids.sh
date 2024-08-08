#!/bin/bash

set -euxo pipefail

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

function os_id() {
  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs
}

function os_version() {
  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs
}

function os_codename() {
  grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs
}

function is_rocky() {
  [[ "$(os_id)" == 'rocky' ]]
}

function is_ubuntu() {
  [[ "$(os_id)" == 'ubuntu' ]]
}

function is_ubuntu20() {
  is_ubuntu && [[ "$(os_version)" == '20.04'* ]]
}

function is_ubuntu22() {
  is_ubuntu && [[ "$(os_version)" == '22.04'* ]]
}

function is_debian() {
  [[ "$(os_id)" == 'debian' ]]
}

function is_debian11() {
  is_debian && [[ "$(os_version)" == '11'* ]]
}

function is_debian12() {
  is_debian && [[ "$(os_version)" == '12'* ]]
}

function os_vercat() {
  if is_ubuntu ; then
      os_version | sed -e 's/[^0-9]//g'
  elif is_rocky ; then
      os_version | sed -e 's/[^0-9].*$//g'
  else
      os_version
  fi
}

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly DEFAULT_DASK_RAPIDS_VERSION="24.06"
readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' ${DEFAULT_DASK_RAPIDS_VERSION})

readonly SPARK_VERSION_ENV=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
readonly DEFAULT_SPARK_RAPIDS_VERSION="24.06"

if [[ "${SPARK_VERSION_ENV%%.*}" == "3" ]]; then
  readonly DEFAULT_CUDA_VERSION="11.8"
  readonly DEFAULT_XGBOOST_VERSION="2.0.3"
  readonly SPARK_VERSION="${SPARK_VERSION_ENV}"
  readonly DEFAULT_XGBOOST_GPU_SUB_VERSION=""
else
  readonly DEFAULT_CUDA_VERSION="10.1"
  readonly DEFAULT_XGBOOST_VERSION="1.0.0"
  readonly DEFAULT_XGBOOST_GPU_SUB_VERSION="Beta5"
  readonly SPARK_VERSION="2.x"
fi

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

readonly RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
readonly RUN_WORKER_ON_MASTER=$(get_metadata_attribute 'dask-cuda-worker-on-master' 'true')

# RAPIDS config
readonly CUDA_VERSION=$(get_metadata_attribute 'cuda-version' ${DEFAULT_CUDA_VERSION})
function is_cuda12() { [[ "${CUDA_VERSION%%.*}" == "12" ]] ; }
function is_cuda11() { [[ "${CUDA_VERSION%%.*}" == "11" ]] ; }

# SPARK config
readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})
readonly XGBOOST_GPU_SUB_VERSION=$(get_metadata_attribute 'spark-gpu-sub-version' ${DEFAULT_XGBOOST_GPU_SUB_VERSION})

# Scala config
readonly SCALA_VER="2.12"

# Dask config
readonly DASK_LAUNCHER=/usr/local/bin/dask-launcher.sh
readonly DASK_SERVICE=dask-cluster
readonly DASK_YARN_CONFIG_FILE=/etc/dask/config.yaml

# Dataproc configurations
readonly SPARK_CONF_DIR='/etc/spark/conf'

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

function install_dask_rapids() {
  if is_cuda12 ; then
    # This task takes a lot of memory and time.  The 15G available in n1-standard-4 is insufficient
    conda config --set channel_priority flexible
    conda create -n "rapids-${RAPIDS_VERSION}" -c rapidsai -c conda-forge -c nvidia  \
      "rapids=${RAPIDS_VERSION}" python="3.11" "cuda-version=${CUDA_VERSION}"
  elif is_cuda11 ; then
    if is_debian11 || is_debian12 || is_ubuntu20 || is_ubuntu22 ; then
        local python_ver="3.10"
    else
        local python_ver="3.9"
    fi
    # Install cudatoolkit, pandas, rapids and cudf
    mamba install -m -n 'dask-rapids' -y --no-channel-priority -c 'conda-forge' -c 'nvidia' -c 'rapidsai' \
      "cudatoolkit=11.0" "pandas<1.5" "rapids" "cudf" "python=${python_ver}"
  fi
}

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-spark-gpu_${SCALA_VER}/${XGBOOST_VERSION}/xgboost4j-spark-gpu_${SCALA_VER}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-gpu_${SCALA_VER}/${XGBOOST_VERSION}/xgboost4j-gpu_${SCALA_VER}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_${SCALA_VER}/${SPARK_RAPIDS_VERSION}/rapids-4-spark_${SCALA_VER}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  else
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  fi
}

function configure_spark() {
  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when the plan is not finalized,
# query explain output won't show GPU operator, if user have doubt
# they can uncomment the line before seeing the GPU plan explain, but AQE on gives user the best performance.
# spark.sql.adaptive.enabled=false
spark.executor.resource.gpu.amount=1
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.dynamicAllocation.enabled=false
spark.sql.autoBroadcastJoinThreshold=10m
spark.sql.files.maxPartitionBytes=512m
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  else
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  fi
}

configure_systemd_dask_service() {
  echo "Configuring systemd Dask service for RAPIDS..."
  local -r dask_worker_local_dir="/tmp/dask"
  local conda_env_bin
  conda_env_bin=$(conda info --base)/bin

  # Replace Dask Launcher file with dask-cuda config
  systemctl stop ${DASK_SERVICE}

  if [[ "${ROLE}" == "Master" ]]; then
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${RUN_WORKER_ON_MASTER}" == true ]]; then
  nvidia-smi -c DEFAULT
  echo "dask-cuda-worker starting, logging to /var/log/dask-cuda-worker.log."
  ${conda_env_bin}/dask-cuda-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-cuda-worker.log 2>&1 &
fi
echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log."
${conda_env_bin}/dask-scheduler > /var/log/dask-scheduler.log 2>&1
EOF
  else
    nvidia-smi -c DEFAULT
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
${conda_env_bin}/dask-cuda-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-cuda-worker.log 2>&1
EOF
  fi
  chmod 750 "${DASK_LAUNCHER}"

  systemctl daemon-reload
  echo "Restarting Dask cluster..."
  systemctl start "${DASK_SERVICE}"
}

function configure_dask_yarn() {
  local base
  base=$(conda info --base)

  # Replace config file on cluster.
  cat <<EOF >"${DASK_YARN_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: python://${base}/bin/python

  worker:
    count: 2
    gpus: 1
    class: "dask_cuda.CUDAWorker"
EOF
}

function main() {
  if [[ "${RUNTIME}" == "DASK" ]]; then
    # Install RAPIDS
    install_dask_rapids

    # In "standalone" mode, Dask relies on a shell script to launch.
    # In "yarn" mode, it relies a config.yaml file.
    if [[ -f "${DASK_LAUNCHER}" ]]; then
      configure_systemd_dask_service
    elif [[ -f "${DASK_YARN_CONFIG_FILE}" ]]; then
      configure_dask_yarn
    fi
    echo "RAPIDS installed with Dask runtime"
  elif [[ "${RUNTIME}" == "SPARK" ]]; then
    install_spark_rapids
    configure_spark
    echo "RAPIDS initialized with Spark runtime"
  else
    echo "Unsupported RAPIDS Runtime: ${RUNTIME}"
    exit 1
  fi

  if [[ "${ROLE}" == "Master" ]]; then
    systemctl restart hadoop-yarn-resourcemanager.service
    # Restart NodeManager on Master as well if this is a single-node-cluster.
    if systemctl status hadoop-yarn-nodemanager; then
      systemctl restart hadoop-yarn-nodemanager.service
    fi
  else
    systemctl restart hadoop-yarn-nodemanager.service
  fi
}

main

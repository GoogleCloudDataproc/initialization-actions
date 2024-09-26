#!/bin/bash

# Copyright 2019,2020,2021,2022,2024 Google LLC
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

# This initialization action script will install rapids on a Dataproc
# cluster.

set -euxo pipefail

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value="${2:-}"
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly SPARK_VERSION_ENV=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
if [[ "${SPARK_VERSION_ENV%%.*}" == "3" ]]; then
  DEFAULT_CUDA_VERSION="12.4"
  readonly DEFAULT_XGBOOST_VERSION="2.0.3"
  readonly SPARK_VERSION="${SPARK_VERSION_ENV}"
  readonly DEFAULT_XGBOOST_GPU_SUB_VERSION=""
else
  DEFAULT_CUDA_VERSION="10.1"
  readonly DEFAULT_XGBOOST_VERSION="1.0.0"
  readonly DEFAULT_XGBOOST_GPU_SUB_VERSION="Beta5"
  readonly SPARK_VERSION="2.x"
fi

# RAPIDS config
readonly RAPIDS_RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
if [[ "${RAPIDS_RUNTIME}" != "SPARK" ]]; then # to match install_gpu_driver.sh ; they should both probably be removed
  DEFAULT_CUDA_VERSION='11.8'
fi
readonly DEFAULT_CUDA_VERSION
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' ${DEFAULT_CUDA_VERSION})

readonly CUDA_VERSION
function is_cuda12() { [[ "${CUDA_VERSION%%.*}" == "12" ]] ; }
function is_cuda11() { [[ "${CUDA_VERSION%%.*}" == "11" ]] ; }

readonly DEFAULT_DASK_RAPIDS_VERSION="24.08"
readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' ${DEFAULT_DASK_RAPIDS_VERSION})

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

readonly RUN_WORKER_ON_MASTER=$(get_metadata_attribute 'dask-cuda-worker-on-master' 'true')

# SPARK config
readonly DEFAULT_SPARK_RAPIDS_VERSION="24.08.0"
readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})
readonly XGBOOST_GPU_SUB_VERSION=$(get_metadata_attribute 'spark-gpu-sub-version' ${DEFAULT_XGBOOST_GPU_SUB_VERSION})

# Scala config
readonly SCALA_VER="2.12"

# Dask config
readonly DASK_RUNTIME="$(/usr/share/google/get_metadata_value attributes/dask-runtime || echo 'standalone')"
readonly DASK_LAUNCHER=/usr/local/bin/dask-launcher.sh
readonly DASK_SERVICE=dask-cluster
readonly DASK_WORKER_SERVICE=dask-worker
readonly DASK_SCHEDULER_SERVICE=dask-scheduler
readonly DASK_YARN_CONFIG_FILE=/etc/dask/config.yaml

# Dataproc configurations
readonly SPARK_CONF_DIR='/etc/spark/conf'

function execute_with_retries() {
  local -r cmd="$*"
  for i in {0..9} ; do
    if eval "$cmd"; then
      return 0 ; fi
    sleep 5
  done
  echo "Cmd '${cmd}' failed."
  return 1
}

readonly conda_env="/opt/conda/miniconda3/envs/dask-rapids"
function install_dask_rapids() {
  if is_cuda12 ; then
    local python_spec="python>=3.11"
    local cuda_spec="cuda-version>=12,<13"
    local dask_spec="dask>=2024.8"
    local numba_spec="numba"
  elif is_cuda11 ; then
    local python_spec="python>=3.9"
    local cuda_spec="cuda-version>=11,<=11.8"
    local dask_spec="dask"
    local numba_spec="numba"
  fi

  local CONDA_PACKAGES=(
    "${cuda_spec}"
    "rapids=${RAPIDS_VERSION}"
    "${dask_spec}"
    "dask-bigquery"
    "dask-ml"
    "dask-sql"
    "cudf"
    "${numba_spec}"
  )

  # Install cuda, rapids, dask
  local is_installed="0"
  mamba="/opt/conda/default/bin/mamba"
  conda="/opt/conda/default/bin/conda"

  for installer in "${mamba}" "${conda}" ; do
    set +e
    test -d "${conda_env}" || \
      time "${installer}" "create" -m -n 'dask-rapids' -y --no-channel-priority \
      -c 'conda-forge' -c 'nvidia' -c 'rapidsai'  \
      ${CONDA_PACKAGES[*]} \
      "${python_spec}"
    if [[ "$?" == "0" ]] ; then
      is_installed="1"
      break
    else
      "${conda}" config --set channel_priority flexible
    fi
    set -e
  done
  if [[ "${is_installed}" == "0" ]]; then
    echo "failed to install dask"
    return 1
  fi
  set -e
}

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    execute_with_retries wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-spark-gpu_${SCALA_VER}/${XGBOOST_VERSION}/xgboost4j-spark-gpu_${SCALA_VER}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    execute_with_retries wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-gpu_${SCALA_VER}/${XGBOOST_VERSION}/xgboost4j-gpu_${SCALA_VER}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    execute_with_retries wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_${SCALA_VER}/${SPARK_RAPIDS_VERSION}/rapids-4-spark_${SCALA_VER}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  else
    execute_with_retries wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    execute_with_retries wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  fi
}

function configure_spark() {
  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when the plan is
# not finalized, query explain output won't show GPU operator, if user
# have doubt they can uncomment the following line before seeing the GPU plan
# explain, but AQE on gives user the best performance.
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

enable_worker_service="0"
function install_systemd_dask_worker() {
  echo "Installing systemd Dask Worker service..."
  local -r dask_worker_local_dir="/tmp/${DASK_WORKER_SERVICE}"

  mkdir -p "${dask_worker_local_dir}"

  local DASK_WORKER_LAUNCHER="/usr/local/bin/${DASK_WORKER_SERVICE}-launcher.sh"

  cat <<EOF >"${DASK_WORKER_LAUNCHER}"
#!/bin/bash
LOGFILE="/var/log/${DASK_WORKER_SERVICE}.log"
nvidia-smi -c DEFAULT
echo "dask-cuda-worker starting, logging to \${LOGFILE}"
${conda_env}/bin/dask-cuda-worker "${MASTER}:8786" --local-directory="${dask_worker_local_dir}" --memory-limit=auto >> "\${LOGFILE}" 2>&1
EOF

  chmod 750 "${DASK_WORKER_LAUNCHER}"

  local -r dask_service_file="/usr/lib/systemd/system/${DASK_WORKER_SERVICE}.service"
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Worker Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_WORKER_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload

  # Enable the service
  if [[ "${ROLE}" != "Master" ]]; then
    enable_worker_service="1"
  else
    # Enable service on single-node cluster (no workers)
    local worker_count="$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)"
    if [[ "${worker_count}" == "0" || "${RUN_WORKER_ON_MASTER}" == "true" ]]; then
      enable_worker_service="1"
    fi
  fi

  if [[ "${enable_worker_service}" == "1" ]]; then
    systemctl enable "${DASK_WORKER_SERVICE}"
    systemctl restart "${DASK_WORKER_SERVICE}"
  fi
}

function configure_dask_yarn() {
  # Replace config file on cluster.
  cat <<EOF >"${DASK_YARN_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: python://${conda_env}/bin/python

  worker:
    count: 2
    gpus: 1
    class: "dask_cuda.CUDAWorker"
EOF
}

function main() {
  if [[ "${RAPIDS_RUNTIME}" == "DASK" ]]; then
    # Install RAPIDS
    install_dask_rapids

    # In "standalone" mode, Dask relies on a shell script to launch.
    # In "yarn" mode, it relies a config.yaml file.
    if [[ "${DASK_RUNTIME}" == "standalone" ]]; then
      install_systemd_dask_worker
    elif [[ "${DASK_RUNTIME}" == "yarn" ]]; then
      configure_dask_yarn
    fi
    echo "RAPIDS installed with Dask runtime"
  elif [[ "${RAPIDS_RUNTIME}" == "SPARK" ]]; then
    install_spark_rapids
    configure_spark
    echo "RAPIDS initialized with Spark runtime"
  else
    echo "Unsupported RAPIDS Runtime: ${RAPIDS_RUNTIME}"
    exit 1
  fi

  if [[ "${ROLE}" == "Master" ]]; then
    systemctl restart hadoop-yarn-resourcemanager.service
    # Restart NodeManager on Master as well if this is a single-node-cluster.
    if systemctl list-units | grep hadoop-yarn-nodemanager; then
      systemctl restart hadoop-yarn-nodemanager.service
    fi
  else
    systemctl restart hadoop-yarn-nodemanager.service
  fi
}

main

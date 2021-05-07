#!/bin/bash

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly DEFAULT_RAPIDS_VERSION="0.19"
readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' ${DEFAULT_RAPIDS_VERSION})

readonly SPARK_VERSION_ENV=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
readonly DEFAULT_SPARK_RAPIDS_VERSION="0.4.1"

if [[ "${SPARK_VERSION_ENV}" == "3"* ]]; then
  readonly DEFAULT_CUDA_VERSION="11.0"
  readonly DEFAULT_CUDF_VERSION="0.18.1"
  readonly DEFAULT_XGBOOST_VERSION="1.3.0"
  readonly DEFAULT_XGBOOST_GPU_SUB_VERSION="0.1.0"
  # TODO: uncomment when Spark 3.1 jars will be released - RAPIDS work with Spark 3.1, this is just for Maven URL
  # readonly SPARK_VERSION="${SPARK_VERSION_ENV}"
  readonly SPARK_VERSION="3.0"
else
  readonly DEFAULT_CUDA_VERSION="10.1"
  readonly DEFAULT_CUDF_VERSION="0.9.2"
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
readonly CUDF_VERSION=$(get_metadata_attribute 'cudf-version' ${DEFAULT_CUDF_VERSION})

# SPARK config
readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})
readonly XGBOOST_GPU_SUB_VERSION=$(get_metadata_attribute 'spark-gpu-sub-version' ${DEFAULT_XGBOOST_GPU_SUB_VERSION})

# Dask config
readonly DASK_LAUNCHER=dask-launcher.sh
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
  local base
  base=$(conda info --base)
  local -r mamba_env=mamba
  
  # Using mamba significantly reduces the conda solve-time. Create a separate conda
  # environment with mamba installed to manage installations.
  conda create -y -n ${mamba_env} -c conda-forge mamba

  # Install RAPIDS, cudatoolkit. Use mamba in new env to resolve base environment
  # Dependency "icu" is also reinstalled here. 
  ${base}/envs/${mamba_env}/bin/mamba install -y \
    -c "rapidsai" -c "nvidia" -c "conda-forge" -c "defaults" \
    "cudatoolkit=${CUDA_VERSION}" "rapids-blazing=${RAPIDS_VERSION}" \
    -p ${base}

  # Remove mamba env
  conda env remove -n ${mamba_env}
}

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local cudf_cuda_version="${CUDA_VERSION//\./-}"
  # Convert "11-0" to "11"
  cudf_cuda_version="${cudf_cuda_version%-0}"

  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  else
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
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
# Rapids Accelerator for Spark can utilize AQE, but when plan is not finalized, 
# query explain output won't show GPU operator, if user have doubt
# they can uncomment the line before to see the GPU plan explan, but AQE on give user the best performance.
# spark.sql.adaptive.enabled=false
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
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
spark.dynamicAllocation.enabled=false
spark.shuffle.service.enabled=false
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  else
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
spark.dynamicAllocation.enabled=false
spark.shuffle.service.enabled=false
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
    # RUNTIME is exposed by the Dask initialization action in
    # "standalone" mode. In "YARN" mode, there is a config.yaml file.
    if [[ -f "${DASK_SERVICE}" ]]; then
      configure_systemd_dask_service
    elif [[ -f "${DASK_YARN_CONFIG_FILE}" ]]; then
      configure_dask_yarn
    fi
    
    # Install RAPIDS
    install_dask_rapids
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

#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script installs Apache Flink (http://flink.apache.org) on a Google Cloud
# Dataproc cluster. This script is based on previous scripts:
# https://github.com/GoogleCloudPlatform/bdutil/tree/master/extensions/flink
#
# To use this script, you will need to configure the following variables to
# match your cluster. For information about which software components
# (and their version) are included in Cloud Dataproc clusters, see the
# Cloud Dataproc Image Version information:
# https://cloud.google.com/dataproc/concepts/dataproc-versions

set -euxo pipefail

readonly NOT_SUPPORTED_MESSAGE="Flink initialization action is not supported on Dataproc ${DATAPROC_VERSION}.
Use Flink Component instead: https://cloud.google.com/dataproc/docs/concepts/components/flink"
[[ $DATAPROC_VERSION != 1.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

# Install directories for Flink and Hadoop.
readonly FLINK_INSTALL_DIR='/usr/lib/flink'
readonly FLINK_WORKING_DIR='/var/lib/flink'
readonly FLINK_YARN_SCRIPT='/usr/bin/flink-yarn-daemon'
readonly FLINK_WORKING_USER='yarn'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'

readonly MASTER_HOSTNAME="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

# The number of buffers for the network stack.
# Flink config entry: taskmanager.network.numberOfBuffers.
readonly FLINK_NETWORK_NUM_BUFFERS=2048

# Heap memory used by the job manager (master) determined by the physical (free) memory of the server.
# Flink config entry: jobmanager.heap.mb.
readonly FLINK_JOBMANAGER_MEMORY_FRACTION='1.0'

# Heap memory used by the task managers (slaves) determined by the physical (free) memory of the servers.
# Flink config entry: taskmanager.heap.mb.
readonly FLINK_TASKMANAGER_MEMORY_FRACTION='1.0'

readonly START_FLINK_YARN_SESSION_METADATA_KEY='flink-start-yarn-session'
# Set this to true to start a flink yarn session at initialization time.
readonly START_FLINK_YARN_SESSION_DEFAULT=true

# Set this to install flink from a snapshot URL instead of apt
readonly FLINK_SNAPSHOT_URL_METADATA_KEY='flink-snapshot-url'

readonly MASTER_ADDITIONAL="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function retry_apt_command() {
  local -r cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_apt_command "apt-get update"
}

function install_apt_get() {
  local pkgs="$*"
  retry_apt_command "apt-get install -y $pkgs"
}

# Returns list of zookeeper servers configured in the zoo.cfg file.
function get_zookeeper_nodes_list() {
  local -r zookeeper_config_file=/etc/zookeeper/conf/zoo.cfg
  local zookeeper_client_port
  zookeeper_client_port=$(grep 'clientPort' "${zookeeper_config_file}" |
    tail -n 1 |
    cut -d '=' -f 2)
  local zookeeper_list
  zookeeper_list=$(grep '^server.' "${zookeeper_config_file}" |
    tac |
    sort -u -t '=' -k1,1 |
    cut -d '=' -f 2 |
    cut -d ':' -f 1 |
    sed "s/$/:${zookeeper_client_port}/" |
    xargs echo |
    sed "s/ /,/g")
  echo "${zookeeper_list}"
}

function install_flink_snapshot() {
  local work_dir
  work_dir="$(mktemp -d)"
  local flink_url
  flink_url="$(/usr/share/google/get_metadata_value "attributes/${FLINK_SNAPSHOT_URL_METADATA_KEY}")"
  local -r flink_local="${work_dir}/flink.tgz"
  local -r flink_toplevel_pattern="${work_dir}/flink-*"

  pushd "${work_dir}"

  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${flink_url}" -o "${flink_local}"
  tar -xzf "${flink_local}"
  rm "${flink_local}"

  # only the first match of the flink toplevel pattern is used
  local flink_toplevel
  flink_toplevel=$(compgen -G "${flink_toplevel_pattern}" | head -n1)
  mv "${flink_toplevel}" "${FLINK_INSTALL_DIR}"

  popd # work_dir
}

function configure_flink() {
  # Number of worker nodes in your cluster
  local num_workers
  num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

  # Number of Flink TaskManagers to use. Reserve 1 node for the JobManager.
  # NB: This assumes > 1 worker node.
  local -r num_taskmanagers="$((num_workers - 1))"

  # Determine the number of task slots per worker.
  # TODO: Dataproc does not currently set the number of worker cores on the
  # master node. However, the spark configuration sets the number of executors
  # to be half the number of CPU cores per worker. We use this value to
  # determine the number of worker cores. Fix this hack when
  # yarn.nodemanager.resource.cpu-vcores is correctly populated.
  local spark_executor_cores
  spark_executor_cores=$(
    grep 'spark\.executor\.cores' /etc/spark/conf/spark-defaults.conf |
      tail -n1 |
      cut -d'=' -f2
  )
  local -r flink_taskmanager_slots="$((spark_executor_cores * 2))"

  # Determine the default parallelism.
  local flink_parallelism
  flink_parallelism="$((num_taskmanagers * flink_taskmanager_slots))"

  # Get worker memory from yarn config.
  local worker_total_mem
  worker_total_mem="$(hdfs getconf -confKey yarn.nodemanager.resource.memory-mb)"
  local flink_jobmanager_memory
  flink_jobmanager_memory=$(python -c \
    "print int(${worker_total_mem} * ${FLINK_JOBMANAGER_MEMORY_FRACTION})")
  local flink_taskmanager_memory
  flink_taskmanager_memory=$(python -c \
    "print int(${worker_total_mem} * ${FLINK_TASKMANAGER_MEMORY_FRACTION})")

  # create working directory
  mkdir -p "${FLINK_WORKING_DIR}"

  # Apply Flink settings by appending them to the default config.
  cat <<EOF >>${FLINK_INSTALL_DIR}/conf/flink-conf.yaml
# Settings applied by Cloud Dataproc initialization action
jobmanager.rpc.address: ${MASTER_HOSTNAME}
jobmanager.heap.mb: ${flink_jobmanager_memory}
taskmanager.heap.mb: ${flink_taskmanager_memory}
taskmanager.numberOfTaskSlots: ${flink_taskmanager_slots}
parallelism.default: ${flink_parallelism}
taskmanager.network.numberOfBuffers: ${FLINK_NETWORK_NUM_BUFFERS}
fs.hdfs.hadoopconf: ${HADOOP_CONF_DIR}
EOF

  if [[ -n "${MASTER_ADDITIONAL}" ]]; then
    local zookeeper_nodes
    zookeeper_nodes="$(get_zookeeper_nodes_list)"
    cat <<EOF >>"${FLINK_INSTALL_DIR}/conf/flink-conf.yaml"
high-availability: zookeeper
high-availability.zookeeper.quorum: ${zookeeper_nodes}
high-availability.zookeeper.storageDir: hdfs:///flink/recovery
high-availability.zookeeper.path.root: /flink
yarn.application-attempts: 10
EOF
  fi

  cat <<EOF >"${FLINK_YARN_SCRIPT}"
#!/bin/bash
set -exuo pipefail
sudo -u yarn -i \
HADOOP_CLASSPATH=$(hadoop classpath) \
HADOOP_CONF_DIR=${HADOOP_CONF_DIR} \
${FLINK_INSTALL_DIR}/bin/yarn-session.sh \
  -n "${num_taskmanagers}" \
  -s "${flink_taskmanager_slots}" \
  -jm "${flink_jobmanager_memory}" \
  -tm "${flink_taskmanager_memory}" \
  -nm flink-dataproc \
  --detached
EOF
  chmod +x "${FLINK_YARN_SCRIPT}"
}

function start_flink_master() {
  local start_yarn_session
  start_yarn_session="$(/usr/share/google/get_metadata_value \
    "attributes/${START_FLINK_YARN_SESSION_METADATA_KEY}" ||
    echo "${START_FLINK_YARN_SESSION_DEFAULT}")"

  # Start Flink master only on the master node ("0"-master in HA mode)
  if [[ "${start_yarn_session}" == "true" && "${HOSTNAME}" == "${MASTER_HOSTNAME}" ]]; then
    "${FLINK_YARN_SCRIPT}"
  else
    echo "Skipping Flink master startup - non primary master node"
  fi
}

function main() {
  # Check if a Flink snapshot URL is specified
  if /usr/share/google/get_metadata_value "attributes/${FLINK_SNAPSHOT_URL_METADATA_KEY}"; then
    install_flink_snapshot || err "Unable to install Flink"
  else
    update_apt_get || err "Unable to update apt-get"
    install_apt_get flink || err "Unable to install flink"
  fi
  configure_flink || err "Flink configuration failed"
  if [[ "${HOSTNAME}" == "${MASTER_HOSTNAME}" ]]; then
    start_flink_master || err "Unable to start Flink master"
  fi
}

main

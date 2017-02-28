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
# Cloud Dataproc Image Version informayion:
# https://cloud.google.com/dataproc/concepts/dataproc-versions

set -x -e

# Install directories for Flink and Hadoop
readonly FLINK_INSTALL_DIR='/usr/lib/flink'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'

# The number of buffers for the network stack
# Flink config entry: taskmanager.network.numberOfBuffers
readonly FLINK_NETWORK_NUM_BUFFERS=2048

# Heap memory used by the job manager (master) determined by the physical (free) memory of the server
# Flink config entry: jobmanager.heap.mb
readonly FLINK_JOBMANAGER_MEMORY_FRACTION='1.0'

# Heap memory used by the task managers (slaves) determined by the physical (free) memory of the servers
# Flink config entry: taskmanager.heap.mb
readonly FLINK_TASKMANAGER_MEMORY_FRACTION='1.0'

# Set this to true to start a flink yarn session at initialization time.
readonly START_FLINK_YARN_SESSION="true"

function configure_flink() {
  # Number of worker nodes in your cluster
  local num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

  # Number of Flink TaskManagers to use. Reserve 1 node for the JobManager.
  # NB: This assumes > 1 worker node.
  local num_taskmanagers="$(($num_workers - 1))"

  # Determine the number of task slots per worker.
  # TODO: Dataproc does not currently set the number of worker cores on the
  # master node. However, the spark configuration sets the number of executors
  # to be half the number of CPU cores per worker. We use this value to
  # determine the number of worker cores. Fix this hack when
  # yarn.nodemanager.resource.cpu-vcores is correctly populated.
  local spark_executor_cores=$(\
    grep 'spark\.executor\.cores' /etc/spark/conf/spark-defaults.conf \
    | tail -n1 | cut -d'=' -f2)
  local flink_taskmanager_slots="$(($spark_executor_cores * 2))"
  # local flink_taskmanager_slots="$(hdfs getconf \
  #   -confKey yarn.nodemanager.resource.cpu-vcores)"

  # Determine the default parallelism
  local flink_parallelism=$(python -c \
      "print ${num_taskmanagers} * ${flink_taskmanager_slots}")

  # Get worker memory from yarn config.
  local worker_total_mem="$(hdfs getconf \
    -confKey yarn.nodemanager.resource.memory-mb)"
  local flink_jobmanager_memory=$(python -c \
      "print int(${worker_total_mem} * ${FLINK_JOBMANAGER_MEMORY_FRACTION})")
  local flink_taskmanager_memory=$(python -c \
      "print int(${worker_total_mem} * ${FLINK_TASKMANAGER_MEMORY_FRACTION})")

  # Apply Flink settings by appending them to the default config
  cat << EOF >> ${FLINK_INSTALL_DIR}/conf/flink-conf.yaml
# Settings applied by Cloud Dataproc initialization action
jobmanager.rpc.address: ${MASTER_HOSTNAME}
jobmanager.heap.mb: ${flink_jobmanager_memory}
taskmanager.heap.mb: ${flink_taskmanager_memory}
taskmanager.numberOfTaskSlots: ${flink_taskmanager_slots}
parallelism.default: ${flink_parallelism}
taskmanager.network.numberOfBuffers: ${FLINK_NETWORK_NUM_BUFFERS}
fs.hdfs.hadoopconf: ${HADOOP_CONF_DIR}
EOF

  if [ "${START_FLINK_YARN_SESSION}" = 'true' ] ; then
    # NB: yarn-session.sh ignores taskmanager.numberOfTaskSlots for some reason.
    # We specify it manually below.
    env HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
      "$FLINK_INSTALL_DIR/bin/yarn-session.sh" \
      -n "${num_taskmanagers}" \
      -s "${flink_taskmanager_slots}" \
      -jm "${flink_jobmanager_memory}" \
      -tm "${flink_taskmanager_memory}" \
      --detached
  fi

}

function main() {
local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
if [[ "${role}" == 'Master' ]]; then
  apt-get install -y flink
  configure_flink
fi
}

main

set +x +e

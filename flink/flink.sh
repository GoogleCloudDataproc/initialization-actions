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

# Number of worker nodes in your cluster
NUM_WORKERS=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
# Scala version on the cluster see the following side for details
SCALA_VERSION="2.10"
# Hadoop version on the cluster see the following side for details
HADOOP_VERSION="2.7"
# Flink version to be installed on the cluster
FLINK_VERSION="1.0.3"

# Location of the FLink binary archive
CONCAT_HADOOP_VERSION=$(echo $HADOOP_VERSION | sed 's/\.//g')
FLINK_HADOOP_URI="flink-${FLINK_VERSION}-bin-hadoop${CONCAT_HADOOP_VERSION}"
SCALA_URI="scala_${SCALA_VERSION}.tgz"
FLINK_TARBALL_URI="http://www-us.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_HADOOP_URI}-${SCALA_URI}"

# Install directories for Flink and Hadoop
FLINK_INSTALL_DIR='/usr/lib/flink'
HADOOP_CONF_DIR='/etc/hadoop/conf'

# Default parallelism (number of concurrent actions per task)
# If set to 'auto', this will be determined automatically
# Flink config entry: parallelism.default
FLINK_PARALLELISM='auto'

# The number of buffers for the network stack
# Flink config entry: taskmanager.network.numberOfBuffers
FLINK_NETWORK_NUM_BUFFERS=2048

# Heap memory used by the job manager (master) determined by the physical (free) memory of the server
# Flink config entry: jobmanager.heap.mb
FLINK_JOBMANAGER_MEMORY_FRACTION='0.8'

# Heap memory used by the task managers (slaves) determined by the physical (free) memory of the servers
# Flink config entry: taskmanager.heap.mb
FLINK_TASKMANAGER_MEMORY_FRACTION='0.8'

# Determine the number of task slots
FLINK_TASKMANAGER_SLOTS=`grep -c processor /proc/cpuinfo`

# Determine the default parallelism
FLINK_PARALLELISM=$(python -c \
    "print ${NUM_WORKERS} * ${FLINK_TASKMANAGER_SLOTS}")

# Calculate the memory allocations, MB, using 'free -m'. Floor to nearest MB.
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
FLINK_JOBMANAGER_MEMORY=$(python -c \
    "print int(${TOTAL_MEM} * ${FLINK_JOBMANAGER_MEMORY_FRACTION})")
FLINK_TASKMANAGER_MEMORY=$(python -c \
    "print int(${TOTAL_MEM} * ${FLINK_TASKMANAGER_MEMORY_FRACTION})")

# Install Flink and tidy things up
FLINK_TARBALL_NAME="flink-${FLINK_VERSION}-bin-hadoop${CONCAT_HADOOP_VERSION}-scala_${SCALA_VERSION}.tgz"
FLINK_EXTRACT_DIRECTORY="flink-${FLINK_VERSION}"
mkdir ${FLINK_INSTALL_DIR}
cd ${FLINK_INSTALL_DIR}
wget ${FLINK_TARBALL_URI}
tar -zxvf ${FLINK_TARBALL_NAME}
mv ${FLINK_EXTRACT_DIRECTORY}/* .
rm ${FLINK_TARBALL_NAME}
rmdir ${FLINK_EXTRACT_DIRECTORY}

# Apply Flink settings by appending them to the default config
cat << EOF >> ${FLINK_INSTALL_DIR}/conf/flink-conf.yaml
# Settings applied by Cloud Dataproc initialization action
jobmanager.rpc.address: ${MASTER_HOSTNAME}
jobmanager.heap.mb: ${FLINK_JOBMANAGER_MEMORY}
taskmanager.heap.mb: ${FLINK_TASKMANAGER_MEMORY}
taskmanager.numberOfTaskSlots: ${FLINK_TASKMANAGER_SLOTS}
parallelism.default: ${FLINK_PARALLELISM}
taskmanager.network.numberOfBuffers: ${FLINK_NETWORK_NUM_BUFFERS}
fs.hdfs.hadoopconf: ${HADOOP_CONF_DIR}
EOF

# Start a Flink YARN session
HADOOP_CONF_DIR=/etc/hadoop/conf ./bin/yarn-session.sh -n ${FLINK_TASKMANAGER_SLOTS} --detached

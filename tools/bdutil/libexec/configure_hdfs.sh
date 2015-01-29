# Copyright 2014 Google Inc. All Rights Reserved.
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

# Configures HDFS

set -e

source hadoop_helpers.sh

if (( ${ENABLE_HDFS} )); then

  HDFS_ADMIN=$(get_hdfs_superuser)

  # Location of HDFS metadata on namenode
  export HDFS_NAME_DIR=/hadoop/dfs/name

  # If disks are mounted use all of them for HDFS data
  MOUNTED_DISKS=($(find /mnt -maxdepth 1 -mindepth 1))
  if [[ ${#MOUNTED_DISKS[@]} -eq 0 ]]; then
    MOUNTED_DISKS=('')
  fi

  # Location of HDFS data blocks on datanodes; for each mounted disk, add the
  # path /mnt/diskname/hadoop/dfs/data as a data directory, or if no mounted
  # disks exist, just go with the absolute path /hadoop/dfs/data.
  HDFS_DATA_DIRS="${MOUNTED_DISKS[@]/%//hadoop/dfs/data}"

  # Do not create HDFS_NAME_DIR, or Hadoop will think it is already formatted
  mkdir -p /hadoop/dfs ${HDFS_DATA_DIRS}

  chown ${HDFS_ADMIN}:hadoop -L -R /hadoop/dfs ${HDFS_DATA_DIRS}

  # Make sure the data dirs have the expected permissions.
  chmod ${HDFS_DATA_DIRS_PERM} ${HDFS_DATA_DIRS}

  # Set general Hadoop environment variables

  # Calculate the memory allocations, MB, using 'free -m'. Floor to nearest MB.
  TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
  NAMENODE_MEM_MB=$(python -c "print int(${TOTAL_MEM} * \
      ${HDFS_MASTER_MEMORY_FRACTION} / 2)")
  SECONDARYNAMENODE_MEM_MB=${NAMENODE_MEM_MB}

  cat << EOF >> ${HADOOP_CONF_DIR}/hadoop-env.sh

# Increase the maximum NameNode / SecondaryNameNode heap.
HADOOP_NAMENODE_OPTS="-Xmx${NAMENODE_MEM_MB}m \${HADOOP_NAMENODE_OPTS}"
HADOOP_SECONDARYNAMENODE_OPTS="-Xmx${SECONDARYNAMENODE_MEM_MB}m \${HADOOP_SECONDARYNAMENODE_OPTS}"
EOF

  # Increase maximum number of files for HDFS
  MAX_FILES=16384
  ulimit -n ${MAX_FILES}
  cat << EOF > /etc/security/limits.d/hadoop.conf
${HDFS_ADMIN} hard nofile ${MAX_FILES}
${HDFS_ADMIN} soft nofile ${MAX_FILES}
EOF

  export HDFS_DATA_DIRS="${HDFS_DATA_DIRS// /,}"

  bdconfig merge_configurations \
      --configuration_file ${HADOOP_CONF_DIR}/hdfs-site.xml \
      --source_configuration_file hdfs-template.xml \
      --resolve_environment_variables \
      --create_if_absent \
      --clobber
fi

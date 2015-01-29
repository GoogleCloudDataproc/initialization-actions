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

# Generates the config files which will be needed by the hadoop servers such
# as 'slaves' listing all worker hostnames, 'masters' listing the master,
# and the xml files which go under the 'conf/' directory of the hadoop
# installation.

set -e

# Set general Hadoop environment variables
JAVA_HOME=$(readlink -f $(which java) | sed 's|/bin/java$||')
# Place HADOOP_LOG_DIR in /hadoop (possibly on larger non-boot-disk)
HADOOP_LOG_DIR=/hadoop/logs
# Used for hadoop.tmp.dir
export HADOOP_TMP_DIR=/hadoop/tmp
mkdir -p ${HADOOP_TMP_DIR} ${HADOOP_LOG_DIR}

# Ideally we expect WORKERS to be an actual array, but if it's a
# space-separated string instead, we'll just cast it as an array first.
if ! declare -p WORKERS | grep -q '^declare \-a'; then
  WORKERS=(${WORKERS})
fi

echo ${WORKERS[@]} | tr ' ' '\n' > ${HADOOP_CONF_DIR}/slaves
echo ${MASTER_HOSTNAME} > ${HADOOP_CONF_DIR}/masters

# Basic configuration not related to GHFS or HDFS.
# Rough rule-of-thumb settings for default maps/reduces taken from
# http://wiki.apache.org/hadoop/HowManyMapsAndReduces
export DEFAULT_NUM_MAPS=$((${NUM_WORKERS} * 10))
export DEFAULT_NUM_REDUCES=$((${NUM_WORKERS} * 4))

export NUM_CORES="$(grep -c processor /proc/cpuinfo)"
export MAP_SLOTS=$(python -c "print int(${NUM_CORES} // \
    ${CORES_PER_MAP_TASK})")
export REDUCE_SLOTS=$(python -c "print int(${NUM_CORES} // \
    ${CORES_PER_REDUCE_TASK})")

# Calculate the memory allocations, MB, using 'free -m'. Floor to nearest MB.
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
HADOOP_MR_MASTER_MEM_MB=$(python -c "print int(${TOTAL_MEM} * \
    ${HADOOP_MASTER_MAPREDUCE_MEMORY_FRACTION})")

# Fix Python 2.6 on CentOS
# TODO(user): Extract this into a helper.
if ! python -c 'import argparse' && [[ -x $(which yum) ]]; then
  yum install -y python-argparse
fi

# MapReduce v2 (and YARN) Configuration
if [[ -x configure_mrv2_mem.py ]]; then
  TEMP_ENV_FILE=$(mktemp /tmp/mrv2_XXX_tmp_env.sh)
  ./configure_mrv2_mem.py \
      --output_file ${TEMP_ENV_FILE} \
      --total_memory ${TOTAL_MEM} \
      --available_memory_ratio ${NODEMANAGER_MEMORY_FRACTION} \
      --total_cores ${NUM_CORES} \
      --cores_per_map ${CORES_PER_MAP_TASK} \
      --cores_per_reduce ${CORES_PER_REDUCE_TASK} \
      --cores_per_app_master ${CORES_PER_APP_MASTER}
  source ${TEMP_ENV_FILE}
  # Leave TMP_ENV_FILE around for debugging purposes.
fi

# Give Hadoop clients 1/4 of available memory
HADOOP_CLIENT_MEM_MB=$(python -c "print int(${TOTAL_MEM} / 4)")

cat << EOF >> ${HADOOP_CONF_DIR}/hadoop-env.sh
export JAVA_HOME=${JAVA_HOME}
export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}

# Increase maximum Hadoop client heap
HADOOP_CLIENT_OPTS="-Xmx${HADOOP_CLIENT_MEM_MB}m \${HADOOP_CLIENT_OPTS}"

# Increase maximum JobTracker heap
HADOOP_JOBTRACKER_OPTS="-Xmx${HADOOP_MR_MASTER_MEM_MB}m \
    \${HADOOP_JOBTRACKER_OPTS}"
EOF

# Place mapred temp directories on non-boot disks for the same reason as logs:
# If disks are mounted use all of them for mapred local data
MOUNTED_DISKS=($(find /mnt -maxdepth 1 -mindepth 1))
if [[ ${#MOUNTED_DISKS[@]} -eq 0 ]]; then
  MOUNTED_DISKS=('')
fi

MAPRED_LOCAL_DIRS="${MOUNTED_DISKS[@]/%//hadoop/mapred/local}"
NODEMANAGER_LOCAL_DIRS="${MOUNTED_DISKS[@]/%//hadoop/yarn/nm-local-dir}"
mkdir -p ${MAPRED_LOCAL_DIRS} ${NODEMANAGER_LOCAL_DIRS}

chgrp hadoop -L -R \
  /hadoop \
  ${HADOOP_LOG_DIR} \
  ${HADOOP_TMP_DIR} \
  ${MAPRED_LOCAL_DIRS} \
  ${NODEMANAGER_LOCAL_DIRS}
chmod g+rwx -R \
  /hadoop \
  ${HADOOP_LOG_DIR} \
  ${MAPRED_LOCAL_DIRS} \
  ${NODEMANAGER_LOCAL_DIRS}
chmod 777 -R ${HADOOP_TMP_DIR}

export MAPRED_LOCAL_DIRS="${MAPRED_LOCAL_DIRS// /,}"
export NODEMANAGER_LOCAL_DIRS="${NODEMANAGER_LOCAL_DIRS// /,}"

YARN_ENV_FILE=${YARN_CONF_DIR:-$HADOOP_CONF_DIR}/yarn-env.sh
if [[ -f ${YARN_ENV_FILE} ]]; then
  cat << EOF >> ${YARN_ENV_FILE}
YARN_RESOURCEMANAGER_OPTS="-Xmx${HADOOP_MR_MASTER_MEM_MB}m \
    \${YARN_RESOURCEMANAGER_OPTS}"
YARN_LOG_DIR=${HADOOP_LOG_DIR}
# The following two YARN_OPTS overrides are provided to
# override any that have previously been set
YARN_OPTS="\$YARN_OPTS -Dhadoop.log.dir=\$YARN_LOG_DIR"
YARN_OPTS="\$YARN_OPTS -Dyarn.log.dir=\$YARN_LOG_DIR"

# Increase maximum YARN client heap
YARN_CLIENT_OPTS="-Xmx${HADOOP_CLIENT_MEM_MB}m \${YARN_CLIENT_OPTS}"
EOF
fi

bdconfig merge_configurations \
    --configuration_file ${HADOOP_CONF_DIR}/core-site.xml \
    --source_configuration_file core-template.xml \
    --resolve_environment_variables \
    --create_if_absent \
    --clobber

bdconfig merge_configurations \
    --configuration_file ${HADOOP_CONF_DIR}/mapred-site.xml \
    --source_configuration_file mapred-template.xml \
    --resolve_environment_variables \
    --create_if_absent \
    --clobber

# Set MapReduce health check script.
if [[ -f mapred-health-check.sh ]]; then
  cp mapred-health-check.sh "${HADOOP_INSTALL_DIR}/bin/"
  MAPRED_HEALTH_CHECK_SCRIPT="${HADOOP_INSTALL_DIR}/bin/mapred-health-check.sh"
  chgrp hadoop "${MAPRED_HEALTH_CHECK_SCRIPT}"
  chmod 775 "${MAPRED_HEALTH_CHECK_SCRIPT}"
  bdconfig set_property \
      --configuration_file ${HADOOP_CONF_DIR}/mapred-site.xml \
      --name 'mapred.healthChecker.script.path' \
      --value "${MAPRED_HEALTH_CHECK_SCRIPT}" \
      --noclobber
fi

if [[ -f yarn-template.xml ]]; then
  bdconfig merge_configurations \
      --configuration_file ${HADOOP_CONF_DIR}/yarn-site.xml \
      --source_configuration_file yarn-template.xml \
      --resolve_environment_variables \
      --create_if_absent \
      --clobber

  if [[ "${DEFAULT_FS}" == "gs" ]]; then
    bdconfig set_property \
        --configuration_file ${HADOOP_CONF_DIR}/yarn-site.xml \
        --name 'yarn.log-aggregation-enable' \
        --value 'true' \
        --clobber
  fi
fi

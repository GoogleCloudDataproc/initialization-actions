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

set -o nounset
set -o errexit

install_application 'python-numpy' 'numpy'

SCALA_TARBALL=${SCALA_TARBALL_URI##*/}
gsutil cp ${SCALA_TARBALL_URI} /home/hadoop/${SCALA_TARBALL}
tar -C /home/hadoop -xzvf /home/hadoop/${SCALA_TARBALL}
mv /home/hadoop/scala*/ ${SCALA_INSTALL_DIR}

# Figure out which tarball to use based on which Hadoop version is being used.
set +o nounset
HADOOP_BIN="sudo -u hadoop ${HADOOP_INSTALL_DIR}/bin/hadoop"
HADOOP_VERSION=$(${HADOOP_BIN} version | tr -cd [:digit:] | head -c1)
set -o nounset
if [[ "${HADOOP_VERSION}" == '2' ]]; then
  SPARK_TARBALL_URI=${SPARK_HADOOP2_TARBALL_URI}
else
  SPARK_TARBALL_URI=${SPARK_HADOOP1_TARBALL_URI}
fi

SPARK_TARBALL=${SPARK_TARBALL_URI##*/}
gsutil cp ${SPARK_TARBALL_URI} /home/hadoop/${SPARK_TARBALL}
tar -C /home/hadoop -xzvf /home/hadoop/${SPARK_TARBALL}
mv /home/hadoop/spark*/ ${SPARK_INSTALL_DIR}

# List all workers for master to ssh into when using start-all.sh.
echo ${WORKERS[@]} | tr ' ' '\n' > ${SPARK_INSTALL_DIR}/conf/slaves

# Find the Hadoop lib dir so that we can add its gcs-connector into the
# Spark classpath.
set +o nounset
if [[ -r "${HADOOP_INSTALL_DIR}/libexec/hadoop-config.sh" ]]; then
  . "${HADOOP_INSTALL_DIR}/libexec/hadoop-config.sh"
fi
if [[ -n "${HADOOP_COMMON_LIB_JARS_DIR}" ]] && \
    [[ -n "${HADOOP_PREFIX}" ]]; then
  LIB_JARS_DIR="${HADOOP_PREFIX}/${HADOOP_COMMON_LIB_JARS_DIR}"
else
  LIB_JARS_DIR="${HADOOP_INSTALL_DIR}/lib"
fi
set -o nounset

GCS_JARNAME=$(grep -o '[^/]*\.jar' <<< ${GCS_CONNECTOR_JAR})
LOCAL_GCS_JAR="${LIB_JARS_DIR}/${GCS_JARNAME}"

# Symlink hadoop's core-site.xml into spark's conf directory.
ln -s ${HADOOP_CONF_DIR}/core-site.xml ${SPARK_INSTALL_DIR}/conf/core-site.xml

# Create directories on the mounted local directory which may point to an
# attached PD for Spark to use for scratch, logs, etc.
SPARK_TMPDIR='/hadoop/spark/tmp'
SPARK_WORKDIR='/hadoop/spark/work'
mkdir -p ${SPARK_TMPDIR} ${SPARK_WORKDIR}
chgrp hadoop -R /hadoop/spark
chmod 777 -R /hadoop/spark

# Calculate the memory allocations, MB, using 'free -m'. Floor to nearest MB.
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
SPARK_WORKER_MEMORY=$(python -c \
    "print int(${TOTAL_MEM} * ${SPARK_WORKER_MEMORY_FRACTION})")
SPARK_DAEMON_MEMORY=$(python -c \
    "print int(${TOTAL_MEM} * ${SPARK_DAEMON_MEMORY_FRACTION})")
SPARK_EXECUTOR_MEMORY=$(python -c \
    "print int(${TOTAL_MEM} * ${SPARK_EXECUTOR_MEMORY_FRACTION})")
set +o nounset
if [[ -n "${NODEMANAGER_MEMORY_FRACTION}" ]]; then
  SPARK_YARN_EXECUTOR_MEMORY=$(python -c "print int(${TOTAL_MEM} * \
      min(${SPARK_EXECUTOR_MEMORY_FRACTION}, ${NODEMANAGER_MEMORY_FRACTION}))")
else
  SPARK_YARN_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY}
fi
set -o nounset

# Determine Spark master using appropriate mode
if [[ ${SPARK_MODE} == 'standalone' ]]; then
  SPARK_MASTER="spark://${MASTER_HOSTNAME}:7077"
elif [[ ${SPARK_MODE} =~ ^(default|yarn-(client|cluster))$ ]]; then
  SPARK_MASTER="${SPARK_MODE}"
else
  echo "Invalid mode: '${SPARK_MODE}'. Preserving default behavior." >&2
  SPARK_MASTER='default'
fi

# Help spark find scala and the GCS connector.
# For Spark 0.9.1 and older, Spark properties must be passed in programmatically
# or as system properties; newer versions introduce spark-defaults.conf but for
# backwards compatibility for now, we'll use system properties via
# SPARK_JAVA_OPTS.
cat << EOF >> ${SPARK_INSTALL_DIR}/conf/spark-env.sh
export SCALA_HOME=${SCALA_INSTALL_DIR}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}m
export SPARK_CLASSPATH=\$SPARK_CLASSPATH:${LOCAL_GCS_JAR}
export SPARK_MASTER_IP=${MASTER_HOSTNAME}
export SPARK_DAEMON_MEMORY=${SPARK_DAEMON_MEMORY}m
export SPARK_WORKER_DIR=${SPARK_WORKDIR}

# Append to front so that user-specified SPARK_JAVA_OPTS at runtime will win.
export SPARK_JAVA_OPTS="-Dspark.executor.memory=${SPARK_EXECUTOR_MEMORY}m \${SPARK_JAVA_OPTS}"
export SPARK_JAVA_OPTS="-Dspark.local.dir=${SPARK_TMPDIR} \${SPARK_JAVA_OPTS}"

# Will be ingored if not running on YARN
export SPARK_JAVA_OPTS="-Dspark.yarn.executor.memoryOverhead=${SPARK_YARN_EXECUTOR_MEMORY}m \${SPARK_JAVA_OPTS}"
EOF

if [[ "${SPARK_MASTER}" != 'default' ]]; then
  echo "export MASTER=${SPARK_MASTER}" >> ${SPARK_INSTALL_DIR}/conf/spark-env.sh
fi

# Add the spark 'bin' path to the .bashrc so that it's easy to call 'spark'
# during interactive ssh session.
add_to_path_at_login "${SPARK_INSTALL_DIR}/bin"

# Assign ownership of everything to the 'hadoop' user.
chown -R hadoop:hadoop /home/hadoop/

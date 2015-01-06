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

set -o errexit

# Figure out which tarball to use based on which Hadoop version is being used.
set +o nounset
HADOOP_BIN="sudo -u hadoop ${HADOOP_INSTALL_DIR}/bin/hadoop"
HADOOP_VERSION=$(${HADOOP_BIN} version | tr -cd [:digit:] | head -c1)
set -o nounset
if [[ "${HADOOP_VERSION}" == '2' ]]; then
  SHARK_TARBALL_URI=${SHARK_HADOOP2_TARBALL_URI}
else
  SHARK_TARBALL_URI=${SHARK_HADOOP1_TARBALL_URI}
fi

SHARK_TARBALL=${SHARK_TARBALL_URI##*/}
gsutil cp ${SHARK_TARBALL_URI} /home/hadoop/${SHARK_TARBALL}
tar -C /home/hadoop -xzvf /home/hadoop/${SHARK_TARBALL}
mv /home/hadoop/shark*/ ${SHARK_INSTALL_DIR}

# Find the Hadoop lib dir so that we can link its gcs-connector into the
# Shark library path.
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
ln -s ${LOCAL_GCS_JAR} ${SHARK_INSTALL_DIR}/lib/

# Calculate the memory allocations, MB, using 'free -m'. Floor to nearest MB.
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
SHARK_MEM=$(python -c \
    "print int(${TOTAL_MEM} * ${SHARK_MEM_FRACTION})")


# Point shark at scala, hadoop, hive, spark, and the spark master.
cat << EOF >> ${SHARK_INSTALL_DIR}/conf/shark-env.sh
export HADOOP_HOME=${HADOOP_INSTALL_DIR}
export SCALA_HOME=${SCALA_INSTALL_DIR}
export SPARK_HOME=${SPARK_INSTALL_DIR}
export SPARK_MEM=${SHARK_MEM}m

# Set spark master by copying from spark-env.sh
$(grep 'MASTER=' ${SPARK_INSTALL_DIR}/conf/spark-env.sh)
EOF

# Add the spark 'bin' path to the .bashrc so that it's easy to call 'spark'
# during interactive ssh session.
add_to_path_at_login "${SHARK_INSTALL_DIR}/bin"

# Assign ownership of everything to the 'hadoop' user.
chown -R hadoop:hadoop /home/hadoop/

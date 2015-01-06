# Copyright 2013 Google Inc. All Rights Reserved.
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

# Downloads and installs the relevant datastore-connector-<version>.jar.
# Also configures it for use with hadoop.

set -e

if (( ${INSTALL_DATASTORE_CONNECTOR} )); then
  if [[ -r "${HADOOP_INSTALL_DIR}/libexec/hadoop-config.sh" ]]; then
    . "${HADOOP_INSTALL_DIR}/libexec/hadoop-config.sh"
  fi
  if [[ -n "${HADOOP_COMMON_LIB_JARS_DIR}" ]] && \
      [[ -n "${HADOOP_PREFIX}" ]]; then
    LIB_JARS_DIR="${HADOOP_PREFIX}/${HADOOP_COMMON_LIB_JARS_DIR}"
  else
    LIB_JARS_DIR="${HADOOP_INSTALL_DIR}/lib"
  fi

  # Grab the connector jarfile, add it to installation /lib directory.
  JARNAME=$(grep -o '[^/]*\.jar' <<< ${DATASTORE_CONNECTOR_JAR})
  LOCAL_JAR="${LIB_JARS_DIR}/${JARNAME}"

  download_bd_resource "${DATASTORE_CONNECTOR_JAR}" "${LOCAL_JAR}"

  chown hadoop:hadoop ${LOCAL_JAR}

  echo "export HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:${LOCAL_JAR}" \
      >> ${HADOOP_CONF_DIR}/hadoop-env.sh

  chown -R hadoop:hadoop ${HADOOP_CONF_DIR}
fi

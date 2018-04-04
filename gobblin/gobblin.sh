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

set -euxo pipefail

readonly PACKAGE_URL="gs://gobblin-dist/gobblin-distribution-0.12.0.rc2.tar.gz"

readonly INSTALL_DIR="/usr/local/lib/gobblin"
readonly INSTALL_BIN="${INSTALL_DIR}/bin"
readonly INSTALL_LIB="${INSTALL_DIR}/lib"
readonly INSTALL_CONF="${INSTALL_DIR}/conf"

readonly HADOOP_LIB="/usr/lib/hadoop/lib"

readonly JAR_NAME_CANONICALIZER="s/([-a-zA-Z0-9]+?)[-]([0-9][0-9.]+?)([-.].*?)?.jar/\1/"

function maybe_symlink() {
  local jar=$1
  if [[ ! -f "${HADOOP_LIB}/${jar}" ]] ; then
    ln -s "${INSTALL_LIB}/${jar}" "${HADOOP_LIB}/${jar}"
  fi
}

# Configure runtime environment.
function configure_env() {
  local nodename=$(hostname)

  sed -E "s/(fs.uri)=(.+)$/\1=hdfs:\/\/${nodename}:8020\//" \
       -i "${INSTALL_CONF}/gobblin-mapreduce.properties"

  sed -E "s/env:GOBBLIN_WORK_DIR/fs.uri/g" \
       -i "${INSTALL_CONF}/gobblin-mapreduce.properties"

  echo "export JAVA_HOME=${JAVA_HOME}" >> "${INSTALL_BIN}/gobblin-env.sh"
  echo "export HADOOP_BIN_DIR=/usr/lib/hadoop/bin" >> "${INSTALL_BIN}/gobblin-env.sh"

  echo "export HADOOP_USER_CLASSPATH_FIRST=true" >> "/etc/hadoop/conf/hadoop-env.sh"

  # Libraries to include on hadoop classpath.
  local lib_prefixes=(
    commons
    config
    data
    gobblin
    gson
    guava
    infuxdb
    javassist
    joda-time
    metrics
    okhttp
    okio
    reactive-streams
    reflections
    restli
    retrofit
    scala-library)

  # Replace these jars.
  rm -f "${HADOOP_LIB}/commons-lang"*
  rm -f "${HADOOP_LIB}/guava"*

  for prefix in "${lib_prefixes[@]}"; do
    for jar in `ls ${INSTALL_LIB}/${prefix}* | sed 's#.*/##'`; do
      maybe_symlink "${jar}"
    done
  done
}

function install_package() {
  # Download binary.
  local temp=$(mktemp -d)
  gsutil cp "${PACKAGE_URL}" "${temp}/package.tar.gz"
  tar -xvf "${temp}/package.tar.gz" -C "${temp}"

  # Setup package.
  install -d "${INSTALL_DIR}"
  cp -r "${temp}/gobblin-dist"/* "${INSTALL_DIR}"

  # Cleanup temp files.
  rm -Rf "${temp}"
}

function main() {
  install_package
  configure_env
}

main


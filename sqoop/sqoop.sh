#!/bin/bash
#    Copyright 2018 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# This script installs Apache Sqoop (http://sqoop.apache.org/) on a Google Cloud
# Dataproc cluster.
#
# Hive-Hcatalog is not installed automatically and if you want
# to run hive jobs than please use this script with hive-hcatalog.sh init action.
# HBase libraries are not installed automatically so in order to run imports to 
# BigTable using sqoop, please run this init action with bigtable.sh.

set -euxo pipefail

readonly SQOOP_HOME='/usr/lib/sqoop'
readonly SQOOP_CODE_LINK='https://github.com/szewi/sqoop'
readonly SQOOP_JAR='https://storage.googleapis.com/sqoop-bigtable-connector/sqoop-1.4.7.jar'
readonly AVRO_JAR='http://repo1.maven.org/maven2/org/apache/avro/avro/1.8.1/avro-1.8.1.jar'
readonly BIGTABLE_HBASE_CLIENT='bigtable-hbase-1.x-hadoop-1.3.0.jar'
readonly BIGTABLE_HBASE_DL_LINK="http://central.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-1.x-hadoop/1.3.0/${BIGTABLE_HBASE_CLIENT}"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_sqoop() {
  git clone -b sqoop-bigtable-client "${SQOOP_CODE_LINK}" "${SQOOP_HOME}" \
    || err 'Cloning Sqoop sources from code location failed.'
  wget -q "${SQOOP_JAR}" -O "${SQOOP_HOME}/sqoop-1.4.7.jar" \
    || err 'Downloading sqoop jar file failed.'
  echo "export PATH=\"${SQOOP_HOME}/bin:$PATH\"" >> /etc/profile
}

function install_sqoop_dependencies_and_connectors() {
  # Sqoop requires avro 1.8.+
  wget -q "${AVRO_JAR}" -O "${SQOOP_HOME}/lib/avro-1.8.1.jar" \
    || err 'Downloading avro jar file failed.'
  # Install sqoop connectors in /usr/lib/sqoop/lib
  wget -q "${BIGTABLE_HBASE_DL_LINK}" -O "${SQOOP_HOME}/lib/${BIGTABLE_HBASE_CLIENT}" \
    || err 'Error getting BigTable-HBase connector.'
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  
  # Only run the installation on Masters
  if [[ "${role}" == 'Master' ]]; then
    install_sqoop
    install_sqoop_dependencies_and_connectors
  fi

}

main

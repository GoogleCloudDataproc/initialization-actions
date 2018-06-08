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
readonly SQOOP_CODE_LINK='https://github.com/apache/sqoop'

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_sqoop() {
  git clone ${SQOOP_CODE_LINK} ${SQOOP_HOME} && cd ${SQOOP_HOME} && ant \
    || err 'Compiling Sqoop from source code failed.'
  echo "export PATH=\"${SQOOP_HOME}/bin:$PATH\"" >> /etc/profile
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  update_apt_get || err 'Unable to update packages lists.'
  apt-get install -y ant || err 'Unable to install ant and hbase.'
  
  # Only run the installation on Masters
  if [[ "${role}" == 'Master' ]]; then
    install_sqoop
  fi

}

main

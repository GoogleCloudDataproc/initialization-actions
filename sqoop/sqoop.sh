#!/bin/bash

# Copyright 2019 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script installs Apache Sqoop (http://sqoop.apache.org/) on a Google Cloud
# Dataproc cluster.

set -euxo pipefail

readonly SQOOP_HOME='/usr/lib/sqoop'
readonly MYSQL_JAR='/usr/share/java/mysql.jar'

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  # Only run the installation on master nodes
  if [[ "${role}" == 'Master' ]]; then
    execute_with_retries "apt-get install -y -q sqoop"
    # Sqoop will use locally available MySQL JDBC driver
    ln -s "${MYSQL_JAR}" "${SQOOP_HOME}/lib/mysql.jar"
  fi
}

main

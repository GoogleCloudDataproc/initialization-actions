#!/bin/bash
# Copyright 2015 Google, Inc.
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

# This init script installs Apache Zeppelin on the master node of a Cloud
# Dataproc cluster. Zeppelin is also configured based on the size of your
# cluster and the versions of Spark/Hadoop which are installed.

set -euxo pipefail


readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly INTERPRETER_FILE='/etc/zeppelin/conf/interpreter.json'
readonly INIT_SCRIPT='/usr/lib/systemd/system/zeppelin-notebook.service'

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

function install_zeppelin(){
  # Install zeppelin. Don't mind if it fails to start the first time.
  apt-get install -y -t jessie-backports zeppelin || dpkg -l zeppelin
  if [ $? != 0 ]; then
    err 'Failed to install zeppelin'
  fi
}

function configure_zeppelin(){
  local zeppelin_version;
  zeppelin_version="$(dpkg-query --showformat='${Version}' --show zeppelin)"

  # Only use sed to modify interpreter.json prior to Zeppelin 0.8.0.
  # The file format has changed in 0.8.0.
  # TODO(karthikpal): Evaluate which of these (if any) are necessary >= 0.8.0
  if dpkg --compare-versions "${zeppelin_version}" '<' 0.8.0; then

    # Wait up to 60s for ${INTERPRETER_FILE} to be available
    for i in {1..6}; do
      if [[ -r "${INTERPRETER_FILE}" ]]; then
        break
      else
        sleep 10
      fi
    done

    if [[ ! -r "${INTERPRETER_FILE}" ]]; then
      err "${INTERPRETER_FILE} is missing"
    fi

    # stop service, systemd will be configured
    service zeppelin stop

    # Set spark.yarn.isPython to fix Zeppelin pyspark in Dataproc 1.0.
    sed -i 's/\(\s*\)"spark\.app\.name[^,}]*/&,\n\1"spark.yarn.isPython": "true"/' \
      "${INTERPRETER_FILE}"

    # Unset Spark Executor memory to let the spark-defaults.conf set it.
    sed -i '/spark\.executor\.memory/d' "${INTERPRETER_FILE}"

    # Set BigQuery project ID if present.
    local project_id;
    project_id="$(/usr/share/google/get_metadata_value ../project/project-id)"
    sed -i "s/\(\"zeppelin.bigquery.project_id\"\)[^,}]*/\1: \"${project_id}\"/" \
      "${INTERPRETER_FILE}"
  fi

  # Link in hive configuration.
  ln -s /etc/hive/conf/hive-site.xml /etc/zeppelin/conf

  local zeppelin_port;
  zeppelin_port="$(/usr/share/google/get_metadata_value attributes/zeppelin-port || true)"
  if [[ -n "${zeppelin_port}" ]]; then
    echo "export ZEPPELIN_PORT=${zeppelin_port}" \
      >> /etc/zeppelin/conf/zeppelin-env.sh
  fi

  # Install matplotlib. Note that this will work in Zeppelin, but not
  # in a vanilla Python interpreter, as that requires an X server to be running
  apt-get install -y python-dev python-tk
  easy_install pip
  pip install --upgrade matplotlib

  # Install R libraries for Zeppelin 0.6.1+
  if dpkg --compare-versions "${zeppelin_version}" '>=' 0.6.1; then
    # TODO(pmkc): Add googlevis to Zeppelin Package recommendations
    apt-get install -y r-cran-googlevis

    # TODO(Aniszewski): Fix SparkR (GitHub issue #198)
  fi
}

function main() {
  if [[ "${ROLE}" == 'Master' ]]; then
    update_apt_get || err 'Failed to update apt-get'
    install_zeppelin
    configure_zeppelin
    systemctl restart zeppelin
  fi
}

main

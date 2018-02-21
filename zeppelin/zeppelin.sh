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
}

function configure_zeppelin(){
  # Ideally we would use Zeppelin's REST API, but it is difficult, especially
  # between versions. So sed the JSON file.

  # Set spark.yarn.isPython to fix Zeppelin pyspark in Dataproc 1.0.
  sed -i 's/\(\s*\)"spark\.app\.name[^,}]*/&,\n\1"spark.yarn.isPython": "true"/' \
    "${INTERPRETER_FILE}"
  # Unset Spark Executor memory to let the spark-defaults.conf set it.
  sed -i '/spark\.executor\.memory/d' "${INTERPRETER_FILE}"

  # Link in hive configuration.
  ln -s /etc/hive/conf/hive-site.xml /etc/zeppelin/conf

  local zeppelin_port;
  zeppelin_port="$(/usr/share/google/get_metadata_value attributes/zeppelin-port || true)"
  if [[ -n "${zeppelin_port}" ]]; then
    echo "export ZEPPELIN_PORT=${zeppelin_port}" \
      >> /etc/zeppelin/conf/zeppelin-env.sh
  fi

  # Install R libraries and configure BigQuery for Zeppelin 0.6.1+
  local zeppelin_version;
  zeppelin_version="$(dpkg-query --showformat='${Version}' --show zeppelin)"
  if dpkg --compare-versions "${zeppelin_version}" '>=' 0.6.1; then
    # Set BigQuery project ID if present.

    local project_id;
    project_id="$(/usr/share/google/get_metadata_value ../project/project-id)"
    sed -i "s/\(\"zeppelin.bigquery.project_id\"\)[^,}]*/\1: \"${project_id}\"/" \
      "${INTERPRETER_FILE}"

    # TODO(pmkc): Add googlevis to Zeppelin Package recommendations
    apt-get install -y r-cran-googlevis

## Uncomment here to compile and install 'mplot' and 'rCharts'.
#    # Install compile dependencies
#    apt-get install -y \
#      r-cran-doparallel r-cran-httr r-cran-memoise r-cran-openssl \
#      r-cran-rcurl r-cran-plyr r-cran-shiny r-cran-glmnet r-cran-data.table \
#      libssl-dev libcurl4-openssl-dev
#
#    cat << EOF > install_script.r
#options('Ncpus'=10, repos = 'http://cran.us.r-project.org')
#install.packages('devtools')
#require('devtools')
## Install version 0.7.7 to avoid dependency on glmulti and RJava
#install_version('mplot', version = '0.7.7', upgrade_dependencies = FALSE)
#install_github('ramnathv/rCharts', upgrade_dependencies = FALSE)
#update.packages('ggplot', ask = FALSE)
#EOF
#    R -f install_script.r
  fi
}

function launch_zeppelin(){
  # Start Zeppelin as systemd job

  cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=Zeppelin Notebook

[Service]
Type=simple
ExecStart=/usr/lib/zeppelin/bin/zeppelin-daemon.sh upstart
ExecStop=/usr/lib/zeppelin/bin/zeppelin-daemon.sh stop
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

  chmod a+rw ${INIT_SCRIPT}

  systemctl daemon-reload
  systemctl enable zeppelin-notebook
  systemctl start zeppelin-notebook
  systemctl status zeppelin-notebook
}

function main() {
  if [[ "${ROLE}" == 'Master' ]]; then
    update_apt_get || err 'Failed to update apt-get'
    install_zeppelin
    configure_zeppelin
    launch_zeppelin
  fi
}

main

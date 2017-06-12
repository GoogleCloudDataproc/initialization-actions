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

set -x -e

# Only run on the master node
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
INTERPRETER_FILE='/etc/zeppelin/conf/interpreter.json'
ZEPPELIN_PORT="$(/usr/share/google/get_metadata_value attributes/zeppelin-port || true)"

if [[ "${ROLE}" == 'Master' ]]; then
  # Install zeppelin. Don't mind if it fails to start the first time.
  apt-get update || true
  apt-get install -y -t jessie-backports zeppelin || dpkg -l zeppelin

  for i in {1..6}; do
    if [[ -r "${INTERPRETER_FILE}" ]]; then
      break
    else
      sleep 10
    fi
  done
  # Ideally we would use Zeppelin's REST API, but it is difficult, especially
  # between versions. So sed the JSON file.
  service zeppelin stop
  # Set spark.yarn.isPython to fix Zeppelin pyspark in Dataproc 1.0.
  sed -i 's/\(\s*\)"spark\.app\.name[^,}]*/&,\n\1"spark.yarn.isPython": "true"/' \
      "${INTERPRETER_FILE}"

  # Link in hive configuration.
  ln -s /etc/hive/conf/hive-site.xml /etc/zeppelin/conf

  if [[ -n "${ZEPPELIN_PORT}" ]]; then
    echo "export ZEPPELIN_PORT=${ZEPPELIN_PORT}" \
        >> /etc/zeppelin/conf/zeppelin-env.sh
  fi

  # Install R libraries and configure BigQuery for Zeppelin 0.6.1+
  ZEPPELIN_VERSION=$(dpkg-query --showformat='${Version}' --show zeppelin)
  if dpkg --compare-versions "${ZEPPELIN_VERSION}" '>=' 0.6.1; then
    # Set BigQuery project ID if present.
    PROJECT=$(/usr/share/google/get_metadata_value ../project/project-id)
    sed -i "s/\(\"zeppelin.bigquery.project_id\"\)[^,}]*/\1: \"${PROJECT}\"/" \
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

  # Restart Zeppelin
  service zeppelin start
fi
